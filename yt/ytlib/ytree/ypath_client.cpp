#include "stdafx.h"
#include "ypath_client.h"

#include "ypath_proxy.h"
#include "ypath_detail.h"
#include "lexer.h"

#include <ytlib/rpc/rpc.pb.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/rpc/message.h>

namespace NYT {
namespace NYTree {

using namespace NBus;
using namespace NRpc;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

TYPath RootMarker("/");
TYPath AttributeMarker("@");

////////////////////////////////////////////////////////////////////////////////

TYPathRequest::TYPathRequest(const Stroka& verb)
    : Verb_(verb)
{ }

IMessage::TPtr TYPathRequest::Serialize()
{
    auto bodyData = SerializeBody();

    TRequestHeader header;
    header.set_path(Path_);
    header.set_verb(Verb_);
    if (HasAttributes()) {
        ToProto(header.mutable_attributes(), Attributes());
    }

    return CreateRequestMessage(
        header,
        MoveRV(bodyData),
        Attachments_);
}

////////////////////////////////////////////////////////////////////////////////

void TYPathResponse::Deserialize(NBus::IMessage* message)
{
    YASSERT(message);

    auto header = GetResponseHeader(message);
    Error_ = GetResponseError(header);
    if (header.has_attributes()) {
        SetAttributes(FromProto(header.attributes()));
    }

    if (Error_.IsOK()) {
        // Deserialize body.
        const auto& parts = message->GetParts();
        YASSERT(parts.size() >=1 );
        DeserializeBody(parts[1]);

        // Load attachments.
        Attachments_ = yvector<TSharedRef>(parts.begin() + 2, parts.end());
    }
}

int TYPathResponse::GetErrorCode() const
{
    return Error_.GetCode();
}

bool TYPathResponse::IsOK() const
{
    return Error_.IsOK();
}

void TYPathResponse::ThrowIfError() const
{
    if (!IsOK()) {
        ythrow yexception() << Error_.ToString();
    }
}

void TYPathResponse::DeserializeBody(const TRef& data)
{
    UNUSED(data);
}

////////////////////////////////////////////////////////////////////////////////

TYPath ComputeResolvedYPath(
    const TYPath& wholePath,
    const TYPath& unresolvedPath)
{
    int resolvedLength = static_cast<int>(wholePath.length()) - static_cast<int>(unresolvedPath.length());
    YASSERT(resolvedLength >= 0 && resolvedLength <= static_cast<int>(wholePath.length()));
    YASSERT(wholePath.substr(resolvedLength) == unresolvedPath);
    // Take care of trailing slash but don't reduce / to empty string.
    return
        resolvedLength > 1 && wholePath[resolvedLength - 1] == '/'
        ? wholePath.substr(0, resolvedLength - 1)
        : wholePath.substr(0, resolvedLength);
}

TYPath CombineYPaths(
    const TYPath& path1,
    const TYPath& path2)
{
    auto token = ChopToken(path2);
    switch (token.GetType()) {
        case ETokenType::Slash:
        case ETokenType::At:
            return path1 + path2;

        default:
            return path1 + '/' + path2;
    }
}

TYPath CombineYPaths(
    const TYPath& path1,
    const TYPath& path2,
    const TYPath& path3)
{
    return CombineYPaths(CombineYPaths(path1, path2), path3);
}

TYPath CombineYPaths(
    const TYPath& path1,
    const TYPath& path2,
    const TYPath& path3,
    const TYPath& path4)
{
    return CombineYPaths(CombineYPaths(CombineYPaths(path1, path2), path3), path4);
}

void ResolveYPath(
    IYPathService* rootService,
    const TYPath& path,
    const Stroka& verb,
    IYPathServicePtr* suffixService,
    TYPath* suffixPath)
{
    YASSERT(rootService);
    YASSERT(suffixService);
    YASSERT(suffixPath);

    IYPathServicePtr currentService = rootService;
    auto currentPath = path;

    while (true) {
        IYPathService::TResolveResult result;
        try {
            result = currentService->Resolve(currentPath, verb);
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error during YPath resolution (Path: %s, Verb: %s, ResolvedPath: %s)\n%s",
                ~path,
                ~verb,
                ~ComputeResolvedYPath(path, currentPath),
                ex.what());
        }

        if (result.IsHere()) {
            *suffixService = currentService;
            *suffixPath = result.GetPath();
            break;
        }

        currentService = result.GetService();
        currentPath = result.GetPath();
    }
}

void OnYPathResponse(
    IMessage::TPtr responseMessage,
    TFuture<IMessage::TPtr>::TPtr asyncResponseMessage,
    const TYPath& path,
    const Stroka& verb,
    const TYPath& resolvedPath)
{
    auto header = GetResponseHeader(~responseMessage);
    auto error = GetResponseError(header);

    if (error.IsOK()) {
        asyncResponseMessage->Set(responseMessage);
    } else {
        Stroka message = Sprintf("Error executing a YPath operation (Path: %s, Verb: %s, ResolvedPath: %s)\n%s",
            ~path,
            ~verb,
            ~resolvedPath,
            ~error.GetMessage());
        SetResponseError(header, TError(error.GetCode(), message));

        auto updatedResponseMessage = SetResponseHeader(~responseMessage, header);
        asyncResponseMessage->Set(updatedResponseMessage);
    }
}

TFuture<IMessage::TPtr>::TPtr
ExecuteVerb(
    IYPathService* service,
    NBus::IMessage* requestMessage)
{
    NLog::TLogger Logger(service->GetLoggingCategory());

    auto requestHeader = GetRequestHeader(requestMessage);
    TYPath path = requestHeader.path();
    Stroka verb = requestHeader.verb();

    IYPathServicePtr suffixService;
    TYPath suffixPath;
    try {
        ResolveYPath(
            service,
            path,
            verb,
            &suffixService,
            &suffixPath);
    } catch (const std::exception& ex) {
        auto responseMessage = NRpc::CreateErrorResponseMessage(TError(
            EYPathErrorCode(EYPathErrorCode::ResolveError),
            ex.what()));
        return New< TFuture<IMessage::TPtr> >(responseMessage);
    }

    requestHeader.set_path(suffixPath);
    auto updatedRequestMessage = SetRequestHeader(requestMessage, requestHeader);

    auto asyncResponseMessage = New< TFuture<IMessage::TPtr> >();
    auto context = CreateYPathContext(
        ~updatedRequestMessage,
        suffixPath,
        verb,
        suffixService->GetLoggingCategory(),
        FromMethod(
            &OnYPathResponse,
            asyncResponseMessage,
            path,
            verb,
            ComputeResolvedYPath(path, suffixPath)));

    try {
        // This should never throw.
        suffixService->Invoke(~context);
    }
    catch (const std::exception& ex) {
        LOG_FATAL("Unexpected exception during verb execution\n%s", ex.what());
    }

    return asyncResponseMessage;
}

void ExecuteVerb(IYPathService* service, IServiceContext* context)
{
    auto context_ = MakeStrong(context);
    auto requestMessage = context->GetRequestMessage();
    ExecuteVerb(service, ~requestMessage)
        ->Subscribe(FromFunctor([=] (NBus::IMessage::TPtr responseMessage) {
            context_->Reply(~responseMessage);
        }));
}

TFuture< TValueOrError<TYson> >::TPtr AsyncYPathGet(IYPathService* service, const TYPath& path)
{
    auto request = TYPathProxy::Get(path);
    return
        ExecuteVerb(service, ~request)
        ->Apply(FromFunctor([] (TYPathProxy::TRspGet::TPtr response)
            {
                return
                    response->IsOK()
                    ? TValueOrError<TYson>(response->value())
                    : TValueOrError<TYson>(response->GetError());
            }));
}

TYson SyncYPathGet(IYPathService* service, const TYPath& path)
{
    auto result = AsyncYPathGet(service, path)->Get();
    if (!result.IsOK()) {
        ythrow yexception() << result.GetMessage();
    }
    return result.Value();
}

INodePtr SyncYPathGetNode(IYPathService* service, const TYPath& path)
{
    auto request = TYPathProxy::GetNode(path);
    auto response = ExecuteVerb(service, ~request)->Get();
    response->ThrowIfError();
    return reinterpret_cast<INode*>(response->value_ptr());
}

void SyncYPathSet(IYPathService* service, const TYPath& path, const TYson& value)
{
    auto request = TYPathProxy::Set(path);
    request->set_value(value);
    auto response = ExecuteVerb(service, ~request)->Get();
    response->ThrowIfError();
}

void SyncYPathSetNode(IYPathService* service, const TYPath& path, INode* value)
{
    auto request = TYPathProxy::SetNode(path);
    request->set_value_ptr(reinterpret_cast<i64>(value));
    auto response = ExecuteVerb(service, ~request)->Get();
    response->ThrowIfError();
}

void SyncYPathRemove(IYPathService* service, const TYPath& path)
{
    auto request = TYPathProxy::Remove(path);
    auto response = ExecuteVerb(service, ~request)->Get();
    response->ThrowIfError();
}

yvector<Stroka> SyncYPathList(IYPathService* service, const TYPath& path)
{
    auto request = TYPathProxy::List(path);
    auto response = ExecuteVerb(service, ~request)->Get();
    response->ThrowIfError();
    return NYT::FromProto<Stroka>(response->keys());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
