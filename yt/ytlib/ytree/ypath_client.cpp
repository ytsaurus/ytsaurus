#include "stdafx.h"
#include "ypath_client.h"
#include "ypath_proxy.h"
#include "ypath_detail.h"
#include "rpc.pb.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/rpc/message.h>

namespace NYT {
namespace NYTree {

using namespace NBus;
using namespace NRpc;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = YTreeLogger;
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

void ChopYPathToken(
    const TYPath& path,
    Stroka* token,
    TYPath* suffixPath)
{
    size_t index = path.find_first_of("/@");
    if (index == TYPath::npos) {
        *token = path;
        *suffixPath = TYPath(path.end(), static_cast<size_t>(0));
    } else {
        switch (path[index]) {
        case '/':
            *token = path.substr(0, index);
            *suffixPath =
                index == path.length() - 1
                ? path.substr(index)
                : path.substr(index + 1);
            break;

        case '@':
            *token = path.substr(0, index);
            *suffixPath = path.substr(index);
            break;

        default:
            YUNREACHABLE();
        }
    }
}

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
    if (path2.has_prefix("@")) {
        return path1 + path2;
    }

    if (path1.empty() || path2.empty()) {
        return path1 + path2;
    }

    if (path1.back() == '/' && path2[0] == '/') {
        return path1 + path2.substr(1);
    }

    if (path1.back() != '/' && path2[0] != '/') {
        return path1 + '/' + path2;
    }

    return path1 + path2;
}

TYPath CombineYPaths(
    const TYPath& path1,
    const TYPath& path2,
    const TYPath& path3)
{
    return CombineYPaths(CombineYPaths(path1, path2), path3);
}

bool IsEmptyYPath(const TYPath& path)
{
    return path.empty();
}

bool IsFinalYPath(const TYPath& path)
{
    return path.empty() || path == RootMarker;
}

bool IsAttributeYPath(const TYPath& path)
{
    return path.has_prefix(AttributeMarker);
}

TYPath ChopYPathAttributeMarker(const TYPath& path)
{
    auto result = path.substr(AttributeMarker.length());
    if (result.has_prefix(AttributeMarker)) {
        ythrow yexception() << "Repeated attribute marker in YPath";
    }
    return result;
}

bool IsLocalYPath(const TYPath& path)
{
    // The empty path is handled by the virtual node itself.
    // All other paths (including "/") are forwarded to the service.
    // Thus "/virtual" denotes the virtual node while "/virtual/" denotes its content.
    // Same applies to the attributes (cf. "/virtual@" vs "/virtual/@").
    return IsEmptyYPath(path) || IsAttributeYPath(path);
}

void ResolveYPath(
    IYPathService* rootService,
    const TYPath& path,
    const Stroka& verb,
    IYPathService::TPtr* suffixService,
    TYPath* suffixPath)
{
    YASSERT(rootService);
    YASSERT(suffixService);
    YASSERT(suffixPath);

    IYPathService::TPtr currentService = rootService;
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
    NBus::IMessage* requestMessage,
    IYPathProcessor* processor)
{
    auto requestHeader = GetRequestHeader(requestMessage);
    TYPath path = requestHeader.path();
    Stroka verb = requestHeader.verb();

    IYPathService::TPtr suffixService;
    TYPath suffixPath;
    try {
        // This may throw.
        processor->Resolve(path, verb, &suffixService, &suffixPath);
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
        ~FromMethod(
            &OnYPathResponse,
            asyncResponseMessage,
            path,
            verb,
            ComputeResolvedYPath(path, suffixPath)));

    try {
        // This should never throw.
        processor->Execute(~suffixService, ~context);
    }
    catch (const std::exception& ex) {
        LOG_FATAL("Unexpected exception during verb execution\n%s", ex.what());
    }

    return asyncResponseMessage;
}

TFuture< TValueOrError<TYson> >::TPtr AsyncYPathGet(IYPathService* rootService, const TYPath& path)
{
    auto request = TYPathProxy::Get(path);
    return
        ExecuteVerb(~request, ~CreateDefaultProcessor(rootService))
        ->Apply(FromFunctor([] (TYPathProxy::TRspGet::TPtr response)
            {
                return
                    response->IsOK()
                    ? TValueOrError<TYson>(response->value())
                    : TValueOrError<TYson>(response->GetError());
            }));
}

TYson SyncYPathGet(IYPathService* rootService, const TYPath& path)
{
    auto result = AsyncYPathGet(rootService, path)->Get();
    if (!result.IsOK()) {
        ythrow yexception() << result.GetMessage();
    }
    return result.Value();
}

INode::TPtr SyncYPathGetNode(IYPathService* rootService, const TYPath& path)
{
    auto request = TYPathProxy::GetNode(path);
    auto response = ExecuteVerb(~request, ~CreateDefaultProcessor(rootService))->Get();
    response->ThrowIfError();
    return reinterpret_cast<INode*>(response->value());
}

void SyncYPathSet(IYPathService* rootService, const TYPath& path, const TYson& value)
{
    auto request = TYPathProxy::Set(path);
    request->set_value(value);
    auto response = ExecuteVerb(~request, ~CreateDefaultProcessor(rootService))->Get();
    response->ThrowIfError();
}

void SyncYPathSetNode(IYPathService* rootService, const TYPath& path, INode* value)
{
    auto request = TYPathProxy::SetNode(path);
    request->set_value(reinterpret_cast<i64>(value));
    auto response = ExecuteVerb(~request, ~CreateDefaultProcessor(rootService))->Get();
    response->ThrowIfError();
}

void SyncYPathRemove(IYPathService* rootService, const TYPath& path)
{
    auto request = TYPathProxy::Remove(path);
    auto response = ExecuteVerb(~request, ~CreateDefaultProcessor(rootService))->Get();
    response->ThrowIfError();
}

yvector<Stroka> SyncYPathList(IYPathService* rootService, const TYPath& path)
{
    auto request = TYPathProxy::List(path);
    auto response = ExecuteVerb(~request, ~CreateDefaultProcessor(rootService))->Get();
    response->ThrowIfError();
    return FromProto<Stroka>(response->keys());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
