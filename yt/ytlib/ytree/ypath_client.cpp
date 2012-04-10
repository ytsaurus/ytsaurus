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

////////////////////////////////////////////////////////////////////////////////

TYPathRequest::TYPathRequest(const Stroka& verb)
    : Verb_(verb)
{ }

IMessage::TPtr TYPathRequest::Serialize()
{
    auto bodyData = SerializeBody();

    NRpc::NProto::TRequestHeader header;
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
    Error_ = TError::FromProto(header.error());
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

TYPath EscapeYPath(const Stroka& value)
{
    // TODO(babenko,roizner): don't escape safe ids
    return SerializeToYson(value, EYsonFormat::Text);
}

TYPath EscapeYPath(i64 value)
{
    return SerializeToYson(value, EYsonFormat::Text);
}

void ResolveYPath(
    IYPathServicePtr rootService,
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
    TPromise<IMessage::TPtr> asyncResponseMessage,
    const TYPath& path,
    const Stroka& verb,
    const TYPath& resolvedPath,
    IMessage::TPtr responseMessage)
{
    auto header = GetResponseHeader(~responseMessage);
    auto error = TError::FromProto(header.error());

    if (error.IsOK()) {
        asyncResponseMessage.Set(responseMessage);
    } else {
        Stroka message = Sprintf("Error executing a YPath operation (Path: %s, Verb: %s, ResolvedPath: %s)\n%s",
            ~path,
            ~verb,
            ~resolvedPath,
            ~error.GetMessage());
        *header.mutable_error() = TError(error.GetCode(), message).ToProto();

        auto updatedResponseMessage = SetResponseHeader(~responseMessage, header);
        asyncResponseMessage.Set(updatedResponseMessage);
    }
}

TFuture<IMessage::TPtr>
ExecuteVerb(
    IYPathServicePtr service,
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
        return MakeFuture(responseMessage);
    }

    requestHeader.set_path(suffixPath);
    auto updatedRequestMessage = SetRequestHeader(requestMessage, requestHeader);

    auto asyncResponseMessage = TPromise<IMessage::TPtr>();
    auto context = CreateYPathContext(
        ~updatedRequestMessage,
        suffixPath,
        verb,
        suffixService->GetLoggingCategory(),
        BIND(
            &OnYPathResponse,
            asyncResponseMessage,
            path,
            verb,
            ComputeResolvedYPath(path, suffixPath)));

    // This should never throw.
    suffixService->Invoke(~context);

    return asyncResponseMessage;
}

void ExecuteVerb(IYPathServicePtr service, IServiceContext* context)
{
    auto context_ = MakeStrong(context);
    auto requestMessage = context->GetRequestMessage();
    ExecuteVerb(service, ~requestMessage)
        .Subscribe(BIND([=] (NBus::IMessage::TPtr responseMessage) {
            context_->Reply(~responseMessage);
        }));
}

TFuture< TValueOrError<TYson> > AsyncYPathGet(IYPathServicePtr service, const TYPath& path)
{
    auto request = TYPathProxy::Get(path);
    return
        ExecuteVerb(service, ~request)
            .Apply(BIND([] (TYPathProxy::TRspGet::TPtr response) {
                return
                    response->IsOK()
                    ? TValueOrError<TYson>(response->value())
                    : TValueOrError<TYson>(response->GetError());
            }));
}

TYson SyncYPathGet(IYPathServicePtr service, const TYPath& path)
{
    auto result = AsyncYPathGet(service, path).Get();
    if (!result.IsOK()) {
        ythrow yexception() << result.GetMessage();
    }
    return result.Value();
}

INodePtr SyncYPathGetNode(IYPathServicePtr service, const TYPath& path)
{
    auto request = TYPathProxy::GetNode(path);
    auto response = ExecuteVerb(service, ~request).Get();
    response->ThrowIfError();
    return reinterpret_cast<INode*>(response->value_ptr());
}

void SyncYPathSet(IYPathServicePtr service, const TYPath& path, const TYson& value)
{
    auto request = TYPathProxy::Set(path);
    request->set_value(value);
    auto response = ExecuteVerb(service, ~request).Get();
    response->ThrowIfError();
}

void SyncYPathSetNode(IYPathServicePtr service, const TYPath& path, INode* value)
{
    auto request = TYPathProxy::SetNode(path);
    request->set_value_ptr(reinterpret_cast<i64>(value));
    auto response = ExecuteVerb(service, ~request).Get();
    response->ThrowIfError();
}

void SyncYPathRemove(IYPathServicePtr service, const TYPath& path)
{
    auto request = TYPathProxy::Remove(path);
    auto response = ExecuteVerb(service, ~request).Get();
    response->ThrowIfError();
}

yvector<Stroka> SyncYPathList(IYPathServicePtr service, const TYPath& path)
{
    auto request = TYPathProxy::List(path);
    auto response = ExecuteVerb(service, ~request).Get();
    response->ThrowIfError();
    return NYT::FromProto<Stroka>(response->keys());
}

void ForceYPath(IMapNodePtr root, const TYPath& path)
{
    INodePtr currentNode = root;
    auto currentPath = path;
    while (true) {
        auto slashToken = ChopToken(currentPath, &currentPath);
        if (slashToken.IsEmpty()) {
            break;
        }
        // TODO(babenko): extract code
        if (slashToken.GetType() != ETokenType::Slash) {
            ythrow yexception() << Sprintf("Unexpected token %s of type %s",
                ~slashToken.ToString().Quote(),
                ~slashToken.GetType().ToString());
        }
        
        INodePtr child;
        auto keyToken = ChopToken(currentPath, &currentPath);
        switch (keyToken.GetType()) {
            case ETokenType::String: {
                auto key = keyToken.GetStringValue();
                child = currentNode->AsMap()->FindChild(key);
                if (!child) {
                    auto factory = currentNode->CreateFactory();
                    child = factory->CreateMap();
                    YVERIFY(currentNode->AsMap()->AddChild(~child, key));
                }
                break;
            }

            case ETokenType::Integer: {
                child = currentNode->AsList()->GetChild(keyToken.GetIntegerValue());
                break;
            }

            default:
                // TODO(babenko): extract code
                ythrow yexception() << Sprintf("Unexpected token %s of type %s",
                    ~keyToken.ToString().Quote(),
                    ~keyToken.GetType().ToString());
        }
        currentNode = child;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
