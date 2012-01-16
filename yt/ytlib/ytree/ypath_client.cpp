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

////////////////////////////////////////////////////////////////////////////////

void OnYPathResponse(
    IMessage::TPtr responseMessage,
    TFuture<IMessage::TPtr>::TPtr asyncResponseMessage,
    const Stroka& verb,
    const TYPath& resolvedPath)
{
    auto header = GetResponseHeader(~responseMessage);
    auto error = GetResponseError(header);

    if (error.IsOK()) {
        asyncResponseMessage->Set(responseMessage);
    } else {
        Stroka message = Sprintf("Error executing a YPath operation (Verb: %s, ResolvedPath: %s)\n%s",
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
    } catch (...) {
        auto responseMessage = NRpc::CreateErrorResponseMessage(TError(
            EYPathErrorCode(EYPathErrorCode::ResolveError),
            CurrentExceptionMessage()));
        return New< TFuture<IMessage::TPtr> >(responseMessage);
    }

    requestHeader.set_path(suffixPath);
    auto updatedRequestMessage = SetRequestHeader(requestMessage, requestHeader);

    auto asyncResponseMessage = New< TFuture<IMessage::TPtr> >();
    auto context = CreateYPathContext(
        ~updatedRequestMessage,
        suffixPath,
        verb,
        YTreeLogger.GetCategory(),
        ~FromMethod(
            &OnYPathResponse,
            asyncResponseMessage,
            verb,
            ComputeResolvedYPath(path, suffixPath)));

    try {
        // This should never throw.
        processor->Execute(~suffixService, ~context);
    }
    catch (...) {
        LOG_FATAL("Unexpected exception during verb execution\n%s", ~CurrentExceptionMessage());
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
