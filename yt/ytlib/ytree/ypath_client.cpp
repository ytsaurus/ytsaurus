#include "stdafx.h"
#include "ypath_client.h"
#include "ypath_rpc.h"
#include "ypath_detail.h"
#include "rpc.pb.h"

#include "../misc/serialize.h"
#include "../rpc/message.h"

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
    YASSERT(message != NULL);

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
    IYPathService* rootService,
    NBus::IMessage* requestMessage,
    IYPathExecutor* executor)
{
    auto requestHeader = GetRequestHeader(requestMessage);
    TYPath path = requestHeader.path();
    Stroka verb = requestHeader.verb();

    IYPathService::TPtr suffixService;
    TYPath suffixPath;
    try {
        // This may throw.
        ResolveYPath(rootService, path, verb, &suffixService, &suffixPath);
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
        executor->ExecuteVerb(~suffixService, ~context);
    }
    catch (...) {
        LOG_FATAL("Unexpected exception during verb execution\n%s", ~CurrentExceptionMessage());
    }

    return asyncResponseMessage;
}

TYson SyncYPathGet(IYPathService* rootService, const TYPath& path)
{
    auto request = TYPathProxy::Get();
    request->SetPath(path);
    auto response = ExecuteVerb(rootService, ~request)->Get();
    response->ThrowIfError();
    return response->value();
}

INode::TPtr SyncYPathGetNode(IYPathService* rootService, const TYPath& path)
{
    auto request = TYPathProxy::GetNode();
    request->SetPath(path);
    auto response = ExecuteVerb(rootService, ~request)->Get();
    response->ThrowIfError();
    return reinterpret_cast<INode*>(response->value());
}

void SyncYPathSet(IYPathService* rootService, const TYPath& path, const TYson& value)
{
    auto request = TYPathProxy::Set();
    request->SetPath(path);
    request->set_value(value);
    auto response = ExecuteVerb(rootService, ~request)->Get();
    response->ThrowIfError();
}

void SyncYPathSetNode(IYPathService* rootService, const TYPath& path, INode* value)
{
    auto request = TYPathProxy::SetNode();
    request->SetPath(path);
    request->set_value(reinterpret_cast<i64>(value));
    auto response = ExecuteVerb(rootService, ~request)->Get();
    response->ThrowIfError();
}

void SyncYPathRemove(IYPathService* rootService, const TYPath& path)
{
    auto request = TYPathProxy::Remove();
    request->SetPath(path);
    auto response = ExecuteVerb(rootService, ~request)->Get();
    response->ThrowIfError();
}

yvector<Stroka> SyncYPathList(IYPathService* rootService, const TYPath& path)
{
    auto request = TYPathProxy::List();
    request->SetPath(path);
    auto response = ExecuteVerb(rootService, ~request)->Get();
    response->ThrowIfError();
    return FromProto<Stroka>(response->keys());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
