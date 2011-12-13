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
    // Serialize body.
    TBlob bodyData;
    if (!SerializeBody(&bodyData)) {
        LOG_FATAL("Error serializing YPath request body");
    }

    // Compose message.
    return CreateRequestMessage(
        TRequestId(),
        Path_,
        Verb_,
        MoveRV(bodyData),
        Attachments_);
}

////////////////////////////////////////////////////////////////////////////////

void TYPathResponse::Deserialize(NBus::IMessage* message)
{
    YASSERT(message != NULL);

    const auto& parts = message->GetParts();
    YASSERT(parts.ysize() >= 1);

    // Deserialize RPC header.
    TResponseHeader header;
    if (!DeserializeProtobuf(&header, parts[0])) {
        LOG_FATAL("Error deserializing YPath response header");
    }

    Error_ = TError(header.errorcode(), header.errormessage());

    if (Error_.IsOK()) {
        // Deserialize body.
        DeserializeBody(parts[1]);

        // Load attachments.
        Attachments_.clear();
        std::copy(
            parts.begin() + 2,
            parts.end(),
            std::back_inserter(Attachments_));
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
    auto parts = responseMessage->GetParts();
    YASSERT(!parts.empty());

    TError error;
    ParseYPathResponseHeader(parts[0], &error);

    if (!error.IsOK()) {
        Stroka message = Sprintf("Error executing a YPath operation (Verb: %s, ResolvedPath: %s)\n%s",
            ~verb,
            ~resolvedPath,
            ~error.GetMessage());
        responseMessage = UpdateYPathResponseHeader(~responseMessage, TError(error.GetCode(), message));
    }

    asyncResponseMessage->Set(responseMessage);
}

TFuture<IMessage::TPtr>::TPtr
ExecuteVerb(
    IYPathService* rootService,
    NBus::IMessage* requestMessage,
    IYPathExecutor* executor)
{
    auto parts = requestMessage->GetParts();
    YASSERT(!parts.empty());
    TYPath path;
    Stroka verb;
    ParseYPathRequestHeader(
        parts[0],
        &path,
        &verb);

    IYPathService::TPtr suffixService;
    TYPath suffixPath;
    try {
        // This may throw.
        ResolveYPath(rootService, path, verb, &suffixService, &suffixPath);
    } catch (...) {
        auto responseMessage = NRpc::CreateErrorResponseMessage(
            NRpc::TRequestId(),
            TError(
            EYPathErrorCode(EYPathErrorCode::ResolveError), CurrentExceptionMessage()));
        return New< TFuture<IMessage::TPtr> >(responseMessage);
    }

    auto updatedRequestMessage = UpdateYPathRequestHeader(
        requestMessage,
        suffixPath,
        verb);

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

    // This should never throw.
    executor->ExecuteVerb(~suffixService, ~context);

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
