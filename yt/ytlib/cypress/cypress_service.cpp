#include "cypress_service.h"

#include "../ytree/yson_writer.h"

namespace NYT {
namespace NCypress {

using namespace NYTree;
using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = CypressLogger;

////////////////////////////////////////////////////////////////////////////////

TCypressService::TCypressService(
    const TConfig& config,
    TCypressManager::TPtr cypressManager,
    IInvoker::TPtr serviceInvoker,
    NRpc::TServer::TPtr server)
    : TMetaStateServiceBase(
        serviceInvoker,
        TCypressServiceProxy::GetServiceName(),
        CypressLogger.GetCategory())
    , Config(config)
    , CypressManager(cypressManager)
{
    YASSERT(~cypressManager != NULL);
    YASSERT(~serviceInvoker != NULL);
    YASSERT(~server!= NULL);

    RegisterMethods();

    server->RegisterService(this);
}

void TCypressService::RegisterMethods()
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Get));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Set));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Lock));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Remove));
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TCypressService, Get)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    // TODO: validate transaction id

    auto root = CypressManager->GetNode(RootNodeId, transactionId);

    Stroka output;
    TStringOutput outputStream(output);
    TYsonWriter writer(&outputStream, false); // TODO: use binary

    try {
        CypressManager->GetYPath(transactionId, path, &writer);
    } catch (...) {
        // TODO: use proper error code
        context->Reply(EErrorCode::ShitHappens);

        // TODO: abort transaction
    }

    response->SetValue(output);
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TCypressService, Set)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();
    Stroka value = request->GetValue();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    // TODO: validate transaction id

    auto root = CypressManager->GetNode(RootNodeId, transactionId);

    NProto::TMsgSetPath message;
    message.SetTransactionId(transactionId.ToProto());
    message.SetPath(path);
    message.SetValue(value);

    try {
        CommitChange(
            this, context, CypressManager, message,
            &TCypressManager::SetYPath,
            ECommitMode::MayFail);
    } catch (...) {
        // TODO: use proper error code
        context->Reply(EErrorCode::ShitHappens);

        // TODO: abort transaction
    }
}

RPC_SERVICE_METHOD_IMPL(TCypressService, Remove)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    auto root = CypressManager->GetNode(RootNodeId, transactionId);

    // TODO: validate transaction id

    NProto::TMsgRemovePath message;
    message.SetTransactionId(transactionId.ToProto());
    message.SetPath(path);

    try {
        CommitChange(
            this, context, CypressManager, message,
            &TCypressManager::RemoveYPath,
            ECommitMode::MayFail);
    } catch (...) {
        // TODO: use proper error code
        context->Reply(EErrorCode::ShitHappens);

        // TODO: abort transaction
    }
}

RPC_SERVICE_METHOD_IMPL(TCypressService, Lock)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();
    auto mode = ELockMode(request->GetMode());

    context->SetRequestInfo("TransactionId: %s, Path: %s, Mode: %s",
        ~transactionId.ToString(),
        ~path,
        ~mode.ToString());

    UNUSED(response);
    YASSERT(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
