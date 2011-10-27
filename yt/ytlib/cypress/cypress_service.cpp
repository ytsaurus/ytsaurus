#include "stdafx.h"
#include "cypress_service.h"
#include "node_proxy.h"

#include "../ytree/yson_writer.h"

namespace NYT {
namespace NCypress {

using namespace NRpc;
using namespace NYTree;
using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = CypressLogger;

////////////////////////////////////////////////////////////////////////////////

TCypressService::TCypressService(
    TCypressManager::TPtr cypressManager,
    TTransactionManager::TPtr transactionManager,
    IInvoker::TPtr serviceInvoker,
    NRpc::TServer::TPtr server)
    : TMetaStateServiceBase(
        serviceInvoker,
        TCypressServiceProxy::GetServiceName(),
        CypressLogger.GetCategory())
    , CypressManager(cypressManager)
    , TransactionManager(transactionManager)
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
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetNodeId));
}

void TCypressService::ValidateTransactionId(const TTransactionId& transactionId)
{
    if (TransactionManager->FindTransaction(transactionId) == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) << 
            Sprintf("Invalid transaction id (TransactionId: %s)", ~transactionId.ToString());
    }
}

void TCypressService::ExecuteRecoverable(
    const TTransactionId& transactionId,
    NRpc::TServiceContext::TPtr context,
    IAction::TPtr action)
{
    if (transactionId != NullTransactionId) {
        ValidateTransactionId(transactionId);
    }

    try {
        action->Do();
    } catch (const TServiceException&) {
        throw;
    } catch (...) {
        context->Reply(EErrorCode::RecoverableError);
    }
}

void TCypressService::ExecuteUnrecoverable(
    const TTransactionId& transactionId,
    NRpc::TServiceContext::TPtr context,
    IAction::TPtr action)
{
    if (transactionId != NullTransactionId) {
        ValidateTransactionId(transactionId);
    }

    try {
        action->Do();
    } catch (const TServiceException&) {
        throw;
    } catch (...) {
        context->Reply(EErrorCode::UnrecoverableError);

        if (transactionId != NullTransactionId) {
            TransactionManager
                ->InitiateAbortTransaction(transactionId)
                ->Commit();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TCypressService, Get)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    ExecuteRecoverable(
        transactionId,
        context->GetUntypedContext(),
        FromFunctor([=] ()
            {
                Stroka output;
                TStringOutput outputStream(output);
                TYsonWriter writer(&outputStream, TYsonWriter::EFormat::Binary);

                CypressManager->GetYPath(transactionId, path, &writer);

                auto* response = &context->Response();
                response->SetValue(output);

                context->Reply();
            }));
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

    ExecuteUnrecoverable(
        transactionId,
        context->GetUntypedContext(),
        FromFunctor([=] ()
            {
                CypressManager
                    ->InitiateSetYPath(transactionId, path, value)
                    ->OnSuccess(this->CreateSuccessHandler(context))
                    ->OnError(this->CreateErrorHandler(context))
                    ->Commit();
            }));
}

RPC_SERVICE_METHOD_IMPL(TCypressService, Remove)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    ExecuteRecoverable(
        transactionId,
        context->GetUntypedContext(),
        FromFunctor([=] ()
            {
                CypressManager
                    ->InitiateRemoveYPath(transactionId, path)
                    ->OnSuccess(this->CreateSuccessHandler(context))
                    ->OnError(this->CreateErrorHandler(context))
                    ->Commit();
            }));
}

RPC_SERVICE_METHOD_IMPL(TCypressService, Lock)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    ExecuteRecoverable(
        transactionId,
        context->GetUntypedContext(),
        FromFunctor([=] ()
            {
                CypressManager
                    ->InitiateLockYPath(transactionId, path)
                    ->OnSuccess(this->CreateSuccessHandler(context))
                    ->OnError(this->CreateErrorHandler(context))
                    ->Commit();
            }));
}

RPC_SERVICE_METHOD_IMPL(TCypressService, GetNodeId)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    Stroka path = request->GetPath();

    context->SetRequestInfo("TransactionId: %s, Path: %s",
        ~transactionId.ToString(),
        ~path);

    ExecuteRecoverable(
        transactionId,
        context->GetUntypedContext(),
        FromFunctor([=] ()
            {
                auto node = CypressManager->NavigateYPath(transactionId, path);
                
                ICypressNodeProxy::TPtr cypressNode(dynamic_cast<ICypressNodeProxy*>(~node));
                if (~cypressNode == NULL) {
                    throw yexception() << "Node has no id";
                }

                response->SetNodeId(cypressNode->GetNodeId().ToProto());
                context->Reply();
            }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
