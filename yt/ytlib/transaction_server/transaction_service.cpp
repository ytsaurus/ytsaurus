#include "stdafx.h"
#include "transaction_service.h"

namespace NYT {
namespace NTransactionServer {

using namespace NRpc;
using namespace NProto;
using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

TTransactionService::TTransactionService(
    IMetaStateManager* metaStateManager,
    TTransactionManager* transactionManager)
    : TMetaStateServiceBase(
        metaStateManager,
        TTransactionServiceProxy::GetServiceName(),
        TransactionServerLogger.GetCategory())
    , TransactionManager(transactionManager)
{
    YASSERT(transactionManager);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(RenewTransactionLease));
}

void TTransactionService::ValidateTransactionId(const TTransactionId& id)
{
    if (!TransactionManager->FindTransaction(id)) {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) <<
            Sprintf("Unknown or expired transaction id (TransactionId: %s)",
                ~id.ToString());
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TTransactionService, StartTransaction)
{
    UNUSED(request);

    context->SetRequestInfo("");

    ValidateLeader();

    TransactionManager
        ->InitiateStartTransaction()
        ->OnSuccess(~FromFunctor([=] (TTransactionId id)
            {
                response->set_transaction_id(id.ToProto());

                context->SetResponseInfo("TransactionId: %s",
                    ~id.ToString());

                context->Reply();
            }))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TTransactionService, CommitTransaction)
{
    UNUSED(response);

    auto id = TTransactionId::FromProto(request->transaction_id());

    context->SetRequestInfo("TransactionId: %s",
        ~id.ToString());
    
    ValidateLeader();
    ValidateTransactionId(id);

    TransactionManager
        ->InitiateCommitTransaction(id)
        ->OnSuccess(~CreateSuccessHandler(~context))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TTransactionService, AbortTransaction)
{
    UNUSED(response);

    auto id = TTransactionId::FromProto(request->transaction_id());

    context->SetRequestInfo("TransactionId: %s",
        ~id.ToString());
    
    ValidateLeader();
    ValidateTransactionId(id);

    TransactionManager
        ->InitiateAbortTransaction(id)
        ->OnSuccess(~CreateSuccessHandler(~context))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TTransactionService, RenewTransactionLease)
{
    UNUSED(response);

    auto id = TTransactionId::FromProto(request->transaction_id());

    context->SetRequestInfo("TransactionId: %s",
        ~id.ToString());
    
    ValidateLeader();
    ValidateTransactionId(id);

    TransactionManager->RenewLease(id);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
