#include "transaction_manager.h"

#include "../misc/serialize.h"
#include "../misc/string.h"

namespace NYT {
namespace NTransaction {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TransactionLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TState
    : public TRefCountedBase
{
public:
    TTransaction::TPtr CreateTransaction()
    {
        TTransactionId id;
        CreateGuid(&id);
        
        TTransaction::TPtr transaction = new TTransaction(id);

        // TOOD: use YVERIFY
        VERIFY(Transactions.insert(MakePair(id, transaction)).Second(), "oops");

        return transaction;
    }

    void RemoveTransaction(TTransaction::TPtr transaction)
    {
        // TOOD: use YVERIFY
        VERIFY(Transactions.erase(transaction->GetId()) == 1, "oops");
    }

    TTransaction::TPtr FindTransaction(TTransactionId id)
    {
        TTransactionMap::iterator it = Transactions.find(id);
        if (it == Transactions.end())
            return NULL;
        else
            return it->Second();
    }

    TTransaction::TPtr GetTransaction(TTransactionId id)
    {
        TTransaction::TPtr transaction = FindTransaction(id);
        if (~transaction == NULL) {
            ythrow TServiceException(EErrorCode::NoSuchTransaction) <<
                Sprintf("unknown or expired transaction %s",
                    ~StringFromGuid(id));
        }
        return transaction;
    }

private:
    typedef yhash_map<TTransactionId, TTransaction::TPtr, TGUIDHash> TTransactionMap;

    TTransactionMap Transactions;

};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    const TConfig& config,
    NRpc::TServer* server)
    : TServiceBase(
        TTransactionManagerProxy::GetServiceName(),
        TransactionLogger.GetCategory())
    , Config(config)
    , ServiceInvoker(server->GetInvoker())
    , LeaseManager(new TLeaseManager())
    , State(new TState())
{
    server->RegisterService(this);
}

void TTransactionManager::RegisterHander(ITransactionHandler::TPtr handler)
{
    Handlers.push_back(handler);
}

TTransaction::TPtr TTransactionManager::FindTransaction(TTransactionId id)
{
    return State->FindTransaction(id);
}

TTransaction::TPtr TTransactionManager::DoStartTransaction()
{
    TTransaction::TPtr transaction = State->CreateTransaction();
    TLeaseManager::TLease lease = LeaseManager->CreateLease(
        Config.TransactionTimeout,
        FromMethod(
            &TTransactionManager::OnTransactionExpired,
            TPtr(this),
            transaction)
        ->Via(ServiceInvoker));
    transaction->SetLease(lease);

    for (THandlers::iterator it = Handlers.begin();
         it != Handlers.end();
         ++it)
    {
        (*it)->OnTransactionStarted(transaction);
    }

    LOG_INFO("Transaction started (TransactionId: %s)",
        ~StringFromGuid(transaction->GetId()));

    return transaction;
}

void TTransactionManager::DoCommitTransaction(TTransaction::TPtr transaction)
{
    // TODO: timing
    for (THandlers::iterator it = Handlers.begin();
         it != Handlers.end();
         ++it)
    {
        (*it)->OnTransactionCommitted(transaction);
    }

    State->RemoveTransaction(transaction);

    LeaseManager->CloseLease(transaction->GetLease());

    LOG_INFO("Transaction committed (TransactionId: %s)",
        ~StringFromGuid(transaction->GetId()));
}

void TTransactionManager::DoAbortTransaction(TTransaction::TPtr transaction)
{
    // TODO: timing
    for (THandlers::iterator it = Handlers.begin();
         it != Handlers.end();
         ++it)
    {
        (*it)->OnTransactionAborted(transaction);
    }

    State->RemoveTransaction(transaction);

    LeaseManager->CloseLease(transaction->GetLease());

    LOG_INFO("Transaction aborted (TransactionId: %s)",
        ~StringFromGuid(transaction->GetId()));
}

void TTransactionManager::OnTransactionExpired( TTransaction::TPtr transaction )
{
    // Check if the transaction is still registered.
    if (~State->FindTransaction(transaction->GetId()) != NULL) {
        LOG_INFO("Transaction expired (TransactionId: %s)",
            ~StringFromGuid(transaction->GetId()));

        DoAbortTransaction(transaction);
    }
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TTransactionManager, StartTransaction)
{
    UNUSED(request);

    context->SetRequestInfo("");

    TTransaction::TPtr transaction = DoStartTransaction();

    response->SetTransactionId(ProtoGuidFromGuid(transaction->GetId()));

    context->SetResponseInfo("TransactionId: %s",
        ~StringFromGuid(transaction->GetId()));

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TTransactionManager, CommitTransaction)
{
    UNUSED(response);

    TTransactionId id = GuidFromProtoGuid(request->GetTransactionId());

    context->SetRequestInfo("TransactionId: %s",
        ~StringFromGuid(id));
    
    TTransaction::TPtr transaction = State->GetTransaction(id);
    DoCommitTransaction(transaction);

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TTransactionManager, AbortTransaction)
{
    UNUSED(response);

    TTransactionId id = GuidFromProtoGuid(request->GetTransactionId());

    context->SetRequestInfo("TransactionId: %s",
        ~StringFromGuid(id));

    TTransaction::TPtr transaction = State->GetTransaction(id);
    DoAbortTransaction(transaction);

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TTransactionManager, RenewTransactionLease)
{
    UNUSED(response);

    TTransactionId id = GuidFromProtoGuid(request->GetTransactionId());

    context->SetRequestInfo("TransactionId: %s",
        ~StringFromGuid(id));

    TTransaction::TPtr transaction = State->GetTransaction(id);
    TLeaseManager::TLease lease = transaction->GetLease();
    LeaseManager->RenewLease(lease);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
