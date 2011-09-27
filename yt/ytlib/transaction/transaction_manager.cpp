#include "transaction_manager.h"
#include "transaction_manager.pb.h"

#include "../misc/serialize.h"

namespace NYT {
namespace NTransaction {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TransactionLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TState
    : public NMetaState::TMetaStatePart
{
public:
    typedef TIntrusivePtr<TState> TPtr;

    TState(
        const TConfig& config,
        NMetaState::TMetaStateManager::TPtr metaStateManager,
        NMetaState::TCompositeMetaState::TPtr metaState)
        : TMetaStatePart(metaStateManager, metaState)
        , Config(config)
        , LeaseManager(New<TLeaseManager>())
    {
        RegisterMethod(this, &TState::StartTransaction);
        RegisterMethod(this, &TState::CommitTransaction);
        RegisterMethod(this, &TState::AbortTransaction);
    }

    TTransactionId StartTransaction(const TMsgCreateTransaction& message)
    {
        TTransactionId id = TTransactionId::FromProto(message.GetTransactionId());

        TTransaction transaction(id);
        if (IsLeader()) {
            CreateLease(transaction);
        }

        YVERIFY(Transactions.Insert(id, transaction));

        FOREACH(auto& handler, Handlers) {
            handler->OnTransactionStarted(transaction);
        }

        LOG_INFO("Transaction started (TransactionId: %s)",
            ~id.ToString());

        return id;
    }

    TVoid CommitTransaction(const TMsgCommitTransaction& message)
    {
        TTransactionId id = TTransactionId::FromProto(message.GetTransactionId());

        TTransaction& transaction = Transactions.GetForUpdate(id);

        // TODO: timing
        FOREACH(auto& handler, Handlers) {
            handler->OnTransactionCommitted(transaction);
        }

        if (IsLeader()) {
            CloseLease(transaction);
        }

        YASSERT(Transactions.Remove(id));

        LOG_INFO("Transaction committed (TransactionId: %s)",
            ~id.ToString());

        return TVoid();
    }
    
    TVoid AbortTransaction(const TMsgAbortTransaction& message)
    {
        TTransactionId id = TTransactionId::FromProto(message.GetTransactionId());

        TTransaction& transaction = Transactions.GetForUpdate(id);

        // TODO: timing

        FOREACH(auto& handler, Handlers) {
            handler->OnTransactionAborted(transaction);
        }

        if (IsLeader()) {
            CloseLease(transaction);
        }

        YASSERT(Transactions.Remove(id));

        LOG_INFO("Transaction aborted (TransactionId: %s)",
            ~id.ToString());

        return TVoid();
    }


    const TTransaction* FindTransaction(const TTransactionId& id)
    {
        const TTransaction* transaction = Transactions.Find(id);
        if (transaction != NULL && IsLeader()) {
            RenewTransactionLease(*transaction);
        }
        return transaction;
    }

    TTransaction* FindTransactionForUpdate(const TTransactionId& id)
    {
        TTransaction* transaction = Transactions.FindForUpdate(id);
        if (transaction != NULL && IsLeader()) {
            RenewTransactionLease(*transaction);
        }
        return transaction;
    }

    const TTransaction& GetTransaction(const TTransactionId& id)
    {
        const TTransaction* transaction = FindTransaction(id);
        YASSERT(transaction != NULL);
        return *transaction;
    }

    TTransaction& GetTransactionForUpdate(const TTransactionId& id)
    {
        TTransaction* transaction = FindTransactionForUpdate(id);
        YASSERT(transaction != NULL);
        return *transaction;
    }

    void RenewTransactionLease(const TTransaction& transaction)
    {
        YASSERT(IsLeader());
        LeaseManager->RenewLease(transaction.Lease);
    }


    void RegisterHander(ITransactionHandler::TPtr handler)
    {
        Handlers.push_back(handler);
    }

private:
    typedef NMetaState::TMetaStateMap<TTransactionId, TTransaction> TTransactionMap;
    typedef yvector<ITransactionHandler::TPtr> THandlers;

    //! Configuration.
    TConfig Config;

    //! Controls leases of running transactions.
    TLeaseManager::TPtr LeaseManager;

    //! Active transaction.
    TTransactionMap Transactions;

    //! Registered handlers.
    THandlers Handlers;

    void CreateLease(TTransaction& transaction)
    {
        YASSERT(IsLeader());
        YASSERT(transaction.Lease == TLeaseManager::TLease());
        transaction.Lease = LeaseManager->CreateLease(
            Config.TransactionTimeout,
            FromMethod(
                &TState::OnTransactionExpired,
                TPtr(this),
                transaction)
            ->Via(GetStateInvoker()));
    }

    void CloseLease(TTransaction& transaction)
    {
        YASSERT(IsLeader());
        YASSERT(transaction.Lease != TLeaseManager::TLease());
        LeaseManager->CloseLease(transaction.Lease);
        transaction.Lease.Drop();
    }

    void OnTransactionExpired(const TTransaction& transaction)
    {
        const TTransactionId& id = transaction.Id;

        // Check if the transaction is still registered.
        if (!Transactions.Contains(id))
            return;

        LOG_INFO("Transaction expired (TransactionId: %s)",
            ~id.ToString());

        TMsgAbortTransaction message;
        message.SetTransactionId(id.ToProto());
        CommitChange(message, FromMethod(&TState::AbortTransaction, TPtr(this)));
    }
    
    void CreateAllLeases()
    {
        FOREACH(auto& pair, Transactions) {
            CreateLease(pair.second);
        }
        LOG_INFO("Created fresh leases for all transactions");
    }

    void CloseAllLeases()
    {
        FOREACH(auto& pair, Transactions) {
            CloseLease(pair.second);
        }
        LOG_INFO("Closed all transaction leases");
    }

    // TMetaStatePart overrides.
    virtual Stroka GetPartName() const
    {
        return "TransactionManager";
    }

    virtual TFuture<TVoid>::TPtr Save(TOutputStream* stream)
    {
        return Transactions.Save(GetSnapshotInvoker(), stream);
    }

    virtual TFuture<TVoid>::TPtr Load(TInputStream* stream)
    {
        return Transactions.Load(GetSnapshotInvoker(), stream);
    }

    virtual void Clear()
    {
        if (IsLeader()) {
            CloseAllLeases();
        }
        Transactions.Clear();
    }

    virtual void OnStartLeading()
    {
        TMetaStatePart::OnStartLeading();
        
        CreateAllLeases();
    }

    virtual void OnStopLeading()
    {
        TMetaStatePart::OnStopLeading();

        CloseAllLeases();
    }
};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    const TConfig& config,
    NMetaState::TMetaStateManager::TPtr metaStateManager,
    NMetaState::TCompositeMetaState::TPtr metaState,
    IInvoker::TPtr serviceInvoker,
    NRpc::TServer::TPtr server)
    : TMetaStateServiceBase(
        serviceInvoker,
        TTransactionManagerProxy::GetServiceName(),
        TransactionLogger.GetCategory())
    , State(New<TState>(
        config,
        metaStateManager,
        metaState))
{
    RegisterMethods();
    metaState->RegisterPart(~State);
    server->RegisterService(this);
}

void TTransactionManager::RegisterMethods()
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(RenewTransactionLease));
}

void TTransactionManager::RegisterHander(ITransactionHandler::TPtr handler)
{
    State->RegisterHander(handler);
}

void TTransactionManager::ValidateTransactionId(const TTransactionId& id)
{
    const TTransaction* transaction = State->FindTransaction(id);
    if (transaction == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) <<
            Sprintf("Unknown or expired transaction %s",
                ~id.ToString());
    }
}

METAMAP_ACCESSORS_FWD(TTransactionManager, Transaction, TTransaction, TTransactionId, *State)

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TTransactionManager, StartTransaction)
{
    UNUSED(request);
    UNUSED(response);

    context->SetRequestInfo("");

    TMsgCreateTransaction message;
    message.SetTransactionId(TTransactionId::Create().ToProto());

    CommitChange(
        this, context, State, message,
        &TState::StartTransaction,
        &TThis::OnTransactionStarted);
}

void TTransactionManager::OnTransactionStarted(
    TTransactionId id,
    TCtxStartTransaction::TPtr context)
{
    TRspStartTransaction* response = &context->Response();

    response->SetTransactionId(id.ToProto());

    context->SetResponseInfo("TransactionId: %s",
        ~id.ToString());

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TTransactionManager, CommitTransaction)
{
    UNUSED(response);

    TTransactionId id = TTransactionId::FromProto(request->GetTransactionId());

    context->SetRequestInfo("TransactionId: %s",
        ~id.ToString());
    
    ValidateTransactionId(id);

    TMsgCommitTransaction message;
    message.SetTransactionId(id.ToProto());

    CommitChange(
        this, context, State, message,
        &TState::CommitTransaction);
}

RPC_SERVICE_METHOD_IMPL(TTransactionManager, AbortTransaction)
{
    UNUSED(response);

    TTransactionId id = TTransactionId::FromProto(request->GetTransactionId());

    context->SetRequestInfo("TransactionId: %s",
        ~id.ToString());
    
    ValidateTransactionId(id);

    TMsgAbortTransaction message;
    message.SetTransactionId(id.ToProto());

    CommitChange(
        this, context, State, message,
        &TState::AbortTransaction);
}

RPC_SERVICE_METHOD_IMPL(TTransactionManager, RenewTransactionLease)
{
    UNUSED(response);

    TTransactionId id = TTransactionId::FromProto(request->GetTransactionId());

    context->SetRequestInfo("TransactionId: %s",
        ~id.ToString());
    
    ValidateTransactionId(id);

    const TTransaction& transaction = State->GetTransaction(id);
    State->RenewTransactionLease(transaction);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
