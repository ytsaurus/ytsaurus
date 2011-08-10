#include "transaction_manager.h"
#include "transaction_manager.pb.h"

#include "../master/map.h"

#include "../misc/serialize.h"

namespace NYT {
namespace NTransaction {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TransactionLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TState
    : public TMetaStatePart
{
public:
    typedef TIntrusivePtr<TState> TPtr;

    TState(
        const TConfig& config,
        TMetaStateManager::TPtr metaStateManager,
        TCompositeMetaState::TPtr metaState)
        : TMetaStatePart(metaStateManager, metaState)
        , Config(config)
        , LeaseManager(new TLeaseManager())
    {
        RegisterMethod(this, &TState::StartTransaction);
        RegisterMethod(this, &TState::CommitTransaction);
        RegisterMethod(this, &TState::AbortTransaction);
    }

    TTransaction::TPtr StartTransaction(const TMsgCreateTransaction& message)
    {
        TTransactionId id = TGuid::FromProto(message.GetTransactionId());

        TTransaction::TPtr transaction = new TTransaction(id);
        if (IsLeader()) {
            CreateLease(transaction);
        }

        YVERIFY(Transactions.Insert(id, transaction));

        for (THandlers::iterator it = Handlers.begin();
             it != Handlers.end();
             ++it)
        {
            (*it)->OnTransactionStarted(transaction);
        }

        LOG_INFO("Transaction started (TransactionId: %s)",
            ~id.ToString());

        return transaction;
    }

    TVoid CommitTransaction(const TMsgCommitTransaction& message)
    {
        TTransactionId id = TGuid::FromProto(message.GetTransactionId());

        TTransaction::TPtr transaction = FindTransaction(id);
        YASSERT(~transaction != NULL);

        // TODO: timing
        for (THandlers::iterator it = Handlers.begin();
             it != Handlers.end();
             ++it)
        {
            (*it)->OnTransactionCommitted(transaction);
        }

        YASSERT(Transactions.Remove(id));

        if (IsLeader()) {
            CloseLease(transaction);
        }

        LOG_INFO("Transaction committed (TransactionId: %s)",
            ~transaction->GetId().ToString());

        return TVoid();
    }
    
    TVoid AbortTransaction(const TMsgAbortTransaction& message)
    {
        TTransactionId id = TGuid::FromProto(message.GetTransactionId());

        TTransaction::TPtr transaction = FindTransaction(id);
        YASSERT(~transaction != NULL);

        // TODO: timing
        for (THandlers::iterator it = Handlers.begin();
             it != Handlers.end();
             ++it)
        {
            (*it)->OnTransactionAborted(transaction);
        }

        YASSERT(Transactions.Remove(id));

        if (IsLeader()) {
            CloseLease(transaction);
        }

        LOG_INFO("Transaction aborted (TransactionId: %s)",
            ~transaction->GetId().ToString());

        return TVoid();
    }


    TTransaction::TPtr FindTransaction(const TTransactionId& id, bool forUpdate = false)
    {
        TTransaction::TPtr transaction = Transactions.Find(id, forUpdate);
        if (~transaction != NULL && IsLeader()) {
            RenewTransactionLease(transaction);
        }
        return transaction;
    }

    TTransaction::TPtr GetTransaction(const TTransactionId& id, bool forUpdate = false)
    {
        TTransaction::TPtr transaction = FindTransaction(id, forUpdate);
        if (~transaction == NULL) {
            ythrow TServiceException(EErrorCode::NoSuchTransaction) <<
                Sprintf("unknown or expired transaction %s",
                    ~id.ToString());
        }
        return transaction;
    }

    void RenewTransactionLease(TTransaction::TPtr transaction)
    {
        YASSERT(IsLeader());
        LeaseManager->RenewLease(transaction->Lease());
    }


    void RegisterHander(ITransactionHandler::TPtr handler)
    {
        Handlers.push_back(handler);
    }

private:
    typedef TMetaStateRefMap<TTransactionId, TTransaction, TTransactionIdHash> TTransactionMap;
    typedef yvector<ITransactionHandler::TPtr> THandlers;

    //! Configuration.
    TConfig Config;

    //! Controls leases of running transactions.
    TLeaseManager::TPtr LeaseManager;

    //! Active transaction.
    TTransactionMap Transactions;

    //! Registered handlers.
    THandlers Handlers;

    void CreateLease(TTransaction::TPtr transaction)
    {
        YASSERT(IsLeader());
        YASSERT(transaction->Lease() == TLeaseManager::TLease());
        transaction->Lease() = LeaseManager->CreateLease(
            Config.TransactionTimeout,
            FromMethod(
            &TState::OnTransactionExpired,
            TPtr(this),
            transaction)
            ->Via(GetStateInvoker()));
    }

    void CloseLease(TTransaction::TPtr transaction)
    {
        YASSERT(IsLeader());
        YASSERT(transaction->Lease() != TLeaseManager::TLease());
        LeaseManager->CloseLease(transaction->Lease());
    }

    void OnTransactionExpired(TTransaction::TPtr transaction)
    {
        TTransactionId id = transaction->GetId();

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
        for (TTransactionMap::TIterator it = Transactions.Begin();
             it != Transactions.End();
             ++it)
        {
            CreateLease(it->Second());
        }
        LOG_INFO("Created fresh leases for all transactions");
    }

    void CloseAllLeases()
    {
        for (TTransactionMap::TIterator it = Transactions.Begin();
             it != Transactions.End();
             ++it)
        {
            CloseLease(it->Second());
        }
        LOG_INFO("Closed all transaction leases");
    }

    // TMetaStatePart overrides.
    virtual Stroka GetPartName() const
    {
        return "TransactionManager";
    }

    virtual TAsyncResult<TVoid>::TPtr Save(TOutputStream& stream)
    {
        return Transactions.Save(GetSnapshotInvoker(), stream);
    }

    virtual TAsyncResult<TVoid>::TPtr Load(TInputStream& stream)
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
        CreateAllLeases();
    }

    virtual void OnStopLeading()
    {
        CloseAllLeases();
    }
};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    const TConfig& config,
    TMetaStateManager::TPtr metaStateManager,
    TCompositeMetaState::TPtr metaState,
    IInvoker::TPtr serviceInvoker,
    NRpc::TServer::TPtr server)
    : TMetaStateServiceBase(
        serviceInvoker,
        TTransactionManagerProxy::GetServiceName(),
        TransactionLogger.GetCategory())
    , State(new TState(
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
    RPC_REGISTER_METHOD(TTransactionManager, StartTransaction);
    RPC_REGISTER_METHOD(TTransactionManager, CommitTransaction);
    RPC_REGISTER_METHOD(TTransactionManager, AbortTransaction);
    RPC_REGISTER_METHOD(TTransactionManager, RenewTransactionLease);
}

void TTransactionManager::RegisterHander(ITransactionHandler::TPtr handler)
{
    State->RegisterHander(handler);
}

TTransaction::TPtr TTransactionManager::FindTransaction(
    const TTransactionId& id,
    bool forUpdate)
{
    return State->FindTransaction(id, forUpdate);
}

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
    TTransaction::TPtr transaction,
    TCtxStartTransaction::TPtr context)
{
    TRspStartTransaction* response = &context->Response();

    response->SetTransactionId(transaction->GetId().ToProto());

    context->SetResponseInfo("TransactionId: %s",
        ~transaction->GetId().ToString());

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TTransactionManager, CommitTransaction)
{
    UNUSED(response);

    TTransactionId id = TGuid::FromProto(request->GetTransactionId());

    context->SetRequestInfo("TransactionId: %s",
        ~id.ToString());
    
    State->GetTransaction(id);

    TMsgCommitTransaction message;
    message.SetTransactionId(id.ToProto());

    CommitChange(
        this, context, State, message,
        &TState::CommitTransaction);
}

RPC_SERVICE_METHOD_IMPL(TTransactionManager, AbortTransaction)
{
    UNUSED(response);

    TTransactionId id = TGuid::FromProto(request->GetTransactionId());

    context->SetRequestInfo("TransactionId: %s",
        ~id.ToString());
    
    State->GetTransaction(id);

    TMsgAbortTransaction message;
    message.SetTransactionId(id.ToProto());

    CommitChange(
        this, context, State, message,
        &TState::AbortTransaction);
}

RPC_SERVICE_METHOD_IMPL(TTransactionManager, RenewTransactionLease)
{
    UNUSED(response);

    TTransactionId id = TGuid::FromProto(request->GetTransactionId());

    context->SetRequestInfo("TransactionId: %s",
        ~id.ToString());
    
    TTransaction::TPtr transaction = State->GetTransaction(id);
    State->RenewTransactionLease(transaction);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
