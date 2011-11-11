#include "stdafx.h"
#include "transaction_manager.h"

#include "../transaction_server/transaction_service_rpc.h"
#include "../actions/signal.h"
#include "../misc/assert.h"
#include "../misc/property.h"
#include "../misc/thread_affinity.h"
#include "../misc/periodic_invoker.h"

namespace NYT {
namespace NTransactionClient {
////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("TransactionClient");

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransaction
    : public ITransaction
{
    DECLARE_BYREF_RW_PROPERTY(OnCommitted, TSignal);
    DECLARE_BYREF_RW_PROPERTY(OnAborted, TSignal);

public:
    typedef TIntrusivePtr<TTransaction> TPtr;

    TTransaction(
        NRpc::IChannel::TPtr cellChannel,
        TTransactionManager::TPtr transactionManager)
        : TransactionManager(transactionManager)
        , Proxy(cellChannel)
        , State(EState::Init)
    {
        VERIFY_THREAD_AFFINITY(ClientThread);
        YASSERT(~cellChannel != NULL);
        YASSERT(~transactionManager != NULL);

        LOG_INFO("Starting transaction");

        auto req = Proxy.StartTransaction();
        auto rsp = req->Invoke(TransactionManager->Config.RpcTimeout)->Get();

        if (!rsp->IsOK()) {
            // No ping tasks are running, so no need to lock here.
            State = EState::Finished;

            // TODO: exception type
            LOG_ERROR_AND_THROW(yexception(), "Error starting transaction (Error: %s)",
                ~rsp->GetError().ToString());
        }

        Id = TTransactionId::FromProto(rsp->GetTransactionId());

        LOG_DEBUG("Transaction started (TransactionId: %s)", ~Id.ToString());

        State = EState::Active;

        TransactionManager->RegisterTransaction(this);

        PingInvoker = New<TPeriodicInvoker>(
            FromMethod(
                &TTransactionManager::PingTransaction,
                transactionManager, 
                Id),
            transactionManager->Config.PingPeriod);
        PingInvoker->Start();
    }

    ~TTransaction()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TransactionManager->UnregisterTransaction(Id);
    }

    TTransactionId GetId() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Id;
    }

    void Commit() 
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        SetFinished();

        LOG_INFO("Committing transaction (TransactionId: %s)", ~Id.ToString());
        
        auto req = Proxy.CommitTransaction();
        req->SetTransactionId(Id.ToProto());
        auto rsp = req->Invoke(TransactionManager->Config.RpcTimeout)->Get();

        if (!rsp->IsOK()) {
            OnAborted_.Fire();

            // TODO: exception type
            LOG_ERROR_AND_THROW(yexception(), "Error committing transaction (TransactionId: %s, Error: %s)",
                ~Id.ToString(),
                ~rsp->GetError().ToString());
        }

        LOG_DEBUG("Transaction committed (TransactionId: %s)", ~Id.ToString());

        OnCommitted_.Fire();
    }

    void Abort()
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        SetFinished();

        LOG_INFO("Aborting transaction (TransactionId: %s)", ~Id.ToString());

        auto req = Proxy.AbortTransaction();
        req->Invoke(TransactionManager->Config.RpcTimeout);

        OnAborted_.Fire();
    }

    void AsyncAbort()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock);
        if (State != EState::Finished) {
            State = EState::Finished;
            guard.Release();

            OnAborted_.Fire();
        }
    }

private:
    void SetFinished()
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (State == EState::Finished) {
            ythrow yexception() << Sprintf("Transaction is already finished (TransactionId: %s)",
                ~Id.ToString());
        }

        YASSERT(State == EState::Active);

        State == EState::Finished;
        PingInvoker->Stop();
    }

    DECLARE_ENUM(EState,
        (Init)
        (Active)
        (Finished)
    );

    TTransactionManager::TPtr TransactionManager;
    TProxy Proxy;
    EState State;

    //! Protects state transitions.
    TSpinLock SpinLock;

    TPeriodicInvoker::TPtr PingInvoker;
    TTransactionId Id;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    const TConfig& config, 
    NRpc::IChannel::TPtr channel)
    : Config(config)
    , Channel(channel)
{
    YASSERT(~channel != NULL);
}

ITransaction::TPtr TTransactionManager::StartTransaction()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return New<TTransaction>(Channel, this);
}

void TTransactionManager::PingTransaction(const TTransactionId& id)
{
    // Check that the transaction is still alive.
    {
        TGuard<TSpinLock> guard(SpinLock);
        auto it = TransactionMap.find(id);
        if (it == TransactionMap.end())
            return;
    }

    TProxy proxy(Channel);
    LOG_DEBUG("Renewing transaction lease (TransactionId: %s)", ~id.ToString());

    auto req = proxy.RenewTransactionLease();
    req->SetTransactionId(id.ToProto());

    req->Invoke(Config.RpcTimeout)->Subscribe(FromMethod(
        &TTransactionManager::OnPingResponse,
        TPtr(this), 
        id));
}

void TTransactionManager::RegisterTransaction(TTransaction::TPtr transaction)
{
    TGuard<TSpinLock> guard(SpinLock);
    YVERIFY(TransactionMap.insert(MakePair(transaction->GetId(), ~transaction)).Second());
}

void TTransactionManager::UnregisterTransaction(const TTransactionId& id)
{
    TGuard<TSpinLock> guard(SpinLock);
    TransactionMap.erase(id);
}

void TTransactionManager::OnPingResponse(
    TProxy::TRspRenewTransactionLease::TPtr rsp,
    const TTransactionId& id)
{
    TTransaction::TPtr transaction;
    {
        TGuard<TSpinLock> guard(SpinLock);
        auto it = TransactionMap.find(id);
        if (it != TransactionMap.end()) {
            transaction = TTransaction::DangerousGetPtr(it->Second());
        }
    }
    
    if (~transaction == NULL) {
        LOG_DEBUG("Renewed lease for a non-existing transaction (TransactionId: %s)", 
            ~id.ToString());
        return;
    }

    if (!rsp->IsOK()) {
        LOG_WARNING("Failed to renew transaction lease (TransactionId: %s)", 
            ~id.ToString());
        transaction->AsyncAbort();
        return;
    }

    LOG_DEBUG("Transaction lease renewed (TransactionId: %s)", 
        ~id.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT