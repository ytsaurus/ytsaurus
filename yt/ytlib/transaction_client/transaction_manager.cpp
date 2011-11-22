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
public:
    typedef TIntrusivePtr<TTransaction> TPtr;

    TTransaction(
        NRpc::IChannel* cellChannel,
        TTransactionManager* transactionManager)
        : TransactionManager(transactionManager)
        , Proxy(cellChannel)
        , State(EState::Active)
        , ErrorMessage("")
    {
        VERIFY_THREAD_AFFINITY(ClientThread);
        YASSERT(cellChannel != NULL);
        YASSERT(transactionManager != NULL);

        LOG_INFO("Starting transaction");

        Proxy.SetTimeout(TransactionManager->Config.RpcTimeout);

        auto req = Proxy.StartTransaction();
        auto rsp = req->Invoke()->Get();

        if (!rsp->IsOK()) {
            // No ping tasks are running, so no need to lock here.
            State = EState::Aborted;
            ErrorMessage = Sprintf("Error starting transaction (Error: %s)",
                ~rsp->GetError().ToString());

            // TODO: exception type
            LOG_ERROR_AND_THROW(yexception(), ~ErrorMessage);
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

        LOG_INFO("Committing transaction (TransactionId: %s)", ~Id.ToString());

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (State != EState::Active) {
                LOG_ERROR_AND_THROW(yexception(), 
                    ~Sprintf("Unable to commit transaction, details: %s", ~ErrorMessage));
            }
            State = EState::Commiting;
        }

        auto req = Proxy.CommitTransaction();
        req->SetTransactionId(Id.ToProto());
        auto rsp = req->Invoke()->Get();

        TGuard<TSpinLock> guard(SpinLock);
        if (!rsp->IsOK()) {
            ErrorMessage = Sprintf("Error committing transaction (TransactionId: %s, Error: %s)",
                ~Id.ToString(),
                ~rsp->GetError().ToString());
            State = EState::Aborted;
            guard.Release();
            
            DoAbort();

            LOG_ERROR_AND_THROW(yexception(), ~ErrorMessage);
        } else {
            ErrorMessage = Sprintf("Transaction committed (TransactionId: %s)", ~Id.ToString());
            State = EState::Commited;
            LOG_DEBUG(~ErrorMessage);
            PingInvoker->Stop();
        }
    }

    void Abort()
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (State != EState::Active) {
                return;
            }
            State = EState::Aborted;
            ErrorMessage = Sprintf("Transaction aborted by client (TransactionId: %s)", 
                ~Id.ToString());
            LOG_INFO(~ErrorMessage);
        }

        // Fire and forget.
        auto req = Proxy.AbortTransaction();
        req->Invoke();

        DoAbort();
    }

    bool SubscribeOnAborted(IAction::TPtr onAborted)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        TGuard<TSpinLock> guard(SpinLock);
        if (State == EState::Active) {
            OnAborted.Subscribe(onAborted);
            return true;
        }
        return false;
    }

    void UnsubscribeOnAborted(IAction::TPtr onAborted)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        TGuard<TSpinLock> guard(SpinLock);
        OnAborted.Unsubscribe(onAborted);
    }

    void AsyncAbort()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (State != EState::Active) {
                return;
            }
            State = EState::Aborted;
        }

        DoAbort();
    }

private:
    void DoAbort()
    {
        PingInvoker->Stop();
        OnAborted.Fire();
        OnAborted.Clear();
    }

    DECLARE_ENUM(EState,
        (Active)
        (Aborted)
        (Commiting)
        (Commited)
    );

    TTransactionManager::TPtr TransactionManager;
    TProxy Proxy;
    EState State;

    //! Protects state transitions.
    TSpinLock SpinLock;
    Stroka ErrorMessage;

    TPeriodicInvoker::TPtr PingInvoker;
    TTransactionId Id;

    TSignal OnAborted;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    const TConfig& config, 
    NRpc::IChannel* channel)
    : Config(config)
    , Channel(channel)
{
    YASSERT(channel != NULL);
}

ITransaction::TPtr TTransactionManager::StartTransaction()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return New<TTransaction>(~Channel, this);
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

    TProxy proxy(~Channel);
    proxy.SetTimeout(Config.RpcTimeout);

    LOG_DEBUG("Renewing transaction lease (TransactionId: %s)", ~id.ToString());

    auto req = proxy.RenewTransactionLease();
    req->SetTransactionId(id.ToProto());

    req->Invoke()->Subscribe(FromMethod(
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
