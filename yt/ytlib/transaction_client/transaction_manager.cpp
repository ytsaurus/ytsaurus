#include "stdafx.h"
#include "transaction_manager.h"

#include <ytlib/transaction_server/transaction_service_proxy.h>
#include <ytlib/actions/signal.h>
#include <ytlib/misc/assert.h>
#include <ytlib/misc/property.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/periodic_invoker.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TransactionClientLogger;

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
    {
        VERIFY_THREAD_AFFINITY(ClientThread);
        YASSERT(cellChannel);
        YASSERT(transactionManager);

        Proxy.SetTimeout(TransactionManager->Config->MasterRpcTimeout);

        auto req = Proxy.StartTransaction();

        LOG_INFO("Starting transaction");
        auto rsp = req->Invoke()->Get();
        if (!rsp->IsOK()) {
            // No ping tasks are running, so no need to lock here.
            State = EState::Aborted;
            LOG_ERROR_AND_THROW(yexception(), "Error starting transaction\n%s",  ~rsp->GetError().ToString());
        }
        Id = TTransactionId::FromProto(rsp->transaction_id());
        State = EState::Active;

        LOG_INFO("Transaction started (TransactionId: %s)", ~Id.ToString());

        TransactionManager->RegisterTransaction(this);

        PingInvoker = New<TPeriodicInvoker>(
            FromMethod(
                &TTransactionManager::PingTransaction,
                TransactionManager, 
                Id),
            TransactionManager->Config->PingPeriod);
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

        MakeStateTransition(EState::Committing);

        auto req = Proxy.CommitTransaction();
        req->set_transaction_id(Id.ToProto());

        LOG_INFO("Committing transaction (TransactionId: %s)", ~Id.ToString());
        auto rsp = req->Invoke()->Get();

        TGuard<TSpinLock> guard(SpinLock);
        if (!rsp->IsOK()) {
            State = EState::Aborted;
            guard.Release();
            
            DoAbort();

            LOG_ERROR_AND_THROW(yexception(), "Error committing transaction (TransactionId: %s)\n%s",
                ~Id.ToString(),
                ~rsp->GetError().ToString());
            return;
        }
        LOG_INFO("Transaction committed (TransactionId: %s)", ~Id.ToString());

        State = EState::Committed;
        PingInvoker->Stop();
    }

    void Abort()
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        MakeStateTransition(EState::Aborted);

        // Fire and forget.
        auto req = Proxy.AbortTransaction();
        req->set_transaction_id(Id.ToProto());
        req->Invoke();

        DoAbort();

        LOG_INFO("Transaction aborted by client (TransactionId: %s)",  ~Id.ToString());
    }

    void SubscribeAborted(IAction::TPtr onAborted)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        
        TGuard<TSpinLock> guard(SpinLock);
        switch (State) {
            case EState::Active:
                Aborted.Subscribe(onAborted);
                break;

            case EState::Aborted:
                guard.Release();
                onAborted->Do();
                break;

            default:
                YUNREACHABLE();
        }
    }

    void UnsubscribeAborted(IAction::TPtr onAborted)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Aborted.Unsubscribe(onAborted);
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
    DECLARE_ENUM(EState,
        (Active)
        (Aborted)
        (Committing)
        (Committed)
    );

    TTransactionManager::TPtr TransactionManager;
    TProxy Proxy;

    //! Protects state transitions.
    TSpinLock SpinLock;
    EState State;

    TPeriodicInvoker::TPtr PingInvoker;
    TTransactionId Id;

    TSignal Aborted;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);


    void MakeStateTransition(EState newState)
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (newState == State)
            return;

        switch (State) {
            case EState::Committed:
                ythrow yexception() << "Transaction is already committed";
                break;

            case EState::Aborted:
                ythrow yexception() << "Transaction is already aborted";
                break;

            case EState::Active:
                State = newState;
                break;

            default:
                YUNREACHABLE();
        }
    }

    void DoAbort()
    {
        PingInvoker->Stop();
        Aborted.Fire();
        Aborted.Clear();
    }

};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    TConfig* config, 
    NRpc::IChannel* channel)
    : Config(config)
    , Channel(channel)
{
    YASSERT(channel);
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
    proxy.SetTimeout(Config->MasterRpcTimeout);

    LOG_DEBUG("Renewing transaction lease (TransactionId: %s)", ~id.ToString());

    auto req = proxy.RenewTransactionLease();
    req->set_transaction_id(id.ToProto());

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
    
    if (!transaction)
        return;

    if (!rsp->IsOK()) {
        if (rsp->GetErrorCode() == TProxy::EErrorCode::NoSuchTransaction) {
            LOG_WARNING("Transaction has expired or was aborted (TransactionId: %s)",
                ~id.ToString());
            transaction->AsyncAbort();
        } else {
            LOG_WARNING("Error renewing transaction lease (TransactionId: %s)\n%s",
                ~id.ToString(),
                ~rsp->GetError().ToString());
        }
        return;
    }

    LOG_DEBUG("Transaction lease renewed (TransactionId: %s)", ~id.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
