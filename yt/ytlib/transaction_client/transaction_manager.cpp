#include "stdafx.h"
#include "transaction_manager.h"

#include <ytlib/misc/assert.h>
#include <ytlib/misc/property.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/actions/signal.h>

namespace NYT {
namespace NTransactionClient {

using namespace NCypress;
using namespace NTransactionServer;

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
        YASSERT(cellChannel);
        YASSERT(transactionManager);
        VERIFY_THREAD_AFFINITY(ClientThread);

        Proxy.SetTimeout(TransactionManager->Config->MasterRpcTimeout);

        LOG_INFO("Starting transaction");
        auto req = TCypressYPathProxy::Create();
        req->set_type(EObjectType::Transaction);
        auto rsp = Proxy.Execute(~req)->Get();
        if (!rsp->IsOK()) {
            // No ping tasks are running, so no need to lock here.
            State = EState::Aborted;
            LOG_ERROR_AND_THROW(yexception(), "Error starting transaction\n%s",  ~rsp->GetError().ToString());
        }
        Id = TTransactionId::FromProto(rsp->id());
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

        LOG_INFO("Committing transaction (TransactionId: %s)", ~Id.ToString());

        auto req = TTransactionYPathProxy::Commit();
        auto rsp = Proxy.Execute(~req, Id)->Get();

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
        auto req = TTransactionYPathProxy::Abort();
        Proxy.Execute(~req, Id);

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
    TCypressServiceProxy Proxy;

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
    , CypressProxy(channel)
{
    YASSERT(channel);

    CypressProxy.SetTimeout(Config->MasterRpcTimeout);
}

ITransaction::TPtr TTransactionManager::Start()
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

    LOG_DEBUG("Renewing transaction lease (TransactionId: %s)", ~id.ToString());

    auto req = TTransactionYPathProxy::RenewLease();
    CypressProxy
        .Execute(~req, id)
        ->Subscribe(FromMethod(
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
    TTransactionYPathProxy::TRspRenewLease::TPtr rsp,
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
        if (rsp->GetErrorCode() == NYTree::EYPathErrorCode::ResolveError) {
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
