#include "stdafx.h"

#include "transaction_manager.h"
#include <yt/ytlib/transaction_manager/transaction_service_rpc.h>

#include "../actions/signal.h"
#include "../misc/assert.h"
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
        NRpc::IChannel::TPtr cellChannel,
        TTransactionManager::TPtr txManager)
        : TransactionManager(txManager)
        , Proxy(new TProxy(cellChannel))
        , State(EState::Init)
        , PingInvoker(NULL)
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        YASSERT(~cellChannel != NULL);
        YASSERT(~txManager != NULL);

        LOG_DEBUG("Starting transaction.");

        auto req = Proxy->StartTransaction();
        auto rsp = req->Invoke(TransactionManager->Config.RpcTimeout)->Get();

        if (!rsp->IsOK()) {
            // No ping tasks are running, so no need to lock here
            State = EState::Done;
            LOG_ERROR("Couldn't start transaction.");

            ythrow yexception() << "Couldn't start transaction.";
        }

        Id = TGuid::FromProto(rsp->GetTransactionId());
        LOG_DEBUG("Started transaction %s", ~Id.ToString());
        State = EState::Active;

        TransactionManager->RegisterTransaction(this);

        PingInvoker = New<TPeriodicInvoker>(
            FromMethod(
                &TTransactionManager::PingTransaction,
                txManager, 
                Id),
            txManager->Config.PingPeriod);
    }

    ~TTransaction()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TransactionManager->UnregisterTransaction(Id);

        OnAsyncAbort();
    }

    TTransactionId GetId() const
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Id;
    }

    void Commit() 
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        SetDoneState();

        LOG_DEBUG("Commiting transaction %s", ~Id.ToString());
        
        auto req = Proxy->CommitTransaction();
        req->SetTransactionId(Id.ToProto());
        auto rsp = req->Invoke(TransactionManager->Config.RpcTimeout)->Get();

        if (!rsp->IsOK()) {
            LOG_ERROR("Transaction %s commit failed.", ~Id.ToString());
            OnAborted.Fire();

            ythrow yexception() << 
                Sprintf("Transaction %s commit failed.", ~Id.ToString());
        }

        LOG_DEBUG("Committed transaction %s", ~Id.ToString());
        OnCommited.Fire();
    }

    void Abort()
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        SetDoneState();
        DoAbort();
    }

    void SubscribeOnCommit(IAction::TPtr callback) 
    {
        VERIFY_THREAD_AFFINITY_ANY();
        OnCommited.Subscribe(callback);
    }

    void SubscribeOnAbort(IAction::TPtr callback)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        OnAborted.Subscribe(callback);
    }

    void OnAsyncAbort()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock);
        if (State != EState::Done) {
            State = EState::Done;
            guard.Release();

            DoAbort();
        }
    }

private:
    void DoAbort()
    {
        // Fire-and-forget rpc abort here
        auto req = Proxy->AbortTransaction();
        req->Invoke(TransactionManager->Config.RpcTimeout);

        OnAborted.Fire();
    }

    void SetDoneState()
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        TGuard<TSpinLock> guard(SpinLock);
        if (State == EState::Done) {
            ythrow yexception() << "Transaction is already finished.";
        }
        State == EState::Done;
    }

    TTransactionId Id;

    TSignal OnCommited;
    TSignal OnAborted;

    NRpc::IChannel::TPtr Channel;
    TPeriodicInvoker::TPtr PingInvoker;

    TTransactionManager::TPtr TransactionManager;

    THolder<TProxy> Proxy;

    DECLARE_ENUM(EState,
        (Init)
        (Active)
        (Done)
    );

    EState State;
    TSpinLock SpinLock;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    const TConfig& config, 
    NRpc::IChannel::TPtr channel)
    : Config(config)
    , Channel(channel)
{ }

ITransaction::TPtr TTransactionManager::StartTransaction()
{
    return New<TTransaction>(Channel, TPtr(this));
}

void TTransactionManager::PingTransaction(const TTransactionId& transactionId)
{
    TProxy proxy(Channel);
    LOG_DEBUG("Renewing lease for transaction %s", ~transactionId.ToString());

    auto req = proxy.RenewTransactionLease();
    req->SetTransactionId(transactionId.ToProto());

    req->Invoke(Config.RpcTimeout)->Subscribe(FromMethod(
        &TTransactionManager::OnPingResponse,
        TPtr(this), 
        transactionId));
}

void TTransactionManager::RegisterTransaction(TTransaction::TPtr tx)
{
    TGuard<TSpinLock> guard(SpinLock);
    YVERIFY(TransactionMap.insert(MakePair(tx->GetId(), ~tx)).Second());
}

void TTransactionManager::UnregisterTransaction(const TTransactionId& transactionId)
{
    TGuard<TSpinLock> guard(SpinLock);
    TransactionMap.erase(transactionId);
}

void TTransactionManager::OnPingResponse(
    TRspRenewTransactionLease::TPtr rsp,
    const TTransactionId& transactionId)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto txIter = TransactionMap.find(transactionId);
    if (txIter == TransactionMap.end()) {
        LOG_INFO("Renew lease response for unregistered transaction %s", 
            ~transactionId.ToString());
    } else if (!rsp->IsOK()){
        LOG_ERROR("Failed to renew lease for transaction %s", 
            ~transactionId.ToString());

        auto tx = TTransaction::DangerousGetPtr(txIter->Second());
        guard.Release();

        if (~tx != NULL) {
            tx->OnAsyncAbort();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT