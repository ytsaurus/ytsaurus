#include "stdafx.h"
#include "transaction_manager.h"

#include <ytlib/misc/assert.h>
#include <ytlib/misc/property.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/actions/signal.h>
#include <ytlib/ytree/serialize.h>

namespace NYT {
namespace NTransactionClient {

using namespace NCypress;
using namespace NTransactionServer;
using namespace NYTree;

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
        INode* manifest,
        const TTransactionId& parentId,
        TTransactionManager* owner)
        : Owner(owner)
        , Proxy(cellChannel)
        , State(EState::Active)
        , Manifest(manifest)
        , ParentId(parentId)
    {
        YASSERT(cellChannel);
        YASSERT(owner);
    }

    TTransaction(
        NRpc::IChannel* cellChannel,
        TTransactionManager* owner,
        const TTransactionId& id)
        : Owner(owner)
        , Proxy(cellChannel)
        , State(EState::Active)
        , Id(id)
        , ParentId(NullTransactionId)
    {
        YASSERT(cellChannel);
        YASSERT(owner);
    }

    void Start()
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        LOG_INFO("Starting transaction");

        auto transactionPath =
            ParentId == NullTransactionId
            ? RootTransactionPath
            : FromObjectId(ParentId);
        auto req = TTransactionYPathProxy::CreateObject(transactionPath);
        req->set_type(EObjectType::Transaction);
        if (Manifest) {
            req->set_manifest(SerializeToYson(~Manifest));
        }
        auto rsp = Proxy.Execute(~req)->Get();
        if (!rsp->IsOK()) {
            // No ping tasks are running, so no need to lock here.
            State = EState::Aborted;
            LOG_ERROR_AND_THROW(yexception(), "Error starting transaction\n%s",  ~rsp->GetError().ToString());
        }
        Id = TTransactionId::FromProto(rsp->object_id());
        State = EState::Active;
        LOG_INFO("Transaction started (TransactionId: %s)", ~Id.ToString());

        Owner->RegisterTransaction(this);

        StartPingInvoker();
    }

    void StartPingInvoker() {
        PingInvoker = New<TPeriodicInvoker>(
            Bind(
                &TTransactionManager::PingTransaction,
                Owner,
                Id),
            Owner->Config->PingPeriod);
        PingInvoker->Start();
    }

    ~TTransaction()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Owner->UnregisterTransaction(Id);
    }

    virtual TTransactionId GetId() const
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Id;
    }

    virtual void Commit() 
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        {
            TGuard<TSpinLock> guard(SpinLock);
            switch (State) {
                case EState::Committed:
                    ythrow yexception() << "Transaction is already committed";
                    break;

                case EState::Aborted:
                    ythrow yexception() << "Transaction is already aborted";
                    break;

                case EState::Active:
                    State = EState::Committed;
                    break;

                default:
                    YUNREACHABLE();
            }
        }

        LOG_INFO("Committing transaction (TransactionId: %s)", ~Id.ToString());

        auto req = TTransactionYPathProxy::Commit(FromObjectId(Id));
        auto rsp = Proxy.Execute(~req)->Get();
        if (!rsp->IsOK()) {
            // Let's pretend the transaction was aborted.
            // No sync here, should be safe.
            State = EState::Aborted;
            
            LOG_ERROR_AND_THROW(yexception(), "Error committing transaction (TransactionId: %s)\n%s",
                ~Id.ToString(),
                ~rsp->GetError().ToString());

            DoAbort();
            return;
        }

        if (PingInvoker) {
            PingInvoker->Stop();
        }

        LOG_INFO("Transaction committed (TransactionId: %s)", ~Id.ToString());
    }

    virtual void Abort()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_INFO("Transaction aborted by client (TransactionId: %s)",  ~Id.ToString());

        FireAbort();
        HandleAbort();
    }

    virtual void SubscribeAborted(const TCallback<void()>& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        
        TGuard<TSpinLock> guard(SpinLock);
        switch (State) {
            case EState::Active:
                Aborted.Subscribe(handler);
                break;

            case EState::Aborted:
                guard.Release();
                handler.Run();
                break;

            default:
                YUNREACHABLE();
        }
    }

    virtual void UnsubscribeAborted(const TCallback<void()>& handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Aborted.Unsubscribe(handler);
    }

    void HandleAbort()
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

    TTransactionId GetParentId() const
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return ParentId;
    }

private:
    DECLARE_ENUM(EState,
        (Active)
        (Aborted)
        (Committing)
        (Committed)
    );

    TTransactionManager::TPtr Owner;
    TCypressServiceProxy Proxy;

    //! Protects state transitions.
    TSpinLock SpinLock;
    EState State;

    TPeriodicInvoker::TPtr PingInvoker;
    TTransactionId Id;
    INodePtr Manifest;
    TTransactionId ParentId;

    TCallbackList<void()> Aborted;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);

    void FireAbort()
    {
        // Fire and forget.
        auto req = TTransactionYPathProxy::Abort(FromObjectId(Id));
        Proxy.Execute(~req);
    }

    void DoAbort()
    {
        if (PingInvoker) {
            PingInvoker->Stop();
        }
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
}

ITransaction::TPtr TTransactionManager::Start(
    INode* manifest,
    const TTransactionId& parentId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto transaction = New<TTransaction>(
        ~Channel,
        manifest,
        parentId,
        this);
    transaction->Start();
    return transaction;
}

ITransaction::TPtr TTransactionManager::Attach(const TTransactionId& id)
{
    // Try to find it among existing
    {
        TGuard<TSpinLock> guard(SpinLock);
        auto it = TransactionMap.find(id);
        if (it != TransactionMap.end()) {
            return it->second;
        }
    }

    auto transaction = New<TTransaction>(~Channel, this, id);
    RegisterTransaction(transaction);
    transaction->StartPingInvoker();
    return transaction;
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

    auto req = TTransactionYPathProxy::RenewLease(FromObjectId(id));
    CypressProxy
        .Execute(~req)
        ->Subscribe(Bind(
            &TTransactionManager::OnPingResponse,
            MakeStrong(this), 
            id));
}

void TTransactionManager::RegisterTransaction(TTransaction::TPtr transaction)
{
    TGuard<TSpinLock> guard(SpinLock);
    YVERIFY(TransactionMap.insert(MakePair(transaction->GetId(), ~transaction)).second);
}

void TTransactionManager::UnregisterTransaction(const TTransactionId& id)
{
    TGuard<TSpinLock> guard(SpinLock);
    TransactionMap.erase(id);
}

void TTransactionManager::OnPingResponse(
    const TTransactionId& id,
    TTransactionYPathProxy::TRspRenewLease::TPtr rsp)
{
    TTransaction::TPtr transaction;
    {
        TGuard<TSpinLock> guard(SpinLock);
        auto it = TransactionMap.find(id);
        if (it != TransactionMap.end()) {
            transaction = TTransaction::DangerousGetPtr(it->second);
        }
    }
    
    if (!transaction)
        return;

    if (!rsp->IsOK()) {
        if (rsp->GetErrorCode() == EYPathErrorCode::ResolveError) {
            LOG_WARNING("Transaction has expired or was aborted (TransactionId: %s)",
                ~id.ToString());
            transaction->HandleAbort();
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
