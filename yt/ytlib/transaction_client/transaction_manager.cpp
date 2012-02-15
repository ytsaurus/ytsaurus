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

    void Start()
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        LOG_INFO("Starting transaction");

        TYPath transactionPath = (ParentId == NullTransactionId)
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

        PingInvoker = New<TPeriodicInvoker>(
            FromMethod(
                &TTransactionManager::PingTransaction,
                Owner, 
                Id),
            Owner->Config->PingPeriod);
        PingInvoker->Start();
    }

    ~TTransaction()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (State == EState::Active) {
                FireAbort();
                State = EState::Aborted;
            }
        }

        Owner->UnregisterTransaction(Id);
    }

    TTransactionId GetId() const
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Id;
    }

    void Commit() 
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

        PingInvoker->Stop();

        LOG_INFO("Transaction committed (TransactionId: %s)", ~Id.ToString());
    }

    void Abort()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_INFO("Transaction aborted by client (TransactionId: %s)",  ~Id.ToString());

        FireAbort();
        HandleAbort();
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
    TNodePtr Manifest;
    TTransactionId ParentId;

    TSignal Aborted;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);

    void FireAbort()
    {
        // Fire and forget.
        auto req = TTransactionYPathProxy::Abort(FromObjectId(Id));
        Proxy.Execute(~req);
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
        ->Subscribe(FromMethod(
            &TTransactionManager::OnPingResponse,
            TPtr(this), 
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
    TTransactionYPathProxy::TRspRenewLease::TPtr rsp,
    const TTransactionId& id)
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
