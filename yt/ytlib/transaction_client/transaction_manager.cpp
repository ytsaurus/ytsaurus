#include "stdafx.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "config.h"
#include "private.h"

#include <ytlib/misc/assert.h>
#include <ytlib/misc/property.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/delayed_invoker.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

namespace NYT {
namespace NTransactionClient {

using namespace NCypressClient;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TransactionClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransaction
    : public ITransaction
{
public:
    DEFINE_BYVAL_RO_PROPERTY(bool, PingAncestors)

    TTransaction(
        NRpc::IChannelPtr cellChannel,
        const TTransactionId& parentId,
        TTransactionManagerPtr owner,
        bool pingAncestors)
        : Owner(owner)
        , Proxy(cellChannel)
        , State(EState::Active)
        , AutoAbort(false)
        , ParentId(parentId)
        , PingAncestors_(pingAncestors)
        , Aborted(NewPromise<void>())
    {
        YCHECK(cellChannel);
        YCHECK(owner);
    }

    TTransaction(
        NRpc::IChannelPtr cellChannel,
        TTransactionManagerPtr owner,
        const TTransactionId& id,
        bool pingAncestors)
        : Owner(owner)
        , Proxy(cellChannel)
        , State(EState::Active)
        , ParentId(NullTransactionId)
        , Id(id)
        , Aborted(NewPromise<void>())
        , PingAncestors_(pingAncestors)
    {
        YCHECK(cellChannel);
        YCHECK(owner);
    }

    ~TTransaction()
    {
        if (AutoAbort && State == EState::Active) {
            InvokeAbort(false);
        }
    }

    void Start(IAttributeDictionary* attributes)
    {
        LOG_INFO("Starting transaction");

        auto transactionPath =
            ParentId == NullTransactionId
            ? RootTransactionPath
            : FromObjectId(ParentId);
        auto req = TTransactionYPathProxy::CreateObject(transactionPath);
        req->set_type(EObjectType::Transaction);
        if (ParentId != NullTransactionId) {
            NMetaState::GenerateRpcMutationId(req);
        }
        if (attributes) {
            ToProto(req->mutable_object_attributes(), *attributes);
        }

        auto rsp = Proxy.Execute(req).Get();
        if (!rsp->IsOK()) {
            // No ping tasks are running, so no need to lock here.
            State = EState::Aborted;
            THROW_ERROR_EXCEPTION("Error starting transaction")
                << rsp->GetError();
        }
        Id = TTransactionId::FromProto(rsp->object_id());

        State = EState::Active;
        AutoAbort = true;

        LOG_INFO("Transaction started (TransactionId: %s)", ~Id.ToString());
    }

    void Attach(bool autoAbort)
    {
        State = EState::Active;
        AutoAbort = autoAbort;

        LOG_INFO("Transaction attached (TransactionId: %s, AutoAbort: %s)",
            ~Id.ToString(),
            ~FormatBool(autoAbort));
    }

    TTransactionId GetId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Id;
    }

    void Commit() override
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        {
            TGuard<TSpinLock> guard(SpinLock);
            switch (State) {
                case EState::Committed:
                    THROW_ERROR_EXCEPTION("Transaction is already committed");
                    break;

                case EState::Aborted:
                    THROW_ERROR_EXCEPTION("Transaction is already aborted");
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
        NMetaState::GenerateRpcMutationId(req);

        auto rsp = Proxy.Execute(req).Get();
        if (!rsp->IsOK()) {
            // Let's pretend the transaction was aborted.
            // No sync here, should be safe.
            State = EState::Aborted;
            
            THROW_ERROR_EXCEPTION("Error committing transaction %s", ~Id.ToString())
                << rsp->GetError();

            FireAbort();
            return;
        }

        LOG_INFO("Transaction committed (TransactionId: %s)", ~Id.ToString());
    }

    void Abort(bool wait) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_INFO("Transaction aborted by client (TransactionId: %s)", ~Id.ToString());

        InvokeAbort(wait);
        HandleAbort();
    }

    void Detach() override
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        {
            TGuard<TSpinLock> guard(SpinLock);
            switch (State) {
                case EState::Committed:
                    THROW_ERROR_EXCEPTION("Transaction is already committed");
                    break;

                case EState::Aborted:
                    THROW_ERROR_EXCEPTION("Transaction is already aborted");
                    break;

                case EState::Active:
                    State = EState::Detached;
                    break;

                case EState::Detached:
                    return;

                default:
                    YUNREACHABLE();
            }
        }

        Owner->UnregisterTransaction(Id);
        Owner.Reset();

        LOG_INFO("Transaction detached (TransactionId: %s)", ~Id.ToString());
    }

    void SubscribeAborted(const TCallback<void()>& handler) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Aborted.Subscribe(handler);
    }

    void UnsubscribeAborted(const TCallback<void()>& handler) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YUNREACHABLE();
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

        FireAbort();
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
        (Detached)
    );

    TTransactionManagerPtr Owner;
    TObjectServiceProxy Proxy;

    //! Protects state transitions.
    TSpinLock SpinLock;
    EState State;
    bool AutoAbort;
    TTransactionId ParentId;

    TTransactionId Id;
    TPromise<void> Aborted;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);


    void InvokeAbort(bool wait)
    {
        // Fire and forget in case of no wait.
        auto req = TTransactionYPathProxy::Abort(FromObjectId(Id));
        if (wait) {
            NMetaState::GenerateRpcMutationId(req);
        }

        auto asyncRsp = Proxy.Execute(req);
        if (wait) {
            auto rsp = asyncRsp.Get();
            if (!rsp->IsOK()) {
                THROW_ERROR_EXCEPTION("Error aborting transaction")
                    << rsp->GetError();
            }
        }
    }

    void FireAbort()
    {
        Aborted.Set();
    }

};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    TTransactionManagerConfigPtr config,
    NRpc::IChannelPtr channel)
    : Config(config)
    , Channel(channel)
    , ObjectProxy(channel)
{
    YCHECK(channel);
}

ITransactionPtr TTransactionManager::Start(
    IAttributeDictionary* attributes,
    const TTransactionId& parentId,
    bool ping,
    bool pingAncestors)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto transaction = New<TTransaction>(
        Channel,
        parentId,
        this,
        pingAncestors);
    transaction->Start(attributes);

    RegisterTransaction(transaction);

    if (ping) {
        SendPing(transaction->GetId());
    }

    return transaction;
}

ITransactionPtr TTransactionManager::Attach(
    const TTransactionId& id,
    bool autoAbort,
    bool ping,
    bool pingAncestors)
{
    // Try to find it among existing
    auto transaction = FindTransaction(id);
    if (transaction) {
        return transaction;
    }

    // Not found, create a new one.
    transaction = New<TTransaction>(
        Channel,
        this,
        id,
        pingAncestors);
    transaction->Attach(autoAbort);

    RegisterTransaction(transaction);

    if (ping) {
        SendPing(transaction->GetId());
    }

    return transaction;
}

void TTransactionManager::RegisterTransaction(TTransactionPtr transaction)
{
    TGuard<TSpinLock> guard(SpinLock);
    YCHECK(TransactionMap.insert(MakePair(transaction->GetId(), transaction)).second);
    LOG_DEBUG("Registered transaction %s", ~transaction->GetId().ToString());
}

void TTransactionManager::UnregisterTransaction(const TTransactionId& id)
{
    TGuard<TSpinLock> guard(SpinLock);
    TransactionMap.erase(id);
}

TTransactionManager::TTransactionPtr TTransactionManager::FindTransaction(const TTransactionId& id)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = TransactionMap.find(id);
    if (it == TransactionMap.end()) {
        return NULL;
    }
    auto transaction = it->second.Lock();
    if (!transaction) {
        TransactionMap.erase(it);
    }
    return transaction;
}

void TTransactionManager::SchedulePing(TTransactionPtr transaction)
{
    TDelayedInvoker::Submit(
        BIND(&TThis::SendPing, MakeStrong(this), transaction->GetId()),
        Config->PingPeriod);
}

void TTransactionManager::SendPing(const TTransactionId& id)
{
    auto transaction = FindTransaction(id);
    if (!transaction) {
        return;
    }

    LOG_DEBUG("Renewing transaction lease (TransactionId: %s)", ~id.ToString());

    auto req = TTransactionYPathProxy::RenewLease(FromObjectId(id));
    req->set_renew_ancestors(transaction->GetPingAncestors());
    ObjectProxy.Execute(req).Subscribe(BIND(
        &TThis::OnPingResponse,
        MakeStrong(this),
        id));
}

void TTransactionManager::OnPingResponse(
    const TTransactionId& id,
    TTransactionYPathProxy::TRspRenewLeasePtr rsp)
{
    auto transaction = FindTransaction(id);
    if (!transaction) {
        return;
    }

    if (!rsp->IsOK()) {
        UnregisterTransaction(id);
        if (rsp->GetError().GetCode() == EYPathErrorCode::ResolveError) {
            LOG_WARNING("Transaction has expired or was aborted (TransactionId: %s)",
                ~id.ToString());
            transaction->HandleAbort();
        } else {
            LOG_WARNING(rsp->GetError(), "Error renewing transaction lease (TransactionId: %s)",
                ~id.ToString());
        }
        return;
    }

    LOG_DEBUG("Transaction lease renewed (TransactionId: %s)", ~id.ToString());

    SchedulePing(transaction);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
