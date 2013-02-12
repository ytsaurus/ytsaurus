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

#include <ytlib/ytree/attributes.h>

namespace NYT {
namespace NTransactionClient {

using namespace NCypressClient;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TransactionClientLogger;

////////////////////////////////////////////////////////////////////////////////

TTransactionStartOptions::TTransactionStartOptions()
    : Ping(true)
    , PingAncestors(false)
    , EnableUncommittedAccounting(true)
    , EnableStagedAccounting(true)
    , Attributes(CreateEphemeralAttributes())
{ }

////////////////////////////////////////////////////////////////////////////////

TTransactionAttachOptions::TTransactionAttachOptions(const TTransactionId& id)
    : Id(id)
    , AutoAbort(true)
    , Ping(true)
    , PingAncestors(false)
{ }

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransaction
    : public ITransaction
{
public:
    explicit TTransaction(TTransactionManagerPtr owner)
        : Owner(owner)
        , AutoAbort(false)
        , Ping(false)
        , PingAncestors(false)
        , Proxy(owner->Channel)
        , State(EState::Active)
        , Aborted(NewPromise<void>())
    {
        YCHECK(owner);
    }

    ~TTransaction()
    {
        if (AutoAbort && State == EState::Active) {
            InvokeAbort(false);
        }
    }

    void Start(const TTransactionStartOptions& options)
    {
        LOG_INFO("Starting transaction");

        AutoAbort = true;
        Ping = options.Ping;
        PingAncestors = options.PingAncestors;

        auto transactionPath =
            options.ParentId == NullTransactionId
            ? RootTransactionPath
            : FromObjectId(options.ParentId);

        auto req = TTransactionYPathProxy::CreateObject(transactionPath);
        req->set_type(EObjectType::Transaction);
        ToProto(req->mutable_object_attributes(), *options.Attributes);

        auto* reqExt = req->MutableExtension(NProto::TReqCreateTransactionExt::create_transaction);
        reqExt->set_enable_uncommitted_accounting(options.EnableUncommittedAccounting);
        reqExt->set_enable_staged_accounting(options.EnableStagedAccounting);
        if (options.Timeout) {
            reqExt->set_timeout(options.Timeout.Get().MilliSeconds());
        }

        if (options.ParentId != NullTransactionId) {
            NMetaState::GenerateRpcMutationId(req);
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

        LOG_INFO("Transaction started: %s (Ping: %s, PingAncestors: %s)",
            ~Id.ToString(),
            ~FormatBool(Ping),
            ~FormatBool(PingAncestors));

        if (Ping) {
            SendPing();
        }
    }

    void Attach(const TTransactionAttachOptions& options)
    {
        Id = options.Id;
        AutoAbort = options.AutoAbort;
        Ping = options.Ping;
        PingAncestors = options.PingAncestors;

        State = EState::Active;

        LOG_INFO("Transaction attached: %s (AutoAbort: %s, Ping: %s, PingAncestors: %s)",
            ~Id.ToString(),
            ~FormatBool(AutoAbort),
            ~FormatBool(Ping),
            ~FormatBool(PingAncestors));

        if (Ping) {
            SendPing();
        }
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

        LOG_INFO("Transaction detached: %s", ~Id.ToString());
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

private:
    DECLARE_ENUM(EState,
        (Active)
        (Aborted)
        (Committing)
        (Committed)
        (Detached)
    );

    TTransactionManagerPtr Owner;
    bool AutoAbort;
    bool Ping;
    bool PingAncestors;

    TObjectServiceProxy Proxy;

    //! Protects state transitions.
    TSpinLock SpinLock;
    EState State;
    TPromise<void> Aborted;

    TTransactionId Id;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);


    void SchedulePing()
    {
        TDelayedInvoker::Submit(
            BIND(&TTransaction::SendPing, MakeWeak(this)),
            Owner->Config->PingPeriod);
    }
    
    void SendPing()
    {
        LOG_DEBUG("Renewing transaction lease: %s", ~Id.ToString());

        auto req = TTransactionYPathProxy::RenewLease(FromObjectId(Id));
        req->set_renew_ancestors(PingAncestors);
        Owner->ObjectProxy.Execute(req).Subscribe(BIND(
            &TTransaction::OnPingResponse,
            MakeWeak(this)));
    }

    void OnPingResponse(TTransactionYPathProxy::TRspRenewLeasePtr rsp)
    {
        if (!rsp->IsOK()) {
            if (rsp->GetError().GetCode() == EYPathErrorCode::ResolveError) {
                LOG_WARNING("Transaction has expired or was aborted: %s",
                    ~Id.ToString());
                HandleAbort();
            } else {
                LOG_WARNING(*rsp, "Error renewing transaction lease: %s",
                    ~Id.ToString());
            }
            return;
        }

        LOG_DEBUG("Transaction lease renewed: %s", ~Id.ToString());

        SchedulePing();
    }


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
    YCHECK(config);
}

ITransactionPtr TTransactionManager::Start(const TTransactionStartOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto transaction = New<TTransaction>(this);
    transaction->Start(options);
    return transaction;
}

ITransactionPtr TTransactionManager::Attach(const TTransactionAttachOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto transaction = New<TTransaction>(this);
    transaction->Attach(options);
    return transaction;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
