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

#include <ytlib/security_client/rpc_helpers.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/ytree/attributes.h>

#include <ytlib/object_client/master_ypath_proxy.h>

namespace NYT {
namespace NTransactionClient {

using namespace NCypressClient;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TransactionClientLogger;

////////////////////////////////////////////////////////////////////////////////

TTransactionStartOptions::TTransactionStartOptions()
    : AutoAbort(true)
    , Ping(true)
    , PingAncestors(false)
    , EnableUncommittedAccounting(true)
    , EnableStagedAccounting(true)
    , RegisterInManager(true)
    , Attributes(CreateEphemeralAttributes())
{ }

////////////////////////////////////////////////////////////////////////////////

TTransactionAttachOptions::TTransactionAttachOptions(const TTransactionId& id)
    : Id(id)
    , AutoAbort(true)
    , Ping(true)
    , PingAncestors(false)
    , RegisterInManager(false)
{ }

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransaction
    : public ITransaction
{
public:
    TTransaction(TTransactionManagerPtr owner, bool autoAbort)
        : Owner(owner)
        , AutoAbort(autoAbort)
        , Ping(false)
        , PingAncestors(false)
        , Proxy(owner->Channel)
        , State(EState::Active)
        , Aborted(NewPromise())
    {
        YCHECK(owner);

        if (AutoAbort) {
            TGuard<TSpinLock> guard(Owner->SpinLock);
            YCHECK(Owner->AliveTransactions.insert(this).second);

            LOG_DEBUG("Transaction registered in manager");
        }
    }

    ~TTransaction()
    {
        if (AutoAbort) {
            if (State == EState::Active) {
                InvokeAbort(false);
            }

            {
                TGuard<TSpinLock> guard(Owner->SpinLock);
                YCHECK(Owner->AliveTransactions.erase(this) == 1);
            }
        }
    }

    TAsyncError Start(const TTransactionStartOptions& options)
    {
        LOG_INFO("Starting transaction");

        Ping = options.Ping;
        PingAncestors = options.PingAncestors;

        auto req = TMasterYPathProxy::CreateObject();
        req->set_type(EObjectType::Transaction);
        ToProto(req->mutable_object_attributes(), *options.Attributes);
        if (options.ParentId != NullTransactionId) {
            ToProto(req->mutable_transaction_id(), options.ParentId);
        }

        auto* reqExt = req->MutableExtension(NProto::TReqCreateTransactionExt::create_transaction_ext);
        reqExt->set_enable_uncommitted_accounting(options.EnableUncommittedAccounting);
        reqExt->set_enable_staged_accounting(options.EnableStagedAccounting);

        if (options.Timeout) {
            reqExt->set_timeout(options.Timeout.Get().MilliSeconds());
        }

        if (options.ParentId != NullTransactionId) {
            NMetaState::SetOrGenerateMutationId(req, options.MutationId);
        }

        auto this_ = MakeStrong(this);
        return Proxy.Execute(req).Apply(
            BIND([this, this_]
                 (TMasterYPathProxy::TRspCreateObjectPtr rsp) -> TError
            {
                if (!rsp->IsOK()) {
                    State = EState::Aborted;
                    return *rsp;
                }

                State = EState::Active;

                Id = FromProto<TTransactionId>(rsp->object_id());
                LOG_INFO("Transaction started: %s (Ping: %s, PingAncestors: %s)",
                    ~ToString(Id),
                    ~FormatBool(Ping),
                    ~FormatBool(PingAncestors));

                if (Ping) {
                    SendPing();
                }

                return TError();
            })
        );
    }

    void Attach(const TTransactionAttachOptions& options)
    {
        Id = options.Id;
        AutoAbort = options.AutoAbort;
        Ping = options.Ping;
        PingAncestors = options.PingAncestors;

        State = EState::Active;

        LOG_INFO("Transaction attached: %s (AutoAbort: %s, Ping: %s, PingAncestors: %s)",
            ~ToString(Id),
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

    TAsyncError AsyncCommit(const NMetaState::TMutationId& mutationId) override
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

        LOG_INFO("Committing transaction (TransactionId: %s)", ~ToString(Id));

        auto req = TTransactionYPathProxy::Commit(FromObjectId(Id));
        NMetaState::SetOrGenerateMutationId(req, mutationId);

        auto rsp = Proxy.Execute(req);

        auto this_ = MakeStrong(this);
        return rsp.Apply(BIND([this, this_] (TTransactionYPathProxy::TRspCommitPtr rsp) {
            if (!rsp->IsOK()) {
                // Let's pretend the transaction was aborted.
                // No sync here, should be safe.
                State = EState::Aborted;
                FireAbort();
                return TError("Error committing transaction %s", ~ToString(Id)) << rsp->GetError();
            }
            LOG_INFO("Transaction committed (TransactionId: %s)", ~ToString(Id));
            return TError();
        }));
    }

    void Commit(const NMetaState::TMutationId& mutationId) override
    {
        AsyncCommit(mutationId).Get();
    }

    void Abort(bool wait, const NMetaState::TMutationId& mutationId) override
    {
        bool generateMutationId = wait;
        auto rspFuture = AsyncAbort(generateMutationId, mutationId);
        if (wait) {
            auto rsp = rspFuture.Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(rsp);
        }
    }

    virtual TAsyncError AsyncAbort(bool generateMutationId, const NMetaState::TMutationId& mutationId) override
    {
        auto this_ = MakeStrong(this);
        return InvokeAbort(generateMutationId, mutationId).Apply(BIND([this, this_] (TTransactionYPathProxy::TRspAbortPtr rsp) {
            if (!rsp->IsOK()) {
                return TError("Error aborting transaction") << rsp->GetError();
            }
            HandleAbort();
            return TError();
        }));
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

        LOG_INFO("Transaction detached: %s", ~ToString(Id));
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
    bool RegisterInManager;

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
        LOG_DEBUG("Pinging transaction: %s", ~ToString(Id));

        auto req = TTransactionYPathProxy::Ping(FromObjectId(Id));
        req->set_ping_ancestors(PingAncestors);
        Owner->ObjectProxy.Execute(req).Subscribe(BIND(
            &TTransaction::OnPingResponse,
            MakeWeak(this)));
    }

    void OnPingResponse(TTransactionYPathProxy::TRspPingPtr rsp)
    {
        if (!rsp->IsOK()) {
            if (rsp->GetError().GetCode() == NYTree::EErrorCode::ResolveError) {
                LOG_WARNING("Transaction has expired or was aborted: %s",
                    ~ToString(Id));
                HandleAbort();
            } else {
                LOG_WARNING(*rsp, "Error pinging transaction: %s",
                    ~ToString(Id));
            }
            return;
        }

        LOG_DEBUG("Transaction pinged: %s", ~ToString(Id));

        SchedulePing();
    }


    TFuture<TTransactionYPathProxy::TRspAbortPtr> InvokeAbort(bool generateMutationId, const NMetaState::TMutationId& mutationId = NMetaState::NullMutationId)
    {
        auto req = TTransactionYPathProxy::Abort(FromObjectId(Id));
        if (generateMutationId) {
            NMetaState::SetOrGenerateMutationId(req, mutationId);
        }
        return Proxy.Execute(req);
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

TFuture<TValueOrError<ITransactionPtr>> TTransactionManager::AsyncStart(
    const TTransactionStartOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    typedef TValueOrError<ITransactionPtr> TOutput;

    auto transaction = New<TTransaction>(this, options.AutoAbort);
    return transaction->Start(options).Apply(
        BIND([=] (TError error) -> TOutput {
            if (error.IsOK()) {
                return TOutput(transaction);
            }
            return TOutput(error);
        })
    );
}

ITransactionPtr TTransactionManager::Start(const TTransactionStartOptions& options)
{
    auto transactionOrError = AsyncStart(options).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError, "Error starting transaction");
    return transactionOrError.Value();
}

ITransactionPtr TTransactionManager::Attach(const TTransactionAttachOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto transaction = New<TTransaction>(this, options.AutoAbort);
    transaction->Attach(options);
    return transaction;
}

void TTransactionManager::AsyncAbortAll()
{
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<ITransactionPtr> transactions;
    {
        TGuard<TSpinLock> guard(SpinLock);
        FOREACH (auto* rawTransaction, AliveTransactions) {
            auto transaction = TRefCounted::DangerousGetPtr(rawTransaction);
            if (transaction) {
                transactions.push_back(transaction);
            }
        }
    }

    FOREACH (const auto& transaction, transactions) {
        transaction->AsyncAbort(false);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
