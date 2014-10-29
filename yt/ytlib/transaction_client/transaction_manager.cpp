#include "stdafx.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "config.h"
#include "private.h"

#include <core/misc/assert.h>
#include <core/misc/property.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/delayed_executor.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <core/rpc/helpers.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <core/ytree/attributes.h>

#include <ytlib/object_client/master_ypath_proxy.h>

namespace NYT {
namespace NTransactionClient {

using namespace NCypressClient;
using namespace NObjectClient;
using namespace NYTree;
using namespace NMetaState;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TransactionClientLogger;

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
    explicit TTransaction(TTransactionManagerPtr owner)
        : Owner_(owner)
        , AutoAbort_(false)
        , Ping_(false)
        , PingAncestors_(false)
        , Proxy_(owner->Channel)
        , State_(EState::Active)
        , Aborted_(NewPromise())
    { }

    ~TTransaction()
    {
        if (AutoAbort_) {
            if (State_ == EState::Active) {
                InvokeAbort(false);
            }

            {
                TGuard<TSpinLock> guard(Owner_->SpinLock);
                YCHECK(Owner_->AliveTransactions.erase(this) == 1);
            }
        }
    }

    TAsyncError Start(const TTransactionStartOptions& options)
    {
        AutoAbort_ = options.AutoAbort;
        Ping_ = options.Ping;
        PingAncestors_ = options.PingAncestors;
        Initialize();

        LOG_INFO("Starting transaction");

        auto req = TMasterYPathProxy::CreateObjects();
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
            SetOrGenerateMutationId(req, options.MutationId);
        }

        auto this_ = MakeStrong(this);
        return Proxy_.Execute(req).Apply(
            BIND([this, this_] (TMasterYPathProxy::TRspCreateObjectsPtr rsp) -> TError {
                if (!rsp->IsOK()) {
                    State_ = EState::Aborted;
                    return *rsp;
                }

                State_ = EState::Active;

                Id_ = FromProto<TTransactionId>(rsp->object_ids(0));
                LOG_INFO("Transaction started (TransactionId: %s, AutoAbort: %s, Ping: %s, PingAncestors: %s)",
                    ~ToString(Id_),
                    ~FormatBool(AutoAbort_),
                    ~FormatBool(Ping_),
                    ~FormatBool(PingAncestors_));

                if (Ping_) {
                    SendPing();
                }

                return TError();
            })
        );
    }

    void Attach(const TTransactionAttachOptions& options)
    {
        Id_ = options.Id;
        AutoAbort_ = options.AutoAbort;
        Ping_ = options.Ping;
        PingAncestors_ = options.PingAncestors;
        Initialize();

        LOG_INFO("Transaction attached (TransactionId: %s, AutoAbort: %s, Ping: %s, PingAncestors: %s)",
            ~ToString(Id_),
            ~FormatBool(AutoAbort_),
            ~FormatBool(Ping_),
            ~FormatBool(PingAncestors_));

        if (Ping_) {
            SendPing();
        }
    }

    virtual TTransactionId GetId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Id_;
    }

    virtual TAsyncError AsyncCommit(const TMutationId& mutationId) override
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            switch (State_) {
                case EState::Committed:
                    THROW_ERROR_EXCEPTION("Transaction is already committed");
                    break;

                case EState::Aborted:
                    THROW_ERROR_EXCEPTION("Transaction is already aborted");
                    break;

                case EState::Active:
                    State_ = EState::Committed;
                    break;

                default:
                    YUNREACHABLE();
            }
        }

        LOG_INFO("Committing transaction (TransactionId: %s)", ~ToString(Id_));

        auto req = TTransactionYPathProxy::Commit(FromObjectId(Id_));
        SetOrGenerateMutationId(req, mutationId);

        auto rsp = Proxy_.Execute(req);

        auto this_ = MakeStrong(this);
        return rsp.Apply(BIND([this, this_] (TTransactionYPathProxy::TRspCommitPtr rsp) -> TError {
            if (!rsp->IsOK()) {
                // Let's pretend the transaction was aborted.
                // No sync here, should be safe.
                State_ = EState::Aborted;
                FireAbort();
                return TError("Error committing transaction %s", ~ToString(Id_))
                    << rsp->GetError();
            }
            LOG_INFO("Transaction committed (TransactionId: %s)", ~ToString(Id_));
            return TError();
        }));
    }

    virtual void Commit(const TMutationId& mutationId) override
    {
        AsyncCommit(mutationId).Get();
    }

    virtual void Abort(bool wait, const TMutationId& mutationId) override
    {
        bool generateMutationId = wait;
        auto rspFuture = AsyncAbort(generateMutationId, mutationId);
        if (wait) {
            auto rsp = rspFuture.Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(rsp);
        }
    }

    virtual TAsyncError AsyncAbort(
        bool generateMutationId,
        const TMutationId& mutationId) override
    {
        auto this_ = MakeStrong(this);
        return InvokeAbort(generateMutationId, mutationId)
            .Apply(BIND([this, this_] (TTransactionYPathProxy::TRspAbortPtr rsp) -> TError {
                if (!rsp->IsOK()) {
                    return TError("Error aborting transaction") << *rsp;
                }
                HandleAbort();
                return TError();
            }));
    }

    virtual TAsyncError AsyncPing() override
    {
        return SendPing();
    }

    virtual void Detach() override
    {
        VERIFY_THREAD_AFFINITY(ClientThread);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            switch (State_) {
                case EState::Committed:
                    THROW_ERROR_EXCEPTION("Transaction is already committed (TransactionId: %s)", ~ToString(Id_));
                    break;

                case EState::Aborted:
                    THROW_ERROR_EXCEPTION("Transaction is already aborted (TransactionId: %s)", ~ToString(Id_));
                    break;

                case EState::Active:
                    State_ = EState::Detached;
                    break;

                case EState::Detached:
                    return;

                default:
                    YUNREACHABLE();
            }
        }

        LOG_INFO("Transaction detached (TransactionId: %s)", ~ToString(Id_));
    }

    virtual void SubscribeAborted(const TCallback<void()>& handler) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Aborted_.Subscribe(handler);
    }

    virtual void UnsubscribeAborted(const TCallback<void()>& handler) override
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

    TTransactionManagerPtr Owner_;
    bool AutoAbort_;
    bool Ping_;
    bool PingAncestors_;

    TObjectServiceProxy Proxy_;

    //! Protects state transitions.
    TSpinLock SpinLock_;
    EState State_;
    TPromise<void> Aborted_;

    TTransactionId Id_;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);


    void Initialize()
    {
        if (AutoAbort_) {
            TGuard<TSpinLock> guard(Owner_->SpinLock);
            YCHECK(Owner_->AliveTransactions.insert(this).second);

            LOG_DEBUG("Transaction registered");
        }        
    }

    void SchedulePing()
    {
        if (Ping_) {
            TDelayedExecutor::Submit(
                BIND(IgnoreResult(&TTransaction::SendPing), MakeWeak(this)),
                Owner_->Config->PingPeriod);
        }
    }

    TAsyncError SendPing()
    {
        LOG_DEBUG("Pinging transaction (TransactionId: %s)", ~ToString(Id_));

        auto req = TTransactionYPathProxy::Ping(FromObjectId(Id_));
        req->set_ping_ancestors(PingAncestors_);
        
        return Owner_->ObjectProxy.Execute(req).Apply(BIND(
            &TTransaction::OnPingResponse,
            MakeStrong(this)));
    }

    TError OnPingResponse(TTransactionYPathProxy::TRspPingPtr rsp)
    {
        if (rsp->IsOK()) {
            LOG_DEBUG("Transaction pinged (TransactionId: %s)", ~ToString(Id_));
            SchedulePing();
        } else {
            if (rsp->GetError().GetCode() == NYTree::EErrorCode::ResolveError) {
                LOG_WARNING("Transaction has expired or was aborted (TransactionId: %s)",
                    ~ToString(Id_));
                HandleAbort();
            } else {
                LOG_WARNING(*rsp, "Error pinging transaction (TransactionId: %s)",
                    ~ToString(Id_));
                SchedulePing();
            }
        }

        return *rsp;
    }


    TFuture<TTransactionYPathProxy::TRspAbortPtr> InvokeAbort(
        bool generateMutationId,
        const TMutationId& mutationId = NullMutationId)
    {
        auto req = TTransactionYPathProxy::Abort(FromObjectId(Id_));
        if (generateMutationId) {
            SetOrGenerateMutationId(req, mutationId);
        }
        return Proxy_.Execute(req);
    }

    void FireAbort()
    {
        Aborted_.Set();
    }

    void HandleAbort()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (State_ != EState::Active) {
                return;
            }
            State_ = EState::Aborted;
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

TFuture<TErrorOr<ITransactionPtr>> TTransactionManager::AsyncStart(
    const TTransactionStartOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    typedef TErrorOr<ITransactionPtr> TOutput;

    auto transaction = New<TTransaction>(this);
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

    auto transaction = New<TTransaction>(this);
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
