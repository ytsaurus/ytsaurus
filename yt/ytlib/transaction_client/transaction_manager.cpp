#include "stdafx.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "config.h"
#include "private.h"

#include <core/misc/assert.h>
#include <core/misc/property.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/delayed_executor.h>

#include <core/rpc/helpers.h>

#include <core/ytree/attributes.h>

#include <ytlib/transaction_client/transaction_ypath.pb.h>

#include <ytlib/hydra/rpc_helpers.h>

#include <ytlib/hive/cell_directory.h>
#include <ytlib/hive/timestamp_provider.h>
#include <ytlib/hive/transaction_supervisor_service_proxy.h>

namespace NYT {
namespace NTransactionClient {

using namespace NYTree;
using namespace NHydra;
using namespace NHive;
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
    , Attributes(CreateEphemeralAttributes())
    , Type(ETransactionType::Master)
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
        return Owner_->TimestampProvider->GenerateNewTimestamp()
            .Apply(BIND(&TTransaction::OnGotStartTimestamp, MakeStrong(this), options));
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

        auto req = Proxy_.CommitTransaction();
        ToProto(req->mutable_transaction_id(), Id_);
        SetOrGenerateMutationId(req, mutationId);

        return req->Invoke().Apply(
            BIND(&TTransaction::OnTransactionCommitted, MakeStrong(this)));
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
        return InvokeAbort(generateMutationId, mutationId)
            .Apply(BIND(&TTransaction::OnTransactionAborted, MakeStrong(this)));
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
                    THROW_ERROR_EXCEPTION("Transaction is already committed");
                    break;

                case EState::Aborted:
                    THROW_ERROR_EXCEPTION("Transaction is already aborted");
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

    virtual void RegisterParticipant(const NElection::TCellGuid& cellGuid) override
    {
        // TODO(babenko): implement
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

    TTransactionSupervisorServiceProxy Proxy_;

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
        TDelayedExecutor::Submit(
            BIND(IgnoreResult(&TTransaction::SendPing), MakeWeak(this)),
            Owner_->Config->PingPeriod);
    }


    TAsyncError OnGotStartTimestamp(
        const TTransactionStartOptions& options,
        TErrorOr<TTimestamp> timestampOrError)
    {
        if (!timestampOrError.IsOK()) {
            return MakeFuture(TError(timestampOrError));
        }

        AutoAbort_ = options.AutoAbort;
        Ping_ = options.Ping;
        PingAncestors_ = options.PingAncestors;
        Initialize();

        auto startTimestamp = timestampOrError.GetValue();

        LOG_INFO("Starting transaction (StartTimestamp: %" PRId64 ")",
            startTimestamp);

        auto req = Proxy_.StartTransaction();
        req->set_start_timestamp(startTimestamp);

        auto* reqExt = req->MutableExtension(NProto::TReqStartTransactionExt::start_transaction_ext);
        if (!options.Attributes->List().empty()) {
            ToProto(reqExt->mutable_attributes(), *options.Attributes);
        }
        if (options.ParentId != NullTransactionId) {
            ToProto(reqExt->mutable_parent_transaction_id(), options.ParentId);
        }
        reqExt->set_enable_uncommitted_accounting(options.EnableUncommittedAccounting);
        reqExt->set_enable_staged_accounting(options.EnableStagedAccounting);
        if (options.Timeout) {
            reqExt->set_timeout(options.Timeout.Get().MilliSeconds());
        }

        if (options.ParentId != NullTransactionId) {
            SetOrGenerateMutationId(req, options.MutationId);
        }

        return req->Invoke().Apply(
            BIND(&TTransaction::OnTransactionStarted, MakeStrong(this)));
    }

    TError OnTransactionStarted(TTransactionSupervisorServiceProxy::TRspStartTransactionPtr rsp)
    {
        if (!rsp->IsOK()) {
            State_ = EState::Aborted;
            return rsp->GetError();
        }

        State_ = EState::Active;

        Id_ = FromProto<TTransactionId>(rsp->transaction_id());
        LOG_INFO("Transaction started (TransactionId: %s, AutoAbort: %s, Ping: %s, PingAncestors: %s)",
            ~ToString(Id_),
            ~FormatBool(AutoAbort_),
            ~FormatBool(Ping_),
            ~FormatBool(PingAncestors_));

        if (Ping_) {
            SendPing();
        }

        return TError();
    }


    TError OnTransactionCommitted(TTransactionSupervisorServiceProxy::TRspCommitTransactionPtr rsp)
    {
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
    }


    TError OnTransactionAborted(TTransactionSupervisorServiceProxy::TRspAbortTransactionPtr rsp)
    {
        if (!rsp->IsOK()) {
            return TError("Error aborting transaction") << *rsp;
        }
        HandleAbort();
        return TError();
    }


    TAsyncError SendPing()
    {
        LOG_DEBUG("Pinging transaction (TransactionId: %s)", ~ToString(Id_));

        auto req = Proxy_.PingTransaction();
        ToProto(req->mutable_transaction_id(), Id_);

        auto* reqExt = req->MutableExtension(NProto::TReqPingTransactionExt::ping_transaction_ext);
        reqExt->set_ping_ancestors(PingAncestors_);
        
        return req->Invoke().Apply(
            BIND(&TTransaction::OnPingResponse, MakeStrong(this)));
    }

    TError OnPingResponse(TTransactionSupervisorServiceProxy::TRspPingTransactionPtr rsp)
    {
        if (rsp->IsOK()) {
            LOG_DEBUG("Transaction pinged (TransactionId: %s)", ~ToString(Id_));

            if (Ping_) {
                SchedulePing();
            }
        } else {
            if (rsp->GetError().GetCode() == NYTree::EErrorCode::ResolveError) {
                LOG_WARNING("Transaction has expired or was aborted (TransactionId: %s)",
                    ~ToString(Id_));
                HandleAbort();
            } else {
                LOG_WARNING(*rsp, "Error pinging transaction (TransactionId: %s)",
                    ~ToString(Id_));
            }
        }

        return *rsp;
    }


    TFuture<TTransactionSupervisorServiceProxy::TRspAbortTransactionPtr> InvokeAbort(
        bool generateMutationId,
        const TMutationId& mutationId = NullMutationId)
    {
        auto req = Proxy_.AbortTransaction();
        ToProto(req->mutable_transaction_id(), Id_);
        if (generateMutationId) {
            SetOrGenerateMutationId(req, mutationId);
        }
        return req->Invoke();
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
    NRpc::IChannelPtr channel,
    NHive::ITimestampProviderPtr timestampProvider,
    NHive::TCellDirectoryPtr cellDirectory)
    : Config(config)
    , Channel(channel)
    , TimestampProvider(timestampProvider)
    , CellDirectory(cellDirectory)
{
    YCHECK(Config);
    YCHECK(Channel);
    YCHECK(TimestampProvider);
    YCHECK(CellDirectory);
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
    return transactionOrError.GetValue();
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
