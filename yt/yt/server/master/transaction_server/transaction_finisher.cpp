#include "transaction_finisher.h"

#include "private.h"

#include "config.h"
#include "transaction.h"
#include "transaction_finisher_host.h"
#include "transaction_manager.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/yt/server/lib/transaction_server/helpers.h>

namespace NYT::NTransactionServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectServer;

using TEphemeralTransactionPtr = TEphemeralObjectPtr<TTransaction>;
using TWeakTransactionPtr = TWeakObjectPtr<TTransaction>;

static constinit const auto Logger = TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

void TTransactionFinishRequestBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, AuthenticationIdentity.User);
    Persist(context, AuthenticationIdentity.UserTag);
    Persist(context, MutationId);
}

void TTransactionCommitRequest::Persist(const TPersistenceContext& context)
{
    TTransactionFinishRequestBase::Persist(context);
    NYT::Persist(context, PrerequisiteTransactionIds);
}

void TTransactionAbortRequest::Persist(const TPersistenceContext& context)
{
    TTransactionFinishRequestBase::Persist(context);
    NYT::Persist(context, Force);
}

void TTransactionExpirationRequest::Persist(const TPersistenceContext& /*context*/)
{ }

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

auto MakeFinishRequestFormatter(const TTransactionFinishRequest& request)
{
    return MakeFormatterWrapper([request] (TStringBuilderBase* builder) {
        Visit(request,
            [&] (const TTransactionCommitRequest& request) {
                builder->AppendFormat(", RequestKind: Commit, PrerequisiteTransactionIds: %v, MutationId: %v, AuthenticationIdentity: %v",
                    request.PrerequisiteTransactionIds,
                    request.MutationId,
                    request.AuthenticationIdentity);
            },
            [&] (const TTransactionAbortRequest& request) {
                builder->AppendFormat(", RequestKind: Abort, Force: %v, MutationId: %v, AtuthenticationIdentity: %v",
                    request.Force,
                    request.MutationId,
                    request.AuthenticationIdentity);
            },
            [&] (const TTransactionExpirationRequest& /*request*/) {
                builder->AppendString(", RequestKind: Expiration");
            });
    });
}

[[nodiscard]] bool CheckTransaction(
    TTransaction* transaction,
    TStringBuf actionDescription,
    auto additionalFormatter)
{
    if (!IsObjectAlive(transaction)) {
        YT_LOG_ALERT("Attempted to %v non-alive transaction (TransactionId: %v%v)",
            actionDescription,
            GetObjectId(transaction),
            additionalFormatter);
        return false;
    }
    if (transaction->GetIsCypressTransaction() && transaction->IsForeign()) {
        YT_LOG_ALERT("Attempted to %v foreign Cypress transaction (TransactionId: %v%v)",
            actionDescription,
            GetObjectId(transaction),
            additionalFormatter);
        return false;
    }

    return true;
}

void NoopFormatter(TStringBuilderBase* /*builder*/)
{ }

[[nodiscard]] bool CheckTransaction(
    TTransaction* transaction,
    TStringBuf actionDescription)
{
    return CheckTransaction(transaction, actionDescription, MakeFormatterWrapper(NoopFormatter));
}

////////////////////////////////////////////////////////////////////////////////

class TRetryQueue
{
public:
    explicit TRetryQueue(TStringBuf action)
        : Action_(action)
    { }

    struct TEnqueueResult
    {
        TInstant Deadline;
        std::optional<int> InvocationIndex;
    };

    //! If retry count is exceeded returns invocation index for #transaction.
    std::optional<TEnqueueResult> Enqueue(TTransaction* transaction)
    {
        YT_ASSERT(transaction);

        auto it = Transactions_.find(transaction);
        if (it == Transactions_.end()) {
            YT_LOG_ALERT("Attempted to enqueue non-registered transaction %v (TransactionId: %v)",
                Action_,
                GetObjectId(transaction));
            return std::nullopt;
        }

        auto& transactionInfo = it->second;
        if (transactionInfo.QueueIterator) {
            YT_LOG_ALERT("Attempted to enqueue already enqueued transaction %v (TransactionId: %v)",
                Action_,
                GetObjectId(transaction));
            return std::nullopt;
        }

        auto& backoffStrategy = transactionInfo.BackoffStrategy;
        auto next = backoffStrategy.Next();
        auto deadline = TInstant::Now() + backoffStrategy.GetBackoff();
        transactionInfo.QueueIterator = Queue_.emplace(
            deadline,
            TEphemeralTransactionPtr(transaction));
        return TEnqueueResult{
            deadline,
            next ? std::nullopt : std::optional(backoffStrategy.GetInvocationIndex()),
        };
    }

    TTransaction* Dequeue()
    {
        if (Queue_.empty()) {
            YT_LOG_ALERT("Attempted to dequeue transaction %v from empty queue",
                Action_);
            return nullptr;
        }

        auto* transaction = Queue_.begin()->second.Get();
        auto finally = Finally([&] {
            Queue_.erase(Queue_.begin());
        });

        YT_ASSERT(transaction);
        auto it = Transactions_.find(transaction);
        if (it == Transactions_.end()) {
            YT_LOG_ALERT("Transaction %v queue is broken: dequeued transaction does not registered (TransactionId: %v)",
                Action_,
                GetObjectId(transaction));
            return nullptr;
        }
        auto& queueIterator = it->second.QueueIterator;
        if (!queueIterator) {
            YT_LOG_ALERT("Transaction %v queue is broken: dequeued transaction has not registered queue iterator (TransactionId: %v)",
                Action_,
                GetObjectId(transaction));
            return nullptr;
        }
        if (*queueIterator != Queue_.begin()) {
            YT_LOG_ALERT("Transaction %v queue is broken: dequeued transaction has invalid queue iterator (TransactionId: %v)",
                Action_,
                GetObjectId(transaction));
            return nullptr;
        }

        queueIterator.reset();
        return transaction;
    }

    std::optional<TInstant> CanDequeue() const
    {
        if (!Queue_.empty() && Queue_.begin()->first < TInstant::Now()) {
            return Queue_.begin()->first;
        }
        return std::nullopt;
    }

    void Add(TTransaction* transaction, const TExponentialBackoffOptions& retryOptions)
    {
        YT_ASSERT(transaction);

        if (!Transactions_.emplace(
            TEphemeralTransactionPtr(transaction),
            TTransactionInfo{.BackoffStrategy = TBackoffStrategy(retryOptions)}).second)
        {
            YT_LOG_ALERT("Transaction %v registered twice (TransactionId: %v)",
                Action_,
                GetObjectId(transaction));
        }
    }

    void Remove(TTransaction* transaction)
    {
        YT_ASSERT(transaction);

        auto it = Transactions_.find(transaction);
        if (it == Transactions_.end()) {
            return;
        }

        if (auto queueIt = it->second.QueueIterator) {
            Queue_.erase(*queueIt);
        }
        Transactions_.erase(it);
    }

    bool Contains(TTransaction* transaction) const
    {
        YT_ASSERT(transaction);

        return Transactions_.contains(transaction);
    }

    int Size() const
    {
        return std::ssize(Transactions_);
    }

    void Clear()
    {
        Transactions_.clear();
        Queue_.clear();
    }

private:
    const TStringBuf Action_;

    using TQueue = std::multimap<TInstant, TEphemeralTransactionPtr>;
    using TQueueIterator = typename TQueue::iterator;

    struct TTransactionInfo
    {
        TBackoffStrategy BackoffStrategy;
        std::optional<TQueueIterator> QueueIterator;
    };
    THashMap<TEphemeralTransactionPtr, TTransactionInfo> Transactions_;
    TQueue Queue_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TTransactionFinisher
    : public ITransactionFinisher
    , public TMasterAutomatonPart
{
public:
    explicit TTransactionFinisher(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::TransactionFinisher)
        , LeasesRevocationQueue_("lease revocation")
        , FinishQueue_("finish")
    {
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TransactionFinisher",
            BIND_NO_PROPAGATE(&TTransactionFinisher::Save, Unretained(this)));
        RegisterLoader(
            "TransactionFinisher",
            BIND_NO_PROPAGATE(&TTransactionFinisher::Load, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(BIND_NO_PROPAGATE(
            &TTransactionFinisher::OnTransactionFinished,
            MakeWeak(this),
            /*abort*/ false));
        transactionManager->SubscribeTransactionAborted(BIND_NO_PROPAGATE(
            &TTransactionFinisher::OnTransactionFinished,
            MakeWeak(this),
            /*abort*/ true));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(
            &TTransactionFinisher::OnDynamicConfigChanged,
            MakeWeak(this)));
    }

    void BeginRequest(
        const NRpc::IServiceContextPtr& context,
        TTransaction* transaction) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (!CheckTransaction(transaction, "begin finish request for", MakeFormatterWrapper([&] (TStringBuilderBase* builder) {
                builder->AppendFormat(", RequestId: %v, Method: %v",
                    context->GetRequestId(),
                    context->GetMethod());
            })) ||
            transaction->GetPersistentState() != ETransactionState::Active ||
            FinishQueue_.Contains(transaction))
        {
            return;
        }

        auto& activeRequests = ActiveRequests_[transaction];
        activeRequests.insert(context->GetRequestId());
        auto requestCount = std::ssize(activeRequests);

        context->SubscribeCanceled(BIND([
            this, this_ = MakeStrong(this), transaction = TEphemeralTransactionPtr(transaction), requestId = context->GetRequestId()
        ] (const TError& error) mutable {
            OnActiveRequestFinished(requestId, std::move(transaction), error);
        }).Via(EpochAutomatonInvoker_));
        context->SubscribeReplied(BIND([
            this, this_ = MakeStrong(this), transaction = TEphemeralTransactionPtr(transaction), requestId = context->GetRequestId()
        ] () mutable {
            OnActiveRequestFinished(requestId, std::move(transaction), {});
        }).Via(EpochAutomatonInvoker_));

        YT_LOG_DEBUG("Active transaction finish request registered (RequestId: %v, TransactionId: %v, Method: %v, ActiveRequestCount: %v)",
            context->GetRequestId(),
            transaction->GetId(),
            context->GetMethod(),
            requestCount);
    }

    void PersistRequest(
        TTransaction* transaction,
        const TTransactionFinishRequest& request,
        bool update) override
    {
        YT_VERIFY(HasMutationContext());

        if (!CheckTransaction(transaction, "persist finish request for", MakeFinishRequestFormatter(request))) {
            return;
        }

        auto leasesState = transaction->GetTransactionLeasesState();
        if (leasesState == ETransactionLeasesState::Active) {
            YT_LOG_ALERT("Attempted to persist finish request for foreign Cypress transaction before lease revocation (TransactionId: %v, LeasesState: %v%v)",
                transaction->GetId(),
                leasesState,
                MakeFinishRequestFormatter(request));
            return;
        }

        auto [it, inserted] = Requests_.emplace(TWeakTransactionPtr(transaction), request);
        if (!inserted) {
            if (!update) {
                YT_LOG_DEBUG("Transaction finish request was already persisted (TransactionId: %v, LeasesState: %v%v)",
                    transaction->GetId(),
                    leasesState,
                    MakeFinishRequestFormatter(it->second));
                return;
            }

            it->second = request;
        }

        YT_LOG_DEBUG("Transaction finish %v (TransactionId: %v, LeasesState: %v%v)",
            inserted ? "request persisted" : "persisted request updated",
            transaction->GetId(),
            leasesState,
            MakeFinishRequestFormatter(request));

        if (inserted && IsLeader()) {
            auto activeRequestCount = GetActiveRequestCount(transaction);
            if (activeRequestCount == 0) {
                ScheduleFinish(transaction);
            } else {
                YT_LOG_DEBUG("Transaction still has active request count (TransactionId: %v, ActiveRequestCount: %v)",
                    transaction->GetId(),
                    activeRequestCount);
            }
        }
    }

    void ScheduleExpiredTransactionLeaseRevocation(TTransaction* transaction) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (!CheckTransaction(transaction, "schedule expired transaction leases revocation for")) {
            return;
        }

        if (transaction->GetPersistentState() != ETransactionState::Active ||
            transaction->GetTransactionLeasesState() != ETransactionLeasesState::Active)
        {
            return;
        }

        LeasesRevocationQueue_.Add(transaction, GetDynamicConfig()->Retries);
        RevokeLeasesForExpiredTransaction(transaction);

        YT_LOG_DEBUG("Lease revocation for expired transaction scheduled (TransactionId: %v)", transaction->GetId());
    }

    TFuture<void> EndRequestAndGetFailedCommitCompletionFuture(
        NRpc::TRequestId requestId,
        TTransactionId transactionId) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->FindTransaction(transactionId);
        if (!IsObjectAlive(transaction) ||
            transaction->GetPersistentState() != ETransactionState::Active)
        {
            return MakeFuture(CreateNoSuchTransactionError(transactionId));
        }

        if (!Requests_.contains(transaction)) {
            return OKFuture;
        }

        // NB: active request has to be unregistered to allow transaction to be
        // aborted by transaction finisher.
        if (auto it = ActiveRequests_.find(transaction); it != ActiveRequests_.end()) {
            // NB: request may be not registered if lease revocation was already
            // started by someone else.
            if (it->second.erase(requestId) == 1) {
                YT_LOG_DEBUG("Active transaction finish request unregistered due to commit failure (RequestId: %v, TransactionId: %v, ActiveRequestCount: %v)",
                    requestId,
                    transactionId,
                    it->second.size());
            }
        }

        auto it = FailedCommitCompletionPromises_.find(transaction);
        if (it == FailedCommitCompletionPromises_.end()) {
            it = EmplaceOrCrash(FailedCommitCompletionPromises_, TEphemeralTransactionPtr(transaction), NewPromise<void>());
        }

        auto activeRequestCount = GetActiveRequestCount(transaction);
        YT_LOG_DEBUG("Active transaction finish request unregistered because of commit failure (RequestId: %v, TransactionId: %v, ActiveRequestCount: %v)",
            requestId,
            transactionId,
            activeRequestCount);

        if (activeRequestCount == 0) {
            // Transaction finish may be already scheduled in this case:
            // 1. Commit request was failed with retriable Sequoia error,
            //    tx finish was scheduled;
            // 2. Commit was retried by client, but error happened in commit
            //    mutation. After that
            //    EndRequestAndGetFailedCommitCompletionFuture() was called but
            //    tx finish from step (1) is still in queue.
            if (!FinishQueue_.Contains(transaction)) {
                ScheduleFinish(transaction);
            }
        }

        return it->second.ToFuture().ToUncancelable();
    }

    void OnProfiling(NProfiling::TSensorBuffer* buffer) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (IsLeader()) {
            buffer->AddGauge("/transaction_finisher/finish_queue_size", FinishQueue_.Size());
            buffer->AddGauge("/transaction_finisher/lease_revocation_queue_size", LeasesRevocationQueue_.Size());
            buffer->AddGauge("/transaction_finisher/persisted_request_count", Requests_.size());
        }
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    TPeriodicExecutorPtr Executor_;

    TRetryQueue LeasesRevocationQueue_;

    TRetryQueue FinishQueue_;

    THashMap<TEphemeralTransactionPtr, THashSet<NRpc::TRequestId>> ActiveRequests_;

    THashMap<TEphemeralTransactionPtr, TPromise<void>> FailedCommitCompletionPromises_;

    // Persistent.
    THashMap<TWeakTransactionPtr, TTransactionFinishRequest> Requests_;

    void ScheduleFinish(TTransaction* transaction)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (!CheckTransaction(transaction, "schedule finish of")) {
            return;
        }

        auto leasesState = transaction->GetTransactionLeasesState();
        if (leasesState == ETransactionLeasesState::Active) {
            YT_LOG_ALERT("Attempted to schedule transaction finish with active leases (TransactionId: %v)",
                transaction->GetId());
            return;
        }

        if (!Requests_.contains(transaction)) {
            YT_LOG_ALERT("Attempted to schedule unregistered transaction finish (TransactionId: %v)",
                transaction->GetId());
            return;
        }

        if (auto activeRequestCount = GetActiveRequestCount(transaction)) {
            YT_LOG_ALERT(
                "Attempted to schedule transaction finish until its active requests are finished "
                "(TransactionId: %v, ActiveRequestCount: %v)",
                transaction->GetId(),
                activeRequestCount);
            return;
        }

        FinishQueue_.Add(transaction, GetDynamicConfig()->Retries);

        if (transaction->GetTransactionLeasesState() == ETransactionLeasesState::Revoked) {
            EnqueueFinish(transaction);
        } else {
            YT_LOG_DEBUG("Delaying transaction finish until lease revocation (TransactionId: %v)",
                transaction->GetId());

            transaction->LeasesRevokedPromise().ToFuture().ToUncancelable().Subscribe(BIND([
                this, this_ = MakeStrong(this), transaction = TEphemeralTransactionPtr(transaction)
            ] (const TError& error) {
                if (!error.IsOK()) {
                    return;
                }

                if (IsObjectAlive(transaction) && transaction->GetPersistentState() == ETransactionState::Active) {
                    EnqueueFinish(transaction.Get());
                }
            }).Via(EpochAutomatonInvoker_));
        }
    }

    NLogging::ELogLevel GetLogLevelForTooManyRetries(const TError& error)
    {
        YT_ASSERT(!error.IsOK());

        if (GetDynamicConfig()->AlertOnTooManyRetries &&
            !NRpc::IsRetriableError(error) &&
            !error.FindMatching(NSequoiaClient::EErrorCode::SequoiaRetriableError))
        {
            return NLogging::ELogLevel::Alert;
        }

        return NLogging::ELogLevel::Warning;
    }

    void EnqueueRevocation(TTransaction* transaction, const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (!CheckTransaction(transaction, "qneueue leases revocation for")) {
            return;
        }

        if (transaction->GetPersistentState() != ETransactionState::Active) {
            YT_LOG_ALERT(
                "Attempted to enqueue leases revocation for non-active Cypress transaction "
                "(TransactionId: %v, PersistentState: %v)",
                transaction->GetId(),
                transaction->GetPersistentState());
            return;
        }

        if (transaction->GetTransactionLeasesState() != ETransactionLeasesState::Active) {
            YT_LOG_ALERT(
                "Attempted to enqueue leases revocation after leases revocation is already started "
                "(TrasactionId: %v, LeasesState: %v)",
                transaction->GetId(),
                transaction->GetTransactionLeasesState());
            return;
        }

        auto enqueueResult = LeasesRevocationQueue_.Enqueue(transaction);
        if (!enqueueResult) {
            return;
        }
        auto [deadline, invocationIndex] = *enqueueResult;

        if (invocationIndex) {
            YT_LOG_EVENT(
                Logger,
                GetLogLevelForTooManyRetries(error),
                "Too many attempts to revoke transaction leases (TransactionId: %v, RetryCount: %v)",
                transaction->GetId(),
                *invocationIndex);
        }

        YT_LOG_DEBUG("Transaction leases revocation enqueued (TransactionId: %v, Deadline: %v)",
            transaction->GetId(),
            deadline);
    }

    void EnqueueFinish(TTransaction* transaction, const TError& error = {})
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (!CheckTransaction(transaction, "enqueue finish of")) {
            return;
        }

        if (transaction->GetPersistentState() != ETransactionState::Active) {
            YT_LOG_ALERT(
                "Attempted to enqueue transaction finish for non-active transaction "
                "(TransactionId: %v, PersistentState: %v)",
                transaction->GetId(),
                transaction->GetPersistentState());
            return;
        }

        if (transaction->GetTransactionLeasesState() != ETransactionLeasesState::Revoked) {
            YT_LOG_ALERT(
                "Attempted to enqueue transaction finish until its leases are revoked "
                "(TransactionId: %v, LeasesState: %v)",
                transaction->GetId(),
                transaction->GetPersistentState());
            return;
        }

        auto enqueueResult = FinishQueue_.Enqueue(transaction);
        if (!enqueueResult) {
            return;
        }
        auto [deadline, invocationIndex] = *enqueueResult;

        if (invocationIndex) {
            YT_LOG_EVENT(
                Logger,
                GetLogLevelForTooManyRetries(error),
                "Too many attempts to finish transaction (TransactionId: %v, RetryCount: %v)",
                transaction->GetId(),
                *invocationIndex);
        }

        YT_LOG_DEBUG("Transaction finish enqueued (TransactionId: %v, Deadline: %v)",
            transaction->GetId(),
            deadline);
    }

    void OnActiveRequestFinished(NRpc::TRequestId requestId, TEphemeralTransactionPtr transaction, const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());
        YT_ASSERT(transaction);

        if (!IsObjectAlive(transaction) || transaction->GetPersistentState() != ETransactionState::Active) {
            YT_LOG_DEBUG(
                error,
                "Active transaction finish request unregistered; transaction is neither alive nor active (RequestId: %v, TransactionId: %v)",
                requestId,
                GetObjectId(transaction));
            YT_ASSERT(!ActiveRequests_.contains(transaction));
            YT_ASSERT(!FinishQueue_.Contains(transaction.Get()));
            YT_ASSERT(!Requests_.contains(transaction.Get()));
            return;
        }

        auto it = ActiveRequests_.find(transaction);
        if (it == ActiveRequests_.end()) {
            YT_LOG_ALERT("No active request count found for active transaction (TransactionId: %v)",
                transaction->GetId());
            return;
        }

        auto& activeRequests = it->second;

        if (activeRequests.erase(requestId) != 1) {
            // Request is already unregistered (e.g. because of commit failure).
            return;
        }

        YT_LOG_DEBUG("Active transaction finish request unregistered (RequestId: %v, TransactionId: %v, ActiveRequestCount: %v)",
            requestId,
            transaction->GetId(),
            activeRequests.size());

        if (!activeRequests.empty()) {
            return;
        }

        if (Requests_.contains(transaction.Get())) {
            ScheduleFinish(transaction.Get());
        }
    }

    void OnTransactionFinished(bool abort, TTransaction* transaction)
    {
        YT_VERIFY(HasMutationContext());

        Requests_.erase(transaction);

        if (IsLeader()) {
            ActiveRequests_.erase(transaction);
            FinishQueue_.Remove(transaction);
            LeasesRevocationQueue_.Remove(transaction);

            auto it = FailedCommitCompletionPromises_.find(transaction);
            if (it != FailedCommitCompletionPromises_.end()) {
                it->second.TrySet(abort ? TError{} : CreateNoSuchTransactionError(transaction->GetId()));
                FailedCommitCompletionPromises_.erase(it);
            }
        }
    }

    void OnLeasesRevocationFinished(TEphemeralTransactionPtr transaction, const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (error.IsOK() &&
            IsObjectAlive(transaction) &&
            transaction->GetTransactionLeasesState() == ETransactionLeasesState::Revoked &&
            LeasesRevocationQueue_.Contains(transaction.Get()))
        {
            LeasesRevocationQueue_.Remove(transaction.Get());
            return;
        }

        if (!IsObjectAlive(transaction) ||
            transaction->GetPersistentState() != ETransactionState::Active ||
            transaction->GetTransactionLeasesState() != ETransactionLeasesState::Active)
        {
            return;
        }

        if (!LeasesRevocationQueue_.Contains(transaction.Get())) {
            return;
        }

        if (error.IsOK()) {
            return;
        }

        YT_LOG_DEBUG(error, "Retrying transaction leases revocation (TransactionId: %v)", transaction->GetId());
        EnqueueRevocation(transaction.Get(), error);
    }

    void RevokeLeasesForExpiredTransaction(TTransaction* transaction)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        NTracing::TTraceContextGuard traceGuard(NTracing::GetOrCreateTraceContext("TransactionLeasesRevocation"));

        auto* transactionFinisherHost = GetTransactionFinisherHost();
        transactionFinisherHost->RevokeLeasesForExpiredTransaction(transaction)
            .Subscribe(BIND([
                this, this_ = MakeStrong(this), transaction = TEphemeralTransactionPtr(transaction)
            ] (const TError& error) mutable {
                OnLeasesRevocationFinished(std::move(transaction), error);
            }).Via(EpochAutomatonInvoker_));
    }

    void OnScanRevocation(TTransaction* transaction)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (!IsObjectAlive(transaction) ||
            transaction->GetPersistentState() != ETransactionState::Active ||
            transaction->GetTransactionLeasesState() != ETransactionLeasesState::Active)
        {
            YT_LOG_DEBUG(
                "Transaction leases revocation is not needed anymore (TransactionId: %v, TransactionState: %v, LeasesState: %v)",
                transaction->GetId(),
                transaction->GetPersistentState(),
                transaction->GetTransactionLeasesState());
            LeasesRevocationQueue_.Remove(transaction);
        }

        RevokeLeasesForExpiredTransaction(transaction);
    }

    void OnScanFinish(TTransaction* transaction)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        NTracing::TTraceContextGuard traceGuard(NTracing::GetOrCreateTraceContext("TransactionFinish"));

        if (!CheckTransaction(transaction, "dequeue finish of")) {
            return;
        }

        auto it = Requests_.find(transaction);
        if (it == Requests_.end()) {
            YT_LOG_ALERT("Transaction finish queue contains non-persisted transaction finish request (TransactionId: %v)",
                transaction->GetId());
            return;
        }
        const auto& finishRequest = it->second;

        YT_LOG_DEBUG("Trying to finish transaction without leases %v", transaction->GetId());

        auto* transactionFinisherHost = GetTransactionFinisherHost();
        Visit(finishRequest,
            [&] (const TTransactionCommitRequest& request) {
                return transactionFinisherHost->MaybeCommitTransactionWithoutLeaseRevocation(
                    transaction,
                    request.MutationId,
                    request.AuthenticationIdentity,
                    request.PrerequisiteTransactionIds);
            },
            [&] (const TTransactionAbortRequest& request) {
                return transactionFinisherHost->MaybeAbortTransactionWithoutLeaseRevocation(
                    transaction,
                    request.MutationId,
                    request.AuthenticationIdentity,
                    request.Force);
            },
            [&] (const TTransactionExpirationRequest& /*request*/) {
                return transactionFinisherHost->MaybeAbortExpiredTransactionWithoutLeaseRevocation(transaction);
            })
            .Subscribe(BIND([
                this, this_ = MakeStrong(this), transaction = TEphemeralTransactionPtr(transaction)
            ] (const TError& error) mutable {
                OnTransactionFinishFinished(std::move(transaction), error);
            }).Via(EpochAutomatonInvoker_));
    }

    void OnScan()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        if (hydraManager->GetReadOnly()) {
            return;
        }

        int scannedTransactionsCount = 0;
        const auto maxTransactionsPerScan = GetDynamicConfig()->MaxTransactionsPerScan;

        while (scannedTransactionsCount < maxTransactionsPerScan && (FinishQueue_.CanDequeue() || LeasesRevocationQueue_.CanDequeue())) {
            ++scannedTransactionsCount;

            auto finishDeadline = FinishQueue_.CanDequeue();
            auto revocationDeadline = LeasesRevocationQueue_.CanDequeue();

            if (revocationDeadline.value_or(TInstant::Max()) < finishDeadline.value_or(TInstant::Max())) {
                OnScanRevocation(LeasesRevocationQueue_.Dequeue());
            } else {
                OnScanFinish(FinishQueue_.Dequeue());
            }
        }

        YT_LOG_DEBUG("Transaction finisher finished its iteration (ScannedTransactionCount: %v)",
            scannedTransactionsCount);
    }

    void OnTransactionFinishFinished(
        TEphemeralTransactionPtr transaction,
        const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (error.IsOK()) {
            return;
        }

        if (!IsObjectAlive(transaction) ||
            transaction->GetPersistentState() != ETransactionState::Active)
        {
            return;
        }

        if (!FinishQueue_.Contains(transaction.Get())) {
            return;
        }

        YT_LOG_DEBUG(error, "Retrying transaction finish (TransactionId: %v)", transaction->GetId());
        EnqueueFinish(transaction.Get(), error);
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr oldConfig)
    {
        auto oldPeriod = oldConfig->TransactionManager->TransactionFinisher->ScanPeriod;
        auto newPeriod = GetDynamicConfig()->ScanPeriod;
        if (IsLeader() && oldPeriod != newPeriod) {
            Executor_->SetPeriod(GetDynamicConfig()->ScanPeriod);
            if (oldPeriod > newPeriod) {
                Executor_->ScheduleOutOfBand();
            }
        }
    }

    int GetActiveRequestCount(TTransaction* transaction)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto it = ActiveRequests_.find(transaction);
        if (it == ActiveRequests_.end()) {
            return 0;
        }
        return std::ssize(it->second);
    }

    ITransactionFinisherHost* GetTransactionFinisherHost()
    {
        return Bootstrap_->GetTransactionManager().Get();
    }

    const TTransactionFinisherConfigPtr& GetDynamicConfig()
    {
        return Bootstrap_->GetDynamicConfig()->TransactionManager->TransactionFinisher;
    }

    void Save(NCellMaster::TSaveContext& context)
    {
        NYT::Save(context, Requests_);
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        NYT::Load(context, Requests_);
    }

    // TMasterAutomatonPart implementation.

    void OnLeaderActive() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        for (const auto& [transaction, request] : Requests_) {
            if (!IsObjectAlive(transaction)) {
                YT_LOG_ALERT("Found persisted finish request for non-alive transaction (TransactionId: %v%v)",
                    transaction->GetId(),
                    MakeFinishRequestFormatter(request));
                continue;
            }

            YT_LOG_DEBUG("Found persisted finish request for alive transation (TransactionId: %v%v)",
                transaction->GetId(),
                MakeFinishRequestFormatter(request));

            switch (transaction->GetTransactionLeasesState()) {
                case ETransactionLeasesState::Active:
                    YT_LOG_ALERT(
                        "Unexpected transaction leases state after persisting transaction finish request "
                        "(TransactionId: %v, LeasesState: %v%v)",
                        transaction->GetId(),
                        transaction->GetTransactionLeasesState(),
                        MakeFinishRequestFormatter(request));
                    break;
                case ETransactionLeasesState::Revoking:
                case ETransactionLeasesState::Revoked:
                    ScheduleFinish(transaction.Get());
                    break;
            }
        }

        Executor_ = New<TPeriodicExecutor>(
            EpochAutomatonInvoker_,
            BIND_NO_PROPAGATE(&TTransactionFinisher::OnScan, MakeWeak(this)),
            GetDynamicConfig()->ScanPeriod);
        Executor_->Start();
    }

    void OnStopLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        ActiveRequests_.clear();
        LeasesRevocationQueue_.Clear();
        FinishQueue_.Clear();

        if (Executor_) {
            YT_UNUSED_FUTURE(Executor_->Stop());
            Executor_.Reset();
        }

        auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");
        for (auto& [transaction, finishedPromise] : FailedCommitCompletionPromises_) {
            finishedPromise.TrySet(error);
        }
        FailedCommitCompletionPromises_.clear();
    }

    void Clear() override
    {
        ActiveRequests_.clear();
        LeasesRevocationQueue_.Clear();
        FinishQueue_.Clear();
        Requests_.clear();
        FailedCommitCompletionPromises_.clear();
    }

    void CheckInvariants() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        for (const auto& [transaction, request] : Requests_) {
            YT_LOG_FATAL_UNLESS(
                IsObjectAlive(transaction),
                "Transaction finisher contains request for non-alive transaction (TransactionId: %v%v)",
                transaction->GetId(),
                MakeFinishRequestFormatter(request));

            YT_LOG_FATAL_IF(transaction->GetTransactionLeasesState() == ETransactionLeasesState::Active,
                "Finish request is persisted but transaction leases state is still active (TransactionId: %v)",
                transaction->GetId());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ITransactionFinisherPtr CreateTransactionFinisher(NCellMaster::TBootstrap* bootstrap)
{
    return New<TTransactionFinisher>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
