#include "strong_ordering_manager.h"

#include <yt/yt/server/lib/hydra/helpers.h>
#include <yt/yt/server/lib/hydra/mutation_context.h>

namespace NYT::NTransactionSupervisor {

using namespace NApi;
using namespace NHydra;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TStrongOrderingManager::TStrongOrderingManager(NLogging::TLogger logger)
    : Logger(std::move(logger))
{ }

bool TStrongOrderingManager::IsUniqueClockSource(TClusterTag clockClusterTag)
{
    if (ClockClusterTag_ == InvalidCellTag) {
        ClockClusterTag_ = clockClusterTag;
    }

    return ClockClusterTag_ == clockClusterTag;
}

TClusterTag TStrongOrderingManager::GetClockSourceClusterTag() const
{
    return ClockClusterTag_;
}

TFuture<void> TStrongOrderingManager::WaitUntilPreparedCommitsFinish()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return PreparedCommitsFinished_.Transform([&] (TPromise<void>& promise) {
        auto preparedCommitCount = PreparedCommitCount_.load(std::memory_order::relaxed);
        if (preparedCommitCount == 0) {
            YT_LOG_DEBUG("No prepared strongly ordered commits");
            return OKFuture;
        }

        if (!promise) {
            promise = NewPromise<void>();
        }

        YT_LOG_DEBUG("Waiting for currently prepared strongly ordered commits to be finished (PreparedCommitCount: %v)",
            preparedCommitCount);
        return promise.ToFuture();
    });
}

void TStrongOrderingManager::OnCommitPrepare(
    TTransactionId transactionId,
    TTimestamp prepareTimestamp,
    bool isCoordinator,
    std::vector<std::string> strongOrderingTags)
{
    RegisterTransaction(transactionId, prepareTimestamp, isCoordinator, std::move(strongOrderingTags));
}

TCommitInfos TStrongOrderingManager::OnCommitReadyToCommit(
    TTransactionId transactionId,
    TTimestamp commitTimestamp,
    TClusterTag commitTimestampClusterTag)
{
    return RecordCommitTimestamp(
        transactionId,
        commitTimestamp,
        commitTimestampClusterTag,
        /*isCoordinator*/ false);
}

TCommitInfos TStrongOrderingManager::OnCommitCommit(
    TTransactionId transactionId,
    TTimestamp commitTimestamp,
    TClusterTag commitTimestampClusterTag,
    bool isCoordinator,
    ECommitState commitState,
    std::optional<TTimestamp> prepareTimestampToValidate)
{
    TCommitInfos transactionsToCommit;
    if (isCoordinator) {
        transactionsToCommit = RecordCommitTimestamp(
            transactionId,
            commitTimestamp,
            commitTimestampClusterTag,
            /*isCoordinator*/ true,
            prepareTimestampToValidate);
    }

    auto moreTransactionsToCommit = PermitCommitFlush(
        transactionId,
        commitTimestamp,
        commitTimestampClusterTag,
        isCoordinator,
        commitState);

    if (transactionsToCommit.empty()) {
        return moreTransactionsToCommit;
    }

    transactionsToCommit.insert(
        transactionsToCommit.end(),
        moreTransactionsToCommit.begin(),
        moreTransactionsToCommit.end());
    return transactionsToCommit;
}

TCommitInfos TStrongOrderingManager::OnCommitAbort(
    TTransactionId transactionId,
    bool isCoordinator)
{
    return UnregisterTransaction(transactionId, isCoordinator);
}

// This is extra validation measure. Can be removed down the line.
void TStrongOrderingManager::PromoteLastCommitTimestamp(
    TTransactionId transactionId,
    TTimestamp commitTimestamp)
{
    auto it = TransactionIdToTransactionInfo_.find(transactionId);
    if (it == TransactionIdToTransactionInfo_.end()) {
        // This means that the transaction is not strongly ordered.
        return;
    }

    YT_VERIFY(it->second.CommitTimestamp == commitTimestamp);

    for (const auto& tag : it->second.StrongOrderingTags) {
        auto* shard = GetShardOrCrash(tag);
        auto previousCommitTimestamp = shard->LastCommitTimestamp;
        if (commitTimestamp < previousCommitTimestamp) {
            YT_LOG_ALERT("Last strongly ordered committed timestamp is greater than current "
                "(PreviousCommitTimestamp: %v, CurrentCommitTimestamp: %v)",
                previousCommitTimestamp,
                commitTimestamp);
        }

        shard->LastCommitTimestamp = commitTimestamp;
    }
}

void TStrongOrderingManager::ValidateProfilingMetricsConsistency() const
{
    int preparedTransactionCount = 0;
    int readyToCommitTransactionCount = 0;
    int readyToFlushTransactionCount = 0;

    for (const auto& [transactionId, transactionInfo] : TransactionIdToTransactionInfo_) {
        if (transactionInfo.CanFlush) {
            ++readyToFlushTransactionCount;
        } else if (transactionInfo.CommitTimestamp != NullTimestamp) {
            ++readyToCommitTransactionCount;
        } else {
            ++preparedTransactionCount;
        }
    }

    YT_LOG_ALERT_UNLESS(
        preparedTransactionCount == PreparedTransactionCount_.load(std::memory_order::relaxed),
        "Prepared transaction count is wrong (ExpectedValue: %v, ActualValue: %v)",
        preparedTransactionCount,
        PreparedTransactionCount_.load(std::memory_order::relaxed));

    YT_LOG_ALERT_UNLESS(
        readyToCommitTransactionCount == ReadyToCommitTransactionCount_.load(std::memory_order::relaxed),
        "Ready to commit transaction count is wrong (ExpectedValue: %v, ActualValue: %v)",
        readyToCommitTransactionCount,
        ReadyToCommitTransactionCount_.load(std::memory_order::relaxed));

    YT_LOG_ALERT_UNLESS(
        readyToFlushTransactionCount == ReadyToFlushTransactionCount_.load(std::memory_order::relaxed),
        "Ready to flush transaction count is wrong (ExpectedValue: %v, ActualValue: %v)",
        readyToFlushTransactionCount,
        ReadyToFlushTransactionCount_.load(std::memory_order::relaxed));
}

void TStrongOrderingManager::OnProfiling(TSensorBuffer* buffer) const
{
    buffer->AddGauge(
        "/transaction_supervisor/strong_ordering_manager/prepared_transaction_count",
        PreparedTransactionCount_.load(std::memory_order::relaxed));
    buffer->AddGauge(
        "/transaction_supervisor/strong_ordering_manager/ready_to_commit_transaction_count",
        ReadyToCommitTransactionCount_.load(std::memory_order::relaxed));
    buffer->AddGauge(
        "/transaction_supervisor/strong_ordering_manager/ready_to_flush_transaction_count",
        ReadyToFlushTransactionCount_.load(std::memory_order::relaxed));
}

void TStrongOrderingManager::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, OrderingTagToShard_);
    Persist(context, TransactionIdToTransactionInfo_);
    Persist(context, MaxObservedTimestamp_);
    Persist(context, PreparedTransactionCount_);
    Persist(context, ReadyToCommitTransactionCount_);
    Persist(context, ReadyToFlushTransactionCount_);

    if (context.IsLoad()) {
        PreparedCommitCount_.store(TransactionIdToTransactionInfo_.size(), std::memory_order::relaxed);
    }
}

void TStrongOrderingManager::Clear()
{
    OrderingTagToShard_.clear();
    TransactionIdToTransactionInfo_.clear();
    MaxObservedTimestamp_ = NullTimestamp;
    PreparedTransactionCount_.store(0, std::memory_order::relaxed);
    ReadyToCommitTransactionCount_.store(0, std::memory_order::relaxed);
    ReadyToFlushTransactionCount_.store(0, std::memory_order::relaxed);
    PreparedCommitCount_.store(0, std::memory_order::relaxed);
    PreparedCommitsFinished_.Transform([] (TPromise<void>& promise) {
        if (promise) {
            promise.TrySet();
            promise.Reset();
        }
    });
}

void TStrongOrderingManager::TStrongOrderingShard::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, CommitTimestampLowerBounds);
    Persist(context, ReadyToCommitTransactions);
    Persist(context, LastCommitTimestamp);
}

void TStrongOrderingManager::TTransactionInfo::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, CommitState);
    Persist(context, PrepareTimestamp);
    Persist(context, CommitTimestampLowerBound);
    Persist(context, CommitTimestamp);
    Persist(context, CommitTimestampClusterTag);
    Persist(context, IsCoordinator);
    Persist(context, CanFlush);
    Persist(context, StrongOrderingTags);
    Persist(context, FirstInShardCounter);
}

TStrongOrderingManager::TStrongOrderingShard* TStrongOrderingManager::GetOrCreateShard(const std::string& tag)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasHydraContext());

    auto [it, emplaced] = OrderingTagToShard_.emplace(tag, TStrongOrderingShard{});
    if (emplaced) {
        YT_LOG_DEBUG("Created strong ordering shard (Tag: %v)",
            tag);
    }

    return &it->second;
}

TStrongOrderingManager::TStrongOrderingShard* TStrongOrderingManager::GetShardOrCrash(const std::string& tag)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasHydraContext());

    return &GetOrCrash(OrderingTagToShard_, tag);
}

bool TStrongOrderingManager::ValidateCommitState(
    TTransactionId transactionId,
    std::vector<ECommitState> expectedStates,
    EActionOnValidationFailure actionOnMissingTransaction,
    EActionOnValidationFailure actionOnUnexpectedState) const
{
    auto transactionInfoIt = TransactionIdToTransactionInfo_.find(transactionId);
    auto isTransactionPresent = transactionInfoIt != TransactionIdToTransactionInfo_.end();

    if (!isTransactionPresent) {
        YT_LOG_DEBUG("Transaction is missing in strong ordering manager (TransactionId: %v, ExpectedStates: %v, Action: %v)",
            transactionId,
            expectedStates,
            actionOnMissingTransaction);

        switch (actionOnMissingTransaction) {
            case EActionOnValidationFailure::None:
                break;

            case EActionOnValidationFailure::Skip:
                return false;

            case EActionOnValidationFailure::Throw: {
                auto error = TError("Cannot find transaction %v, while it is expected to be in one of %v states",
                    transactionId,
                    expectedStates);
                THROW_ERROR WrapHydraError(std::move(error));
            }

            case EActionOnValidationFailure::Crash:
                YT_LOG_FATAL("Cannot find transaction (TransactionId: %v, ExpectedStates: %v)",
                    transactionId,
                    expectedStates);
        }
    }

    auto state = transactionInfoIt->second.CommitState;
    if (std::find(expectedStates.begin(), expectedStates.end(), state) == expectedStates.end()) {
        YT_LOG_DEBUG(
            "Transaction is in an unexpected state in strong ordering manager "
            "(TransactionId: %v, ActualState: %v, ExpectedStates: %v, Action: %v)",
            transactionId,
            state,
            expectedStates,
            actionOnMissingTransaction);

        switch (actionOnUnexpectedState) {
            case EActionOnValidationFailure::None:
                break;

            case EActionOnValidationFailure::Skip:
                return false;

            case EActionOnValidationFailure::Throw: {
                auto error = TError("Transaction %v is in state %v, while expected states are %v",
                    transactionId,
                    state,
                    expectedStates);
                THROW_ERROR WrapHydraError(std::move(error));
            }

            case EActionOnValidationFailure::Crash:
                YT_LOG_FATAL("Transaction is in an unexpected state (TransactionId: %v, State: %v, ExpectedState: %v)",
                    transactionId,
                    state,
                    expectedStates);
        }
    }

    return true;
}

std::optional<TTransactionId> TStrongOrderingManager::RemoveCommitTimestampLowerBound(
    TStrongOrderingShard* shard,
    TTimestamp commitTimestampLowerBound)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasHydraContext());

    auto it = GetIteratorOrCrash(shard->CommitTimestampLowerBounds, commitTimestampLowerBound);
    --it->second;

    YT_VERIFY(it->second >= 0);

    auto smallestLowerBoundRemoved = shard->CommitTimestampLowerBounds.begin() == it && it->second == 0;
    if (it->second == 0) {
        shard->CommitTimestampLowerBounds.erase(it);
    }

    // Iff this transaction has the smallest commit timestamp lower bound,
    // then it might allow some other transaction to be flushed.
    if (!smallestLowerBoundRemoved) {
        return std::nullopt;
    }

    if (shard->ReadyToCommitTransactions.empty()) {
        return std::nullopt;
    }

    auto smallestCommitTimestamp = shard->ReadyToCommitTransactions.begin()->first;
    // If smallest lower bound is greater than the smallest commit timestamp,
    // then transaction should have been marked as the first one in the shard already.
    if (commitTimestampLowerBound > smallestCommitTimestamp) {
        return std::nullopt;
    }

    return MaybeMarkTransactionAsFirstInShard(shard);
}

std::optional<TTransactionId> TStrongOrderingManager::MaybeMarkTransactionAsFirstInShard(
    TStrongOrderingShard* shard)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasHydraContext());

    if (shard->ReadyToCommitTransactions.empty()) {
        return std::nullopt;
    }

    auto [smallestCommitTimestamp, transactionId] = *(shard->ReadyToCommitTransactions.begin());
    // If commit timestamp lower bound is equal to the commit timestamp, then
    // we could rely on the fact that commit timestamps are unique and let the
    // commit happen. But it would make it harder to debug and the profit seems
    // to be minimal.
    if (!shard->CommitTimestampLowerBounds.empty() && shard->CommitTimestampLowerBounds.begin()->first <= smallestCommitTimestamp) {
        return std::nullopt;
    }

    // Transaction is the first one in this shard and its commit is not
    // obstructed by any other transaction.
    auto& transactionInfo = GetOrCrash(TransactionIdToTransactionInfo_, transactionId);
    transactionInfo.FirstInShardCounter++;

    if (transactionInfo.CanFlush &&
        transactionInfo.FirstInShardCounter == std::ssize(transactionInfo.StrongOrderingTags))
    {
        return transactionId;
    }

    return std::nullopt;
}

TTimestamp TStrongOrderingManager::ObserveTimestamp(TTimestamp timestamp)
{
    MaxObservedTimestamp_ = std::max(MaxObservedTimestamp_, timestamp);
    return MaxObservedTimestamp_;
}

TCommitInfos TStrongOrderingManager::FlushCommits(std::vector<TTransactionId> transactionsToFlush)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasHydraContext());

    TCommitInfos transactionsToCommit;
    auto currentTransactionIndex = 0;
    while (currentTransactionIndex < std::ssize(transactionsToFlush)) {
        auto transactionId = transactionsToFlush[currentTransactionIndex];

        auto& transactionInfo = GetOrCrash(TransactionIdToTransactionInfo_, transactionId);
        auto commitTimestamp = transactionInfo.CommitTimestamp;
        YT_LOG_DEBUG("Flushing stronly ordered transaction (TransactionId: %v, CommitTimestamp: %v)",
            transactionId,
            commitTimestamp);

        YT_LOG_FATAL_UNLESS(transactionInfo.CanFlush,
            "Transaction not marked as flushable was passed to be flushed (TransactionId: %v)",
            transactionId);

        YT_LOG_FATAL_UNLESS(transactionInfo.FirstInShardCounter == std::ssize(transactionInfo.StrongOrderingTags),
            "Transaction was passed to be flushed with wrong first in shard counter "
            "(TransactionId: %v, FirstInShardCounter: %v, StrongOrderingTagCount: %v)",
            transactionId,
            transactionInfo.FirstInShardCounter,
            std::ssize(transactionInfo.StrongOrderingTags));

        // TODO(h0pless): This is for validation only. Can be removed after manager is stable.
        const auto& strongOrderingTags = transactionInfo.StrongOrderingTags;
        for (const auto& tag : strongOrderingTags) {
            auto* shard = GetShardOrCrash(tag);

            auto [smallestCommitTimestamp, transactionIdToVerify] = *(shard->ReadyToCommitTransactions.begin());
            YT_LOG_FATAL_UNLESS(commitTimestamp == smallestCommitTimestamp,
                "Cannot commit transaction since transaction with smaller commit timestamp is not committed yet "
                "(TransactionId: %v, CommitTimestamp: %v, SmallestCommitTimestamp: %v, StrongOrderingTag: %v)",
                transactionId,
                commitTimestamp,
                smallestCommitTimestamp,
                tag);

            YT_VERIFY(transactionIdToVerify == transactionId);

            const auto& commitTimestampLowerBounds = shard->CommitTimestampLowerBounds;
            YT_LOG_FATAL_UNLESS(commitTimestampLowerBounds.empty() || commitTimestampLowerBounds.begin()->first >= commitTimestamp,
                "Cannot commit transaction as there is a prepared transaction with lesser commit timestamp lower bound "
                "(TransactionId: %v, MinCommitTimestampLowerBound: %v, CommitTimestamp: %v, StrongOrderingTag: %v)",
                transactionId,
                commitTimestampLowerBounds.begin()->first,
                commitTimestamp,
                tag);
        }

        TCommitInfo commitInfo{
            .TransactionId = transactionId,
            .IsCoordinator = transactionInfo.IsCoordinator,
            .CommitTimestamp = transactionInfo.CommitTimestamp,
            .CommitTimestampClusterTag = transactionInfo.CommitTimestampClusterTag,
        };
        transactionsToCommit.emplace_back(std::move(commitInfo));

        auto moreTransactionsToFlush = ForgetTransaction(transactionId);
        transactionsToFlush.insert(
            transactionsToFlush.end(),
            moreTransactionsToFlush.begin(),
            moreTransactionsToFlush.end());
        ++currentTransactionIndex;
    }

    return transactionsToCommit;
}

std::vector<TTransactionId> TStrongOrderingManager::ForgetTransaction(TTransactionId transactionId)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasHydraContext());

    auto transactionInfoIt = GetIteratorOrCrash(TransactionIdToTransactionInfo_, transactionId);
    const auto& transactionInfo = transactionInfoIt->second;

    auto state = transactionInfo.CommitState;
    const auto& strongOrderingTags = transactionInfo.StrongOrderingTags;

    auto preparedCommitCount = PreparedCommitCount_.fetch_sub(1, std::memory_order::relaxed);
    YT_LOG_DEBUG("Removing transaction from strong ordering manager (TransactionId: %v, State: %v, PreparedCommitCount: %v, StrongOrderingTags: %v)",
        transactionId,
        state,
        preparedCommitCount,
        MakeShrunkFormattableView(strongOrderingTags, TDefaultFormatter(), /*limit*/ 100));

    std::vector<TTransactionId> transactionsToFlush;
    if (state == ECommitState::Prepare) {
        YT_VERIFY(transactionInfo.CommitTimestamp == NullTimestamp);
        YT_VERIFY(transactionInfo.CommitTimestampClusterTag == InvalidCellTag);
        YT_VERIFY(transactionInfo.CanFlush == false);

        for (const auto& tag : strongOrderingTags) {
            auto* shard = GetShardOrCrash(tag);
            auto transactionToFlush = RemoveCommitTimestampLowerBound(shard, transactionInfo.CommitTimestampLowerBound);
            if (transactionToFlush) {
                transactionsToFlush.push_back(*transactionToFlush);
            }
        }
    } else if (state == ECommitState::ReadyToCommit || state == ECommitState::Commit) {
        for (const auto& tag : strongOrderingTags) {
            auto* shard = GetShardOrCrash(tag);
            auto transactionToRemoveIt = GetIteratorOrCrash(shard->ReadyToCommitTransactions, transactionInfo.CommitTimestamp);
            auto isRemovedTransactionFirst = transactionToRemoveIt == shard->ReadyToCommitTransactions.begin();
            shard->ReadyToCommitTransactions.erase(transactionToRemoveIt);

            if (isRemovedTransactionFirst) {
                auto transactionToFlush = MaybeMarkTransactionAsFirstInShard(shard);
                if (transactionToFlush) {
                    transactionsToFlush.push_back(*transactionToFlush);
                }
            }
        }
    }

    // This is quite implicit, but adding an extra enum to distinguish the current state feels wasteful.
    if (transactionInfo.CanFlush) {
        ReadyToFlushTransactionCount_.fetch_sub(1, std::memory_order::relaxed);
    } else if (transactionInfo.CommitTimestamp != NullTimestamp) {
        ReadyToCommitTransactionCount_.fetch_sub(1, std::memory_order::relaxed);
    } else {
        PreparedTransactionCount_.fetch_sub(1, std::memory_order::relaxed);
    }

    for (const auto& tag : strongOrderingTags) {
        MaybeRemoveShard(tag);
    }

    if (preparedCommitCount == 1) {
        PreparedCommitsFinished_.Transform([&] (TPromise<void>& promise) {
            if (promise) {
                promise.Set();
                promise.Reset();
            }
        });
    }
    TransactionIdToTransactionInfo_.erase(transactionInfoIt);
    return transactionsToFlush;
}

void TStrongOrderingManager::MaybeRemoveShard(const std::string& tag)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasHydraContext());

    auto it = GetIteratorOrCrash(OrderingTagToShard_, tag);
    if (it->second.CommitTimestampLowerBounds.empty() && it->second.ReadyToCommitTransactions.empty()) {
        OrderingTagToShard_.erase(it);
        YT_LOG_DEBUG("Strong ordering shard removed (Tag: %v)",
            tag);
    }
}

void TStrongOrderingManager::RegisterTransaction(
    TTransactionId transactionId,
    TTimestamp prepareTimestamp,
    bool isCoordinator,
    std::vector<std::string> strongOrderingTags)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasHydraContext());

    YT_VERIFY(!strongOrderingTags.empty());

    auto commitTimestampLowerBound = ObserveTimestamp(prepareTimestamp) + 1;
    auto preparedCommitCount = PreparedCommitCount_.fetch_add(1, std::memory_order::relaxed);
    YT_LOG_DEBUG(
        "Transaction prepare timestamp is added to strong ordering manager "
        "(TransactionId: %v, PrepareTimestamp: %v, CommitTimestampLowerBound: %v, PreparedCommitCount: %v, StrongOrderingTags: %v)",
        transactionId,
        prepareTimestamp,
        commitTimestampLowerBound,
        preparedCommitCount,
        MakeShrunkFormattableView(strongOrderingTags, TDefaultFormatter(), /*limit*/ 100));

    for (const auto& tag : strongOrderingTags) {
        auto* shard = GetOrCreateShard(tag);
        ++shard->CommitTimestampLowerBounds[commitTimestampLowerBound];
    }

    TTransactionInfo transactionInfo{
        .CommitState = ECommitState::Prepare,
        .PrepareTimestamp = prepareTimestamp,
        .CommitTimestampLowerBound = commitTimestampLowerBound,
        .CommitTimestamp = NullTimestamp,
        .CommitTimestampClusterTag = InvalidCellTag,
        .IsCoordinator = isCoordinator,
        .CanFlush = false,
        .StrongOrderingTags = std::move(strongOrderingTags),
    };

    PreparedTransactionCount_.fetch_add(1, std::memory_order::relaxed);

    EmplaceOrCrash(TransactionIdToTransactionInfo_, transactionId, std::move(transactionInfo));
}

TCommitInfos TStrongOrderingManager::RecordCommitTimestamp(
    TTransactionId transactionId,
    TTimestamp commitTimestamp,
    TClusterTag commitTimestampClusterTag,
    bool isCoordinator,
    std::optional<TTimestamp> prepareTimestampToValidate)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasHydraContext());

    // If transaction is:
    // - Missing on the coordinator, then a crash is in order.
    // - In a wrong state on the coordinator, then a crash is in order.
    // - Missing on the participant, then it's unusual, but it can happen
    //   if ReadyToCommit request arrived after Commit or Abort and the
    //   transaction was already removed from the strong ordering manager.
    // - In a wrong state on the participant, then the same logic applies,
    //   but to a lesser extent, since the transaction was not flushed yet.
    if (!ValidateCommitState(
        transactionId,
        {ECommitState::Prepare},
        /*actionOnMissingTransaction*/ isCoordinator
            ? EActionOnValidationFailure::Crash
            : EActionOnValidationFailure::Throw,
        /*actionOnUnexpectedState*/ isCoordinator
            ? EActionOnValidationFailure::Crash
            : EActionOnValidationFailure::Skip))
    {
        return {};
    }

    auto& transactionInfo = GetOrCrash(TransactionIdToTransactionInfo_, transactionId);
    auto prepareTimestamp = transactionInfo.PrepareTimestamp;
    auto commitTimestampLowerBound = transactionInfo.CommitTimestampLowerBound;

    YT_LOG_FATAL_IF(prepareTimestampToValidate && prepareTimestamp != *prepareTimestampToValidate,
        "Locally saved prepare timestamp differs from the prepare timestamp in TCommit (TransactionId: %v, LocalValue: %v, CommitValue: %v)",
        transactionId,
        prepareTimestamp,
        *prepareTimestampToValidate);

    YT_LOG_FATAL_IF(commitTimestamp < commitTimestampLowerBound,
        "Commit timestamp lower bound estimation was incorrect (TransactionId: %v, CommitTimestampLowerBound: %v, CommitTimestamp: %v)",
        transactionId,
        transactionInfo.CommitTimestampLowerBound,
        commitTimestamp);

    ObserveTimestamp(commitTimestamp);

    const auto& strongOrderingTags = transactionInfo.StrongOrderingTags;
    YT_LOG_DEBUG(
        "Replacing prepare timestamp with commit timestamp in strong ordering manager (TransactionId: %v, "
        "PrepareTimestamp: %v, CommitTimestampLowerBound: %v, CommitTimestamp: %v, CommitTimestampClusterTag: %v, StrongOrderingTags: %v)",
        transactionId,
        prepareTimestamp,
        commitTimestampLowerBound,
        commitTimestamp,
        commitTimestampClusterTag,
        MakeShrunkFormattableView(strongOrderingTags, TDefaultFormatter(), /*limit*/ 100));

    YT_VERIFY(transactionInfo.CommitState == ECommitState::Prepare);
    YT_VERIFY(transactionInfo.CanFlush == false);

    transactionInfo.CommitTimestamp = commitTimestamp;
    transactionInfo.CommitTimestampClusterTag = commitTimestampClusterTag;
    transactionInfo.CommitState = ECommitState::ReadyToCommit;

    std::vector<TTransactionId> transactionsToFlush;
    for (const auto& tag : strongOrderingTags) {
        auto* shard = GetShardOrCrash(tag);
        EmplaceOrCrash(shard->ReadyToCommitTransactions, commitTimestamp, transactionId);
        auto transactionToFlush = RemoveCommitTimestampLowerBound(shard, commitTimestampLowerBound);
        if (transactionToFlush) {
            transactionsToFlush.push_back(*transactionToFlush);
        }
    }

    PreparedTransactionCount_.fetch_sub(1, std::memory_order::relaxed);
    ReadyToCommitTransactionCount_.fetch_add(1, std::memory_order::relaxed);

    return FlushCommits(std::move(transactionsToFlush));
}

TCommitInfos TStrongOrderingManager::PermitCommitFlush(
    TTransactionId transactionId,
    TTimestamp commitTimestamp,
    TClusterTag commitTimestampClusterTag,
    bool isCoordinator,
    ECommitState commitState)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasHydraContext());

    YT_LOG_FATAL_UNLESS(
        commitState == ECommitState::ReadyToCommit && isCoordinator ||
        commitState == ECommitState::Commit && !isCoordinator,
        "Transaction commit was marked as flushable while being in a wrong commit state "
        "(TransactionId: %v, IsCoordinator: %v, CommitState: %v)",
        transactionId,
        isCoordinator,
        commitState);

    // Coordinator is in charge of committing this transaction, and strict
    // enforcement here seems warranted.
    // Participant, on the other hand, might suffer from duplicate requests
    // and/or reordering. If commit is in an unexpected state, then it's
    // in "Commit" state, meaning that this is a duplicate request, which can
    // be skipped. If transaction is missing entirely, then it was probably
    // committed already, which transaction supervisor should handle.
    // In the future, when transaction supervisor will not check for outdated
    // transactions, this validation should be revisited.
    if (!ValidateCommitState(
        transactionId,
        isCoordinator
            ? std::vector{ECommitState::ReadyToCommit}
            : std::vector{ECommitState::Prepare, ECommitState::ReadyToCommit},
        /*actionOnMissingTransaction*/ EActionOnValidationFailure::Crash,
        /*actionOnUnexpectedState*/ isCoordinator
            ? EActionOnValidationFailure::Crash
            : EActionOnValidationFailure::Skip))
    {
        return {};
    }

    auto& transactionInfo = GetIteratorOrCrash(TransactionIdToTransactionInfo_, transactionId)->second;

    TCommitInfos transactionsToCommit;
    // This can happen on the participant if Commit request arrives before ReadyToCommit one does.
    if (transactionInfo.CommitState == ECommitState::Prepare) {
        YT_VERIFY(!transactionInfo.IsCoordinator);
        transactionsToCommit = RecordCommitTimestamp(
            transactionId,
            commitTimestamp,
            commitTimestampClusterTag,
            isCoordinator);
    }

    // Sanity checks.
    YT_VERIFY(transactionInfo.CommitState == ECommitState::ReadyToCommit);
    YT_VERIFY(transactionInfo.IsCoordinator == isCoordinator);

    const auto& strongOrderingTags = transactionInfo.StrongOrderingTags;

    YT_LOG_DEBUG(
        "Marking transaction commit as ready to be flushed (TransactionId: %v, CommitTimestamp: %v, "
        "CommitTimestampClusterTag: %v, IsCoordinator: %v, StrongOrderingTags: %v)",
        transactionId,
        commitTimestamp,
        commitTimestampClusterTag,
        transactionInfo.IsCoordinator,
        MakeShrunkFormattableView(strongOrderingTags, TDefaultFormatter(), /*limit*/ 100));

    transactionInfo.CommitState = commitState;
    transactionInfo.CanFlush = true;

    ReadyToCommitTransactionCount_.fetch_sub(1, std::memory_order::relaxed);
    ReadyToFlushTransactionCount_.fetch_add(1, std::memory_order::relaxed);

    // Transaction is the first in every shard, it should be committed.
    if (transactionInfo.FirstInShardCounter == std::ssize(transactionInfo.StrongOrderingTags)) {
        auto additionalTransactionsToCommit = FlushCommits({{transactionId}});
        transactionsToCommit.insert(
            transactionsToCommit.end(),
            additionalTransactionsToCommit.begin(),
            additionalTransactionsToCommit.end());
    }

    return transactionsToCommit;
}

TCommitInfos TStrongOrderingManager::UnregisterTransaction(
    TTransactionId transactionId,
    bool isCoordinator)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasHydraContext());

    // If transaction is:
    // - Missing on the coordinator, then it is probably because it was removed
    //   during the flush, but prepare (in the late prepare mode) has failed and
    //   now the transaction is being aborted.
    // - In a wrong state on the coordinator, then a crash is in order.
    // - Missing on the participant, then there is no need to do anything, since
    //   it usually means that the Abort request arrived before the Prepare request.
    // - In a wrong state on the participant, then a crash is in order.
    if (!ValidateCommitState(
        transactionId,
        {ECommitState::Prepare, ECommitState::ReadyToCommit},
        /*actionOnMissingTransaction*/ EActionOnValidationFailure::Skip,
        /*actionOnUnexpectedState*/ EActionOnValidationFailure::Crash))
    {
        return {};
    }

    auto& transactionInfo = GetOrCrash(TransactionIdToTransactionInfo_, transactionId);
    auto commitState = transactionInfo.CommitState;
    auto commitTimestamp = transactionInfo.CommitTimestamp;
    const auto& strongOrderingTags = transactionInfo.StrongOrderingTags;

    YT_LOG_DEBUG(
        "Transaction was not committed and is being unregistered from the strong ordering manager "
        "(TransactionId: %v, PrepareTimestamp: %v, CommitTimestampLowerBound: %v, State: %v, "
        "CommitTimestamp: %v, IsCoordinator: %v, StrongOrderingTags: %v)",
        transactionId,
        transactionInfo.PrepareTimestamp,
        transactionInfo.CommitTimestampLowerBound,
        commitState,
        commitTimestamp,
        isCoordinator,
        MakeShrunkFormattableView(strongOrderingTags, TDefaultFormatter(), /*limit*/ 100));

    YT_VERIFY(commitState == ECommitState::Prepare || commitState == ECommitState::ReadyToCommit);

    auto transactionsToFlush = ForgetTransaction(transactionId);
    return FlushCommits(std::move(transactionsToFlush));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
