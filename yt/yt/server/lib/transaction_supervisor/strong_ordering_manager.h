#pragma once

#include "public.h"
#include "commit.h"

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EActionOnValidationFailure,
    ((None)     (0))
    ((Skip)     (1))
    ((Throw)    (2))
    ((Crash)    (3))
);

////////////////////////////////////////////////////////////////////////////////

struct TCommitInfo
{
    TTransactionId TransactionId;
    bool IsCoordinator;

    TTimestamp CommitTimestamp;
    NApi::TClusterTag CommitTimestampClusterTag;
};

using TCommitInfos = std::vector<TCommitInfo>;

////////////////////////////////////////////////////////////////////////////////

class TStrongOrderingManager
{
public:
    explicit TStrongOrderingManager(NLogging::TLogger logger);

    bool IsUniqueClockSource(NApi::TClusterTag clockClusterTag);

    NApi::TClusterTag GetClockSourceClusterTag() const;

    TFuture<void> WaitUntilPreparedCommitsFinish();

    void OnCommitPrepare(
        TTransactionId transactionId,
        TTimestamp prepareTimestamp,
        bool isCoordinator,
        std::vector<std::string> strongOrderingTags);

    // Only called on participant.
    [[nodiscard]] TCommitInfos OnCommitReadyToCommit(
        TTransactionId transactionId,
        TTimestamp commitTimestamp,
        NApi::TClusterTag commitTimestampClusterTag);

    [[nodiscard]] TCommitInfos OnCommitCommit(
        TTransactionId transactionId,
        TTimestamp commitTimestamp,
        NApi::TClusterTag commitTimestampClusterTag,
        bool isCoordinator,
        ECommitState commitState,
        std::optional<TTimestamp> prepareTimestampToValidate = std::nullopt);

    [[nodiscard]] TCommitInfos OnCommitAbort(
        TTransactionId transactionId,
        bool isCoordinator);

    // This is extra validation measure. Can be removed down the line.
    void PromoteLastCommitTimestamp(
        TTransactionId transactionId,
        TTimestamp commitTimestamp);

    void ValidateProfilingMetricsConsistency() const;

    void OnProfiling(NProfiling::TSensorBuffer* buffer) const;
    void Persist(const TStreamPersistenceContext& context);
    void Clear();

private:
    /*
     *  NB:
     *  On the coordinator ReadyToCommit and Commit stages happen simultaneously.
     *  On the participant ReadyToCommit and Commit stages happen simultaneously iff Commit request arrived before ReadyToCommit request.
     *
     *                                                                              Note that commit and flush might happen at the same time!
     *  Logical stages:                 Prepare                  ReadyToCommit                  Commit                      Flush
     *  +---------------------------+      |                           |                           |                           |
     *  |    Commit Ts Estimation   |      |###########################|---------------------------|---------------------------|
     *  +---------------------------+      |                           |                           |                           |
     *  |    Ready to commit Txs    |      |---------------------------|###########################|###########################|
     *  +---------------------------+      |                           |                           |                           |
     *  |  Transaction to tx info   |      |###########################|###########################|###########################|
     *  +---------------------------+      |                           |                           |                           |
     */

    struct TStrongOrderingShard
    {
        // Commit timestamp is always generated after all participants have gone through
        // the Prepare phase. Assuming that there is a single monotonic clock source,
        // which is a valid assumption for this class, if Prepare request for transaction T
        // arrives after timestamp X was observed in some other request, then commit timestamp
        // for T must be greater than X.
        // Example:
        // T1 Prepare request received with prepare timestamp 5.
        // T1 ReadyToCommit request received with commit timestamp is 7.
        // T2 Prepare request is received with prepare timestamp 2.
        // Since T2 Prepare request is currently being processed, and timestamp 7 was seen,
        // it can be assumed that T2 commit timestamp is greater than 7.
        // Please note that two transactions can have the same commit timestamp lower bound
        // estimation, which means that the counter has to be stored alongside the timestamp.
        std::map<TTimestamp, int> CommitTimestampLowerBounds;

        // A map of all transactions with known commit timestamps.
        // If current cell is a coordinator for a transaction T, then T can be committed
        // once it's the first transaction in this map and its commit timestamp is greater
        // than all commit timestamp lower bounds.
        // If current cell is a participant, then T can be committed only after "Commit"
        // request was received in addition to all of the above.
        std::map<TTimestamp, TTransactionId> ReadyToCommitTransactions;

        // This field is used for validation only. It can be removed.
        TTimestamp LastCommitTimestamp = NTransactionClient::NullTimestamp;

        void Persist(const TStreamPersistenceContext& context);
    };

    struct TTransactionInfo
    {
        ECommitState CommitState;

        // Prepare timestamp is used for validation only; the lower bound is used for faster
        // order resolution.
        TTimestamp PrepareTimestamp;
        TTimestamp CommitTimestampLowerBound;
        // Coordinator can access prepare and commit timestamps from commit, but the participant cannot.
        TTimestamp CommitTimestamp = NTransactionClient::NullTimestamp;

        NApi::TClusterTag CommitTimestampClusterTag = NObjectClient::InvalidCellTag;
        bool IsCoordinator = false;
        bool CanFlush = false;
        std::vector<std::string> StrongOrderingTags;

        // If a transaction has commit timestamp smaller than all other commit
        // timestamps and lower bound estimations in some shard, then it will
        // always be the first one to be committed. This is true, because the
        // clock is monotonic. Thus, it's enough to keep track in how many
        // shards this transaction is already guaranteed to be the first.
        int FirstInShardCounter = 0;

        void Persist(const TStreamPersistenceContext& context);
    };

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    const NLogging::TLogger Logger;

    // Used to ensure that clock source doesn't change, since monotonicity is important.
    // Transient. Making this field persistent will make it impossible to change clock
    // source even when updating using the snapshot with read-only, but this kind of
    // configuration change is sometimes needed.
    NApi::TClusterTag ClockClusterTag_ = NObjectClient::InvalidCellTag;

    THashMap<std::string, TStrongOrderingShard> OrderingTagToShard_;
    THashMap<TTransactionId, TTransactionInfo> TransactionIdToTransactionInfo_;

    // This field is needed to estimate commit timestamp lower bound; see comment in TStrongOrderingShard.
    TTimestamp MaxObservedTimestamp_ = NTransactionClient::NullTimestamp;

    // Used by profiling only.
    std::atomic<int> PreparedTransactionCount_ = 0;
    std::atomic<int> ReadyToCommitTransactionCount_ = 0;
    std::atomic<int> ReadyToFlushTransactionCount_ = 0;

    std::atomic<int> PreparedCommitCount_ = 0;
    NThreading::TAtomicObject<TPromise<void>> PreparedCommitsFinished_;

    TStrongOrderingShard* GetOrCreateShard(const std::string& tag);
    TStrongOrderingShard* GetShardOrCrash(const std::string& tag);

    bool ValidateCommitState(
        TTransactionId transactionId,
        std::vector<ECommitState> expectedStates,
        EActionOnValidationFailure actionOnMissingTransaction,
        EActionOnValidationFailure actionOnUnexpectedState) const;

    // Returns a transaction that becomes ready to be flushed as the result of
    // commit timestamp lower bound removal.
    [[nodiscard]] std::optional<TTransactionId> RemoveCommitTimestampLowerBound(
        TStrongOrderingShard* shard,
        TTimestamp commitTimestampLowerBound);

    // Checks that the first transaction with a known commit timestamp:
    // 1. has commit timestamp smaller than all commit timestamps in this shard;
    // 2. has commit timestamp smaller than all commit timestamp lower bounds in this shard.
    // If both are true, then increments FirstInShardCounter for that transaction.
    // Returns a transaction iff it is ready to be flushed.
    [[nodiscard]] std::optional<TTransactionId> MaybeMarkTransactionAsFirstInShard(
        TStrongOrderingShard* shard);

    TTimestamp ObserveTimestamp(TTimestamp timestamp);

    [[nodiscard]] TCommitInfos FlushCommits(std::vector<TTransactionId> transactionsToFlush);

    [[nodiscard]] std::vector<TTransactionId> ForgetTransaction(TTransactionId transactionId);

    void MaybeRemoveShard(const std::string& tag);

    void RegisterTransaction(
        TTransactionId transactionId,
        TTimestamp prepareTimestamp,
        bool isCoordinator,
        std::vector<std::string> strongOrderingTags);

    [[nodiscard]] TCommitInfos RecordCommitTimestamp(
        TTransactionId transactionId,
        TTimestamp commitTimestamp,
        NApi::TClusterTag commitTimestampClusterTag,
        bool isCoordinator,
        std::optional<TTimestamp> prepareTimestampToValidate = std::nullopt);

    [[nodiscard]] TCommitInfos PermitCommitFlush(
        TTransactionId transactionId,
        TTimestamp commitTimestamp,
        NApi::TClusterTag commitTimestampClusterTag,
        bool isCoordinator,
        ECommitState commitState);

    [[nodiscard]] TCommitInfos UnregisterTransaction(
        TTransactionId transactionId,
        bool isCoordinator);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
