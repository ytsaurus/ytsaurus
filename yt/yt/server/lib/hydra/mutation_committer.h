#pragma once

#include "private.h"
#include "decorated_automaton.h"
#include "hydra_service_proxy.h"
#include "mutation_context.h"
#include "distributed_hydra_manager.h"

#include <yt/yt/server/lib/hydra/changelog_acquisition.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/invoker_alarm.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/mpsc_queue.h>
#include <yt/yt/core/misc/ring_queue.h>

#include <yt/yt/library/tracing/async_queue_trace.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TMutationDraft
{
    TMutationRequest Request;
    TPromise<TMutationResponse> Promise;
    ui64 RandomSeed;
};

using TMutationDraftQueue = TMpscQueue<TMutationDraft>;
using TMutationDraftQueuePtr = TIntrusivePtr<TMutationDraftQueue>;

////////////////////////////////////////////////////////////////////////////////

class TCommitterBase
    : public TRefCounted
{
public:
    //! Raised on mutation logging failure.
    DEFINE_SIGNAL(void(const TError& error), LoggingFailed);

    TFuture<void> GetLastLoggedMutationFuture();
    TFuture<void> GetLastOffloadedMutationsFuture();
    void RegisterNextChangelog(int id, IChangelogPtr changelog);

protected:
    const TConfigWrapperPtr Config_;
    const TDistributedHydraManagerOptions Options_;
    const TDecoratedAutomatonPtr DecoratedAutomaton_;
    TEpochContext* const EpochContext_;
    const NLogging::TLogger Logger;

    const NElection::TCellManagerPtr CellManager_;

    NProto::TMutationHeader MutationHeader_;
    TFuture<void> LastLoggedMutationFuture_ = VoidFuture;

    TFuture<void> LastOffloadedMutationsFuture_ = VoidFuture;

    TCompactFlatMap<int, IChangelogPtr, 4> NextChangelogs_;
    IChangelogPtr Changelog_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    TCommitterBase(
        TConfigWrapperPtr config,
        const TDistributedHydraManagerOptions& options,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TEpochContext* epochContext,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler,
        IChangelogPtr changelog);

    TFuture<void> ScheduleApplyMutations(std::vector<TPendingMutationPtr> mutations);

    TErrorOr<IChangelogPtr> ExtractNextChangelog(TVersion version);
    TError PrepareNextChangelog(TVersion version);
    void CloseChangelog(const IChangelogPtr& changelog);
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAcceptMutationsMode,
    (Slow)
    (Fast)
);

////////////////////////////////////////////////////////////////////////////////

//! Manages commits carried out by a leader.
/*!
 *  \note Thread affinity: ControlThread
 */
class TLeaderCommitter
    : public TCommitterBase
{
public:
    TLeaderCommitter(
        TConfigWrapperPtr config,
        const TDistributedHydraManagerOptions& options,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TLeaderLeasePtr leaderLease,
        TMutationDraftQueuePtr mutationDraftQueue,
        IChangelogPtr changelog,
        TReachableState reachableState,
        TEpochContext* epochContext,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler);

    TVersion GetNextLoggedVersion() const;
    i64 GetLoggedSequenceNumber() const;
    i64 GetLastOffloadedSequenceNumber() const;

    bool CanBuildSnapshot() const;
    TFuture<int> BuildSnapshot(bool waitForCompletion, bool readOnly);
    std::optional<TFuture<int>> GetLastSnapshotFuture(bool waitForCompletion, bool readOnly);

    TFuture<void> GetLastMutationFuture();

    void Reconfigure();

    void Start();
    void Stop();

    void SerializeMutations();

    void BuildMonitoring(NYTree::TFluentMap fluent);

private:
    const TMutationDraftQueuePtr MutationDraftQueue_;
    const TLeaderLeasePtr LeaderLease_;

    const NConcurrency::TPeriodicExecutorPtr FlushMutationsExecutor_;
    const NConcurrency::TPeriodicExecutorPtr SerializeMutationsExecutor_;
    const NConcurrency::TPeriodicExecutorPtr CheckpointCheckExecutor_;

    struct TPeerState
    {
        i64 NextExpectedSequenceNumber = -1;
        i64 LastLoggedSequenceNumber = -1;

        int InFlightRequestCount = 0;
        int InFlightMutationCount = 0;
        i64 InFlightMutationDataSize = 0;
        EAcceptMutationsMode Mode = EAcceptMutationsMode::Slow;
    };
    std::vector<TPeerState> PeerStates_;

    ui64 LastRandomSeed_ = 0;

    TReachableState InitialState_;
    TReachableState CommittedState_;
    i64 LastOffloadedSequenceNumber_ = 0;
    i64 NextLoggedSequenceNumber_ = 0;
    TVersion NextLoggedVersion_;

    bool AcquiringChangelog_ = false;

    bool RotatingChangelog_ = false;

    TInstant SnapshotBuildDeadline_ = TInstant::Max();

    struct TShapshotInfo
    {
        int SnapshotId = -1;
        // Build a snapshot right after this mutation.
        i64 SequenceNumber = -1;
        std::vector<bool> HasResponse;
        int ResponseCount = 0;
        std::vector<std::optional<TChecksum>> Checksums;
        bool ReadOnly = false;

        TPromise<int> Promise = NewPromise<int>();
    };
    std::optional<TShapshotInfo> LastSnapshotInfo_;

    i64 MutationQueueDataSize_ = 0;
    std::deque<TPendingMutationPtr> MutationQueue_;

    NProfiling::TSummary BatchSizeSummary_;
    NProfiling::TSummary MutationQueueSizeSummary_;
    NProfiling::TSummary MutationQueueDataSizeSummary_;

    void FlushMutations();
    void OnMutationsAcceptedByFollower(
        int followerId,
        int mutationCount,
        i64 mutationsDataSize,
        const TInternalHydraServiceProxy::TErrorOrRspAcceptMutationsPtr& rspOrError);
    void MaybeFlushMutations();

    void DrainQueue();

    void LogMutations(std::vector<TMutationDraft> mutationDrafts);
    void OnMutationsLogged(
        i64 firstSequenceNumber,
        i64 lastSequenceNumber,
        const TError& error);

    void MaybePromoteCommittedSequenceNumber();
    void OnCommittedSequenceNumberUpdated();

    void MaybeCheckpoint();
    void Checkpoint();
    void UpdateSnapshotBuildDeadline();
    void OnChangelogAcquired(const TErrorOr<IChangelogPtr>& changelogsOrError);

    void OnLocalSnapshotBuilt(int snapshotId, const TErrorOr<TRemoteSnapshotParams>& rspOrError);
    void OnSnapshotResponse(int peerId);
    void OnSnapshotsComplete();
};

DEFINE_REFCOUNTED_TYPE(TLeaderCommitter)

////////////////////////////////////////////////////////////////////////////////

//! Manages commits carried out by a follower.
/*!
 *  \note Thread affinity: ControlThread
 */
class TFollowerCommitter
    : public TCommitterBase
{
public:
    TFollowerCommitter(
        TConfigWrapperPtr config,
        const TDistributedHydraManagerOptions& options,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TEpochContext* epochContext,
        NLogging::TLogger logger,
        NProfiling::TProfiler /*profiler*/);

    bool AcceptMutations(
        i64 startSequenceNumber,
        const std::vector<TSharedRef>& recordsData);

    void LogMutations();

    struct TCommitMutationsResult
    {
        i64 FirstSequenceNumber;
        i64 LastSequenceNumber;
    };

    TFuture<TCommitMutationsResult> CommitMutations(i64 committedSequenceNumber);

    //! Forwards a given mutation to the leader via RPC.
    TFuture<TMutationResponse> Forward(TMutationRequest&& request);

    i64 GetLoggedSequenceNumber() const;
    i64 GetExpectedSequenceNumber() const;
    void SetSequenceNumber(i64 number);

    void BuildMonitoring(NYTree::TFluentMap fluent);

    void CatchUp();

    //! Cleans things up, aborts all pending mutations with a human-readable error.
    void Stop();

private:
    const TPromise<void> CaughtUpPromise_ = NewPromise<void>();

    // Accepted, but not logged.
    TRingQueue<TPendingMutationPtr> AcceptedMutations_;
    i64 LastAcceptedSequenceNumber_ = 0;

    // Logged, but not committed.
    TRingQueue<TPendingMutationPtr> LoggedMutations_;
    i64 LastLoggedSequenceNumber_ = 0;

    i64 CommittedSequenceNumber_ = -1;

    bool LoggingMutations_ = false;

    bool RecoveryComplete_ = true;

    void CheckIfCaughtUp();

    void DoAcceptMutation(const TSharedRef& recordData);
    void OnMutationsLogged(
        i64 firstSequenceNumber,
        i64 lastMutationSequenceNumber,
        const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TFollowerCommitter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
