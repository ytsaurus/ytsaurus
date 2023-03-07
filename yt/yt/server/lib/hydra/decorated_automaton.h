#pragma once

#include "private.h"
#include "distributed_hydra_manager.h"
#include "mutation_context.h"

#include <yt/server/lib/election/public.h>

#include <yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/client/hydra/version.h>

#include <yt/core/actions/cancelable_context.h>
#include <yt/core/actions/future.h>

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/async_batcher.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/ref.h>
#include <yt/core/misc/ring_queue.h>

#include <yt/core/rpc/public.h>

#include <atomic>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TEpochContext
    : public TRefCounted
{
    NElection::TCellManagerPtr CellManager;
    IChangelogStorePtr ChangelogStore;
    TVersion ReachableVersion;

    IInvokerPtr EpochSystemAutomatonInvoker;
    IInvokerPtr EpochUserAutomatonInvoker;
    IInvokerPtr EpochControlInvoker;
    TCheckpointerPtr Checkpointer;
    TLeaderRecoveryPtr LeaderRecovery;
    TFollowerRecoveryPtr FollowerRecovery;
    TLeaderCommitterPtr LeaderCommitter;
    TFollowerCommitterPtr FollowerCommitter;
    TLeaseTrackerPtr LeaseTracker;
    NConcurrency::TPeriodicExecutorPtr HeartbeatMutationCommitExecutor;

    std::atomic_flag Restarting = ATOMIC_FLAG_INIT;
    bool LeaderLeaseExpired = false;

    TIntrusivePtr<NConcurrency::TAsyncBatcher<void>> LeaderSyncBatcher;
    std::optional<TVersion> LeaderSyncVersion;
    TPromise<void> LeaderSyncPromise;

    TPeerId LeaderId = InvalidPeerId;
    TEpochId EpochId;
    TCancelableContextPtr CancelableContext = New<TCancelableContext>();
};

DEFINE_REFCOUNTED_TYPE(TEpochContext)

////////////////////////////////////////////////////////////////////////////////

class TSystemLockGuard
    : private TNonCopyable
{
public:
    TSystemLockGuard() = default;
    TSystemLockGuard(TSystemLockGuard&& other);
    ~TSystemLockGuard();

    TSystemLockGuard& operator = (TSystemLockGuard&& other);

    void Release();

    explicit operator bool() const;

    static TSystemLockGuard Acquire(TDecoratedAutomatonPtr automaton);

private:
    explicit TSystemLockGuard(TDecoratedAutomatonPtr automaton);

    TDecoratedAutomatonPtr Automaton_;

};

////////////////////////////////////////////////////////////////////////////////

class TUserLockGuard
    : private TNonCopyable
{
public:
    TUserLockGuard() = default;
    TUserLockGuard(TUserLockGuard&& other);
    ~TUserLockGuard();

    TUserLockGuard& operator = (TUserLockGuard&& other);

    void Release();

    explicit operator bool() const;

    static TUserLockGuard TryAcquire(TDecoratedAutomatonPtr automaton);

private:
    explicit TUserLockGuard(TDecoratedAutomatonPtr automaton);

    TDecoratedAutomatonPtr Automaton_;

};

////////////////////////////////////////////////////////////////////////////////

class TDecoratedAutomaton
    : public TRefCounted
{
public:
    TDecoratedAutomaton(
        TDistributedHydraManagerConfigPtr config,
        const TDistributedHydraManagerOptions& options,
        IAutomatonPtr automaton,
        IInvokerPtr automatonInvoker,
        IInvokerPtr controlInvoker,
        ISnapshotStorePtr snapshotStore,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler);

    void Initialize();
    void OnStartLeading(TEpochContextPtr epochContext);
    void OnLeaderRecoveryComplete();
    void OnStopLeading();
    void OnStartFollowing(TEpochContextPtr epochContext);
    void OnFollowerRecoveryComplete();
    void OnStopFollowing();

    IInvokerPtr CreateGuardedUserInvoker(IInvokerPtr underlyingInvoker);
    IInvokerPtr GetDefaultGuardedUserInvoker();
    IInvokerPtr GetSystemInvoker();

    EPeerState GetState() const;

    TEpochContextPtr GetEpochContext();

    TVersion GetLoggedVersion() const;
    void SetLoggedVersion(TVersion version);

    ui64 GetRandomSeed() const;
    i64 GetSequenceNumber() const;

    void SetChangelog(IChangelogPtr changelog);

    int GetRecordCountSinceLastCheckpoint() const;
    i64 GetDataSizeSinceLastCheckpoint() const;
    TInstant GetSnapshotBuildDeadline() const;

    TVersion GetAutomatonVersion() const;
    void RotateAutomatonVersion(int segmentId);

    TVersion GetCommittedVersion() const;

    TVersion GetPingVersion() const;

    void LoadSnapshot(
        int snapshotId,
        TVersion version,
        i64 sequenceNumber,
        ui64 randomSeed,
        NConcurrency::IAsyncZeroCopyInputStreamPtr reader);

    void ValidateSnapshot(NConcurrency::IAsyncZeroCopyInputStreamPtr reader);

    void ApplyMutationDuringRecovery(const TSharedRef& recordData);

    TFuture<TMutationResponse> TryBeginKeptRequest(const TMutationRequest& request);

    struct TPendingMutation
    {
        TPendingMutation(
            TVersion version,
            TMutationRequest&& request,
            TInstant timestamp,
            ui64 randomSeed,
            ui64 prevRandomSeed,
            i64 sequenceNumber,
            NTracing::TTraceContextPtr traceContext)
            : Version(version)
            , Request(request)
            , Timestamp(timestamp)
            , RandomSeed(randomSeed)
            , PrevRandomSeed(prevRandomSeed)
            , SequenceNumber(sequenceNumber)
            , TraceContext(std::move(traceContext))
        { }

        TVersion Version;
        TMutationRequest Request;
        TInstant Timestamp;
        ui64 RandomSeed;
        ui64 PrevRandomSeed;
        i64 SequenceNumber;
        NTracing::TTraceContextPtr TraceContext;
        TPromise<TMutationResponse> LocalCommitPromise = NewPromise<TMutationResponse>();
    };

    const TPendingMutation& LogLeaderMutation(
        TInstant timestamp,
        TMutationRequest&& request,
        NTracing::TTraceContextPtr traceContext,
        TSharedRef* recordData,
        TFuture<void>* localFlushFuture);
    const TPendingMutation& LogFollowerMutation(
        const TSharedRef& recordData,
        TFuture<void>* localFlushFuture);

    TFuture<TRemoteSnapshotParams> BuildSnapshot();

    TFuture<void> RotateChangelog();

    void CommitMutations(TVersion version, bool mayYield);

    bool HasReadyMutations() const;

    void RotateAutomatonVersionAfterRecovery();

    TReign GetCurrentReign() const;
    EFinalRecoveryAction GetFinalRecoveryAction() const;

    bool IsBuildingSnapshotNow() const;
    int GetLastSuccessfulSnapshotId() const;

    void SetLastLeadingSegmentId(int segmentId);
    int GetLastLeadingSegmentId() const;

private:
    friend class TUserLockGuard;
    friend class TSystemLockGuard;

    class TGuardedUserInvoker;
    class TSystemInvoker;
    class TSnapshotBuilderBase;
    class TForkSnapshotBuilder;
    class TSwitchableSnapshotWriter;
    class TNoForkSnapshotBuilder;

    const TDistributedHydraManagerConfigPtr Config_;
    const TDistributedHydraManagerOptions Options_;
    const NElection::TCellManagerPtr CellManager_;
    const IAutomatonPtr Automaton_;
    const IInvokerPtr AutomatonInvoker_;
    const IInvokerPtr DefaultGuardedUserInvoker_;
    const IInvokerPtr ControlInvoker_;
    const IInvokerPtr SystemInvoker_;
    const ISnapshotStorePtr SnapshotStore_;

    std::atomic<int> UserLock_ = {0};
    std::atomic<int> SystemLock_ = {0};

    NConcurrency::TReaderWriterSpinLock EpochContextLock_;
    TEpochContextPtr EpochContext_;

    IChangelogPtr Changelog_;
    TFuture<IChangelogPtr> NextChangelogFuture_;

    int RecoveryRecordCount_ = 0;
    i64 RecoveryDataSize_ = 0;

    std::atomic<EPeerState> State_ = {EPeerState::Stopped};

    // AutomatonVersion_ <= CommittedVersion_ <= LoggedVersion_
    // LoggedVersion_ is only maintained when the peer is active, e.g. not during recovery.
    std::atomic<TVersion> LoggedVersion_ = {};
    std::atomic<TVersion> AutomatonVersion_ = {};
    std::atomic<TVersion> CommittedVersion_ = {};
    std::atomic<ui64> RandomSeed_ = {};
    std::atomic<i64> SequenceNumber_ = {};

    bool RotatingChangelog_ = false;

    //! AutomatonVersion_ <= SnapshotVersion_
    TVersion SnapshotVersion_;
    TPromise<TRemoteSnapshotParams> SnapshotParamsPromise_;
    std::atomic<bool> BuildingSnapshot_ = false;
    TInstant SnapshotBuildDeadline_ = TInstant::Max();
    std::atomic<int> LastSuccessfulSnapshotId_ = -1;

    int LastLeadingSegmentId_ = -1;

    NProto::TMutationHeader MutationHeader_; // pooled instance
    TRingQueue<TPendingMutation> PendingMutations_;

    NProfiling::TAggregateGauge BatchCommitTimeCounter_{"/batch_commit_time"};

    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler;

    ui64 GetLastLoggedRandomSeed() const;
    i64 GetLastLoggedSequenceNumber() const;

    void RotateAutomatonVersionIfNeeded(TVersion mutationVersion);
    void DoApplyMutation(TMutationContext* context);

    bool TryAcquireUserLock();
    void ReleaseUserLock();
    void AcquireSystemLock();
    void ReleaseSystemLock();

    void CancelSnapshot(const TError& error);

    void StartEpoch(TEpochContextPtr epochContext);
    void StopEpoch();

    void DoRotateChangelog();

    void ApplyPendingMutations(bool mayYield);

    TFuture<void> SaveSnapshot(NConcurrency::IAsyncOutputStreamPtr writer);
    void MaybeStartSnapshotBuilder();

    bool IsRecovery();

    void UpdateLastSuccessfulSnapshotInfo(const TErrorOr<TRemoteSnapshotParams>& snapshotInfoOrError);
    void UpdateSnapshotBuildDeadline();

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

DEFINE_REFCOUNTED_TYPE(TDecoratedAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
