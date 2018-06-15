#pragma once

#include "private.h"
#include "distributed_hydra_manager.h"
#include "mutation_context.h"

#include <yt/server/election/public.h>

#include <yt/ytlib/hydra/hydra_manager.pb.h>
#include <yt/ytlib/hydra/version.h>

#include <yt/core/actions/cancelable_context.h>
#include <yt/core/actions/future.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/ref.h>
#include <yt/core/misc/ring_queue.h>

#include <yt/core/rpc/public.h>

#include <atomic>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TEpochContext
    : public TRefCounted
{
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

    std::atomic<bool> Restarting = {false};

    TPromise<void> ActiveUpstreamSyncPromise;
    TPromise<void> PendingUpstreamSyncPromise;
    bool UpstreamSyncDeadlineReached = false;
    NProfiling::TCpuInstant UpstreamSyncStartTime;

    TNullable<TVersion> LeaderSyncVersion;
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
        NElection::TCellManagerPtr cellManager,
        IAutomatonPtr automaton,
        IInvokerPtr automatonInvoker,
        IInvokerPtr controlInvoker,
        ISnapshotStorePtr snapshotStore,
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

    TVersion GetLoggedVersion() const;
    void SetLoggedVersion(TVersion version);

    void SetChangelog(IChangelogPtr changelog);

    int GetRecordCountSinceLastCheckpoint() const;
    i64 GetDataSizeSinceLastCheckpoint() const;
    TInstant GetLastSnapshotTime() const;

    TVersion GetAutomatonVersion() const;
    void RotateAutomatonVersion(int segmentId);

    TVersion GetCommittedVersion() const;

    TVersion GetPingVersion() const;

    void LoadSnapshot(
        int snapshotId,
        TVersion version,
        NConcurrency::IAsyncZeroCopyInputStreamPtr reader);

    void ApplyMutationDuringRecovery(const TSharedRef& recordData);

    TFuture<TMutationResponse> TryBeginKeptRequest(const TMutationRequest& request);

    const TMutationRequest& LogLeaderMutation(
        TMutationRequest&& request,
        TSharedRef* recordData,
        TFuture<void>* localFlushResult,
        TFuture<TMutationResponse>* commitResult);

    void LogFollowerMutation(
        const TSharedRef& recordData,
        TFuture<void>* localFlushResult);

    TFuture<TRemoteSnapshotParams> BuildSnapshot();

    TFuture<void> RotateChangelog();

    void CommitMutations(TVersion version, bool mayYield);

    bool HasReadyMutations() const;

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

    TEpochContextPtr EpochContext_;
    IChangelogPtr Changelog_;

    int RecoveryRecordCount_ = 0;
    i64 RecoveryDataSize_ = 0;

    std::atomic<EPeerState> State_ = {EPeerState::Stopped};

    // AutomatonVersion_ <= CommittedVersion_ <= LoggedVersion_
    // LoggedVersion_ is only maintained when the peer is active, e.g. not during recovery.
    std::atomic<TVersion> LoggedVersion_ = {};
    std::atomic<TVersion> AutomatonVersion_ = {};
    std::atomic<TVersion> CommittedVersion_ = {};

    bool RotatingChangelog_ = false;

    //! AutomatonVersion_ <= SnapshotVersion_
    TVersion SnapshotVersion_;
    TPromise<TRemoteSnapshotParams> SnapshotParamsPromise_;
    std::atomic_flag BuildingSnapshot_ = ATOMIC_FLAG_INIT;
    TInstant LastSnapshotTime_;

    struct TPendingMutation
    {
        TPendingMutation(
            TVersion version,
            TMutationRequest&& request,
            TInstant timestamp,
            ui64 randomSeed)
            : Version(version)
            , Request(std::move(request))
            , Timestamp(timestamp)
            , RandomSeed(randomSeed)
            , CommitPromise(NewPromise<TMutationResponse>())
        { }

        TVersion Version;
        TMutationRequest Request;
        TInstant Timestamp;
        ui64 RandomSeed;
        TPromise<TMutationResponse> CommitPromise;
    };

    NProto::TMutationHeader MutationHeader_; // pooled instance
    TRingQueue<TPendingMutation> PendingMutations_;

    TRingQueue<NRpc::TMutationId> PendingMutationIds_;

    NProfiling::TAggregateGauge BatchCommitTimeCounter_ = {"/batch_commit_time"};

    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler;


    void RotateAutomatonVersionIfNeeded(TVersion mutationVersion);
    void DoApplyMutation(TMutationContext* context);

    bool TryAcquireUserLock();
    void ReleaseUserLock();
    void AcquireSystemLock();
    void ReleaseSystemLock();

    void CancelSnapshot();

    void StartEpoch(TEpochContextPtr epochContext);
    void StopEpoch();

    void DoRotateChangelog();

    void ApplyPendingMutations(bool mayYield);

    TFuture<void> SaveSnapshot(NConcurrency::IAsyncOutputStreamPtr writer);
    void MaybeStartSnapshotBuilder();

    bool IsRecovery();

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

DEFINE_REFCOUNTED_TYPE(TDecoratedAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
