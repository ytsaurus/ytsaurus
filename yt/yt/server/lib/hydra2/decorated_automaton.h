#pragma once

#include "private.h"

#include <yt/yt/server/lib/hydra_common/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/mutation_context.h>
#include <yt/yt/server/lib/hydra_common/private.h>

#include <yt/yt/server/lib/election/public.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/async_batcher.h>
#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/ref.h>
#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/library/profiling/sensor.h>

#include <atomic>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

struct TEpochContext
    : public TRefCounted
{
    NElection::TCellManagerPtr CellManager;
    NHydra::IChangelogStorePtr ChangelogStore;
    NHydra::TVersion ReachableVersion;

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
    NConcurrency::TPeriodicExecutorPtr AlivePeersUpdateExecutor;

    std::atomic_flag Restarting = ATOMIC_FLAG_INIT;
    bool LeaderSwitchStarted = false;
    bool LeaderLeaseExpired = false;

    TIntrusivePtr<NConcurrency::TAsyncBatcher<void>> LeaderSyncBatcher;
    std::optional<NHydra::TVersion> LeaderSyncVersion;
    TPromise<void> LeaderSyncPromise;
    NProfiling::TWallTimer LeaderSyncTimer;

    TPeerId LeaderId = InvalidPeerId;
    TEpochId EpochId;
    TAtomicObject<NElection::TPeerIdSet> AlivePeerIds;

    TCancelableContextPtr CancelableContext;
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

struct IChangelogDiscarder
    : public TRefCounted
{
    virtual void CloseChangelog(TFuture<NHydra::IChangelogPtr> changelogFuture, int changelogId) = 0;
    virtual void CloseChangelog(const NHydra::IChangelogPtr& changelog, int changelogId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChangelogDiscarder)

////////////////////////////////////////////////////////////////////////////////

class TDecoratedAutomaton
    : public TRefCounted
{
public:
    TDecoratedAutomaton(
        NHydra::TDistributedHydraManagerConfigPtr config,
        const NHydra::TDistributedHydraManagerOptions& options,
        NHydra::IAutomatonPtr automaton,
        IInvokerPtr automatonInvoker,
        IInvokerPtr controlInvoker,
        NHydra::ISnapshotStorePtr snapshotStore,
        NHydra::TStateHashCheckerPtr stateHashChecker,
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

    NHydra::TVersion GetLoggedVersion() const;
    void SetLoggedVersion(NHydra::TVersion version);

    ui64 GetRandomSeed() const;
    i64 GetSequenceNumber() const;

    ui64 GetStateHash() const;

    void SetChangelog(NHydra::IChangelogPtr changelog);

    int GetRecordCountSinceLastCheckpoint() const;
    i64 GetDataSizeSinceLastCheckpoint() const;
    TInstant GetSnapshotBuildDeadline() const;

    NHydra::TVersion GetAutomatonVersion() const;
    void RotateAutomatonVersion(int segmentId);

    NHydra::TVersion GetCommittedVersion() const;

    NHydra::TVersion GetPingVersion() const;

    void LoadSnapshot(
        int snapshotId,
        NHydra::TVersion version,
        i64 sequenceNumber,
        ui64 randomSeed,
        ui64 stateHash,
        TInstant timestamp,
        NConcurrency::IAsyncZeroCopyInputStreamPtr reader);

    void ValidateSnapshot(NConcurrency::IAsyncZeroCopyInputStreamPtr reader);

    void ApplyMutationDuringRecovery(const TSharedRef& recordData);

    TFuture<NHydra::TMutationResponse> TryBeginKeptRequest(const NHydra::TMutationRequest& request);

    struct TPendingMutation
    {
        TPendingMutation(
            NHydra::TVersion version,
            NHydra::TMutationRequest&& request,
            TInstant timestamp,
            ui64 randomSeed,
            ui64 prevRandomSeed,
            i64 sequenceNumber)
            : Version(version)
            , Request(request)
            , Timestamp(timestamp)
            , RandomSeed(randomSeed)
            , PrevRandomSeed(prevRandomSeed)
            , SequenceNumber(sequenceNumber)
        { }

        NHydra::TVersion Version;
        NHydra::TMutationRequest Request;
        TInstant Timestamp;
        ui64 RandomSeed;
        ui64 PrevRandomSeed;
        i64 SequenceNumber;
        TPromise<NHydra::TMutationResponse> LocalCommitPromise = NewPromise<NHydra::TMutationResponse>();
    };

    const TPendingMutation& LogLeaderMutation(
        TInstant timestamp,
        NHydra::TMutationRequest&& request,
        TSharedRef* recordData,
        TFuture<void>* localFlushFuture);
    const TPendingMutation& LogFollowerMutation(
        const TSharedRef& recordData,
        TFuture<void>* localFlushFuture);

    TFuture<NHydra::TRemoteSnapshotParams> BuildSnapshot();

    TFuture<void> RotateChangelog();

    void CommitMutations(NHydra::TVersion version, bool mayYield);

    bool HasReadyMutations() const;

    void RotateAutomatonVersionAfterRecovery();

    NHydra::TReign GetCurrentReign() const;
    NHydra::EFinalRecoveryAction GetFinalRecoveryAction() const;

    bool IsBuildingSnapshotNow() const;
    int GetLastSuccessfulSnapshotId() const;

private:
    friend class TUserLockGuard;
    friend class TSystemLockGuard;

    class TGuardedUserInvoker;
    class TSystemInvoker;
    class TSnapshotBuilderBase;
    class TForkSnapshotBuilder;
    class TSwitchableSnapshotWriter;
    class TNoForkSnapshotBuilder;

    const NLogging::TLogger Logger;

    const NHydra::TDistributedHydraManagerConfigPtr Config_;
    const NHydra::TDistributedHydraManagerOptions Options_;
    const NElection::TCellManagerPtr CellManager_;
    const NHydra::IAutomatonPtr Automaton_;
    const IInvokerPtr AutomatonInvoker_;
    const IInvokerPtr DefaultGuardedUserInvoker_;
    const IInvokerPtr ControlInvoker_;
    const IInvokerPtr SystemInvoker_;
    NHydra::ISnapshotStorePtr SnapshotStore_;
    const NHydra::TStateHashCheckerPtr StateHashChecker_;
    const IChangelogDiscarderPtr ChangelogDiscarder_;

    std::atomic<int> UserLock_ = {0};
    std::atomic<int> SystemLock_ = {0};

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, EpochContextLock_);
    TEpochContextPtr EpochContext_;

    NHydra::IChangelogPtr Changelog_;
    TFuture<NHydra::IChangelogPtr> NextChangelogFuture_;

    int RecoveryRecordCount_ = 0;
    i64 RecoveryDataSize_ = 0;

    std::atomic<EPeerState> State_ = {EPeerState::Stopped};

    // AutomatonVersion_ <= CommittedVersion_ <= LoggedVersion_
    // LoggedVersion_ is only maintained when the peer is active, e.g. not during recovery.
    std::atomic<NHydra::TVersion> LoggedVersion_ = {};
    std::atomic<NHydra::TVersion> AutomatonVersion_ = {};
    std::atomic<NHydra::TVersion> CommittedVersion_ = {};
    std::atomic<ui64> RandomSeed_ = {};
    std::atomic<i64> SequenceNumber_ = {};
    std::atomic<ui64> StateHash_ = {};

    TInstant Timestamp_ = {};

    bool RotatingChangelog_ = false;

    //! AutomatonVersion_ <= SnapshotVersion_
    NHydra::TVersion SnapshotVersion_;
    TPromise<NHydra::TRemoteSnapshotParams> SnapshotParamsPromise_;
    std::atomic<bool> BuildingSnapshot_ = false;
    TInstant SnapshotBuildDeadline_ = TInstant::Max();
    std::atomic<int> LastSuccessfulSnapshotId_ = -1;

    NHydra::NProto::TMutationHeader MutationHeader_; // pooled instance
    TRingQueue<TPendingMutation> PendingMutations_;

    NProfiling::TEventTimer BatchCommitTimer_;
    NProfiling::TTimeGauge SnapshotLoadTime_;

    ui64 GetLastLoggedRandomSeed() const;
    i64 GetLastLoggedSequenceNumber() const;

    void RotateAutomatonVersionIfNeeded(NHydra::TVersion mutationVersion);
    void DoApplyMutation(NHydra::TMutationContext* mutationContext);

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

    bool IsRecovery() const;
    bool IsMutationLoggingEnabled() const;

    void UpdateLastSuccessfulSnapshotInfo(const TErrorOr<NHydra::TRemoteSnapshotParams>& snapshotInfoOrError);
    void UpdateSnapshotBuildDeadline();

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TDecoratedAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
