#pragma once

#include "private.h"

#include <yt/yt/server/lib/hydra_common/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/mutation_context.h>
#include <yt/yt/server/lib/hydra_common/private.h>

#include <yt/yt/server/lib/election/public.h>

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/async_batcher.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <library/cpp/yt/memory/ref.h>

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
    NConcurrency::TPeriodicExecutorPtr AlivePeersUpdateExecutor;

    std::atomic_flag Restarting = ATOMIC_FLAG_INIT;
    bool LeaderSwitchStarted = false;
    bool LeaderLeaseExpired = false;

    TIntrusivePtr<NConcurrency::TAsyncBatcher<void>> LeaderSyncBatcher;
    std::optional<TVersion> LeaderSyncVersion;
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
    virtual void CloseChangelog(TFuture<IChangelogPtr> changelogFuture, int changelogId) = 0;
    virtual void CloseChangelog(const IChangelogPtr& changelog, int changelogId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChangelogDiscarder)

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
        TStateHashCheckerPtr stateHashChecker,
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

    ui64 GetStateHash() const;

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
        ui64 stateHash,
        TInstant timestamp,
        NConcurrency::IAsyncZeroCopyInputStreamPtr reader);

    void CheckInvariants();

    void ApplyMutationDuringRecovery(const TSharedRef& recordData);

    TFuture<TMutationResponse> TryBeginKeptRequest(const TMutationRequest& request);

    struct TPendingMutation
    {
        TPendingMutation(
            TVersion version,
            std::unique_ptr<TMutationRequest> request,
            TInstant timestamp,
            ui64 randomSeed,
            ui64 prevRandomSeed,
            i64 sequenceNumber)
            : Version(version)
            , Request(std::move(request))
            , Timestamp(timestamp)
            , RandomSeed(randomSeed)
            , PrevRandomSeed(prevRandomSeed)
            , SequenceNumber(sequenceNumber)
        { }

        TVersion Version;
        std::unique_ptr<TMutationRequest> Request;
        TInstant Timestamp;
        ui64 RandomSeed;
        ui64 PrevRandomSeed;
        i64 SequenceNumber;
        TPromise<TMutationResponse> LocalCommitPromise = NewPromise<TMutationResponse>();
    };

    const TPendingMutation& LogLeaderMutation(
        TInstant timestamp,
        std::unique_ptr<TMutationRequest> request,
        TSharedRef* recordData,
        TFuture<void>* localFlushFuture);
    const TPendingMutation& LogFollowerMutation(
        const TSharedRef& recordData,
        TFuture<void>* localFlushFuture);

    TFuture<TRemoteSnapshotParams> BuildSnapshot(bool readOnly);

    TFuture<void> RotateChangelog();

    void CommitMutations(TVersion version, bool mayYield);

    bool HasReadyMutations() const;

    void RotateAutomatonVersionAfterRecovery();

    TReign GetCurrentReign() const;
    EFinalRecoveryAction GetFinalRecoveryAction() const;

    bool IsBuildingSnapshotNow() const;
    int GetLastSuccessfulSnapshotId() const;
    bool GetLastSuccessfulSnapshotReadOnly() const;

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

    const TDistributedHydraManagerConfigPtr Config_;
    const TDistributedHydraManagerOptions Options_;
    const NElection::TCellManagerPtr CellManager_;
    const IAutomatonPtr Automaton_;
    const IInvokerPtr AutomatonInvoker_;
    const IInvokerPtr DefaultGuardedUserInvoker_;
    const IInvokerPtr ControlInvoker_;
    const IInvokerPtr SystemInvoker_;
    const ISnapshotStorePtr SnapshotStore_;
    const TStateHashCheckerPtr StateHashChecker_;
    const IChangelogDiscarderPtr ChangelogDiscarder_;

    std::atomic<int> UserLock_ = {0};
    std::atomic<int> SystemLock_ = {0};

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, EpochContextLock_);
    TEpochContextPtr EpochContext_;

    IChangelogPtr Changelog_;
    TFuture<IChangelogPtr> NextChangelogFuture_;

    int RecoveryRecordCount_ = 0;
    i64 RecoveryDataSize_ = 0;

    std::atomic<EPeerState> State_ = EPeerState::Stopped;

    // AutomatonVersion_ <= CommittedVersion_ <= LoggedVersion_
    // LoggedVersion_ is only maintained when the peer is active, e.g. not during recovery.
    std::atomic<TVersion> LoggedVersion_ = {};
    std::atomic<TVersion> AutomatonVersion_ = {};
    std::atomic<TVersion> CommittedVersion_ = {};
    std::atomic<ui64> RandomSeed_ = {};
    std::atomic<i64> SequenceNumber_ = {};
    std::atomic<ui64> StateHash_ = {};

    TInstant Timestamp_ = {};

    bool RotatingChangelog_ = false;

    //! AutomatonVersion_ <= SnapshotVersion_
    TVersion SnapshotVersion_;
    bool SnapshotReadOnly_ = false;
    TPromise<TRemoteSnapshotParams> SnapshotParamsPromise_;
    std::atomic<bool> BuildingSnapshot_ = false;
    TInstant SnapshotBuildDeadline_ = TInstant::Max();
    std::atomic<int> LastSuccessfulSnapshotId_ = -1;
    std::atomic<bool> LastSuccessfulSnapshotReadOnly_ = false;

    NProto::TMutationHeader MutationHeader_; // pooled instance
    TRingQueue<TPendingMutation> PendingMutations_;

    NProfiling::TEventTimer BatchCommitTimer_;
    NProfiling::TTimeGauge SnapshotLoadTime_;

    TForkCountersPtr ForkCounters_;

    ui64 GetLastLoggedRandomSeed() const;
    i64 GetLastLoggedSequenceNumber() const;

    void RotateAutomatonVersionIfNeeded(TVersion mutationVersion);
    void DoApplyMutation(TMutationContext* mutationContext);

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

    void UpdateLastSuccessfulSnapshotInfo(const TErrorOr<TRemoteSnapshotParams>& snapshotInfoOrError);
    void UpdateSnapshotBuildDeadline();

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TDecoratedAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
