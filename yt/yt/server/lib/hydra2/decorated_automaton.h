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

#include <library/cpp/yt/memory/ref.h>
#include <library/cpp/yt/memory/new.h>

#include <atomic>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

struct TPendingMutation final
{
    TPendingMutation(
        TVersion version,
        NHydra::TMutationRequest&& request,
        TInstant timestamp,
        ui64 randomSeed,
        ui64 prevRandomSeed,
        i64 sequenceNumber,
        int term,
        TSharedRef serializedMutation,
        TPromise<NHydra::TMutationResponse> promise = {});

    TVersion Version;
    NHydra::TMutationRequest Request;
    TInstant Timestamp;
    ui64 RandomSeed;
    ui64 PrevRandomSeed;
    i64 SequenceNumber;
    int Term;
    TSharedRef RecordData;
    TPromise<NHydra::TMutationResponse> LocalCommitPromise;
};

DEFINE_REFCOUNTED_TYPE(TPendingMutation)

using TPendingMutationPtr = TIntrusivePtr<TPendingMutation>;

////////////////////////////////////////////////////////////////////////////////

struct TEpochContext
    : public TRefCounted
{
    NElection::TCellManagerPtr CellManager;
    NHydra::IChangelogStorePtr ChangelogStore;
    TReachableState ReachableState;
    int Term = NHydra::InvalidTerm;

    IInvokerPtr EpochSystemAutomatonInvoker;
    IInvokerPtr EpochUserAutomatonInvoker;
    IInvokerPtr EpochControlInvoker;
    TRecoveryPtr Recovery;
    TLeaderCommitterPtr LeaderCommitter;
    TFollowerCommitterPtr FollowerCommitter;
    TLeaseTrackerPtr LeaseTracker;

    NConcurrency::TPeriodicExecutorPtr HeartbeatMutationCommitExecutor;
    NConcurrency::TPeriodicExecutorPtr AlivePeersUpdateExecutor;

    std::atomic_flag Restarting = ATOMIC_FLAG_INIT;
    bool LeaderSwitchStarted = false;
    bool LeaderLeaseExpired = false;
    bool AcquiringChangelog = false;
    bool CatchingUp = false;

    TIntrusivePtr<NConcurrency::TAsyncBatcher<void>> LeaderSyncBatcher;
    std::optional<i64> LeaderSyncSequenceNumber;
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
        TConfigWrapperPtr config,
        const NHydra::TDistributedHydraManagerOptions& options,
        NHydra::IAutomatonPtr automaton,
        IInvokerPtr automatonInvoker,
        IInvokerPtr controlInvoker,
        NHydra::ISnapshotStorePtr snapshotStore,
        NHydra::TStateHashCheckerPtr stateHashChecker,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler);

    void Initialize();
    void ResetState();
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

    TEpochContextPtr GetEpochContext() const;
    TEpochId GetEpochId() const;

    ui64 GetStateHash() const;
    i64 GetSequenceNumber() const;
    i64 GetRandomSeed() const;
    int GetLastMutationTerm() const;

    i64 GetMutationCountSinceLastSnapshot() const;
    i64 GetMutationSizeSinceLastSnapshot() const;

    i64 GetReliablyAppliedSequenceNumber() const;

    TReachableState GetReachableState() const;

    TVersion GetAutomatonVersion() const;

    void LoadSnapshot(
        int snapshotId,
        int lastMutationTerm,
        TVersion version,
        i64 sequenceNumber,
        ui64 randomSeed,
        ui64 stateHash,
        TInstant timestamp,
        NConcurrency::IAsyncZeroCopyInputStreamPtr reader);

    TFuture<NHydra::TMutationResponse> TryBeginKeptRequest(const NHydra::TMutationRequest& request);

    TFuture<NHydra::TRemoteSnapshotParams> BuildSnapshot(
        int snapshotId,
        i64 sequenceNumber,
        bool readOnly);

    void CheckInvariants();

    void ApplyMutationDuringRecovery(const TSharedRef& recordData);

    void ApplyMutations(const std::vector<TPendingMutationPtr>& mutations);

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

    const TConfigWrapperPtr Config_;
    const NHydra::TDistributedHydraManagerOptions Options_;
    const NElection::TCellManagerPtr CellManager_;
    const NHydra::IAutomatonPtr Automaton_;
    const IInvokerPtr AutomatonInvoker_;
    const IInvokerPtr DefaultGuardedUserInvoker_;
    const IInvokerPtr ControlInvoker_;
    const IInvokerPtr SystemInvoker_;
    const NHydra::ISnapshotStorePtr SnapshotStore_;
    const NHydra::TStateHashCheckerPtr StateHashChecker_;

    std::atomic<int> UserLock_ = 0;
    std::atomic<int> SystemLock_ = 0;

    TAtomicObject<TEpochContextPtr> EpochContext_;

    NHydra::IChangelogPtr Changelog_;

    std::atomic<EPeerState> State_ = EPeerState::Stopped;

    // Last applied mutation.
    std::atomic<TVersion> AutomatonVersion_;
    std::atomic<ui64> RandomSeed_;
    std::atomic<i64> SequenceNumber_;
    std::atomic<ui64> StateHash_;
    std::atomic<int> LastMutationTerm_ = NHydra::InvalidTerm;

    std::atomic<i64> ReliablyAppliedSequenceNumber_;

    std::atomic<i64> MutationCountSinceLastSnapshot_;
    std::atomic<i64> MutationSizeSinceLastSnapshot_;

    TInstant Timestamp_;

    int NextSnapshotId_ = -1;
    bool NextSnapshotReadOnly_ = false;
    // SequenceNumber_ <= NextSnapshotSequenceNumber_
    i64 NextSnapshotSequenceNumber_ = -1;
    TPromise<NHydra::TRemoteSnapshotParams> SnapshotParamsPromise_;
    std::atomic<bool> BuildingSnapshot_ = false;
    std::atomic<int> LastSuccessfulSnapshotId_ = NHydra::InvalidSegmentId;
    std::atomic<bool> LastSuccessfulSnapshotReadOnly_ = false;

    NProfiling::TTimeGauge SnapshotLoadTime_;

    TForkCountersPtr ForkCounters_;

    void ApplyMutation(const TPendingMutationPtr& mutation);
    void DoApplyMutation(NHydra::TMutationContext* mutationContext, TVersion mutationVersion);

    bool TryAcquireUserLock();
    void ReleaseUserLock();
    void AcquireSystemLock();
    void ReleaseSystemLock();

    void ClearState();

    void CancelSnapshot(const TError& error);

    void StartEpoch(TEpochContextPtr epochContext);
    void StopEpoch();

    TFuture<void> SaveSnapshot(NConcurrency::IAsyncOutputStreamPtr writer);
    void MaybeStartSnapshotBuilder();

    bool IsRecovery() const;
    bool IsMutationLoggingEnabled() const;

    void UpdateLastSuccessfulSnapshotInfo(const TErrorOr<NHydra::TRemoteSnapshotParams>& snapshotInfoOrError);

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TDecoratedAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
