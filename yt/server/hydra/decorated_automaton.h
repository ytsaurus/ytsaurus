#pragma once

#include "private.h"
#include "mutation_context.h"

#include <core/misc/ref.h>
#include <core/misc/ring_queue.h>

#include <core/concurrency/thread_affinity.h>

#include <core/actions/future.h>

#include <core/rpc/public.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <ytlib/hydra/version.h>
#include <ytlib/hydra/hydra_manager.pb.h>

#include <server/election/election_manager.h>

#include <atomic>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TEpochContext
    : public NElection::TEpochContext
{
    TEpochContext()
    {
        Restarted.clear();
    }

    IInvokerPtr EpochSystemAutomatonInvoker;
    IInvokerPtr EpochUserAutomatonInvoker;
    IInvokerPtr EpochControlInvoker;
    TCheckpointerPtr Checkpointer;
    TLeaderRecoveryPtr LeaderRecovery;
    TFollowerRecoveryPtr FollowerRecovery;
    TLeaderCommitterPtr LeaderCommitter;
    TFollowerCommitterPtr FollowerCommitter;
    TFollowerTrackerPtr FollowerTracker;
    std::atomic_flag Restarted;
};

DEFINE_REFCOUNTED_TYPE(TEpochContext)

////////////////////////////////////////////////////////////////////////////////

class TSystemLockGuard
    : private TNonCopyable
{
public:
    TSystemLockGuard();
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
    TUserLockGuard();
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
        NElection::TCellManagerPtr cellManager,
        IAutomatonPtr automaton,
        IInvokerPtr automatonInvoker,
        IInvokerPtr controlInvoker,
        ISnapshotStorePtr snapshotStore,
        IChangelogStorePtr changelogStore,
        NProfiling::TProfiler profiler);

    void OnStartLeading();
    void OnLeaderRecoveryComplete();
    void OnStopLeading();
    void OnStartFollowing();
    void OnFollowerRecoveryComplete();
    void OnStopFollowing();

    DEFINE_BYVAL_RO_PROPERTY(EPeerState, State);

    IInvokerPtr CreateGuardedUserInvoker(IInvokerPtr underlyingInvoker);
    IInvokerPtr GetSystemInvoker();

    TVersion GetLoggedVersion() const;
    void SetLoggedVersion(TVersion version);

    void SetChangelog(IChangelogPtr changelog);

    i64 GetLoggedDataSize() const;
    TInstant GetLastSnapshotTime() const;

    TVersion GetAutomatonVersion() const;
    void RotateAutomatonVersion(int segmentId);

    IAutomatonPtr GetAutomaton();

    void Clear();
    void LoadSnapshot(TVersion version, TInputStream* input);

    void ApplyMutationDuringRecovery(const TSharedRef& recordData);

    void LogLeaderMutation(
        const TMutationRequest& request,
        TSharedRef* recordData,
        TFuture<void>* localFlushResult,
        TFuture<TMutationResponse>* commitResult);

    void CancelPendingLeaderMutations(const TError& error);

    void LogFollowerMutation(
        const TSharedRef& recordData,
        TFuture<void>* localFlushResult);

    TFuture<TRemoteSnapshotParams> BuildSnapshot();

    TFuture<void> RotateChangelog(TEpochContextPtr epochContext);

    void CommitMutations(TVersion version);

    TMutationContext* GetMutationContext();

private:
    friend class TUserLockGuard;
    friend class TSystemLockGuard;
    class TGuardedUserInvoker;
    class TSystemInvoker;
    class TSnapshotBuilder;

    TDistributedHydraManagerConfigPtr Config_;
    NElection::TCellManagerPtr CellManager_;
    IAutomatonPtr Automaton_;

    IInvokerPtr AutomatonInvoker_;
    IInvokerPtr ControlInvoker_;

    std::atomic<int> UserLock_;
    std::atomic<int> SystemLock_;
    IInvokerPtr SystemInvoker_;

    ISnapshotStorePtr SnapshotStore_;
    IChangelogStorePtr ChangelogStore_;

    TEpochId Epoch_;
    TMutationContext* MutationContext_ = nullptr;
    IChangelogPtr Changelog_;

    std::atomic<TVersion> LoggedVersion_;
    std::atomic<TVersion> AutomatonVersion_;

    TVersion SnapshotVersion_;
    TPromise<TRemoteSnapshotParams> SnapshotParamsPromise_;
    std::atomic_flag BuildingSnapshot_;
    TInstant LastSnapshotTime_;

    struct TPendingMutation
    {
        TVersion Version;
        TMutationRequest Request;
        TInstant Timestamp;
        ui64 RandomSeed;
        TPromise<TMutationResponse> CommitPromise;
    };

    NProto::TMutationHeader MutationHeader_; // pooled instance
    TRingQueue<TPendingMutation> PendingMutations_;

    NProfiling::TAggregateCounter BatchCommitTimeCounter_;

    NLog::TLogger Logger;
    NProfiling::TProfiler Profiler;


    void RotateAutomatonVersionIfNeeded(TVersion mutationVersion);
    void DoApplyMutation(TMutationContext* context, bool recovery);

    bool TryAcquireUserLock();
    void ReleaseUserLock();
    void AcquireSystemLock();
    void ReleaseSystemLock();

    void Reset();

    void DoRotateChangelog();

    void SaveSnapshot(TOutputStream* output);
    void MaybeStartSnapshotBuilder();


    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

DEFINE_REFCOUNTED_TYPE(TDecoratedAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
