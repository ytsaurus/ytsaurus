#pragma once

#include "private.h"
#include "mutation_context.h"

#include <core/misc/ref.h>
#include <core/misc/ring_queue.h>

#include <core/concurrency/thread_affinity.h>

#include <core/actions/invoker.h>

#include <core/logging/tagged_logger.h>

#include <core/profiling/profiler.h>

#include <ytlib/hydra/version.h>
#include <ytlib/hydra/hydra_manager.pb.h>

namespace NYT {
namespace NHydra {

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
        IChangelogStorePtr changelogStore);

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

    TVersion GetAutomatonVersion() const;

    IAutomatonPtr GetAutomaton();

    void Clear();
    void Save(TOutputStream* output);
    void Load(int snapshotId, TInputStream* input);

    void ApplyMutationDuringRecovery(const TSharedRef& recordData);
    void RotateChangelogDuringRecovery();

    void LogMutationAtLeader(
        const TMutationRequest& request,
        TSharedRef* recordData,
        TFuture<void>* logResult,
        TPromise<TErrorOr<TMutationResponse>> commitResult);

    void LogMutationAtFollower(
        const TSharedRef& recordData,
        TFuture<void>* logResult);

    TFuture<TErrorOr<TSnapshotInfo>> BuildSnapshot();

    TFuture<void> RotateChangelog();

    void CommitMutations(TVersion version);

    bool FindKeptResponse(const TMutationId& id, TSharedRef* data);

    TMutationContext* GetMutationContext();

private:
    class TGuardedUserInvoker;
    class TSystemInvoker;
    class TSnapshotBuilder;

    TDistributedHydraManagerConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    IAutomatonPtr Automaton;

    IInvokerPtr AutomatonInvoker;
    IInvokerPtr ControlInvoker;

    TAtomic UserEnqueueLock;
    TAtomic SystemLock;
    IInvokerPtr SystemInvoker;

    ISnapshotStorePtr SnapshotStore;
    IChangelogStorePtr ChangelogStore;

    TResponseKeeperPtr ResponseKeeper;

    TEpochId Epoch;
    TMutationContext* MutationContext;
    IChangelogPtr CurrentChangelog;

    TSpinLock VersionSpinLock;
    TVersion LoggedVersion;
    TVersion AutomatonVersion;

    TVersion SnapshotVersion;
    TPromise<TErrorOr<TSnapshotInfo>> SnapshotInfoPromise;

    struct TPendingMutation
    {
        TVersion Version;
        TMutationRequest Request;
        TInstant Timestamp;
        ui64 RandomSeed;
        TPromise<TErrorOr<TMutationResponse>> CommitPromise;
    };

    NProto::TMutationHeader MutationHeader; // pooled instance
    TRingQueue<TPendingMutation> PendingMutations;

    NProfiling::TAggregateCounter BatchCommitTimeCounter;

    NLog::TTaggedLogger Logger;


    void DoApplyMutation(const TSharedRef& recordData);
    void DoApplyMutation(TMutationContext* context);

    IChangelogPtr GetCurrentChangelog();

    bool AcquireUserEnqueueLock();
    void ReleaseUserEnqueueLock();
    void AcquireSystemLock();
    void ReleaseSystemLock();

    void Reset();

    void DoRotateChangelog(IChangelogPtr changelog);

    void MaybeStartSnapshotBuilder();


    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(IOThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
