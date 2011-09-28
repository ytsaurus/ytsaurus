#pragma once

#include "common.h"
#include "change_log.h"
#include "change_log_cache.h"
#include "meta_state.h"
#include "meta_state_manager_rpc.h"
#include "snapshot.h"
#include "snapshot_creator.h"
#include "recovery.h"
#include "cell_manager.h"
#include "change_committer.h"
#include "follower_tracker.h"

#include "../election/election_manager.h"
#include "../rpc/service.h"
#include "../rpc/server.h"
#include "../actions/invoker.h"
#include "../misc/thread_affinity.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TLeaderPinger;

class TMetaStateManager
    : public NRpc::TServiceBase
    , public NElection::IElectionCallbacks
{
public:
    typedef TIntrusivePtr<TMetaStateManager> TPtr;
    typedef TMetaStateManagerConfig TConfig;

    DECLARE_ENUM(ECommitResult,
        (Committed)
        (MaybeCommitted)
        (NotCommitted)
        (InvalidState)
    );

    typedef TFuture<ECommitResult> TCommitResult;

    TMetaStateManager(
        const TConfig& config,
        IInvoker::TPtr controlInvoker,
        IMetaState::TPtr metaState,
        NRpc::TServer::TPtr server);

    ~TMetaStateManager();

    /*!
     * \note Thread affinity: any
     */
    void Start();

    // TODO: provide stop method

    /*!
     * \note Thread affinity: any
     */
    EPeerState GetState() const;

    /*!
     * \note Thread affinity: StateThread
     */
    TCommitResult::TPtr CommitChangeSync(
        const TSharedRef& changeData);

    /*!
     * \note Thread affinity: StateThread
     */
    TCommitResult::TPtr CommitChangeSync(
        IAction::TPtr changeAction,
        const TSharedRef& changeData);

    /*!
     * \note Thread affinity: any
     */
    TCommitResult::TPtr CommitChangeAsync(
        const TSharedRef& changeData);

    /*!
     * \note Thread affinity: any
     */
    TCommitResult::TPtr CommitChangeAsync(
        IAction::TPtr changeAction,
        const TSharedRef& changeData);

private:
    typedef TMetaStateManager TThis;
    typedef TMetaStateManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ScheduleSync);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, Sync);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, GetSnapshotInfo);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ReadSnapshot);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, GetChangeLogInfo);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ReadChangeLog);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ApplyChanges);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, AdvanceSegment);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, PingLeader);

    void RegisterMethods();
    void SendSync(TPeerId peerId, TEpoch epoch);

    void OnLeaderRecoveryComplete(TRecovery::EResult result);
    void OnFollowerRecoveryComplete(TRecovery::EResult result);

    // Service thread
    void OnLocalCommit(
        TChangeCommitter::EResult result,
        TCtxApplyChanges::TPtr context);

    void Restart();

    void StartEpoch(const TEpoch& epoch);
    void StopEpoch();

    // Thread-neutral.
    void OnCreateLocalSnapshot(
        TSnapshotCreator::TLocalResult result,
        TCtxAdvanceSegment::TPtr context);

    void OnApplyChange();

    ECommitResult OnChangeCommit(TChangeCommitter::EResult result);

    // IElectionCallbacks members
    virtual void OnStartLeading(const TEpoch& epoch);
    virtual void OnStopLeading();
    virtual void OnStartFollowing(TPeerId leaderId, const TEpoch& myEpoch);
    virtual void OnStopFollowing();
    virtual TPeerPriority GetPriority();
    virtual Stroka FormatPriority(TPeerPriority priority);

    EPeerState State;
    TConfig Config;
    TPeerId LeaderId;
    TCellManager::TPtr CellManager;
    IInvoker::TPtr ControlInvoker;
    IInvoker::TPtr StateInvoker;
    NElection::TElectionManager::TPtr ElectionManager;
    TChangeLogCache::TPtr ChangeLogCache;
    TSnapshotStore::TPtr SnapshotStore;
    TDecoratedMetaState::TPtr MetaState;
    IAction::TPtr OnApplyChangeAction;

    // Per epoch, service thread
    TEpoch Epoch;
    TCancelableInvoker::TPtr ServiceEpochInvoker;
    TSnapshotCreator::TPtr SnapshotCreator;
    TLeaderRecovery::TPtr LeaderRecovery;
    TFollowerRecovery::TPtr FollowerRecovery;
    TChangeCommitter::TPtr ChangeCommitter;
    TIntrusivePtr<TFollowerTracker> FollowerTracker;
    TIntrusivePtr<TLeaderPinger> LeaderPinger;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
