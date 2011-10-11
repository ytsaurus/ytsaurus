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
#include "../actions/signal.h"
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

    typedef TFuture<ECommitResult> TCommitResult;

    TMetaStateManager(
        const TConfig& config,
        IInvoker::TPtr controlInvoker,
        IMetaState::TPtr metaState,
        NRpc::TServer::TPtr server);

    ~TMetaStateManager();

    //! Boots up the manager.
    /*!
     * \note Thread affinity: any
     */
    void Start();

    // TODO: provide stop method

    //! Returns the status as seen in the control thread.
    /*!
     * \note Thread affinity: ControlThread
     */
    EPeerStatus GetControlStatus() const;

    //! Returns the status as seen in the state thread.
    /*!
     * \note Thread affinity: StateThread
     */
    EPeerStatus GetStateStatus() const;

    //! Returns an invoker used for updating the state.
    /*!
     * \note Thread affinity: any
     */
    IInvoker::TPtr GetStateInvoker();

    //! Returns a cancelable invoker that corresponds to the state thread and is only valid
    //! for the duration of the current epoch.
    /*!
     * \note Thread affinity: StateThread
     */
    IInvoker::TPtr GetEpochStateInvoker();

    //! Returns an invoker used for updating the state.
    /*!
     * \note Thread affinity: any
     */
    IInvoker::TPtr GetSnapshotInvoker();

    /*!
     * \note Thread affinity: StateThread
     */
    TCommitResult::TPtr CommitChangeSync(
        const TSharedRef& changeData,
        ECommitMode mode = ECommitMode::NeverFails);

    /*!
     * \note Thread affinity: StateThread
     */
    TCommitResult::TPtr CommitChangeSync(
        IAction::TPtr changeAction,
        const TSharedRef& changeData,
        ECommitMode mode = ECommitMode::NeverFails);

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

    //! Returns monitoring info.
    /*!
     * \note Thread affinity: any
     */
    void GetMonitoringInfo(NYTree::IYsonConsumer* consumer);

    TSignal& OnStartLeading2();
    TSignal& OnStopLeading2();
    TSignal& OnStartFollowing2();
    TSignal& OnStopFollowing2();
    TSignal& OnRecoveryComplete2();

private:
    typedef TMetaStateManager TThis;
    typedef TMetaStateManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    EPeerStatus ControlStatus;
    EPeerStatus StateStatus;
    TConfig Config;
    TPeerId LeaderId;
    TCellManager::TPtr CellManager;
    IInvoker::TPtr ControlInvoker;
    NElection::TElectionManager::TPtr ElectionManager;
    TChangeLogCache::TPtr ChangeLogCache;
    TSnapshotStore::TPtr SnapshotStore;
    TDecoratedMetaState::TPtr MetaState;

    // Per epoch, service thread
    TEpoch Epoch;
    TCancelableInvoker::TPtr EpochControlInvoker;
    TCancelableInvoker::TPtr EpochStateInvoker;
    TSnapshotCreator::TPtr SnapshotCreator;
    TLeaderRecovery::TPtr LeaderRecovery;
    TFollowerRecovery::TPtr FollowerRecovery;

    TLeaderCommitter::TPtr LeaderCommitter;
    TFollowerCommitter::TPtr FollowerCommitter;

    TIntrusivePtr<TFollowerTracker> FollowerTracker;
    TIntrusivePtr<TLeaderPinger> LeaderPinger;

    TSignal OnStartLeading_;
    TSignal OnStopLeading_;
    TSignal OnStartFollowing_;
    TSignal OnStopFollowing_;
    TSignal OnRecoveryComplete_;

    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, Sync);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, GetSnapshotInfo);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ReadSnapshot);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, GetChangeLogInfo);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ReadChangeLog);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, ApplyChanges);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, AdvanceSegment);
    RPC_SERVICE_METHOD_DECL(NMetaState::NProto, PingLeader);

    void RegisterMethods();
    void SendSync(const TEpoch& epoch, TCtxSync::TPtr context);

    void OnLeaderRecoveryComplete(TRecovery::EResult result);
    void OnFollowerRecoveryComplete(TRecovery::EResult result);

    void OnLocalCommit(
        TLeaderCommitter::EResult result,
        TCtxApplyChanges::TPtr context);

    void Restart();

    void OnCreateLocalSnapshot(
        TSnapshotCreator::TLocalResult result,
        TCtxAdvanceSegment::TPtr context);

    void OnApplyChange();

    ECommitResult OnChangeCommit(TLeaderCommitter::EResult result);

    // IElectionCallbacks implementation.
    virtual void OnStartLeading(const TEpoch& epoch);
    virtual void OnStopLeading();
    virtual void OnStartFollowing(TPeerId leaderId, const TEpoch& myEpoch);
    virtual void OnStopFollowing();
    virtual TPeerPriority GetPriority();
    virtual Stroka FormatPriority(TPeerPriority priority);


    void DoStartLeading();
    void DoLeaderRecoveryComplete();
    void DoStopLeading();

    void DoStartFollowing();
    void DoFollowerRecoveryComplete();
    void DoStopFollowing();

    void StartControlEpoch(const TEpoch& epoch);
    void StopControlEpoch();

    void StartStateEpoch();
    void StopStateEpoch();

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
