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

#include "../election/election_manager.h"
#include "../rpc/service.h"
#include "../rpc/server.h"
#include "../actions/invoker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TFollowerTracker;
class TLeaderPinger;

////////////////////////////////////////////////////////////////////////////////

class TMetaStateManager
    : public NRpc::TServiceBase
    , public IElectionCallbacks
{
public:
    typedef TIntrusivePtr<TMetaStateManager> TPtr;
    typedef TMetaStateManagerConfig TConfig;

    DECLARE_ENUM(EState, 
        (Stopped)
        (Elections)
        (FollowerRecovery)
        (Following)
        (LeaderRecovery)
        (Leading)
    );

    DECLARE_ENUM(ECommitResult,
        (Committed)
        (MaybeCommitted)
        (NotCommitted)
        (InvalidState)
    );

    typedef TAsyncResult<ECommitResult> TCommitResult;

    TMetaStateManager(
        const TConfig& config,
        IInvoker::TPtr serviceInvoker,
        IMetaState::TPtr metaState,
        NRpc::TServer::TPtr server);
    ~TMetaStateManager();

    void Start();

    EState GetState() const;

    TCommitResult::TPtr CommitChange(
        IAction::TPtr changeAction,
        TSharedRef changeData);

private:
    typedef TMetaStateManagerProxy TProxy;
    typedef NRpc::TTypedServiceException<TProxy::EErrorCode> TServiceException;

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

    // Service thread
    void OnLeaderRecovery(TRecovery::EResult result);
    void OnFollowerRecovery(TRecovery::EResult result);

    // Service thread
    void OnLocalCommit(
        TChangeCommitter::EResult result,
        TCtxApplyChanges::TPtr context);

    // Thread-neutral.
    void Restart();

    // Service thread
    void StartEpoch(const TEpoch& epoch);
    void StopEpoch();

    // Thread-neutral.
    void OnCreateLocalSnapshot(
        TSnapshotCreator::TLocalResult result,
        TCtxAdvanceSegment::TPtr context);

    // State invoker.
    void OnApplyChange();

    // Thread-neutral.
    ECommitResult OnChangeCommit(TChangeCommitter::EResult result);

    // IElectionCallbacks members
    virtual void OnStartLeading(TEpoch epoch);
    virtual void OnStopLeading();
    virtual void OnStartFollowing(TPeerId leaderId, TEpoch myEpoch);
    virtual void OnStopFollowing();
    virtual TPeerPriority GetPriority();
    virtual Stroka FormatPriority(TPeerPriority priority);

    EState State;
    TConfig Config;
    TPeerId LeaderId;
    TCellManager::TPtr CellManager;
    IInvoker::TPtr ServiceInvoker;
    IInvoker::TPtr StateInvoker;
    TElectionManager::TPtr ElectionManager;
    TChangeLogCache::TPtr ChangeLogCache;
    TSnapshotStore::TPtr SnapshotStore;
    TDecoratedMetaState::TPtr MetaState;

    // Per epoch, service thread
    TEpoch Epoch;
    TCancelableInvoker::TPtr ServiceEpochInvoker;
    TSnapshotCreator::TPtr SnapshotCreator;
    TLeaderRecovery::TPtr LeaderRecovery;
    TFollowerRecovery::TPtr FollowerRecovery;
    TChangeCommitter::TPtr ChangeCommitter;
    TIntrusivePtr<TFollowerTracker> FollowerTracker;
    TIntrusivePtr<TLeaderPinger> LeaderPinger;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace
