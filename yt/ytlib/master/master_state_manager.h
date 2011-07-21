#pragma once

#include "common.h"
#include "change_log.h"
#include "change_log_cache.h"
#include "master_state.h"
#include "master_state_manager_rpc.h"
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

class TFollowerStateTracker;
class TLeaderPinger;

////////////////////////////////////////////////////////////////////////////////

class TMasterStateManager
    : public NRpc::TServiceBase
    , public IElectionCallbacks
{
public:
    typedef TIntrusivePtr<TMasterStateManager> TPtr;

    struct TConfig
    {
        Stroka LogLocation;
        Stroka SnapshotLocation;
        i32 MaxChangeCount;

        TConfig()
            : LogLocation(".")
            , SnapshotLocation(".")
            , MaxChangeCount(1000)
        { }

        void Read(TJsonObject* json)
        {
            TryRead(json, L"LogLocation", &LogLocation);
            TryRead(json, L"SnapshotLocation", &SnapshotLocation);
        }
    };

    TMasterStateManager(
        const TConfig& config,
        TCellManager::TPtr cellManager,
        IInvoker::TPtr serviceInvoker,
        IMasterState::TPtr masterState,
        NRpc::TServer::TPtr server);
    ~TMasterStateManager();

    // TODO: refactor; in-class declarations should be logically regroupped
    void Start();

    DECLARE_ENUM(EState, 
        (Stopped)
        (Elections)
        (FollowerRecovery)
        (Following)
        (LeaderRecovery)
        (Leading)
    );

    // TODO: add paired setter
    // TODO: force_inline
    EState GetState() const;

    DECLARE_ENUM(ECommitResult,
        (Committed)
        (MaybeCommitted)
        (NotCommitted)
        (InvalidState)
    );

    typedef TAsyncResult<ECommitResult> TCommitResult;

    TCommitResult::TPtr CommitChange(
        IAction::TPtr changeAction,
        TSharedRef changeData);

private:
    typedef TMasterStateManagerProxy TProxy;
    typedef NRpc::TTypedServiceException<TProxy::EErrorCode> TServiceException;

    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, ScheduleSync);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, Sync);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, GetSnapshotInfo);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, ReadSnapshot);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, GetChangeLogInfo);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, ReadChangeLog);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, ApplyChange);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, CreateSnapshot);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, PingLeader);

    void RegisterMethods();
    void SendSync(TMasterId masterId, TMasterEpoch epoch);

    // Service thread
    void OnLeaderRecovery(TRecovery::EResult result);
    void OnFollowerRecovery(TRecovery::EResult result);

    // Service thread
    void OnLocalCommit(
        TChangeCommitter::EResult result,
        TCtxApplyChange::TPtr context,
        const TMasterEpoch& epoch);

    // TODO: which thread?
    void Restart();

    // Service thread
    void StartEpoch(const TMasterEpoch& epoch);
    void StopEpoch();

    // Thread-neutral.
    void OnCreateLocalSnapshot(
        TSnapshotCreator::TLocalResult result,
        TCtxCreateSnapshot::TPtr context);

    // State invoker.
    void OnApplyChange();

    // TODO: which thread?
    ECommitResult OnChangeCommit(TChangeCommitter::EResult result);

    // IElectionCallbacks members
    virtual void StartLeading(TMasterEpoch epoch);
    virtual void StopLeading();
    virtual void StartFollowing(TMasterId leaderId, TMasterEpoch myEpoch);
    virtual void StopFollowing();
    virtual TMasterPriority GetPriority();
    virtual Stroka FormatPriority(TMasterPriority priority);

    EState State;
    TConfig Config;
    TMasterId LeaderId;
    TCellManager::TPtr CellManager;
    IInvoker::TPtr ServiceInvoker;
    IInvoker::TPtr StateInvoker;
    TElectionManager::TPtr ElectionManager;
    TChangeLogCache::TPtr ChangeLogCache;
    TSnapshotStore::TPtr SnapshotStore;
    TDecoratedMasterState::TPtr MasterState;

    // Per epoch, service thread
    TMasterEpoch Epoch;
    // TODO: refactor
    TMasterEpoch MyEpoch;
    IInvoker::TPtr EpochInvoker;
    TSnapshotCreator::TPtr SnapshotCreator;
    TLeaderRecovery::TPtr LeaderRecovery;
    TFollowerRecovery::TPtr FollowerRecovery;
    TChangeCommitter::TPtr ChangeCommitter;
    TIntrusivePtr<TFollowerStateTracker> FollowerStateTracker;
    TIntrusivePtr<TLeaderPinger> LeaderPinger;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace
