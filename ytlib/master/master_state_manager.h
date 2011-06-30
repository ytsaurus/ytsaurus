#pragma once

#include "common.h"
#include "change_log.h"
#include "change_log_cache.h"
#include "election_manager.h"
#include "master_state.h"
#include "master_state_manager_rpc.h"
#include "snapshot.h"
#include "snapshot_creator.h"
#include "recovery.h"
#include "cell_manager.h"
#include "change_committer.h"

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
            : MaxChangeCount(1000)
        { }
    };

    TMasterStateManager(
        const TConfig& config,
        TCellManager::TPtr cellManager,
        IInvoker::TPtr serviceInvoker,
        IMasterState::TPtr masterState,
        NRpc::TServer* server);
    ~TMasterStateManager();

    void Start();

    enum EState
    {
        S_Stopped,
        S_Elections,
        S_FollowerRecovery,
        S_Following,
        S_LeaderRecovery,
        S_Leading
    };

    EState GetState() const;

    enum ECommitResult
    {
        CR_Committed,
        CR_MaybeCommitted,
        CR_NotCommitted,
        CR_InvalidState
    };

    typedef TAsyncResult<ECommitResult> TCommitResult;

    TCommitResult::TPtr CommitChange(TSharedRef change);

private:
    typedef TMasterStateManagerProxy TProxy;

    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, GetCurrentState);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, GetSnapshotInfo);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, ReadSnapshot);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, GetChangeLogInfo);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, ReadChangeLog);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, ApplyChange);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, CreateSnapshot);
    RPC_SERVICE_METHOD_DECL(NRpcMasterStateManager, PingLeader);

    void RegisterMethods();

     // Work thread
    void DoGetCurrentState(
        TCtxGetCurrentState::TPtr context,
        TMasterEpoch epoch);

    // Service thread
    void OnLeaderRecovery(TMasterRecovery::EResult result);
    void OnFollowerRecovery(TMasterRecovery::EResult result);

    // TODO: which thread?
    void OnLocalCommit(
        TChangeCommitter::EResult result,
        TCtxApplyChange::TPtr context);

    // TODO: which thread?
    void Restart();

    // TODO: which thread?
    void StartEpoch(TMasterEpoch epoch);
    // TODO: which thread?
    void StopEpoch();

    // TODO: which thread?
    void OnCreateLocalSnapshot(
        TSnapshotCreator::TLocalResult result,
        TCtxCreateSnapshot::TPtr context);

    // TODO: which thread?
    void OnApplyChange();

    // IElectionCallbacks members
    virtual void StartLeading(TMasterEpoch epoch);
    virtual void StopLeading();
    virtual void StartFollowing(TMasterId leaderId, TMasterEpoch epoch);
    virtual void StopFollowing();
    virtual i64 GetPriority();
    virtual Stroka FormatPriority(i64 priority);

    EState State;
    TConfig Config;
    TMasterId LeaderId;
    TCellManager::TPtr CellManager;
    IInvoker::TPtr ServiceInvoker;
    IInvoker::TPtr WorkQueue;
    TElectionManager::TPtr ElectionManager;
    TChangeLogCache::TPtr ChangeLogCache;
    THolder<TSnapshotStore> SnapshotStore;
    THolder<TDecoratedMasterState> MasterState;

    // Per epoch, service thread
    TMasterEpoch Epoch;
    IInvoker::TPtr EpochInvoker;
    TSnapshotCreator::TPtr SnapshotCreator;
    TMasterRecovery::TPtr Recovery;
    TChangeCommitter::TPtr ChangeCommitter;
    TIntrusivePtr<TFollowerStateTracker> FollowerStateTracker;
    TIntrusivePtr<TLeaderPinger> LeaderPinger;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace
