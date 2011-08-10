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

class TFollowerTracker;
class TLeaderPinger;

////////////////////////////////////////////////////////////////////////////////

class TMetaStateManager
    : public NRpc::TServiceBase
    , public IElectionCallbacks
{
public:
    typedef TIntrusivePtr<TMetaStateManager> TPtr;

    struct TConfig
    {
        Stroka LogLocation;
        Stroka SnapshotLocation;
        i32 MaxRecordCount;

        TConfig()
            : LogLocation(".")
            , SnapshotLocation(".")
            , MaxRecordCount(1000)
        { }

        void Read(TJsonObject* json)
        {
            TryRead(json, L"LogLocation", &LogLocation);
            TryRead(json, L"SnapshotLocation", &SnapshotLocation);
        }
    };

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
        TCellManager::TPtr cellManager,
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

    RPC_SERVICE_METHOD_DECL(NRpcMetaStateManager, ScheduleSync);
    RPC_SERVICE_METHOD_DECL(NRpcMetaStateManager, Sync);
    RPC_SERVICE_METHOD_DECL(NRpcMetaStateManager, GetSnapshotInfo);
    RPC_SERVICE_METHOD_DECL(NRpcMetaStateManager, ReadSnapshot);
    RPC_SERVICE_METHOD_DECL(NRpcMetaStateManager, GetChangeLogInfo);
    RPC_SERVICE_METHOD_DECL(NRpcMetaStateManager, ReadChangeLog);
    RPC_SERVICE_METHOD_DECL(NRpcMetaStateManager, ApplyChange);
    RPC_SERVICE_METHOD_DECL(NRpcMetaStateManager, CreateSnapshot);
    RPC_SERVICE_METHOD_DECL(NRpcMetaStateManager, PingLeader);

    void RegisterMethods();
    void SendSync(TPeerId peerId, TEpoch epoch);

    // Service thread
    void OnLeaderRecovery(TRecovery::EResult result);
    void OnFollowerRecovery(TRecovery::EResult result);

    // Service thread
    void OnLocalCommit(
        TChangeCommitter::EResult result,
        TCtxApplyChange::TPtr context);

    // Thread-neutral.
    void Restart();

    // Service thread
    void StartEpoch(const TEpoch& epoch);
    void StopEpoch();

    // Thread-neutral.
    void OnCreateLocalSnapshot(
        TSnapshotCreator::TLocalResult result,
        TCtxCreateSnapshot::TPtr context);

    // State invoker.
    void OnApplyChange();

    // Thread-neutral.
    ECommitResult OnChangeCommit(TChangeCommitter::EResult result);

    // IElectionCallbacks members
    virtual void StartLeading(TEpoch epoch);
    virtual void StopLeading();
    virtual void StartFollowing(TPeerId leaderId, TEpoch myEpoch);
    virtual void StopFollowing();
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
