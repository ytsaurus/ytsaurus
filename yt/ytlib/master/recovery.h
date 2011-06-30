#pragma once

#include "common.h"
#include "master_state.h"
#include "decorated_master_state.h"
#include "master_state_manager_rpc.h"
#include "snapshot_downloader.h"
#include "change_log_downloader.h"
#include "change_log_cache.h"
#include "election_manager.h"
#include "snapshot.h"
#include "snapshot_store.h"
#include "cell_manager.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMasterRecovery
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TMasterRecovery> TPtr;
    typedef TMasterStateManagerProxy TProxy;

    enum EResult
    {
        E_OK,
        E_Failed
    };

    typedef TAsyncResult<EResult> TResult;

    // TODO: refactor!!!
    TMasterRecovery(
        const TSnapshotDownloader::TConfig& snapshotDownloaderConfig,
        const TChangeLogDownloader::TConfig& changeLogDownloaderConfig,
        TCellManager::TPtr cellManager,
        TDecoratedMasterState* decoratedState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore* snapshotStore,
        TMasterEpoch epoch,
        TMasterId leaderId,
        IInvoker::TPtr serviceInvoker,
        IInvoker::TPtr epochInvoker,
        IInvoker::TPtr workQueue);

    TResult::TPtr RecoverLeader(TMasterStateId stateId);
    TResult::TPtr RecoverFollower();

private:
    // Work thread
    EResult DoRecoverLeader(TMasterStateId targetStateId);
    TResult::TPtr OnGetCurrentStateResponse(TProxy::TRspGetCurrentState::TPtr response);
    TResult::TPtr DoRecoverFollower(TMasterStateId targetStateId, i32 maxSnapshotId);
    TResult::TPtr ApplyPostponedChanges(TAutoPtr< yvector<TBlob> > changes); // Work thread
    void ApplyChangeLog(
        TChangeLog::TPtr changeLog,
        i32 startRecordId,
        i32 recordCount);

     // Service thread
    TResult::TPtr CapturePostponedChanges();

    TSnapshotDownloader::TConfig SnapshotDownloaderConfig;
    TChangeLogDownloader::TConfig ChangeLogDownloaderConfig;
    TCellManager::TPtr CellManager;
    TDecoratedMasterState* DecoratedState;
    TChangeLogCache::TPtr ChangeLogCache;
    TSnapshotStore* SnapshotStore;
    TMasterEpoch Epoch;
    TMasterId LeaderId;
    IInvoker::TPtr ServiceInvoker;
    IInvoker::TPtr EpochInvoker;
    IInvoker::TPtr WorkQueue;

     // Service thread
    yvector<TBlob> PostponedChanges;
    TMasterStateId CurrentPostponedStateId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
