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
        TDecoratedMasterState::TPtr decoratedState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore* snapshotStore,
        TMasterEpoch epoch,
        TMasterId leaderId,
        IInvoker::TPtr serviceInvoker,
        IInvoker::TPtr epochInvoker,
        IInvoker::TPtr workQueue);

    TResult::TPtr RecoverLeader(TMasterStateId stateId);
    TResult::TPtr RecoverFollower();

    //! Postpones incoming request for advancing the current segment in the master state.
    /*!
     * \param stateId State in which the segment should be changed.
     * \returns True when applicable request is coherent with the postponed state
     * and postponing succeeded.
     */
    EResult PostponeSegmentAdvance(const TMasterStateId& stateId);
    //! Postpones incoming change to the master state.
    /*!
     * \param change Incoming change.
     * \param stateId State in which the change should be applied.
     * \returns True when applicable change is coherent with the postponed state
     * and postponing succeeded.
     */
    EResult PostponeChange(const TMasterStateId& stateId, const TSharedRef& change);

private:
    struct TPostponedChange
    {
        enum EType
        {
            T_Change,
            T_SegmentAdvance
        };

        EType Type;
        TSharedRef ChangeData;

        static TPostponedChange CreateChange(const TSharedRef& changeData)
        {
            return TPostponedChange(T_Change, changeData);
        }

        static TPostponedChange CreateSegmentAdvance()
        {
            return TPostponedChange(T_SegmentAdvance, TSharedRef());
        }

    private:
        TPostponedChange(EType type, const TSharedRef& changeData)
            : Type(type)
            , ChangeData(changeData)
        { }
    };

    typedef yvector<TPostponedChange> TPostponedChanges;

    // Service thread
    TPostponedChanges PostponedChanges;
    TMasterStateId PostponedStateId;

    // Work thread
    EResult DoRecoverLeader(TMasterStateId targetStateId);
    TResult::TPtr OnGetCurrentStateResponse(TProxy::TRspGetCurrentState::TPtr response);
    TResult::TPtr DoRecoverFollower(TMasterStateId targetStateId, i32 maxSnapshotId);
    TResult::TPtr ApplyPostponedChanges(TAutoPtr<TPostponedChanges> changes); // Work thread
    void ApplyChangeLog(
        TChangeLog::TPtr changeLog,
        i32 startRecordId,
        i32 recordCount);

     // Service thread
    TResult::TPtr CapturePostponedChanges();

    TSnapshotDownloader::TConfig SnapshotDownloaderConfig;
    TChangeLogDownloader::TConfig ChangeLogDownloaderConfig;
    TCellManager::TPtr CellManager;
    TDecoratedMasterState::TPtr MasterState;
    TChangeLogCache::TPtr ChangeLogCache;
    TSnapshotStore* SnapshotStore;
    TMasterEpoch Epoch;
    TMasterId LeaderId;
    IInvoker::TPtr ServiceInvoker;
    IInvoker::TPtr EpochInvoker;
    IInvoker::TPtr WorkQueue;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
