#pragma once

#include "meta_state.h"
#include "persistent_state_manager.h"
#include "decorated_meta_state.h"
#include "meta_state_manager_proxy.h"
#include "snapshot_downloader.h"
#include "change_log_downloader.h"
#include "change_log_cache.h"
#include "snapshot.h"
#include "snapshot_store.h"
#include "cell_manager.h"

#include <ytlib/election/election_manager.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TLeaderRecovery;
class TFollowerRecovery;

//! Base class for both leader and follower recovery models.
class TRecovery
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TRecovery> TPtr;

    DECLARE_ENUM(EResult,
        (OK)
        (Failed)
    );

    typedef TFuture<EResult> TAsyncResult;

    //! Constructs an instance.
    /*!
     * \note Thread affinity: ControlThread.
     */
    TRecovery(
        TPersistentStateManagerConfig* config,
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr decoratedState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore::TPtr snapshotStore,
        TEpoch epoch,
        TPeerId leaderId,
        IInvoker::TPtr controlInvoker);

    /*!
     * \note Thread affinity: Any.
     */
    void Stop();

    virtual TAsyncResult::TPtr Run() = 0;

protected:
    friend class TLeaderRecovery;
    friend class TFollowerRecovery;

    typedef TMetaStateManagerProxy TProxy;

    //! Must be derived the the inheritors to control the recovery behavior.
    /*!
     * \note Thread affinity: Any.
     */
    virtual bool IsLeader() const = 0;

    //! Recovers to a desired state by first loading a snapshot
    //! and then applying changelogs, if necessary.
    /*!
     *  \param targetVersion A version to reach.
     *  \param snapshotId A snapshot to start recovery with.
     *  \returns An async result that gets when the recovery completes.
     *  
     *  \note Thread affinity: StateThread.
     */
    TAsyncResult::TPtr RecoverFromSnapshotAndChangeLog(
        TMetaVersion targetVersion,
        i32 snapshotId);

    //! Recovers to a desired state by applying changelogs.
    /*!
     *  \param targetVersion A version to reach.
     *  \param expectedPrevRecordCount The 'PrevRecordCount' value that
     *  the first changelog is expected to have.
     *  \returns An async result that gets when the recovery completes.
     *  
     *  Additional unnamed parameters are due to implementation details.
     * 
     *  \note Thread affinity: StateThread.
     */
    TAsyncResult::TPtr RecoverFromChangeLog(
        TMetaVersion targetVersion,
        i32 expectedPrevRecordCount);

    //! Applies records from a given changes up to a given one.
    /*!
     *  The current segment id should match that of #changeLog.
     *  
     *  The methods ensured that no change is applied twice.
     *  In particular, if the 'record count' of part the current version is positive, it skips
     *  the suitable prefix of #changeLog.
     *
     *  \param changeLog A changelog to apply.
     *  \param targetRecordCount The 'record count' part of the desired target version.
     *  
     * \note Thread affinity: StateThread.
     */
    void ReplayChangeLog(
        TAsyncChangeLog& changeLog,
        i32 targetRecordCount);

    // Any thread.
    TPersistentStateManagerConfig::TPtr Config;
    TCellManager::TPtr CellManager;
    TDecoratedMetaState::TPtr MetaState;
    TChangeLogCache::TPtr ChangeLogCache;
    TSnapshotStore::TPtr SnapshotStore;
    TEpoch Epoch;
    TPeerId LeaderId;
    TCancelableInvoker::TPtr CancelableControlInvoker;
    TCancelableInvoker::TPtr CancelableStateInvoker;

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

//! Drives leader recovery.
class TLeaderRecovery
    : public TRecovery
{
public:
    typedef TIntrusivePtr<TLeaderRecovery> TPtr;

    //! Constructs an instance.
    /*!
     * \note Thread affinity: ControlThread.
     */
    TLeaderRecovery(
        TPersistentStateManagerConfig* config,
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr decoratedState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore::TPtr snapshotStore,
        TEpoch epoch,
        IInvoker::TPtr controlInvoker);

    //! Performs leader recovery loading the latest snapshot and applying the changelogs.
    /*!
     * \note Thread affinity: ControlThread.
     */
    virtual TAsyncResult::TPtr Run();

private:
    virtual bool IsLeader() const;

};

////////////////////////////////////////////////////////////////////////////////

//! Drives follower recovery.
class TFollowerRecovery
    : public TRecovery
{
public:
    typedef TIntrusivePtr<TFollowerRecovery> TPtr;

    //! Constructs an instance.
    /*!
     * \note Thread affinity: ControlThread.
     */
    TFollowerRecovery(
        TPersistentStateManagerConfig* config,
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr decoratedState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore::TPtr snapshotStore,
        TEpoch epoch,
        TPeerId leaderId,
        IInvoker::TPtr controlInvoker,
        TMetaVersion targetVersion,
        i32 maxSnapshotId);

    //! Performs follower recovery brining the follower up-to-date and synchronized with the leader.
    /*!
     * \note Thread affinity: ControlThread.
     */
    virtual TAsyncResult::TPtr Run();

    //! Postpones an incoming request for advancing the current segment.
    /*!
     * \param version Version at which the segment should be changed.
     * \returns True when applicable request is coherent with the postponed version.
     * 
     * \note Thread affinity: ControlThread.
     */
    EResult PostponeSegmentAdvance(const TMetaVersion& version);

    //! Postpones incoming changes.
    /*!
     * \param changes Incoming changes.
     * \param version Version at which the changes should be applied.
     * \returns True when the change is coherent with the postponed version.
     * 
     * \note Thread affinity: ControlThread.
     */
    EResult PostponeChanges(
        const TMetaVersion& version,
        const yvector<TSharedRef>& changes);

private:
    struct TPostponedChange
    {
        DECLARE_ENUM(EType,
            (Change)
            (SegmentAdvance)
        );

        EType Type;
        TSharedRef ChangeData;

        static TPostponedChange CreateChange(const TSharedRef& changeData)
        {
            return TPostponedChange(EType::Change, changeData);
        }

        static TPostponedChange CreateSegmentAdvance()
        {
            return TPostponedChange(EType::SegmentAdvance, TSharedRef());
        }

    private:
        TPostponedChange(EType type, const TSharedRef& changeData)
            : Type(type)
            , ChangeData(changeData)
        { }
    };

    typedef yvector<TPostponedChange> TPostponedChanges;

    // Any thread.
    TAsyncResult::TPtr Result;
    TMetaVersion TargetVersion;
    i32 MaxSnapshotId;

    // Control thread
    TPostponedChanges PostponedChanges;
    TMetaVersion PostponedVersion;
    
    TAsyncResult::TPtr OnSyncReached(EResult result);

    TAsyncResult::TPtr CapturePostponedChanges();
    TAsyncResult::TPtr ApplyPostponedChanges(TAutoPtr<TPostponedChanges> changes);

    virtual bool IsLeader() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
