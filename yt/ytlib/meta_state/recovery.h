#pragma once

#include "common.h"
#include "meta_state.h"
#include "decorated_meta_state.h"
#include "meta_state_manager_rpc.h"
#include "snapshot_downloader.h"
#include "change_log_downloader.h"
#include "change_log_cache.h"
#include "snapshot.h"
#include "snapshot_store.h"
#include "cell_manager.h"

#include "../election/election_manager.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TLeaderRecovery;
class TFollowerRecovery;

class TRecovery
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TRecovery> TPtr;

    DECLARE_ENUM(EResult,
        (OK)
        (Failed)
    );

    typedef TFuture<EResult> TResult;

    TRecovery(
        const TMetaStateManagerConfig& config,
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr decoratedState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore::TPtr snapshotStore,
        TEpoch epoch,
        TPeerId leaderId,
        IInvoker::TPtr controlInvoker);

    void Stop();

protected:
    friend class TLeaderRecovery;
    friend class TFollowerRecovery;

    typedef TMetaStateManagerProxy TProxy;

    virtual bool IsLeader() const = 0;

    // State thread
    TResult::TPtr RecoverFromSnapshot(
        TMetaVersion targetVersion,
        i32 snapshotId);
    TResult::TPtr RecoverFromChangeLog(
        TVoid,
        TSnapshotReader::TPtr,
        TMetaVersion targetVersion,
        i32 expectedPrevRecordCount);
    void ApplyChangeLog(
        TAsyncChangeLog& changeLog,
        i32 targetRecordCount);

    // Thread-neutral.
    TMetaStateManagerConfig Config;
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

class TLeaderRecovery
    : public TRecovery
{
public:
    typedef TIntrusivePtr<TLeaderRecovery> TPtr;

    /*!
     * \note Thread affinity: ControlThread.
     */
    TLeaderRecovery(
        const TMetaStateManagerConfig& config,
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr decoratedState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore::TPtr snapshotStore,
        TEpoch epoch,
        TPeerId leaderId,
        IInvoker::TPtr controlInvoker);

    //! Performs leader recovery loading the latest snapshot and applying the changelogs.
    /*!
     * \note Thread affinity: ControlThread.
     */
    TResult::TPtr Run();

private:
    virtual bool IsLeader() const;

};

////////////////////////////////////////////////////////////////////////////////

class TFollowerRecovery
    : public TRecovery
{
public:
    typedef TIntrusivePtr<TFollowerRecovery> TPtr;

    /*!
     * \note Thread affinity: ControlThread.
     */
    TFollowerRecovery(
        const TMetaStateManagerConfig& config,
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr decoratedState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore::TPtr snapshotStore,
        TEpoch epoch,
        TPeerId leaderId,
        IInvoker::TPtr controlInvoker);

    //! Performs follower recovery brining the follower up-to-date and synched with the leader.
    /*!
     * \note Thread affinity: ControlThread.
     */
    TResult::TPtr Run();

    //! Postpones incoming request for advancing the current segment in the master state.
    /*!
     * \param version State in which the segment should be changed.
     * \returns True when applicable request is coherent with the postponed state
     * and postponing succeeded.
     * \note Thread affinity: ControlThread.
     */
    EResult PostponeSegmentAdvance(const TMetaVersion& version);

    //! Postpones incoming change to the master state.
    /*!
     * \param change Incoming change.
     * \param version State in which the change should be applied.
     * \returns True when applicable change is coherent with the postponed state
     * and postponing succeeded.
     * \note Thread affinity: ControlThread.
     */
    EResult PostponeChange(const TMetaVersion& version, const TSharedRef& change);

    //! Handles sync response from the leader
    /*!
     * \param version Current state at leader.
     * \param epoch Current epoch at leader.
     * \param maxSnapshotId Maximum snapshot id at leader.
     * \note Thread affinity: ControlThread.
     */
    void Sync(
        const TMetaVersion& version,
        const TEpoch& epoch,
        i32 maxSnapshotId);

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

    // Thread-neutral.
    TResult::TPtr Result;

    // Control thread
    TPostponedChanges PostponedChanges;
    TMetaVersion PostponedVersion;
    bool SyncReceived;

    // Control thread
    void OnSyncTimeout();
    TResult::TPtr CapturePostponedChanges();
    void OnSync(TProxy::TRspSync::TPtr response);

    // Thread-neutral.
    virtual bool IsLeader() const;
    TResult::TPtr OnSyncReached(EResult result);

    // State thread.
    TResult::TPtr ApplyPostponedChanges(TAutoPtr<TPostponedChanges> changes);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
