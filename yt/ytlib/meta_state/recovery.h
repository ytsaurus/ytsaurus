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

    typedef TAsyncResult<EResult> TResult;

    TRecovery(
        const TMetaStateManagerConfig& config,
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr decoratedState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore::TPtr snapshotStore,
        TEpoch epoch,
        TPeerId leaderId,
        IInvoker::TPtr serviceInvoker);

    void Stop();

protected:
    friend class TLeaderRecovery;
    friend class TFollowerRecovery;

    typedef TMetaStateManagerProxy TProxy;

    virtual bool IsLeader() const = 0;

    // Work thread
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
    TCancelableInvoker::TPtr CancelableServiceInvoker;
    TCancelableInvoker::TPtr CancelableStateInvoker;

};

////////////////////////////////////////////////////////////////////////////////

class TLeaderRecovery
    : public TRecovery
{
public:
    typedef TIntrusivePtr<TLeaderRecovery> TPtr;

    TLeaderRecovery(
        const TMetaStateManagerConfig& config,
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr decoratedState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore::TPtr snapshotStore,
        TEpoch epoch,
        TPeerId leaderId,
        IInvoker::TPtr serviceInvoker);

    //! Performs leader recovery loading the latest snapshot and applying the changelogs.
    TResult::TPtr Run(const TMetaVersion& version);

private:
    virtual bool IsLeader() const;

};

////////////////////////////////////////////////////////////////////////////////

class TFollowerRecovery
    : public TRecovery
{
public:
    typedef TIntrusivePtr<TFollowerRecovery> TPtr;

    TFollowerRecovery(
        const TMetaStateManagerConfig& config,
        TCellManager::TPtr cellManager,
        TDecoratedMetaState::TPtr decoratedState,
        TChangeLogCache::TPtr changeLogCache,
        TSnapshotStore::TPtr snapshotStore,
        TEpoch epoch,
        TPeerId leaderId,
        IInvoker::TPtr serviceInvoker);

    //! Performs follower recovery brining the follower up-to-date and synched with the leader.
    TResult::TPtr Run();

    //! Postpones incoming request for advancing the current segment in the master state.
    /*!
     * \param version State in which the segment should be changed.
     * \returns True when applicable request is coherent with the postponed state
     * and postponing succeeded.
     */
    EResult PostponeSegmentAdvance(const TMetaVersion& version);
    //! Postpones incoming change to the master state.
    /*!
     * \param change Incoming change.
     * \param version State in which the change should be applied.
     * \returns True when applicable change is coherent with the postponed state
     * and postponing succeeded.
     */
    EResult PostponeChange(const TMetaVersion& version, const TSharedRef& change);
    //! Handles sync response from the leader
    /*!
     * \param version Current state at leader.
     * \param epoch Current epoch at leader.
     * \param maxSnapshotId Maximum snapshot id at leader.
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

    // Service thread
    TPostponedChanges PostponedChanges;
    TMetaVersion PostponedVersion;
    bool SyncReceived;

     // Service thread
    void OnSyncTimeout();
    TResult::TPtr CapturePostponedChanges();

    // Thread-neutral.
    virtual bool IsLeader() const;
    TResult::TPtr OnSyncReached(EResult result);

    // Work thread.
    TResult::TPtr ApplyPostponedChanges(TAutoPtr<TPostponedChanges> changes);

};

////////////////////////////////////////////////////////////////////////////////
} // namespace NYT
