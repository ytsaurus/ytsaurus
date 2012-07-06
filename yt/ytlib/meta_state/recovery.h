#pragma once

#include "public.h"
#include "meta_state_manager_proxy.h"
#include "meta_version.h"

#include <ytlib/election/election_manager.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! Base class for both leader and follower recovery models.
class TRecovery
    : public TRefCounted
{
public:
    DECLARE_ENUM(EResult,
        (OK)
        (Failed)
    );

    typedef TFuture<EResult> TAsyncResult;
    typedef TPromise<EResult> TAsyncPromise;

    //! Constructs an instance.
    /*!
     * \note Thread affinity: ControlThread
     */
    TRecovery(
        TPersistentStateManagerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedMetaStatePtr decoratedState,
        TChangeLogCachePtr changeLogCache,
        TSnapshotStorePtr snapshotStore,
        const TEpoch& epoch,
        TPeerId leaderId,
        IInvokerPtr epochControlInvoker,
        IInvokerPtr epochStateInvoker);

    virtual TAsyncResult Run() = 0;

protected:
    friend class TLeaderRecovery;
    friend class TFollowerRecovery;

    typedef TMetaStateManagerProxy TProxy;

    //! Must be derived the the inheritors to control the recovery behavior.
    /*!
     * \note Thread affinity: Any.
     */
    virtual bool IsLeader() const = 0;

    //! Recovers to the desired state by first loading a snapshot
    //! and then applying changelogs, if necessary.
    /*!
     *  \param targetVersion A version to reach.
     *  \returns A future that gets set when the recovery completes.
     *  
     *  \note Thread affinity: StateThread
     */
    TAsyncResult RecoverToState(const TMetaVersion& targetVersion);

    //! Recovers to the desired state by first loading the given snapshot
    //! and then applying changelogs, if necessary.
    /*!
     *  \param targetVersion A version to reach.
     *  \param snapshotId A snapshot to start recovery with.
     *  \returns A future that gets set when the recovery completes.
     *  
     *  \note Thread affinity: StateThread
     */
    TAsyncResult RecoverToStateWithChangeLog(
        const TMetaVersion& targetVersion,
        i32 snapshotId);

    //! Recovers to the desired state by applying changelogs.
    /*!
     *  \param targetVersion A version to reach.
     *  \param expectedPrevRecordCount The 'PrevRecordCount' value that
     *  the first changelog is expected to have.
     *  \returns A future that gets set when the recovery completes.
     *  
     *  Additional unnamed parameters are due to implementation details.
     * 
     *  \note Thread affinity: StateThread
     */
    TAsyncResult ReplayChangeLogs(
        const TMetaVersion& targetVersion,
        i32 expectedPrevRecordCount);

    //! Applies records from a given changes up to a given one.
    /*!
     *  The current segment id should match that of #changeLog.
     *  
     *  The methods ensured that no mutation is applied twice.
     *  In particular, if the 'record count' of part the current version is positive, it skips
     *  the suitable prefix of #changeLog.
     *
     *  \param changeLog A changelog to apply.
     *  \param targetRecordCount The 'record count' part of the desired target version.
     *  
     * \note Thread affinity: StateThread
     */
    void ReplayChangeLog(
        TAsyncChangeLog& changeLog,
        i32 targetRecordCount);

    // Any thread.
    TPersistentStateManagerConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    TDecoratedMetaStatePtr DecoratedState;
    TChangeLogCachePtr ChangeLogCache;
    TSnapshotStorePtr SnapshotStore;
    TEpoch Epoch;
    TPeerId LeaderId;
    IInvokerPtr EpochControlInvoker;
    IInvokerPtr EpochStateInvoker;

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

//! Drives leader recovery.
class TLeaderRecovery
    : public TRecovery
{
public:
    //! Constructs an instance.
    /*!
     * \note Thread affinity: ControlThread
     */
    TLeaderRecovery(
        TPersistentStateManagerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedMetaStatePtr decoratedState,
        TChangeLogCachePtr changeLogCache,
        TSnapshotStorePtr snapshotStore,
        const TEpoch& epoch,
        IInvokerPtr epochControlInvoker,
        IInvokerPtr epochStateInvoker);

    //! Performs leader recovery loading the latest snapshot and applying the changelogs.
    /*!
     * \note Thread affinity: ControlThread
     */
    virtual TAsyncResult Run();

private:
    virtual bool IsLeader() const;

};

////////////////////////////////////////////////////////////////////////////////

//! Drives follower recovery.
class TFollowerRecovery
    : public TRecovery
{
public:
    //! Constructs an instance.
    /*!
     * \note Thread affinity: ControlThread
     */
    TFollowerRecovery(
        TPersistentStateManagerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedMetaStatePtr decoratedState,
        TChangeLogCachePtr changeLogCache,
        TSnapshotStorePtr snapshotStore,
        const TEpoch& epoch,
        TPeerId leaderId,
        IInvokerPtr epochControlInvoker,
        IInvokerPtr epochStateInvoker,
        const TMetaVersion& targetVersion);

    //! Performs follower recovery brining the follower up-to-date and synchronized with the leader.
    /*!
     * \note Thread affinity: ControlThread
     */
    virtual TAsyncResult Run();

    //! Postpones an incoming request for advancing the current segment.
    /*!
     * \param version Version at which the segment should be changed.
     * \returns True when applicable request is coherent with the postponed version.
     * 
     * \note Thread affinity: ControlThread
     */
    EResult PostponeSegmentAdvance(const TMetaVersion& version);

    //! Postpones incoming changes.
    /*!
     * \param recordsData Incoming records.
     * \param version Version at which the changes should be applied.
     * \returns True when the mutation is coherent with the postponed version.
     * 
     * \note Thread affinity: ControlThread
     */
    EResult PostponeMutations(
        const TMetaVersion& version,
        const std::vector<TSharedRef>& recordsData);

private:
    struct TPostponedMutation
    {
        DECLARE_ENUM(EType,
            (Mutation)
            (SegmentAdvance)
        );

        EType Type;
        TSharedRef RecordData;

        static TPostponedMutation CreateMutation(const TSharedRef& recordData)
        {
            return TPostponedMutation(EType::Mutation, recordData);
        }

        static TPostponedMutation CreateSegmentAdvance()
        {
            return TPostponedMutation(EType::SegmentAdvance, TSharedRef());
        }

    private:
        TPostponedMutation(EType type, const TSharedRef& recordData)
            : Type(type)
            , RecordData(recordData)
        { }
    };

    typedef std::vector<TPostponedMutation> TPostponedMutations;

    // Any thread.
    TAsyncPromise Promise;
    TMetaVersion TargetVersion;

    // Control thread
    TPostponedMutations PostponedMutations;
    TMetaVersion PostponedVersion;
    
    TAsyncResult OnSyncReached(EResult result);
    TAsyncResult CapturePostponedMutations();
    TAsyncResult ApplyPostponedMutations(TAutoPtr<TPostponedMutations> changes);

    virtual bool IsLeader() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
