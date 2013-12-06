#pragma once

#include "private.h"

#include <core/concurrency/thread_affinity.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/election/public.h>

#include <ytlib/hydra/version.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Base class for both leader and follower recovery models.
class TRecovery
    : public TRefCounted
{
public:
    TRecovery(
        TDistributedHydraManagerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        IChangelogStorePtr changelogStore,
        ISnapshotStorePtr snapshotStore,
        const TEpochId& epoch,
        TPeerId leaderId,
        IInvokerPtr epochAutomatonInvoker);

protected:
    friend class TLeaderRecovery;
    friend class TFollowerRecovery;

    //! Must be derived the the inheritors to control the recovery behavior.
    virtual bool IsLeader() const = 0;

    //! Recovers to the desired state by first loading an appropriate snapshot
    //! and then applying changelogs, if necessary.
    void RecoverToVersion(TVersion targetVersion);

    //! Recovers to the desired version by first loading the given snapshot
    //! and then applying changelogs, if necessary.
    void RecoverToVersionWithSnapshot(TVersion targetVersion, int snapshotId);

    //! Recovers to the desired state by applying changelogs.
    void ReplayChangelogs(TVersion targetVersion, int expectedPrevRecordCount);

    //! Synchronizes the changelog at follower with the leader, i.e.
    //! downloads missing records or truncates redundant ones.
    void SyncChangelog(IChangelogPtr changelog);

    //! Applies records from a given changes up to a given one.
    /*!
     *  The current segment id should match that of #changeLog.
     *  The methods ensures that no mutation is applied twice.
     */
    void ReplayChangelog(IChangelogPtr changelog, int targetRecordId);


    TDistributedHydraManagerConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    TDecoratedAutomatonPtr DecoratedAutomaton;
    IChangelogStorePtr ChangelogStore;
    ISnapshotStorePtr SnapshotStore;
    TEpochId EpochId;
    TPeerId LeaderId;
    IInvokerPtr EpochAutomatonInvoker;
    TVersion SyncVersion;

    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

};

////////////////////////////////////////////////////////////////////////////////

//! Drives leader recovery.
class TLeaderRecovery
    : public TRecovery
{
public:
    TLeaderRecovery(
        TDistributedHydraManagerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        IChangelogStorePtr changelogStore,
        ISnapshotStorePtr snapshotStore,
        const TEpochId& epochId,
        IInvokerPtr epochAutomatonInvoker);

    //! Performs leader recovery up to a given version.
    TAsyncError Run(TVersion targetVersion);

private:
    TError DoRun(TVersion targetVersion, int snapshotId);

    virtual bool IsLeader() const;

};

////////////////////////////////////////////////////////////////////////////////

//! Drives follower recovery.
class TFollowerRecovery
    : public TRecovery
{
public:
    TFollowerRecovery(
        TDistributedHydraManagerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        IChangelogStorePtr changelogStore,
        ISnapshotStorePtr snapshotStore,
        const TEpochId& epoch,
        TPeerId leaderId,
        IInvokerPtr epochStateInvoker);

    //! Performs follower recovery bringing the follower up-to-date and synchronized with the leader.
    TAsyncError Run(TVersion syncVersion);

    //! Postpones an incoming request for changelog rotation.
    TError PostponeChangelogRotation(TVersion version);

    //! Postpones incoming changes.
    TError PostponeMutations(
        TVersion version,
        const std::vector<TSharedRef>& recordsData);

private:
    struct TPostponedMutation
    {
        DECLARE_ENUM(EType,
            (Mutation)
            (ChangelogRotation)
        );

        EType Type;
        TSharedRef RecordData;

        static TPostponedMutation CreateMutation(const TSharedRef& recordData)
        {
            return TPostponedMutation(EType::Mutation, recordData);
        }

        static TPostponedMutation CreateChangelogRotation()
        {
            return TPostponedMutation(EType::ChangelogRotation, TSharedRef());
        }

        TPostponedMutation(EType type, const TSharedRef& recordData)
            : Type(type)
            , RecordData(recordData)
        { }

    };

    typedef std::vector<TPostponedMutation> TPostponedMutations;

    TSpinLock SpinLock;
    TPostponedMutations PostponedMutations;
    TVersion PostponedVersion;

    TError DoRun(TVersion syncVersion);

    virtual bool IsLeader() const;

};

//////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
