#pragma once

#include "private.h"
#include "distributed_hydra_manager.h"

#include <yt/ytlib/election/public.h>

#include <yt/ytlib/hydra/version.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/ref.h>
#include <yt/core/misc/variant.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TRecoveryBase
    : public TRefCounted
{
protected:
    TRecoveryBase(
        TDistributedHydraManagerConfigPtr config,
        const TDistributedHydraManagerOptions& options,
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        IChangelogStorePtr changelogStore,
        ISnapshotStorePtr snapshotStore,
        NRpc::TResponseKeeperPtr responseKeeper,
        TEpochContext* epochContext,
        TVersion syncVersion);

    //! Must be derived by the inheritors to control the recovery behavior.
    virtual bool IsLeader() const = 0;

    //! Recovers to the desired state by first loading an appropriate snapshot
    //! and then applying changelogs, if necessary.
    void RecoverToVersion(TVersion targetVersion);


    const TDistributedHydraManagerConfigPtr Config_;
    const TDistributedHydraManagerOptions Options_;
    const NElection::TCellManagerPtr CellManager_;
    const TDecoratedAutomatonPtr DecoratedAutomaton_;
    const IChangelogStorePtr ChangelogStore_;
    const ISnapshotStorePtr SnapshotStore_;
    const NRpc::TResponseKeeperPtr ResponseKeeper_;
    TEpochContext* const EpochContext_;
    const TVersion SyncVersion_;

    NLogging::TLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

private:
    //! Synchronizes the changelog at follower with the leader, i.e.
    //! downloads missing records or truncates redundant ones.
    void SyncChangelog(IChangelogPtr changelog, int changelogId);

    //! Applies records from a given changes up to a given one.
    /*!
     *  The current segment id should match that of #changeLog.
     *  The methods ensures that no mutation is applied twice.
     */
    void ReplayChangelog(IChangelogPtr changelog, int changelogId, int targetRecordId);

};

////////////////////////////////////////////////////////////////////////////////

//! Drives the leader recovery.
/*!
 *  \note
 *  Thread affinity: any
 */
class TLeaderRecovery
    : public TRecoveryBase
{
public:
    TLeaderRecovery(
        TDistributedHydraManagerConfigPtr config,
        const TDistributedHydraManagerOptions& options,
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        IChangelogStorePtr changelogStore,
        ISnapshotStorePtr snapshotStore,
        NRpc::TResponseKeeperPtr responseKeeper,
        TEpochContext* epochContext);

    //! Performs leader recovery up to #TEpochContext::ReachableVersion.
    TFuture<void> Run();

private:
    void DoRun();

    virtual bool IsLeader() const;

};

DEFINE_REFCOUNTED_TYPE(TLeaderRecovery)

////////////////////////////////////////////////////////////////////////////////

//! Drives the follower recovery.
/*!
 *  \note
 *  Thread affinity: any
 */
class TFollowerRecovery
    : public TRecoveryBase
{
public:
    TFollowerRecovery(
        TDistributedHydraManagerConfigPtr config,
        const TDistributedHydraManagerOptions& options,
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        IChangelogStorePtr changelogStore,
        ISnapshotStorePtr snapshotStore,
        NRpc::TResponseKeeperPtr responseKeeper,
        TEpochContext* epochContext,
        TVersion syncVersion);

    //! Performs follower recovery bringing the follower up-to-date and synchronized with the leader.
    TFuture<void> Run();

    //! Postpones an incoming request for changelog rotation.
    //! Returns |false| is no more postponed are can be accepted; the caller must back off and retry.
    bool PostponeChangelogRotation(TVersion version);

    //! Postpones incoming mutations.
    //! Returns |false| is no more postponed are can be accepted; the caller must back off and retry.
    bool PostponeMutations(TVersion version, const std::vector<TSharedRef>& recordsData);

    //! Notifies the recovery process about the latest committed version available at leader.
    void SetCommittedVersion(TVersion version);

private:
    struct TPostponedMutation
    {
        TSharedRef RecordData;
    };

    struct TPostponedChangelogRotation
    { };

    using TPostponedAction = TVariant<TPostponedMutation, TPostponedChangelogRotation>;

    TSpinLock SpinLock_;
    std::vector<TPostponedAction> PostponedActions_;
    bool NoMorePostponedActions_ = false;
    TVersion PostponedVersion_;
    TVersion CommittedVersion_;

    void DoRun();

    virtual bool IsLeader() const;

};

DEFINE_REFCOUNTED_TYPE(TFollowerRecovery)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
