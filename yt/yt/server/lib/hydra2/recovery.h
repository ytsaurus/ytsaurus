#pragma once

#include "private.h"

#include <yt/yt/server/lib/hydra_common/distributed_hydra_manager.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/ref.h>

#include <yt/yt/core/rpc/public.h>

#include <variant>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

class TRecoveryBase
    : public TRefCounted
{
protected:
    TRecoveryBase(
        NHydra::TDistributedHydraManagerConfigPtr config,
        const NHydra::TDistributedHydraManagerOptions& options,
        const NHydra::TDistributedHydraManagerDynamicOptions& dynamicOptions,
        TDecoratedAutomatonPtr decoratedAutomaton,
        NHydra::IChangelogStorePtr changelogStore,
        NHydra::ISnapshotStorePtr snapshotStore,
        NRpc::TResponseKeeperPtr responseKeeper,
        TEpochContext* epochContext,
        NHydra::TVersion syncVersion,
        NLogging::TLogger logger);

    //! Must be derived by the inheritors to control the recovery behavior.
    virtual bool IsLeader() const = 0;

    //! Recovers to the desired state by first loading an appropriate snapshot
    //! and then applying changelogs, if necessary.
    void RecoverToVersion(NHydra::TVersion targetVersion);


    const NHydra::TDistributedHydraManagerConfigPtr Config_;
    const NHydra::TDistributedHydraManagerOptions Options_;
    const NHydra::TDistributedHydraManagerDynamicOptions DynamicOptions_;
    const TDecoratedAutomatonPtr DecoratedAutomaton_;
    const NHydra::IChangelogStorePtr ChangelogStore_;
    const NHydra::ISnapshotStorePtr SnapshotStore_;
    const NRpc::TResponseKeeperPtr ResponseKeeper_;
    TEpochContext* const EpochContext_;
    const NHydra::TVersion SyncVersion_;
    const NLogging::TLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

private:
    //! Synchronizes the changelog at follower with the leader, i.e.
    //! downloads missing records or truncates redundant ones.
    void SyncChangelog(NHydra::IChangelogPtr changelog, int changelogId);

    //! Applies records from a given changes up to a given one.
    /*!
     *  The current segment id should match that of #changeLog.
     *  The methods ensures that no mutation is applied twice.
     */
    bool ReplayChangelog(NHydra::IChangelogPtr changelog, int changelogId, int targetRecordId);
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
        NHydra::TDistributedHydraManagerConfigPtr config,
        const NHydra::TDistributedHydraManagerOptions& options,
        const NHydra::TDistributedHydraManagerDynamicOptions& dynamicOptions,
        TDecoratedAutomatonPtr decoratedAutomaton,
        NHydra::IChangelogStorePtr changelogStore,
        NHydra::ISnapshotStorePtr snapshotStore,
        NRpc::TResponseKeeperPtr responseKeeper,
        TEpochContext* epochContext,
        NLogging::TLogger logger);

    //! Performs leader recovery up to #TEpochContext::ReachableVersion.
    TFuture<void> Run();

private:
    void DoRun();

    bool IsLeader() const override;

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
        NHydra::TDistributedHydraManagerConfigPtr config,
        const NHydra::TDistributedHydraManagerOptions& options,
        const NHydra::TDistributedHydraManagerDynamicOptions& dynamicOptions,
        TDecoratedAutomatonPtr decoratedAutomaton,
        NHydra::IChangelogStorePtr changelogStore,
        NHydra::ISnapshotStorePtr snapshotStore,
        NRpc::TResponseKeeperPtr responseKeeper,
        TEpochContext* epochContext,
        NHydra::TVersion syncVersion,
        NLogging::TLogger logger);

    //! Performs follower recovery bringing the follower up-to-date and synchronized with the leader.
    TFuture<void> Run();

    //! Postpones an incoming request for changelog rotation.
    //! Returns |false| is no more postponed are can be accepted; the caller must back off and retry.
    bool PostponeChangelogRotation(NHydra::TVersion version);

    //! Postpones incoming mutations.
    //! Returns |false| is no more postponed are can be accepted; the caller must back off and retry.
    bool PostponeMutations(NHydra::TVersion version, const std::vector<TSharedRef>& recordsData);

    //! Notifies the recovery process about the latest committed version available at leader.
    void SetCommittedVersion(NHydra::TVersion version);

private:
    struct TPostponedMutation
    {
        TSharedRef RecordData;
    };

    struct TPostponedChangelogRotation
    { };

    using TPostponedAction = std::variant<TPostponedMutation, TPostponedChangelogRotation>;

    YT_DECLARE_SPINLOCK(NThreading::TSpinLock, SpinLock_);
    std::vector<TPostponedAction> PostponedActions_;
    bool NoMorePostponedActions_ = false;
    NHydra::TVersion PostponedVersion_;
    NHydra::TVersion CommittedVersion_;

    void DoRun();

    bool IsLeader() const override;

};

DEFINE_REFCOUNTED_TYPE(TFollowerRecovery)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
