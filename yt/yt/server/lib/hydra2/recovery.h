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

class TRecovery
    : public TRefCounted
{
public:
    TRecovery(
        NHydra::TDistributedHydraManagerConfigPtr config,
        const NHydra::TDistributedHydraManagerOptions& options,
        const NHydra::TDistributedHydraManagerDynamicOptions& dynamicOptions,
        TDecoratedAutomatonPtr decoratedAutomaton,
        NHydra::IChangelogStorePtr changelogStore,
        NHydra::ISnapshotStorePtr snapshotStore,
        NRpc::TResponseKeeperPtr responseKeeper,
        TEpochContext* epochContext,
        TReachableState targetState,
        bool isLeader,
        NLogging::TLogger logger);

    TFuture<void> Run(int term);

    const NHydra::TDistributedHydraManagerConfigPtr Config_;
    const NHydra::TDistributedHydraManagerOptions Options_;
    const NHydra::TDistributedHydraManagerDynamicOptions DynamicOptions_;
    const TDecoratedAutomatonPtr DecoratedAutomaton_;
    const NHydra::IChangelogStorePtr ChangelogStore_;
    const NHydra::ISnapshotStorePtr SnapshotStore_;
    const NRpc::TResponseKeeperPtr ResponseKeeper_;
    TEpochContext* const EpochContext_;
    const TReachableState TargetState_;
    const bool IsLeader_;
    const NLogging::TLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

private:
    //! Recovers to the desired state by first loading an appropriate snapshot
    //! and then applying changelogs, if necessary.
    void Recover(int term);

    //! Synchronizes the changelog at follower with the leader, i.e.
    //! downloads missing records or truncates redundant ones.
    void SyncChangelog(NHydra::IChangelogPtr changelog, int changelogId);

    //! Applies records from a given changes up to a given one.
    /*!
     *  The current segment id should match that of #changeLog.
     *  The methods ensures that no mutation is applied twice.
     */
    bool ReplayChangelog(NHydra::IChangelogPtr changelog, int changelogId, i64 sequenceNumber);
};

DEFINE_REFCOUNTED_TYPE(TRecovery)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
