#pragma once

#include "private.h"
#include "distributed_hydra_manager.h"

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TRecovery
    : public TRefCounted
{
public:
    TRecovery(
        TDistributedHydraManagerConfigPtr config,
        const TDistributedHydraManagerOptions& options,
        const TDistributedHydraManagerDynamicOptions& dynamicOptions,
        TDecoratedAutomatonPtr decoratedAutomaton,
        IChangelogStorePtr changelogStore,
        ISnapshotStorePtr snapshotStore,
        NRpc::IResponseKeeperPtr responseKeeper,
        TEpochContext* epochContext,
        TReachableState targetState,
        bool isLeader,
        NLogging::TLogger logger);

    TFuture<void> Run();

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const TDistributedHydraManagerOptions Options_;
    const TDistributedHydraManagerDynamicOptions DynamicOptions_;
    const TDecoratedAutomatonPtr DecoratedAutomaton_;
    const IChangelogStorePtr ChangelogStore_;
    const ISnapshotStorePtr SnapshotStore_;
    const NRpc::IResponseKeeperPtr ResponseKeeper_;
    TEpochContext* const EpochContext_;
    const TReachableState TargetState_;
    const bool IsLeader_;
    const NLogging::TLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    //! Recovers to the desired state by first loading an appropriate snapshot
    //! and then applying changelogs, if necessary.
    void DoRun();

    //! Synchronizes the changelog at follower with the leader, i.e.
    //! downloads missing records or truncates redundant ones.
    void SyncChangelog(const IChangelogPtr& changelog);

    //! Applies records from a given changes up to a given one.
    /*!
     *  The current segment id should match that of #changeLog.
     *  The method ensures that no mutation is applied twice.
     */
    void ReplayChangelog(const IChangelogPtr& changelog, i64 sequenceNumber);

    void RecoverFromCurrentStateUsingChangelog(int changelogId);

    void FinishRecovery();
};

DEFINE_REFCOUNTED_TYPE(TRecovery)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
