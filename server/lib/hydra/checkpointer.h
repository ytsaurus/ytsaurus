#pragma once

#include "private.h"
#include "distributed_hydra_manager.h"

#include <yt/ytlib/election/public.h>

#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/error.h>

#include <tuple>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TCheckpointer
    : public TRefCounted
{
public:
    TCheckpointer(
        TDistributedHydraManagerConfigPtr config,
        const TDistributedHydraManagerOptions& options,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TLeaderCommitterPtr leaderCommitter,
        ISnapshotStorePtr snapshotStore,
        TEpochContext* epochContext,
        NLogging::TLogger logger);

    //! A changelog rotation result.
    using TRotateChangelogResult = TFuture<void>;

    struct TBuildSnapshotResult
    {
        TRotateChangelogResult RotateChangelogResult;
        TFuture<TRemoteSnapshotParams> SnapshotConstructionResult;
        int SnapshotId;
    };

    //! Starts a distributed changelog rotation.
    /*!
     *  \returns
     *  \note Thread affinity: AutomatonThread
     */
    TRotateChangelogResult RotateChangelog();

    //! Starts a distributed changelog rotation followed by snapshot construction.
    /*!
     *  \returns
     *  \note Thread affinity: AutomatonThread
     */
    TBuildSnapshotResult BuildSnapshot(bool setReadOnly);

    //! Returns |true| iff a snapshot can be built.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    bool CanBuildSnapshot() const;

    //! Returns |true| iff changelogs can be rotated.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    bool CanRotateChangelogs() const;

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const TDistributedHydraManagerOptions Options_;
    const TDecoratedAutomatonPtr DecoratedAutomaton_;
    TEpochContext* const EpochContext_;
    const NLogging::TLogger Logger;

    const NElection::TCellManagerPtr CellManager_;

    bool BuildingSnapshot_ = false;
    bool RotatingChangelogs_ = false;

    class TSession;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

};

DEFINE_REFCOUNTED_TYPE(TCheckpointer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
