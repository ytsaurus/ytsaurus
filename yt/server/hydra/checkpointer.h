#pragma once

#include "private.h"

#include <core/misc/checksum.h>
#include <core/misc/error.h>

#include <core/actions/signal.h>

#include <core/concurrency/thread_affinity.h>

#include <core/logging/log.h>

#include <ytlib/election/public.h>

#include <tuple>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TCheckpointer
    : public TRefCounted
{
public:
    TCheckpointer(
        TDistributedHydraManagerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TLeaderCommitterPtr leaderCommitter,
        ISnapshotStorePtr snapshotStore,
        TEpochContext* epochContext);

    //! A changelog rotation result.
    typedef TFuture<void> TRotateChangelogResult;

    //! A pair consisting of a changelog rotation result and a snapshot construction result.
    typedef std::tuple<
        TFuture<void>,
        TFuture<TRemoteSnapshotParams>
    > TBuildSnapshotResult;

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
    TBuildSnapshotResult BuildSnapshot();

    //! Returns |true| iff a snapshot can be built.
    /*!
     *  \note Thread affinity: any
     */
    bool CanBuildSnapshot() const;

    //! Returns |true| iff changelogs can be rotated.
    /*!
     *  \note Thread affinity: any
     */
    bool CanRotateChangelogs() const;

private:
    TDistributedHydraManagerConfigPtr Config_;
    NElection::TCellManagerPtr CellManager_;
    TDecoratedAutomatonPtr DecoratedAutomaton_;
    TLeaderCommitterPtr LeaderCommitter_;
    ISnapshotStorePtr SnapshotStore_;
    TEpochContext* EpochContext_;

    bool BuildingSnapshot_ = false;
    bool RotatingChangelogs_ = false;

    NLog::TLogger Logger;

    class TSession;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

};

DEFINE_REFCOUNTED_TYPE(TCheckpointer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
