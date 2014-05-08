#pragma once

#include "private.h"

#include <core/misc/checksum.h>
#include <core/misc/error.h>

#include <core/concurrency/thread_affinity.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/election/public.h>

#include <ytlib/hydra/version.h>
#include <ytlib/hydra/hydra_service_proxy.h>

#include <atomic>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TChangelogRotation
    : public TExtrinsicRefCounted
{
public:
    TChangelogRotation(
        TDistributedHydraManagerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedAutomatonPtr decoratedAutomaton,
        TLeaderCommitterPtr leaderCommitter,
        ISnapshotStorePtr snapshotStore,
        const TEpochId& epochId,
        IInvokerPtr epochControlInvoker,
        IInvokerPtr epochAutomatonInvoker);

    //! Starts a distributed changelog rotation.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    TFuture<TError> RotateChangelog();

    //! Starts a distributed changelog rotation followed by snapshot construction.
    /*!
     *  \note Thread affinity: AutomatonThread
     */
    TFuture<TErrorOr<TRemoteSnapshotParams>> BuildSnapshot();

    //! Returns |true| iff a snapshot is currently being built.
    /*!
     *  \note Thread affinity: any
     */
    bool IsSnapshotInProgress() const;

private:
    TDistributedHydraManagerConfigPtr Config_;
    NElection::TCellManagerPtr CellManager_;
    TDecoratedAutomatonPtr DecoratedAutomaton_;
    TLeaderCommitterPtr LeaderCommitter_;
    ISnapshotStorePtr SnapshotStore_;
    TEpochId EpochId_;
    IInvokerPtr EpochControlInvoker_;
    IInvokerPtr EpochAutomatonInvoker_;

    std::atomic<int> SnapshotsInProgress_;

    NLog::TTaggedLogger Logger;

    class TSession;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
