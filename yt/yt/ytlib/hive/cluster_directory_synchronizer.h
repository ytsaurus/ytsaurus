#pragma once

#include "public.h"

#include <yt/yt/client/hive/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/signal.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

struct IClusterDirectorySynchronizer
    : public virtual TRefCounted
{
    //! Starts periodic syncs.
    virtual void Start() = 0;

    //! Stops periodic syncs.
    virtual void Stop() = 0;

    //! Returns a future that gets set with the next sync attempt.
    //! Starts the synchronizer if not started yet.
    //! Returns the result of the sync attempt.
    //! Might still fail in case of more complex issues, rather than problems with specific clusters.
    virtual TFuture<TClusterDirectoryUpdateResult> TrySync(bool force = false) = 0;

    //! Returns a future that gets set with the next sync.
    //! Starts the synchronizer if not started yet.
    virtual TFuture<void> Sync(bool force = false) = 0;

    //! Return a future that gets set once the first sync is complete.
    virtual TFuture<void> GetFirstSuccessfulSyncFuture() = 0;

    //! Return a future that gets set once the first sync is complete for the given cluster.
    //! Resolves to an error if the cluster is not found during first successful sync.
    virtual TFuture<void> GetFirstSuccessfulClusterSyncFuture(const std::string& clusterName) = 0;

    //! Raised with each synchronization (either successful or not).
    DECLARE_INTERFACE_SIGNAL(void(const TError&), Synchronized);
};

DEFINE_REFCOUNTED_TYPE(IClusterDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

IClusterDirectorySynchronizerPtr CreateClusterDirectorySynchronizer(
    TClusterDirectorySynchronizerConfigPtr config,
    NApi::IConnectionPtr directoryConnection,
    TClusterDirectoryPtr clusterDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
