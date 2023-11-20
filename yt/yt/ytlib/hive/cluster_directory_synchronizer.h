#pragma once

#include "public.h"

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

    //! Returns a future that gets set with the next sync.
    //! Starts the synchronizer if not started yet.
    virtual TFuture<void> Sync(bool force = false) = 0;

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
