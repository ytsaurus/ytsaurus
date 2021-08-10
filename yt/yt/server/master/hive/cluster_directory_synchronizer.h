#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/signal.h>

#include <yt/yt/server/master/object_server/public.h>
#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

struct IClusterDirectorySynchronizer
    : public TRefCounted
{
    //! Starts periodic syncs.
    virtual void Start() = 0;

    //! Stops periodic syncs.
    virtual void Stop() = 0;

    //! Returns a future that gets set with the next sync.
    //! Starts the synchronizer if not started yet.
    virtual TFuture<void> Sync(bool force = false) = 0;

    //! Reconfigure synchronizer.
    virtual void Reconfigure(const TClusterDirectorySynchronizerConfigPtr& config) = 0;

    //! Raised with each synchronization (either successful or not).
    DECLARE_INTERFACE_SIGNAL(void(const TError&), Synchronized);
};

DEFINE_REFCOUNTED_TYPE(IClusterDirectorySynchronizer)

IClusterDirectorySynchronizerPtr CreateClusterDirectorySynchronizer(
    TClusterDirectorySynchronizerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap,
    NHiveClient::TClusterDirectoryPtr clusterDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
