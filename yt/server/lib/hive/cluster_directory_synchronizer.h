#pragma once

#include "public.h"

#include <yt/core/actions/future.h>
#include <yt/core/actions/signal.h>

#include <yt/server/object_server/public.h>
#include <yt/server/cell_master/public.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectorySynchronizer
    : public TRefCounted
{
public:
    TClusterDirectorySynchronizer(
        const TClusterDirectorySynchronizerConfigPtr& config,
        NCellMaster::TBootstrap* bootstrap,
        const NHiveClient::TClusterDirectoryPtr& clusterDirectory);

    //! Starts periodic syncs.
    void Start();

    //! Stops periodic syncs.
    void Stop();

    //! Returns a future that gets set with the next sync.
    //! Starts the synchronizer if not started yet.
    TFuture<void> Sync(bool force = false);

    //! Raised with each synchronization (either successful or not).
    DECLARE_SIGNAL(void(const TError&), Synchronized);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TClusterDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
