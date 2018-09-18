#pragma once

#include "public.h"

#include <yt/client/api/public.h>

#include <yt/core/actions/future.h>
#include <yt/core/actions/signal.h>

namespace NYT {
namespace NHiveClient {

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectorySynchronizer
    : public TRefCounted
{
public:
    TClusterDirectorySynchronizer(
        TClusterDirectorySynchronizerConfigPtr config,
        NApi::IConnectionPtr directoryConnection,
        TClusterDirectoryPtr clusterDirectory);
    ~TClusterDirectorySynchronizer();

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

} // namespace NHiveClient
} // namespace NYT
