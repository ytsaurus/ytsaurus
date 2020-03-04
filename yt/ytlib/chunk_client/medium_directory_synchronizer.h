#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/core/actions/future.h>
#include <yt/core/actions/signal.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMediumDirectorySynchronizer
    : public TRefCounted
{
public:
    TMediumDirectorySynchronizer(
        TMediumDirectorySynchronizerConfigPtr config,
        NApi::IConnectionPtr clusterConnection,
        TMediumDirectoryPtr mediumDirectory);
    ~TMediumDirectorySynchronizer();

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

DEFINE_REFCOUNTED_TYPE(TMediumDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
