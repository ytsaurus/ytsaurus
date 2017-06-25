#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

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

    //! Returns a future that gets set with the next sync.
    TFuture<void> Sync();

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
