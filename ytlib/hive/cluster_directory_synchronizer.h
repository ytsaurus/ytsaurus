#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/core/actions/future.h>

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

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TClusterDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT
