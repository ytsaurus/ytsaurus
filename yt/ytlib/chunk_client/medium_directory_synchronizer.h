#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMediumDirectorySynchronizer
    : public TRefCounted
{
public:
    TMediumDirectorySynchronizer(
        TMediumDirectorySynchronizerConfigPtr config,
        TMediumDirectoryPtr mediumDirectory,
        NApi::INativeClientPtr client);
    ~TMediumDirectorySynchronizer();

    //! Synchronizes the medium directory (passed in ctor)
    //! with the cluster metadata. The returned future is set once
    //! the sync is complete (either successfully or not).
    //! Periodic syncs start upon the first call to #Sync.
    TFuture<void> Sync();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TMediumDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
