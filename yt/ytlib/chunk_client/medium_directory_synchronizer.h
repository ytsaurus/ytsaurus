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
        NApi::INativeConnectionPtr connection);
    ~TMediumDirectorySynchronizer();

    TFuture<void> Sync();

    void Stop();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TMediumDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
