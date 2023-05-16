#pragma once

#include "deferred_chunk_meta.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMemoryTrackedDeferredChunkMeta
    : public TDeferredChunkMeta
{
public:
    TMemoryTrackedDeferredChunkMeta() = default;
    explicit TMemoryTrackedDeferredChunkMeta(TMemoryUsageTrackerGuard guard);

    void UpdateMemoryUsage();

private:
    TMemoryUsageTrackerGuard Guard_;
};

DEFINE_REFCOUNTED_TYPE(TMemoryTrackedDeferredChunkMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
