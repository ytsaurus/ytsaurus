#include "memory_tracked_deferred_chunk_meta.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TMemoryTrackedDeferredChunkMeta::TMemoryTrackedDeferredChunkMeta(TMemoryUsageTrackerGuard guard)
{
    Guard_ = std::move(guard);
}

void TMemoryTrackedDeferredChunkMeta::UpdateMemoryUsage()
{
    if (Guard_) {
        Guard_.SetSize(ByteSize());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
