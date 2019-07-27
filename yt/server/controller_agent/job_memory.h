#pragma once

#include "public.h"

#include <yt/server/lib/chunk_pools/chunk_pool.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

i64 GetFootprintMemorySize();
i64 GetYTAllocLargeUnreclaimableBytes();

i64 GetInputIOMemorySize(
    const NScheduler::TJobIOConfigPtr& ioConfig,
    const NChunkPools::TChunkStripeStatistics& stat);

i64 GetSortInputIOMemorySize(const NChunkPools::TChunkStripeStatistics& stat);

i64 GetIntermediateOutputIOMemorySize(const NScheduler::TJobIOConfigPtr& ioConfig);

i64 GetOutputWindowMemorySize(const NScheduler::TJobIOConfigPtr& ioConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

