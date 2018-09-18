#pragma once

#include "public.h"

#include <yt/server/chunk_pools/chunk_pool.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

i64 GetFootprintMemorySize();
i64 GetYTAllocLargeUnreclaimableBytes();

i64 GetInputIOMemorySize(
    NScheduler::TJobIOConfigPtr ioConfig,
    const NChunkPools::TChunkStripeStatistics& stat);

i64 GetSortInputIOMemorySize(const NChunkPools::TChunkStripeStatistics& stat);

i64 GetIntermediateOutputIOMemorySize(NScheduler::TJobIOConfigPtr ioConfig);

i64 GetOutputWindowMemorySize(NScheduler::TJobIOConfigPtr ioConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

