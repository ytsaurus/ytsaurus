#pragma once

#include "public.h"
#include "chunk_pool.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

i64 GetFootprintMemorySize();
i64 GetLFAllocBufferSize();

i64 GetInputIOMemorySize(
    TJobIOConfigPtr ioConfig,
    const TChunkStripeStatistics& stat);

i64 GetSortInputIOMemorySize(const TChunkStripeStatistics& stat);

i64 GetIntermediateOutputIOMemorySize(TJobIOConfigPtr ioConfig);

i64 GetOutputWindowMemorySize(TJobIOConfigPtr ioConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

