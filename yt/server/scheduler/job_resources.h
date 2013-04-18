#pragma once

#include "public.h"
#include "chunk_pool.h"

#include <ytlib/node_tracker_client/node.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

const NNodeTrackerClient::NProto::TNodeResources& MinSpareNodeResources();

i64 GetFootprintMemorySize();
i64 GetLFAllocBufferSize();

i64 GetInputIOMemorySize(
    TJobIOConfigPtr ioConfig,
    const TChunkStripeStatistics& stat);

i64 GetSortInputIOMemorySize(
    TJobIOConfigPtr ioConfig,
    const TChunkStripeStatistics& stat);

i64 GetOutputIOMemorySize(
    TJobIOConfigPtr ioConfig,
    int outputStreamCount);

i64 GetOutputWindowMemorySize(TJobIOConfigPtr ioConfig);

i64 GetIOMemorySize(
    TJobIOConfigPtr ioConfig,
    int outputStreamCount,
    const TChunkStripeStatisticsVector& stat);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
