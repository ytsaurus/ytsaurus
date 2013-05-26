#include "stdafx.h"
#include "job_resources.h"

#include <ytlib/chunk_client/private.h>

namespace NYT {
namespace NScheduler {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NNodeTrackerClient::NProto;

using NChunkClient::MaxPrefetchWindow;
using NChunkClient::ChunkReaderMemorySize;

////////////////////////////////////////////////////////////////////

//! Additive term for each job memory usage.
//! Accounts for job proxy process and other lightweight stuff.
static const i64 FootprintMemorySize = (i64) 256 * 1024 * 1024;

//! Memory overhead caused by LFAlloc.
static const i64 LFAllocBufferSize = (i64) 64 * 1024 * 1024;

//! Nodes having less free memory are considered fully occupied,
//! thus no scheduling attempts will be made.
static const i64 LowWatermarkMemorySize = (i64) 256 * 1024 * 1024;

////////////////////////////////////////////////////////////////////

TNodeResources GetMinSpareResources()
{
    TNodeResources result;
    result.set_user_slots(1);
    result.set_cpu(1);
    result.set_memory(LowWatermarkMemorySize);
    return result;
}

const TNodeResources& MinSpareNodeResources()
{
    static auto result = GetMinSpareResources();
    return result;
}

i64 GetFootprintMemorySize()
{
    return FootprintMemorySize + GetLFAllocBufferSize();
}

i64 GetLFAllocBufferSize()
{
    return LFAllocBufferSize;
}

i64 GetOutputWindowMemorySize(TJobIOConfigPtr ioConfig)
{
    return
        ioConfig->TableWriter->SendWindowSize +
        ioConfig->TableWriter->EncodeWindowSize;
}

i64 GetIntermediateOutputIOMemorySize(TJobIOConfigPtr ioConfig)
{
    return
        (GetOutputWindowMemorySize(ioConfig) +
        ioConfig->TableWriter->MaxBufferSize) * 2; // possibly writing two (or even more) chunks at the time of chunk change
}

i64 GetInputIOMemorySize(
    TJobIOConfigPtr ioConfig,
    const TChunkStripeStatistics& stat)
{
    YCHECK(stat.ChunkCount > 0);

    int concurrentReaders = std::min(stat.ChunkCount, MaxPrefetchWindow);

    i64 bufferSize = std::min(
        stat.DataSize,
        concurrentReaders * (ioConfig->TableReader->WindowSize + ioConfig->TableReader->GroupSize));
    bufferSize += concurrentReaders * ChunkReaderMemorySize;

    return std::min(bufferSize, ioConfig->TableReader->MaxBufferSize);
}

i64 GetSortInputIOMemorySize(
    TJobIOConfigPtr ioConfig,
    const TChunkStripeStatistics& stat)
{
    YCHECK(stat.ChunkCount > 0);
    return stat.DataSize + stat.ChunkCount * ChunkReaderMemorySize;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

