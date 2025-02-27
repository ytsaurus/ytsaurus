#include "job_memory.h"

#include <yt/yt/client/table_client/config.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/scheduler/config.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkClient;
using namespace NChunkPools;
using namespace NScheduler;
using namespace NTableClient;

using NChunkClient::ChunkReaderMemorySize;

////////////////////////////////////////////////////////////////////////////////

//! Additive term for each job memory usage.
//! Accounts for job proxy process and other lightweight stuff.
static const i64 FootprintMemorySize = 128_MB;

static const i64 ChunkSpecOverhead = 1000;

////////////////////////////////////////////////////////////////////////////////

i64 GetFootprintMemorySize()
{
    return FootprintMemorySize;
}

i64 GetOutputWindowMemorySize(const TJobIOConfigPtr& ioConfig)
{
    return
        ioConfig->TableWriter->SendWindowSize +
        ioConfig->TableWriter->EncodeWindowSize;
}

i64 GetIntermediateOutputIOMemorySize(const TJobIOConfigPtr& ioConfig)
{
    auto result = GetOutputWindowMemorySize(ioConfig) +
        ioConfig->TableWriter->MaxBufferSize;

    return result;
}

i64 GetInputIOMemorySize(
    const TJobIOConfigPtr& ioConfig,
    const TChunkStripeStatistics& stat)
{
    if (stat.ChunkCount == 0)
        return 0;

    int concurrentReaders = std::min(stat.ChunkCount, ioConfig->TableReader->MaxParallelReaders);

    // Group can be overcommitted by one block.
    i64 groupSize = stat.MaxBlockSize + ioConfig->TableReader->GroupSize;
    i64 windowSize = std::max(stat.MaxBlockSize, ioConfig->TableReader->WindowSize);

    // Data weight here is upper bound on the cumulative size of uncompressed blocks.
    i64 bufferSize = std::min(stat.DataWeight, concurrentReaders * (windowSize + groupSize));
    // One block for table chunk reader.
    bufferSize += concurrentReaders * (ChunkReaderMemorySize + stat.MaxBlockSize);

    i64 maxBufferSize = std::max(ioConfig->TableReader->MaxBufferSize, 2 * stat.MaxBlockSize);

    i64 blockCacheSize = ioConfig->BlockCache->CompressedData->Capacity + ioConfig->BlockCache->UncompressedData->Capacity;

    return std::min(bufferSize, maxBufferSize) + stat.ChunkCount * ChunkSpecOverhead + blockCacheSize;
}

i64 GetSortInputIOMemorySize(const TChunkStripeStatistics& stat)
{
    static const double dataOverheadFactor = 0.05;

    if (stat.ChunkCount == 0)
        return 0;

    return static_cast<i64>(
        stat.DataWeight * (1 + dataOverheadFactor) +
        stat.ChunkCount * (ChunkReaderMemorySize + ChunkSpecOverhead));
}

////////////////////////////////////////////////////////////////////////////////

TOverrunTableWriteBufferMemoryInfo::TOverrunTableWriteBufferMemoryInfo(
    TJobId jobId,
    i64 reservedMemoryForJobProxyWithFixedBuffer,
    i64 reservedMemoryForJobProxyWithEstimatedBuffer)
    : JobId_(jobId)
    , ReservedMemoryForJobProxyWithFixedBuffer_(reservedMemoryForJobProxyWithFixedBuffer)
    , ReservedMemoryForJobProxyWithEstimatedBuffer_(reservedMemoryForJobProxyWithEstimatedBuffer)
{ }

std::strong_ordering TOverrunTableWriteBufferMemoryInfo::operator <=> (const TOverrunTableWriteBufferMemoryInfo& other) const
{
    auto relativeDifference = GetRelativeDifference();
    auto otherRelativeDifference = other.GetRelativeDifference();

    if (relativeDifference < otherRelativeDifference) {
        return std::strong_ordering::greater;
    } else if (relativeDifference > otherRelativeDifference) {
        return std::strong_ordering::less;
    } else {
        return GetJobId() <=> other.GetJobId();
    }
}

double TOverrunTableWriteBufferMemoryInfo::GetRelativeDifference() const
{
    if (ReservedMemoryForJobProxyWithFixedBuffer_ > 0) {
        double difference = ReservedMemoryForJobProxyWithEstimatedBuffer_ - ReservedMemoryForJobProxyWithFixedBuffer_;
        return difference / ReservedMemoryForJobProxyWithFixedBuffer_;
    } else {
        return 0;
    }
}

TJobId TOverrunTableWriteBufferMemoryInfo::GetJobId() const
{
    return JobId_;
}

i64 TOverrunTableWriteBufferMemoryInfo::GetReservedMemoryForJobProxyWithFixedBuffer() const
{
    return ReservedMemoryForJobProxyWithFixedBuffer_;
}

i64 TOverrunTableWriteBufferMemoryInfo::GetReservedMemoryForJobProxyWithEstimatedBuffer() const
{
    return ReservedMemoryForJobProxyWithEstimatedBuffer_;
}

void TOverrunTableWriteBufferMemoryInfo::Persist(const NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, JobId_);
    Persist(context, ReservedMemoryForJobProxyWithFixedBuffer_);
    Persist(context, ReservedMemoryForJobProxyWithEstimatedBuffer_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
