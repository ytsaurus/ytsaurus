#include <yt/yt/server/lib/chunk_pools/input_stream.h>
#include <yt/yt/server/lib/chunk_pools/ordered_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/unordered_chunk_pool.h>

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/yt/ytlib/chunk_pools/chunk_pool_factory.h>

#include <yt/yt/client/table_client/row_buffer.h>

namespace NYT::NChunkPools {

using namespace NControllerAgent;
using namespace NLogging;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const i64 InfiniteCount = std::numeric_limits<i64>::max() / 4;
static const int InfinitePartitionCount = std::numeric_limits<int>::max() / 4;
static const i64 InfiniteWeight = std::numeric_limits<i64>::max() / 4;
static const double SliceDataWeightMultiplier = 0.51;

////////////////////////////////////////////////////////////////////////////////

IJobSizeConstraintsPtr CreateJobSizeConstraints(i64 dataWeightPerPartition, std::optional<int> maxPartitionCount)
{
    return CreateExplicitJobSizeConstraints(
        /*canAdjustDataWeightPerJob*/ maxPartitionCount.has_value(),
        /*isExplicitJobCount*/ maxPartitionCount.has_value(),
        /*jobCount*/ maxPartitionCount.value_or(InfinitePartitionCount),
        /*dataWeightPerJob*/ dataWeightPerPartition,
        /*primaryDataWeightPerJob*/ InfiniteWeight,
        /*maxDataSlicesPerJob*/ InfiniteCount,
        /*maxDataWeightPerJob*/ InfiniteWeight,
        /*primaryMaxDataWeightPerJob*/ InfiniteWeight,
        /*inputSliceDataWeight*/ std::clamp<i64>(SliceDataWeightMultiplier * dataWeightPerPartition, 1, dataWeightPerPartition),
        /*inputSliceRowCount*/ InfiniteCount,
        /*foreignSliceDataWeight*/ 0,
        /*samplingRate*/ std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

IChunkPoolPtr CreateChunkPool(
    ETablePartitionMode partitionMode,
    i64 dataWeightPerPartition,
    std::optional<int> maxPartitionCount,
    TLogger logger)
{
    auto jobSizeConstraints = CreateJobSizeConstraints(dataWeightPerPartition, maxPartitionCount);

    switch (partitionMode) {
        case ETablePartitionMode::Ordered:
            return CreateOrderedChunkPool(
                TOrderedChunkPoolOptions{
                    .MaxTotalSliceCount = InfiniteCount,
                    .MinTeleportChunkSize = InfiniteWeight,
                    .JobSizeConstraints = jobSizeConstraints,
                    .EnablePeriodicYielder = true,
                    .ShouldSliceByRowIndices = true,
                    .Logger = std::move(logger),
                },
                TInputStreamDirectory());

        case ETablePartitionMode::Sorted:
            THROW_ERROR_EXCEPTION("Sorted partitioning is not supported yet");

        case ETablePartitionMode::Unordered:
            return CreateUnorderedChunkPool(
                TUnorderedChunkPoolOptions{
                    .JobSizeConstraints = CreateJobSizeConstraints(dataWeightPerPartition, maxPartitionCount),
                    .RowBuffer = New<TRowBuffer>(),
                    .Logger = std::move(logger),
                },
                TInputStreamDirectory());

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
