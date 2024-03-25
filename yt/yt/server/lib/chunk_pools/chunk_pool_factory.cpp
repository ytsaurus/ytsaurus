#include <yt/yt/server/lib/chunk_pools/input_stream.h>
#include <yt/yt/server/lib/chunk_pools/ordered_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/unordered_chunk_pool.h>

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/yt/ytlib/chunk_pools/chunk_pool_factory.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/misc/numeric_helpers.h>

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

class TPartitionTablesJobSizeConstraints
    : public IJobSizeConstraints
{
public:
    TPartitionTablesJobSizeConstraints() = default;

    TPartitionTablesJobSizeConstraints(
        i64 dataWeightPerPartition,
        std::optional<int> maxPartitionCount)
        : DataWeightPerPartition_(dataWeightPerPartition)
        , MaxPartitionCount_(maxPartitionCount)
    { }

    bool CanAdjustDataWeightPerJob() const override
    {
        return IsExplicitJobCount();
    }

    bool IsExplicitJobCount() const override
    {
        return MaxPartitionCount_.has_value();
    }

    int GetJobCount() const override
    {
        return MaxPartitionCount_.value_or(InfinitePartitionCount);
    }

    i64 GetDataWeightPerJob() const override
    {
        if (MaxPartitionCount_) {
            if (InputDataWeight_ > 0) {
                return DivCeil<i64>(InputDataWeight_, *MaxPartitionCount_);
            }
            return InfiniteWeight;
        }
        return DataWeightPerPartition_;
    }

    i64 GetPrimaryDataWeightPerJob() const override
    {
        return InfiniteWeight;
    }

    i64 GetMaxDataSlicesPerJob() const override
    {
        return InfiniteCount;
    }

    i64 GetMaxDataWeightPerJob() const override
    {
        return InfiniteWeight;
    }

    i64 GetMaxPrimaryDataWeightPerJob() const override
    {
        return InfiniteWeight;
    }

    i64 GetInputSliceDataWeight() const override
    {
        return std::clamp<i64>(SliceDataWeightMultiplier * DataWeightPerPartition_, 1, DataWeightPerPartition_);
    }

    i64 GetInputSliceRowCount() const override
    {
        return InfiniteCount;
    }

    std::optional<i64> GetBatchRowCount() const override
    {
        return {};
    }

    i64 GetForeignSliceDataWeight() const override
    {
        return 0;
    }

    std::optional<double> GetSamplingRate() const override
    {
        return std::nullopt;
    }

    i64 GetSamplingDataWeightPerJob() const override
    {
        YT_ABORT();
    }

    i64 GetSamplingPrimaryDataWeightPerJob() const override
    {
        YT_ABORT();
    }

    double GetDataWeightPerJobRetryFactor() const override
    {
        return 2.0;
    }

    i64 GetMaxBuildRetryCount() const override
    {
        return 5;
    }

    void UpdateInputDataWeight(i64 inputDataWeight) override
    {
        InputDataWeight_ = inputDataWeight;
    }

    void UpdatePrimaryInputDataWeight(i64 /*inputDataWeight*/) override
    { }

    void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;

        Persist(context, InputDataWeight_);
        Persist(context, DataWeightPerPartition_);
        Persist(context, MaxPartitionCount_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TPartitionTablesJobSizeConstraints, 0x25335b8e);

    i64 InputDataWeight_ = 0;
    i64 DataWeightPerPartition_;
    std::optional<int> MaxPartitionCount_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TPartitionTablesJobSizeConstraints);
DEFINE_REFCOUNTED_TYPE(TPartitionTablesJobSizeConstraints)

////////////////////////////////////////////////////////////////////////////////

IJobSizeConstraintsPtr CreateJobSizeConstraints(i64 dataWeightPerPartition, std::optional<int> maxPartitionCount)
{
    return New<TPartitionTablesJobSizeConstraints>(
        dataWeightPerPartition,
        maxPartitionCount);
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
                    .JobSizeConstraints = jobSizeConstraints,
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
