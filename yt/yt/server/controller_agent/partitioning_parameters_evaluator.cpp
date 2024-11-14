#include "partitioning_parameters_evaluator.h"

#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/ytlib/scheduler/config.h>

namespace NYT::NControllerAgent {

using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

class TPartitioningParametersEvaluator
    : public IPartitioningParametersEvaluator
{
public:
    TPartitioningParametersEvaluator(
        TSortOperationSpecBasePtr spec,
        TSortOperationOptionsBasePtr options,
        i64 totalEstimatedInputDataWeight,
        i64 totalEstimatedInputUncompressedDataSize,
        i64 totalEstimatedInputValueCount,
        double inputCompressionRatio,
        int partitionJobCount)
        : Spec_(std::move(spec))
        , Options_(std::move(options))
        , TotalEstimatedInputDataWeight_(totalEstimatedInputDataWeight)
        , TotalEstimatedInputUncompressedDataSize_(totalEstimatedInputUncompressedDataSize)
        , TotalEstimatedInputValueCount_(totalEstimatedInputValueCount)
        , InputCompressionRatio_(inputCompressionRatio)
        , PartitionJobCount_(partitionJobCount)
    { }

    i64 SuggestSampleCount() const override
    {
        return static_cast<i64>(Spec_->SamplesPerPartition) * SuggestPartitionCount(
            /*fetchedSamplesCount*/ std::nullopt,
            /*forceLegacy*/ false);
    }

    int SuggestPartitionCount(std::optional<i64> fetchedSamplesCount, bool forceLegacy) const override
    {
        int partitionCount = Spec_->UseNewPartitionsHeuristic && !forceLegacy
            ? SuggestPartitionCountNew()
            : SuggestPartitionCountLegacy();

        if (fetchedSamplesCount) {
            // Don't create more partitions than we have samples (plus one).
            partitionCount = std::min<i64>(partitionCount, *fetchedSamplesCount + 1);
        }

        return partitionCount;
    }

    int SuggestMaxPartitionFactor(int finalPartitionCount) const override
    {
        if (Spec_->MaxPartitionFactor) {
            return *Spec_->MaxPartitionFactor;
        }

        if (!Spec_->UseNewPartitionsHeuristic) {
            return finalPartitionCount;
        }

        i64 partitionFactorLimit = ComputePartitionFactorLimit();

        auto maxPartitionsWithDepth = [](i64 depth, i64 partitionFactor) {
            i64 partitions = 1;
            for (i64 level = 1; level <= depth; ++level) {
                partitions *= partitionFactor;
            }

            return partitions;
        };

        i64 depth = 1;
        while (maxPartitionsWithDepth(depth, partitionFactorLimit) < finalPartitionCount) {
            ++depth;
        }

        int maxPartitionFactor = 2;
        while (maxPartitionsWithDepth(depth, maxPartitionFactor) < finalPartitionCount) {
            ++maxPartitionFactor;
        }

        return maxPartitionFactor;
    }

private:
    const TSortOperationSpecBasePtr Spec_;
    const TSortOperationOptionsBasePtr Options_;
    const i64 TotalEstimatedInputDataWeight_;
    const i64 TotalEstimatedInputUncompressedDataSize_;
    const i64 TotalEstimatedInputValueCount_;
    const double InputCompressionRatio_;
    const int PartitionJobCount_;

    i64 ComputePartitionFactorLimit() const
    {
        return std::min<i64>(
            Options_->MaxPartitionFactor,
            DivCeil(Spec_->PartitionJobIO->TableWriter->MaxBufferSize, Options_->MinUncompressedBlockSize));
    }

    i64 ComputeDataWeightAfterPartition() const
    {
        return 1 + static_cast<i64>(TotalEstimatedInputDataWeight_ * Spec_->MapSelectivityFactor);
    }

    int SuggestPartitionCountNew() const
    {
        i64 dataWeightAfterPartition = ComputeDataWeightAfterPartition();

        i64 partitionFactorLimit = ComputePartitionFactorLimit();

        int partitionCount;

        if (Spec_->PartitionCount) {
            partitionCount = *Spec_->PartitionCount;
        } else if (Spec_->PartitionDataWeight) {
            partitionCount = DivCeil(dataWeightAfterPartition, *Spec_->PartitionDataWeight);
        } else {
            i64 partitionSize = std::max<i64>(Spec_->DataWeightPerShuffleJob * Spec_->PartitionSizeFactor, 1);
            partitionCount = DivCeil(dataWeightAfterPartition, partitionSize);

            if (partitionCount == 1 && TotalEstimatedInputUncompressedDataSize_ > Spec_->DataWeightPerShuffleJob) {
                // Sometimes data size can be much larger than data weight.
                // Let's protect from such outliers and prevent simple sort in such case.
                partitionCount = DivCeil(TotalEstimatedInputUncompressedDataSize_, Spec_->DataWeightPerShuffleJob);
            } else if (partitionCount == 1 && TotalEstimatedInputValueCount_ > Options_->MaxValueCountPerSimpleSortJob) {
                // In simple sort job memory usage is proportional to value count.
                // For very sparse tables, value count may be large, while data weight and data size remain small.
                // Multi-phase sorting doesn't materialize all row values in memory, and we should fallback to it in such corner case.
                partitionCount = 2;
            } else if (partitionCount <= partitionFactorLimit) {
                // If partition count is small, fallback to old heuristic.
                partitionCount = SuggestPartitionCountLegacy();

                // Keep partitioning single-phase.
                partitionCount = std::min<i64>(partitionCount, partitionFactorLimit);
            }
        }

        return std::clamp(partitionCount, 1, Options_->MaxNewPartitionCount);
    }

    int SuggestPartitionCountLegacy() const
    {
        i64 dataWeightAfterPartition = 1 + static_cast<i64>(TotalEstimatedInputDataWeight_ * Spec_->MapSelectivityFactor);

        // Use int64 during the initial stage to avoid overflow issues.
        i64 partitionCount;

        if (Spec_->PartitionCount) {
            partitionCount = *Spec_->PartitionCount;
        } else if (Spec_->PartitionDataWeight) {
            partitionCount = DivCeil<i64>(dataWeightAfterPartition, *Spec_->PartitionDataWeight);
        } else {
            // Rationale and details are on the wiki.
            // https://wiki.yandex-team.ru/yt/design/partitioncount/
            i64 uncompressedBlockSize = static_cast<i64>(Options_->CompressedBlockSize / InputCompressionRatio_);
            uncompressedBlockSize = std::min(uncompressedBlockSize, Spec_->PartitionJobIO->TableWriter->BlockSize);

            // Just in case compression ratio is very large.
            uncompressedBlockSize = std::max(i64(1), uncompressedBlockSize);

            // Product may not fit into i64.
            double partitionDataWeight = sqrt(dataWeightAfterPartition) * sqrt(uncompressedBlockSize);
            partitionDataWeight = std::max(partitionDataWeight, static_cast<double>(Options_->MinPartitionWeight));

            i64 maxPartitionCount = Spec_->PartitionJobIO->TableWriter->MaxBufferSize / uncompressedBlockSize;
            partitionCount = std::min(static_cast<i64>(dataWeightAfterPartition / partitionDataWeight), maxPartitionCount);

            if (partitionCount == 1 && TotalEstimatedInputUncompressedDataSize_ > Spec_->DataWeightPerShuffleJob) {
                // Sometimes data size can be much larger than data weight.
                // Let's protect from such outliers and prevent simple sort in such case.
                partitionCount = DivCeil(TotalEstimatedInputUncompressedDataSize_, Spec_->DataWeightPerShuffleJob);
            } else if (partitionCount > 1) {
                // Calculate upper limit for partition data weight.
                auto uncompressedSortedChunkSize = static_cast<i64>(Spec_->SortJobIO->TableWriter->DesiredChunkSize / InputCompressionRatio_);
                uncompressedSortedChunkSize = std::max<i64>(1, uncompressedSortedChunkSize);
                auto maxInputStreamsPerPartition = std::max<i64>(1, Spec_->MaxDataWeightPerJob / uncompressedSortedChunkSize);
                auto maxPartitionDataWeight = std::max<i64>(Options_->MinPartitionWeight, static_cast<i64>(0.9 * maxInputStreamsPerPartition * Spec_->DataWeightPerShuffleJob));

                if (dataWeightAfterPartition / partitionCount > maxPartitionDataWeight) {
                    partitionCount = dataWeightAfterPartition / maxPartitionDataWeight;
                }
            }
        }

        // Cast to int32 is safe since MaxPartitionCount is int32.
        return AdjustPartitionCountToWriterBufferSize(
            static_cast<int>(std::clamp<i64>(partitionCount, 1, Options_->MaxPartitionCount)));
    }

    int AdjustPartitionCountToWriterBufferSize(int partitionCount) const
    {
        i64 partitionJobCount = std::max(PartitionJobCount_, 1);
        i64 bufferSize = std::min(
            Spec_->PartitionJobIO->TableWriter->MaxBufferSize,
            DivCeil<i64>(ComputeDataWeightAfterPartition(), partitionJobCount));

        i64 partitionBufferSize = bufferSize / partitionCount;
        if (partitionBufferSize < Options_->MinUncompressedBlockSize) {
            return std::max(bufferSize / Options_->MinUncompressedBlockSize, (i64)1);
        } else {
            return partitionCount;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IPartitioningParametersEvaluatorPtr CreatePartitioningParametersEvaluator(
    TSortOperationSpecBasePtr spec,
    TSortOperationOptionsBasePtr options,
    i64 totalEstimatedInputDataWeight,
    i64 totalEstimatedInputUncompressedDataSize,
    i64 totalEstimatedInputValueCount,
    double inputCompressionRatio,
    int partitionJobCount)
{
    YT_VERIFY(totalEstimatedInputDataWeight > 0);
    return New<TPartitioningParametersEvaluator>(
        std::move(spec),
        std::move(options),
        totalEstimatedInputDataWeight,
        totalEstimatedInputUncompressedDataSize,
        totalEstimatedInputValueCount,
        inputCompressionRatio,
        partitionJobCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
