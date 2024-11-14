#pragma once

#include "private.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct IPartitioningParametersEvaluator
    : public TRefCounted
{
    virtual i64 SuggestSampleCount() const = 0;

    virtual int SuggestPartitionCount(
        std::optional<i64> fetchedSamplesCount = std::nullopt,
        bool forceLegacy = false) const = 0;

    virtual int SuggestMaxPartitionFactor(int finalPartitionCount) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IPartitioningParametersEvaluator)

////////////////////////////////////////////////////////////////////////////////

IPartitioningParametersEvaluatorPtr CreatePartitioningParametersEvaluator(
    NScheduler::TSortOperationSpecBasePtr spec,
    TSortOperationOptionsBasePtr options,
    i64 totalEstimatedInputDataWeight,
    i64 totalEstimatedInputUncompressedDataSize,
    i64 totalEstimatedInputValueCount,
    double inputCompressionRatio,
    int partitionJobCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
