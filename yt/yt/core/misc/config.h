#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TLogDigestConfig
    : public NYTree::TYsonSerializable
{
public:
    // We will round each sample x to the range from [(1 - RelativePrecision)*x, (1 + RelativePrecision)*x].
    // This parameter affects the memory usage of the digest, it is proportional to
    // log(UpperBound / LowerBound) / log(1 + RelativePrecision).
    double RelativePrecision;

    // The bounds of the range operated by the class.
    double LowerBound;
    double UpperBound;

    // The value that is returned when there are no samples in the digest.
    std::optional<double> DefaultValue;

    TLogDigestConfig(double lowerBound, double upperBound, double defaultValue)
        : TLogDigestConfig()
    {
        LowerBound = lowerBound;
        UpperBound = upperBound;
        DefaultValue = defaultValue;
    }

    TLogDigestConfig()
    {
        RegisterParameter("relative_precision", RelativePrecision)
            .Default(0.01)
            .GreaterThan(0);

        RegisterParameter("lower_bound", LowerBound)
            .GreaterThan(0);

        RegisterParameter("upper_bound", UpperBound)
            .GreaterThan(0);

        RegisterParameter("default_value", DefaultValue);

        RegisterPostprocessor([&] () {
            // If there are more than 1000 buckets, the implementation of TLogDigest
            // becomes inefficient since it stores information about at least that many buckets.
            const int maxBucketCount = 1000;
            double bucketCount = log(UpperBound / LowerBound) / log(1 + RelativePrecision);
            if (bucketCount > maxBucketCount) {
                THROW_ERROR_EXCEPTION("Bucket count is too large")
                    << TErrorAttribute("bucket_count", bucketCount)
                    << TErrorAttribute("max_bucket_count", maxBucketCount);
            }
            if (DefaultValue && (*DefaultValue < LowerBound || *DefaultValue > UpperBound)) {
                THROW_ERROR_EXCEPTION("Default value should be between lower bound and uppper bound")
                    << TErrorAttribute("default_value", *DefaultValue)
                    << TErrorAttribute("lower_bound", LowerBound)
                    << TErrorAttribute("upper_bound", UpperBound);
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TLogDigestConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EHistoricUsageAggregationMode,
    ((None)                     (0))
    ((ExponentialMovingAverage) (1))
);

class THistoricUsageConfig
    : public NYTree::TYsonSerializable
{
public:
    EHistoricUsageAggregationMode AggregationMode;

    //! Parameter of exponential moving average (EMA) of the aggregated usage.
    //! Roughly speaking, it means that current usage ratio is twice as relevant for the
    //! historic usage as the usage ratio alpha seconds ago.
    //! EMA for unevenly spaced time series was adapted from here: https://clck.ru/HaGZs
    double EmaAlpha;

    THistoricUsageConfig()
    {
        RegisterParameter("aggregation_mode", AggregationMode)
            .Default(EHistoricUsageAggregationMode::None);

        RegisterParameter("ema_alpha", EmaAlpha)
            // TODO(eshcherbin): Adjust.
            .Default(1.0 / (24.0 * 60.0 * 60.0))
            .GreaterThanOrEqual(0.0);
    }
};

DEFINE_REFCOUNTED_TYPE(THistoricUsageConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
