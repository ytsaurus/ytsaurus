#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TLogDigestConfig
    : public NYTree::TYsonStruct
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

    REGISTER_YSON_STRUCT(TLogDigestConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("relative_precision", &TLogDigestConfig::RelativePrecision)
            .Default(0.01)
            .GreaterThan(0);

        registrar.Parameter("lower_bound", &TLogDigestConfig::LowerBound)
            .GreaterThan(0);

        registrar.Parameter("upper_bound", &TLogDigestConfig::UpperBound)
            .GreaterThan(0);

        registrar.Parameter("default_value", &TLogDigestConfig::DefaultValue);

        registrar.Postprocessor([] (TLogDigestConfig* config) {
            // If there are more than 1000 buckets, the implementation of TLogDigest
            // becomes inefficient since it stores information about at least that many buckets.
            const int maxBucketCount = 1000;
            double bucketCount = log(config->UpperBound / config->LowerBound) / log(1 + config->RelativePrecision);
            if (bucketCount > maxBucketCount) {
                THROW_ERROR_EXCEPTION("Bucket count is too large")
                    << TErrorAttribute("bucket_count", bucketCount)
                    << TErrorAttribute("max_bucket_count", maxBucketCount);
            }
            if (config->DefaultValue && (*config->DefaultValue < config->LowerBound || *config->DefaultValue > config->UpperBound)) {
                THROW_ERROR_EXCEPTION("Default value should be between lower bound and upper bound")
                    << TErrorAttribute("default_value", *config->DefaultValue)
                    << TErrorAttribute("lower_bound", config->LowerBound)
                    << TErrorAttribute("upper_bound", config->UpperBound);
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
    : public NYTree::TYsonStruct
{
public:
    EHistoricUsageAggregationMode AggregationMode;

    //! Parameter of exponential moving average (EMA) of the aggregated usage.
    //! Roughly speaking, it means that current usage ratio is twice as relevant for the
    //! historic usage as the usage ratio alpha seconds ago.
    //! EMA for unevenly spaced time series was adapted from here: https://clck.ru/HaGZs
    double EmaAlpha;

    REGISTER_YSON_STRUCT(THistoricUsageConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("aggregation_mode", &THistoricUsageConfig::AggregationMode)
            .Default(EHistoricUsageAggregationMode::None);

        registrar.Parameter("ema_alpha", &THistoricUsageConfig::EmaAlpha)
            // TODO(eshcherbin): Adjust.
            .Default(1.0 / (24.0 * 60.0 * 60.0))
            .GreaterThanOrEqual(0.0);
    }
};

DEFINE_REFCOUNTED_TYPE(THistoricUsageConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
