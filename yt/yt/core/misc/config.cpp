#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TLogDigestConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("relative_precision", &TThis::RelativePrecision)
        .Default(0.01)
        .GreaterThan(0);

    registrar.Parameter("lower_bound", &TThis::LowerBound)
        .GreaterThan(0);

    registrar.Parameter("upper_bound", &TThis::UpperBound)
        .GreaterThan(0);

    registrar.Parameter("default_value", &TThis::DefaultValue);

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

////////////////////////////////////////////////////////////////////////////////

void THistoricUsageConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("aggregation_mode", &TThis::AggregationMode)
        .Default(EHistoricUsageAggregationMode::None);

    registrar.Parameter("ema_alpha", &TThis::EmaAlpha)
        // TODO(eshcherbin): Adjust.
        .Default(1.0 / (24.0 * 60.0 * 60.0))
        .GreaterThanOrEqual(0.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
