#pragma once

#include "throughput_throttler.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TFairThrottlerConfig
    : public NYTree::TYsonSerializable
{
    i64 TotalLimit;

    TDuration DistributionPeriod;

    int BucketAccumulationTicks;

    int GlobalAccumulationTicks;

    TFairThrottlerConfig();
};

DEFINE_REFCOUNTED_TYPE(TFairThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TFairThrottlerBucketConfig
    : public NYTree::TYsonSerializable
{
    double Weight;

    std::optional<i64> Limit;
    std::optional<double> RelativeLimit;
    std::optional<i64> GetLimit(i64 totalLimit);

    std::optional<i64> Guarantee;
    std::optional<double> RelativeGuarantee;
    std::optional<i64> GetGuarantee(i64 totalLimit);

    TFairThrottlerBucketConfig();
};

DEFINE_REFCOUNTED_TYPE(TFairThrottlerBucketConfig)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSharedBucket)

//! TFairThrottler manages a group of throttlers, distributing traffic according to fair share policy.
/*!
 *  TFairThrottler distributes TotalLimit * DistributionPeriod bytes every DistributionPeriod.
 *
 *  At the beginning of the period, TFairThrottler distributes new quota between buckets. Buckets
 *  accumulate quota for N ticks. After N ticks overflown quota is transferred into shared bucket.
 *  Shared bucket accumulates quota for M ticks. Overflown quota from shared bucket is discarded.
 *
 *  Throttled requests may consume quota from both local bucket and shared bucket.
 */
class TFairThrottler
    : public TRefCounted
{
public:
    TFairThrottler(
        TFairThrottlerConfigPtr config,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler);

    IThroughputThrottlerPtr CreateBucketThrottler(
        const TString& name,
        TFairThrottlerBucketConfigPtr config);

    void Reconfigure(
        TFairThrottlerConfigPtr config,
        const THashMap<TString, TFairThrottlerBucketConfigPtr>& bucketConfigs);

    static std::vector<i64> ComputeFairDistribution(
        i64 totalLimit,
        const std::vector<double>& weights,
        const std::vector<i64>& demands,
        const std::vector<std::optional<i64>>& limits);

private:
    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler_;

    TSharedBucketPtr SharedBucket_;

    struct TBucket
    {
        TFairThrottlerBucketConfigPtr Config;
        TBucketThrottlerPtr Throttler;
    };

    // Protects all Config_ and Buckets_.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TFairThrottlerConfigPtr Config_;
    THashMap<TString, TBucket> Buckets_;

    void DoUpdateLimits();
    void UpdateLimits(TInstant at);
    void ScheduleLimitUpdate(TInstant at);
};

DEFINE_REFCOUNTED_TYPE(TFairThrottler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
