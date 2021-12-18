#pragma once

#include "throughput_throttler.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TFairThrottlerConfig
    : public NYTree::TYsonSerializable
{
    i64 TotalLimit;

    TDuration DistributionPeriod;

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

    TFairThrottlerBucketConfig();
};

DEFINE_REFCOUNTED_TYPE(TFairThrottlerBucketConfig)

////////////////////////////////////////////////////////////////////////////////

//! TFairThrottler manages a group of throttlers, distributing traffic according to fair share policy.
/*!
 *  TFairThrottler distributes TotalLimit * DistributionPeriod bytes every DistributionPeriod.
 *
 *  At the beginning of the period, TFairThrottler distributes optimistic quota between buckets.
 *  Bucket throttler consume optimistic quota without blocking. When optimistic quota is exhausted,
 *  requests queue inside bucket.
 *
 *  At the end of the period, remaining quota is drained from idle buckets and distributed between
 *  busy buckets.
 *
 *  At last, optimistic quota for next iteration is computed.
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

    NProfiling::TCounter UnfairBytes_;
    NProfiling::TCounter BlockedBytes_;


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
