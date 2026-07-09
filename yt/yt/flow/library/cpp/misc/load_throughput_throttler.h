#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TLoadThroughputThrottlerSpec
    : public NConcurrency::TThroughputThrottlerConfig
{
    double Alpha{};
    double InitialRowSize{};
    double InitialKeySize{};

    REGISTER_YSON_STRUCT(TLoadThroughputThrottlerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLoadThroughputThrottlerSpec);

////////////////////////////////////////////////////////////////////////////////

class TLoadThroughputThrottler
    : public TRefCounted
{
public:
    struct TStatistics
    {
        double ExpectedRowSize;
        double ExpectedKeySize;
    };

    TLoadThroughputThrottler(std::string name, NLogging::TLogger logger, NProfiling::TProfiler profiler);
    TLoadThroughputThrottler(NConcurrency::IReconfigurableThroughputThrottlerPtr underlying, NLogging::TLogger logger, NProfiling::TProfiler profiler);

    void Reconfigure(TLoadThroughputThrottlerSpecPtr spec);

    TFuture<void> ThrottleRows(TStringBuf tag, i64 rows);
    TFuture<void> ThrottleKeys(TStringBuf tag, i64 keys);

    void RegisterRows(TStringBuf tag, const std::vector<i64>& rowSizes);
    void RegisterKeys(TStringBuf tag, const std::vector<i64>& keySizes);

private:
    TAtomicIntrusivePtr<TLoadThroughputThrottlerSpec> Spec_;
    const NConcurrency::IReconfigurableThroughputThrottlerPtr ThroughputThrottler_;
    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<std::string, TStatistics> Statistics_;

    TStatistics& GetOrCreateStatistics(TStringBuf tag, TGuard<NThreading::TSpinLock>& guard);
};

DEFINE_REFCOUNTED_TYPE(TLoadThroughputThrottler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
