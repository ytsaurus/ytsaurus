#include "load_throughput_throttler.h"

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TLoadThroughputThrottlerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("alpha", &TThis::Alpha)
        .Default(0.999);
    registrar.Parameter("initial_row_size", &TThis::InitialRowSize)
        .Default(1000);
    registrar.Parameter("initial_key_size", &TThis::InitialKeySize)
        .Default(500);
}

////////////////////////////////////////////////////////////////////////////////

TLoadThroughputThrottler::TLoadThroughputThrottler(
    std::string name,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
    : Spec_(New<TLoadThroughputThrottlerSpec>())
    , ThroughputThrottler_(NConcurrency::CreateNamedReconfigurableThroughputThrottler(Spec_.Acquire(), TString(name), logger, profiler))
    , Logger(logger)
    , Profiler_(profiler)
{ }

TLoadThroughputThrottler::TLoadThroughputThrottler(
    NConcurrency::IReconfigurableThroughputThrottlerPtr underlying,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
    : Spec_(New<TLoadThroughputThrottlerSpec>())
    , ThroughputThrottler_(std::move(underlying))
    , Logger(logger)
    , Profiler_(profiler)
{
    ThroughputThrottler_->Reconfigure(Spec_.Acquire());
}

void TLoadThroughputThrottler::Reconfigure(TLoadThroughputThrottlerSpecPtr spec)
{
    Spec_ = std::move(spec);
    ThroughputThrottler_->Reconfigure(Spec_.Acquire());
}

TFuture<void> TLoadThroughputThrottler::ThrottleRows(TStringBuf tag, i64 rows)
{
    auto estimatedSize = [&] () {
        auto guard = Guard(Lock_);
        return GetOrCreateStatistics(tag, guard).ExpectedRowSize * rows;
    }();
    return ThroughputThrottler_->Throttle(estimatedSize);
}

TFuture<void> TLoadThroughputThrottler::ThrottleKeys(TStringBuf tag, i64 keys)
{
    auto estimatedSize = [&] () {
        auto guard = Guard(Lock_);
        return GetOrCreateStatistics(tag, guard).ExpectedKeySize * keys;
    }();
    return ThroughputThrottler_->Throttle(estimatedSize);
}

void TLoadThroughputThrottler::RegisterRows(TStringBuf tag, const std::vector<i64>& rowSizes)
{
    auto guard = Guard(Lock_);
    auto alpha = Spec_.Acquire()->Alpha;
    auto& current = GetOrCreateStatistics(tag, guard).ExpectedRowSize;
    for (auto size : rowSizes) {
        current = current * alpha + size * (1 - alpha);
    }
}

void TLoadThroughputThrottler::RegisterKeys(TStringBuf tag, const std::vector<i64>& keySizes)
{
    auto guard = Guard(Lock_);
    auto alpha = Spec_.Acquire()->Alpha;
    auto& current = GetOrCreateStatistics(tag, guard).ExpectedKeySize;
    for (auto size : keySizes) {
        current = current * alpha + size * (1 - alpha);
    }
}

TLoadThroughputThrottler::TStatistics& TLoadThroughputThrottler::GetOrCreateStatistics(TStringBuf tag, TGuard<NThreading::TSpinLock>& /*guard*/)
{
    auto [iter, emplaced] = Statistics_.try_emplace(tag);
    if (!emplaced) {
        auto spec = Spec_.Acquire();
        iter->second.ExpectedRowSize = spec->InitialRowSize;
        iter->second.ExpectedKeySize = spec->InitialKeySize;
    }
    return iter->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
