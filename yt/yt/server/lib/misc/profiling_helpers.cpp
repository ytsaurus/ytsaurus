#include "profiling_helpers.h"

#include <yt/yt/library/ytprof/heap_profiler.h>

#include <yt/yt/core/concurrency/fls.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/tls_cache.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NYPath;
using namespace NYson;
using namespace NYTProf;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const TString UnknownProfilingTag("unknown");

////////////////////////////////////////////////////////////////////////////////

TServiceProfilerGuard::TServiceProfilerGuard()
    : TraceContext_(NTracing::TryGetCurrentTraceContext())
    , StartTime_(NProfiling::GetCpuInstant())
{ }

TServiceProfilerGuard::~TServiceProfilerGuard()
{
    Timer_.Record(NProfiling::CpuDurationToDuration(NProfiling::GetCpuInstant() - StartTime_));

    if (!TraceContext_) {
        return;
    }

    NTracing::FlushCurrentTraceContextElapsedTime();
    TimeCounter_.Add(CpuDurationToDuration(TraceContext_->GetElapsedCpuTime()));
}

void TServiceProfilerGuard::Start(const TMethodCounters& counters)
{
    counters.RequestCount.Increment();

    TimeCounter_ = counters.CpuTime;
    Timer_ = counters.RequestDuration;
}

////////////////////////////////////////////////////////////////////////////////

TTestAllocGuard::TTestAllocGuard(
        i64 allocationPartSize,
        std::function<void()> constructCallback,
        std::function<void()> destructCallback,
        TDuration delayBeforeDestruct,
        IInvokerPtr destructCallbackInvoker)
    : Raw_(TString(allocationPartSize, 'x'))
    , Active_(true)
    , ConstructCallback_(std::move(constructCallback))
    , DestructCallback_(std::move(destructCallback))
    , DelayBeforeDestruct_(delayBeforeDestruct)
    , DestructCallbackInvoker_(std::move(destructCallbackInvoker))
{
    ConstructCallback_();
}

TTestAllocGuard::TTestAllocGuard(TTestAllocGuard&& other)
{
    *this = std::move(other);
}

TTestAllocGuard& TTestAllocGuard::operator=(TTestAllocGuard&& other)
{
    Raw_ = std::move(other.Raw_);
    Active_ = other.Active_;
    ConstructCallback_ = std::move(other.ConstructCallback_);
    DestructCallback_ = std::move(other.DestructCallback_);
    DelayBeforeDestruct_ = other.DelayBeforeDestruct_;
    DestructCallbackInvoker_ = std::move(other.DestructCallbackInvoker_);

    other.Active_ = false;

    return *this;
}

TTestAllocGuard::~TTestAllocGuard()
{
    if (Active_) {
        Active_ = false;

        NConcurrency::TDelayedExecutor::MakeDelayed(
            DelayBeforeDestruct_,
            DestructCallbackInvoker_ ? DestructCallbackInvoker_ : GetCurrentInvoker())
            .Subscribe(BIND([
                    destruct = std::move(DestructCallback_),
                    raw = std::move(Raw_)
                ] (const NYT::TErrorOr<void>& /*errorOrVoid*/) {
                    Y_UNUSED(raw);
                    destruct();
                }));
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TTestAllocGuard> MakeTestHeapAllocation(
    i64 AllocationSize,
    TDuration AllocationReleaseDelay,
    std::function<void()> constructCallback,
    std::function<void()> destructCallback,
    IInvokerPtr destructCallbackInvoker,
    i64 allocationPartSize)
{
    std::vector<TTestAllocGuard> testHeap;

    for (i64 i = 0; i < AllocationSize; i += allocationPartSize) {
        testHeap.emplace_back(
            allocationPartSize,
            constructCallback,
            destructCallback,
            AllocationReleaseDelay,
            destructCallbackInvoker);
    }

    return testHeap;
}

////////////////////////////////////////////////////////////////////////////////

void CollectHeapUsageStatistics(
    IYsonConsumer* consumer,
    const std::vector<TString>& memoryTagsList)
{
    const auto memorySnapshot = GetMemoryUsageSnapshot();
    YT_VERIFY(memorySnapshot);

    auto heapUsageStatistics = BuildYsonStringFluently<EYsonType::MapFragment>();

    heapUsageStatistics
        .Item("heap_usage_statistics").DoMapFor(
            memoryTagsList,
            [&] (TFluentMap fluent, const auto& tag) {
                fluent.Item(tag).DoMap([&] (TFluentMap fluent) {
                    auto memoryUsageStatistic = BuildYsonStringFluently<EYsonType::MapFragment>();

                    memoryUsageStatistic.DoFor(
                        memorySnapshot->GetUsage(tag),
                        [] (TFluentMap fluent, const auto& pair) {
                            fluent.Item(pair.first).Value(pair.second);
                        });

                    fluent.GetConsumer()->OnRaw(memoryUsageStatistic.Finish());
                });
            });

    consumer->OnRaw(heapUsageStatistics.Finish());
}

////////////////////////////////////////////////////////////////////////////////

THeapUsageProfiler::THeapUsageProfiler(
    std::vector<TString> tags,
    IInvokerPtr invoker,
    std::optional<TDuration> updatePeriod,
    NProfiling::TProfiler profiler)
    : Profiler_(std::move(profiler))
    , TagTypes_(std::move(tags))
    , UpdateExecutor_(New<TPeriodicExecutor>(
        std::move(invoker),
        BIND(&THeapUsageProfiler::UpdateGauges, MakeWeak(this)),
        std::move(updatePeriod)))
{
    UpdateExecutor_->Start();
}

void THeapUsageProfiler::UpdateGauges()
{
    const auto memorySnapshot = GetMemoryUsageSnapshot();
    YT_VERIFY(memorySnapshot);

    for (const auto& tagType : TagTypes_) {
        auto& heapUsageMap = HeapUsageByType_.emplace(tagType, THashMap<TString, TGauge>{}).first->second;

        for (const auto& [tag, usage] : memorySnapshot->GetUsage(tagType)) {
            auto gauge = heapUsageMap.find(tag);

            if (gauge.IsEnd()) {
                auto pair = heapUsageMap.emplace(tag, Profiler_
                    .WithTag(tagType, tag)
                    .Gauge(tagType));
                gauge = pair.first;
            }

            gauge->second.Update(usage);
        }
    }
}

///////////////////////////////////////////////////////////////////

THeapUsageProfilerPtr CreateHeapProfilerWithTags(
    std::vector<TString>&& tags,
    IInvokerPtr invoker,
    std::optional<TDuration> updatePeriod)
{
    return New<THeapUsageProfiler>(
        std::move(tags),
        std::move(invoker),
        std::move(updatePeriod));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
