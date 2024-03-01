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

TTestAllocationGuard::TTestAllocationGuard(
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

TTestAllocationGuard::TTestAllocationGuard(TTestAllocationGuard&& other)
{
    *this = std::move(other);
}

TTestAllocationGuard& TTestAllocationGuard::operator=(TTestAllocationGuard&& other)
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

TTestAllocationGuard::~TTestAllocationGuard()
{
    if (Active_) {
        Active_ = false;

        NConcurrency::TDelayedExecutor::MakeDelayed(
            DelayBeforeDestruct_,
            DestructCallbackInvoker_ ? DestructCallbackInvoker_ : GetCurrentInvoker())
            .Subscribe(BIND([
                    destruct = std::move(DestructCallback_),
                    raw = std::move(Raw_)
                ] (const TErrorOr<void>& /*errorOrVoid*/) {
                    Y_UNUSED(raw);
                    destruct();
                }));
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TTestAllocationGuard> MakeTestHeapAllocation(
    i64 AllocationSize,
    TDuration AllocationReleaseDelay,
    std::function<void()> constructCallback,
    std::function<void()> destructCallback,
    IInvokerPtr destructCallbackInvoker,
    i64 allocationPartSize)
{
    std::vector<TTestAllocationGuard> testHeap;

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
    YT_VERIFY(consumer);

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

} // namespace NYT
