#include "profiling_helpers.h"
#include "yt/yt/core/profiling/timing.h"
#include "yt/yt/core/tracing/trace_context.h"

#include <yt/yt/core/misc/tls_cache.h>

#include <yt/yt/core/concurrency/fls.h>

namespace NYT {

using namespace NProfiling;
using namespace NYPath;

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

} // namespace NYT
