#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/tracing/public.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

std::optional<std::string> GetCurrentProfilingUser();
std::optional<std::string> GetProfilingUser(const NRpc::TAuthenticationIdentity& identity);

////////////////////////////////////////////////////////////////////////////////

struct TMethodCounters
{
    TMethodCounters() = default;
    explicit TMethodCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TTimeCounter CpuTime;
    NProfiling::TCounter RequestCount;
    NProfiling::TEventTimer RequestDuration;
};

////////////////////////////////////////////////////////////////////////////////

class TServiceProfilerGuard
{
public:
    TServiceProfilerGuard();
    ~TServiceProfilerGuard();

    TServiceProfilerGuard(const TServiceProfilerGuard&) = delete;
    TServiceProfilerGuard(TServiceProfilerGuard&&) = default;

    void Start(const TMethodCounters& counters);

protected:
    NTracing::TTraceContextPtr TraceContext_;
    NProfiling::TCpuInstant StartTime_;

    NProfiling::TTimeCounter TimeCounter_;
    NProfiling::TEventTimer Timer_;
};

////////////////////////////////////////////////////////////////////////////////

//! Produces allocation on heap and keeps it for testing.
class TTestAllocationGuard
{
public:
    TTestAllocationGuard(
        i64 allocationPartSize,
        std::function<void()> constructCallback,
        std::function<void()> destructCallback,
        TDuration delayBeforeDestruct = TDuration::Zero(),
        IInvokerPtr destructCallbackInvoker = nullptr);
    TTestAllocationGuard(const TTestAllocationGuard& other) = delete;
    TTestAllocationGuard(TTestAllocationGuard&& other);

    TTestAllocationGuard& operator=(const TTestAllocationGuard& other) = delete;
    TTestAllocationGuard& operator=(TTestAllocationGuard&& other);

    ~TTestAllocationGuard();

private:
    TString Raw_;
    bool Active_ = false;
    std::function<void()> ConstructCallback_;
    std::function<void()> DestructCallback_;
    TDuration DelayBeforeDestruct_;
    IInvokerPtr DestructCallbackInvoker_;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TTestAllocationGuard> MakeTestHeapAllocation(
    i64 allocationSize,
    TDuration allocationReleaseDelay,
    std::function<void()> constructCallback = [] {},
    std::function<void()> destructCallback = [] {},
    IInvokerPtr destructCallbackInvoker = nullptr,
    i64 allocationPartSize = 1_MB);

////////////////////////////////////////////////////////////////////////////////

void DumpGlobalMemoryUsageSnapshot(
    NYson::IYsonConsumer* consumer,
    const std::vector<TAllocationTagKey>& tagKeys);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer

#define PROFILING_HELPERS_H_
#include "profiling_helpers-inl.h"
#undef PROFILING_HELPERS_H_
