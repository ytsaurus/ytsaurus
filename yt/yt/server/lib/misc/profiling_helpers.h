#pragma once

#include <yt/yt/core/tracing/public.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

extern const TString UnknownProfilingTag;

////////////////////////////////////////////////////////////////////////////////

//! Adds user tag to #tags and returns the resultant tag list.
NProfiling::TTagIdList AddUserTag(NProfiling::TTagIdList tags, const NRpc::TAuthenticationIdentity& identity);

//! Adds the current user tag (installed via TUserTagTag) to #tags and returns the resultant tag list.
NProfiling::TTagIdList AddCurrentUserTag(NProfiling::TTagIdList tags);

std::optional<TString> GetCurrentProfilingUser();

std::optional<TString> GetProfilingUser(const NRpc::TAuthenticationIdentity& identity);

////////////////////////////////////////////////////////////////////////////////

struct TMethodCounters
{
    TMethodCounters() = default;

    explicit TMethodCounters(const NProfiling::TRegistry& profiler)
        : CpuTime(profiler.TimeCounter("/cumulative_cpu_time"))
        , RequestCount(profiler.Counter("/request_count"))
        , RequestDuration(profiler.Timer("/request_duration"))
    { }

    NProfiling::TTimeCounter CpuTime;
    NProfiling::TCounter RequestCount;
    NProfiling::TEventTimer RequestDuration;
};

class TServiceProfilerGuard
{
public:
    TServiceProfilerGuard();
    ~TServiceProfilerGuard();

    TServiceProfilerGuard(const TServiceProfilerGuard& ) = delete;

    void Start(const TMethodCounters& counters);

protected:
    NTracing::TTraceContextPtr TraceContext_;
    NProfiling::TCpuInstant StartTime_;

    NProfiling::TTimeCounter TimeCounter_;
    NProfiling::TEventTimer Timer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
