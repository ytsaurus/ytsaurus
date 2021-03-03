#pragma once

#include <yt/core/tracing/public.h>

#include <yt/core/profiling/public.h>

#include <yt/core/ypath/public.h>

#include <yt/core/rpc/public.h>

#include <yt/library/profiling/sensor.h>

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

class TServiceProfilerGuard
{
public:
    TServiceProfilerGuard();
    ~TServiceProfilerGuard();

    TServiceProfilerGuard(const TServiceProfilerGuard& ) = delete;

    void SetTimer(
        NProfiling::TTimeCounter timeCounter,
        NProfiling::TEventTimer timer);

protected:
    NTracing::TTraceContextPtr TraceContext_;
    NProfiling::TCpuInstant StartTime_;

    NProfiling::TTimeCounter TimeCounter_;
    NProfiling::TEventTimer Timer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
