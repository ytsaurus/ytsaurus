#include "profiling_helpers.h"
#include "yt/yt/core/profiling/timing.h"
#include "yt/yt/core/tracing/trace_context.h"

#include <yt/yt/core/misc/tls_cache.h>

#include <yt/yt/core/concurrency/fls.h>

#include <yt/yt/core/profiling/profiler.h>
#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/rpc/authentication_identity.h>

namespace NYT {

using namespace NProfiling;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

const TString UnknownProfilingTag("unknown");

////////////////////////////////////////////////////////////////////////////////

std::optional<TString> GetCurrentProfilingUser()
{
    return GetProfilingUser(NRpc::GetCurrentAuthenticationIdentity());
}

std::optional<TString> GetProfilingUser(const NRpc::TAuthenticationIdentity& identity)
{
    if (&identity == &NRpc::GetRootAuthenticationIdentity()) {
        return {};
    }
    return identity.User;
}

////////////////////////////////////////////////////////////////////////////////

TServiceProfilerGuard::TServiceProfilerGuard()
    : TraceContext_(NTracing::GetCurrentTraceContext())
    , StartTime_(NProfiling::GetCpuInstant())
{ }

TServiceProfilerGuard::~TServiceProfilerGuard()
{
    Timer_.Record(NProfiling::CpuDurationToDuration(NProfiling::GetCpuInstant() - StartTime_));

    if (!TraceContext_) {
        return;
    }

    NTracing::FlushCurrentTraceContextTime();
    TimeCounter_.Add(CpuDurationToDuration(TraceContext_->GetElapsedCpuTime()));
}

void TServiceProfilerGuard::Start(const TMethodCounters& counters)
{
    counters.RequestCount.Increment();

    TimeCounter_ = counters.CpuTime;
    Timer_ = counters.RequestDuration;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
