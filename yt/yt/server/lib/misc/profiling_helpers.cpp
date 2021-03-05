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

struct TUserTagTrait
{
    using TKey = TString;
    using TValue = TTagId;

    static const TString& ToKey(const TString& user)
    {
        return user;
    }

    static TTagId ToValue(const TString& user)
    {
        return TProfileManager::Get()->RegisterTag("user", user);
    }
};

TTagIdList AddUserTag(TTagIdList tags, const NRpc::TAuthenticationIdentity& identity)
{
    tags.push_back(GetLocallyCachedValue<TUserTagTrait>(identity.UserTag));
    return tags;
}

TTagIdList AddCurrentUserTag(TTagIdList tags)
{
    const auto& identity = NRpc::GetCurrentAuthenticationIdentity();
    if (&identity == &NRpc::GetRootAuthenticationIdentity()) {
        return tags;
    }
    return AddUserTag(tags, identity);
}

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
    if (!TraceContext_) {
        return;
    }

    NTracing::FlushCurrentTraceContextTime();
    TimeCounter_.Add(CpuDurationToDuration(TraceContext_->GetElapsedCpuTime()));
}

void TServiceProfilerGuard::SetTimer(
    NProfiling::TTimeCounter timeCounter,
    NProfiling::TEventTimer timer)
{
    TimeCounter_ = timeCounter;
    Timer_.Record(NProfiling::CpuDurationToDuration(NProfiling::GetCpuInstant() - StartTime_));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
