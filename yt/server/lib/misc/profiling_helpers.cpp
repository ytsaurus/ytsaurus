#include "profiling_helpers.h"

#include <yt/core/misc/tls_cache.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/profile_manager.h>

namespace NYT {

using namespace NProfiling;
using namespace NYPath;

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

TTagIdList AddUserTag(const TString& user, TTagIdList tags)
{
    tags.push_back(GetLocallyCachedValue<TUserTagTrait>(user));
    return tags;
}

////////////////////////////////////////////////////////////////////////////////

struct TServiceProfilerCounters
{
    using TKey = std::pair<TYPath, TTagIdList>;

    explicit TServiceProfilerCounters(const TKey& key)
        : RequestCount(key.first + "/request_count", key.second)
        , RequestExecutionTime(key.first + "/request_time", key.second)
        , CumulativeTime(key.first + "/cumulative_time", key.second)
    { }

    TMonotonicCounter RequestCount;
    TAggregateGauge RequestExecutionTime;
    TMonotonicCounter CumulativeTime;
};

using TServiceProfilerTrait = TProfilerTrait<TServiceProfilerCounters::TKey, TServiceProfilerCounters>;

////////////////////////////////////////////////////////////////////////////////

TServiceProfilerGuard::TServiceProfilerGuard(
    const TProfiler* profiler,
    const TYPath& path)
    : Profiler_(profiler)
    , Path_(path)
    , StartInstant_(GetCpuInstant())
{ }

TServiceProfilerGuard::~TServiceProfilerGuard()
{
    if (!Enabled_ || GetProfilerTags().empty()) {
        return;
    }

    auto value = CpuDurationToValue(GetCpuInstant() - StartInstant_);
    auto& counters = GetLocallyGloballyCachedValue<TServiceProfilerTrait>(TServiceProfilerCounters::TKey{Path_, TagIds_});
    Profiler_->Increment(counters.RequestCount, 1);
    Profiler_->Update(counters.RequestExecutionTime, value);
}

void TServiceProfilerGuard::SetProfilerTags(TTagIdList tags)
{
    TagIds_ = std::move(tags);
}

const TTagIdList& TServiceProfilerGuard::GetProfilerTags() const
{
    return TagIds_;
}

void TServiceProfilerGuard::Disable()
{
    Enabled_ = false;
}

////////////////////////////////////////////////////////////////////////////////

TCumulativeServiceProfilerGuard::TCumulativeServiceProfilerGuard(
    const TProfiler* profiler,
    const TYPath& path)
    : TServiceProfilerGuard(profiler, path)
{ }

TCumulativeServiceProfilerGuard::~TCumulativeServiceProfilerGuard()
{
    if (!Enabled_ || GetProfilerTags().empty()) {
        return;
    }

    auto value = CpuDurationToValue(GetCpuInstant() - StartInstant_);
    auto& counters = GetLocallyGloballyCachedValue<TServiceProfilerTrait>(TServiceProfilerCounters::TKey{Path_, TagIds_});
    Profiler_->Increment(counters.CumulativeTime, value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
