#pragma once

#include <yt/core/profiling/public.h>

#include <yt/core/ypath/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Adds user tag to #tags and returns the resultant tag list.
NProfiling::TTagIdList AddUserTag(NProfiling::TTagIdList tags, const NRpc::TAuthenticationIdentity& identity);

//! Adds the current user tag (installed via TUserTagTag) to #tags and returns the resultant tag list.
NProfiling::TTagIdList AddCurrentUserTag(NProfiling::TTagIdList tags);

////////////////////////////////////////////////////////////////////////////////

//! Trait to store profiler counters in TLS cache.
template <typename TCountersKey, typename TCounters>
struct TProfilerTrait
{
    using TKey = TCountersKey;
    using TValue = TCounters;

    static TKey ToKey(const TKey& key);
    static TValue ToValue(const TKey& key);
};

template <typename TCounters>
using TTagListProfilerTrait = TProfilerTrait<NProfiling::TTagIdList, TCounters>;

////////////////////////////////////////////////////////////////////////////////

class TServiceProfilerGuard
{
public:
    TServiceProfilerGuard(
        const NProfiling::TProfiler* profiler,
        const NYPath::TYPath& path);

    ~TServiceProfilerGuard();

    void SetProfilerTags(NProfiling::TTagIdList tags);
    const NProfiling::TTagIdList& GetProfilerTags() const;
    void Disable();

protected:
    const NProfiling::TProfiler* Profiler_;
    const NYPath::TYPath Path_;
    NProfiling::TCpuInstant StartInstant_;
    NProfiling::TTagIdList TagIds_;
    bool Enabled_ = true;
};

////////////////////////////////////////////////////////////////////////////////

class TCumulativeServiceProfilerGuard
    : public TServiceProfilerGuard
{
public:
    TCumulativeServiceProfilerGuard(
        const NProfiling::TProfiler* profiler,
        const NYPath::TYPath& path);

    ~TCumulativeServiceProfilerGuard();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PROFILING_HELPERS_INL_H_
#include "profiling_helpers-inl.h"
#undef PROFILING_HELPERS_H_

