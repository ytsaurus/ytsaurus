#ifndef TABLET_PROFILING_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include tablet_profiling_base.h"
// For the sake of sane code completion.
#include "tablet_profiling_base.h"
#endif

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TCounter>
TCounter* TUserTaggedCounter<TCounter>::Get(
    bool disabled,
    const std::optional<std::string>& userTag,
    const NProfiling::TProfiler& profiler)
{
    if (disabled) {
        static TCounter counter;
        return &counter;
    }

    return Counters.FindOrInsert(userTag, [&] {
        if (userTag) {
            return TCounter{profiler.WithTag("user", *userTag)};
        } else {
            return TCounter{profiler};
        }
    }).first;
}

template <class TCounter>
TCounter* TUserTaggedCounter<TCounter>::Get(
    bool disabled,
    const std::optional<std::string>& userTag,
    const NProfiling::TProfiler& tabletProfiler,
    const NProfiling::TProfiler& mediumProfiler,
    const NProfiling::TProfiler& mediumHistogramProfiler,
    const NTableClient::TTableSchemaPtr& schema)
{
    if (disabled) {
        static TCounter counter;
        return &counter;
    }

    return Counters.FindOrInsert(userTag, [&] {
        if (userTag) {
            return TCounter(
                tabletProfiler.WithTag("user", *userTag),
                mediumProfiler.WithTag("user", *userTag),
                mediumHistogramProfiler,
                schema);
        } else {
            return TCounter(
                tabletProfiler,
                mediumProfiler,
                mediumHistogramProfiler,
                schema);
        }
    }).first;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
