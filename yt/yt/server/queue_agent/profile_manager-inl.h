#ifndef PROFILE_MANAGER_INL_H_
#error "Direct inclusion of this file is not allowed, include profile_manager.h"
// For the sake of sane code completion.
#include "profile_manager.h"
#endif
#undef PROFILE_MANAGER_INL_H_

#include "helpers.h"

namespace NYT::NQueueAgent::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <EObjectKind Kind, typename TRow>
NProfiling::TTagSet CreateObjectProfilingTags(
    const TRow& row,
    bool enablePathAggregation,
    bool addObjectType,
    std::optional<bool> leading)
{
    std::string profilingTag = row.GetProfilingTag().value_or(NoneProfilingTag);
    auto pathTag = TrimProfilingTagValue(row.Ref.Path);

    NProfiling::TTagSet tags;
    tags.AddRequiredTag({Format("%lv_cluster", Kind), row.Ref.Cluster});

    if (enablePathAggregation) {
        tags.AddTag({Format("%lv_path", Kind), pathTag}, /*parent*/ -1); // Parent is queue_cluster.
        tags.AddTag({Format("%lv_tag", Kind), profilingTag}, /*parent*/ -2); // Parent is queue_cluster.
        if (addObjectType) {
            tags.AddTag({"object_type", ToOptionalString(row.ObjectType).value_or(NoneObjectType)}, /*parent*/ -3); // Parent is queue_cluster.
        }
    } else {
        tags.AddRequiredTag({Format("%lv_path", Kind), pathTag});
        tags.AddRequiredTag({Format("%lv_tag", Kind), profilingTag});
        if (addObjectType) {
            tags.AddRequiredTag({"object_type", ToOptionalString(row.ObjectType).value_or(NoneObjectType)});
        }
    }

    if (leading.has_value()) {
        tags.AddRequiredTag({"leading", leading ? "true" : "false"});
    }

    return tags;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TSnapshotPtr>
TProfileManagerBase<TSnapshotPtr>::TProfileManagerBase(
    std::initializer_list<std::pair<EProfilerScope, NProfiling::TProfiler>> profilerByScope)
{
    YT_VERIFY(profilerByScope.size() == TEnumTraits<EProfilerScope>::GetDomainSize());
    for (const auto& [scope, profiler] : profilerByScope) {
        YT_VERIFY(ProfilerByScope_.IsValidIndex(scope));
        ProfilerByScope_[scope] = profiler;
    }
}

template <typename TSnapshotPtr>
const NProfiling::TProfiler& TProfileManagerBase<TSnapshotPtr>::GetProfiler(EProfilerScope scope) const
{
    YT_VERIFY(ProfilerByScope_.IsValidIndex(scope));
    return ProfilerByScope_[scope];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent::NDetail
