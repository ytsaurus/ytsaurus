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
    const TProfilingOptions& options)
{
    auto pathTag = TrimProfilingTagValue(row.Path.GetPath());

    NProfiling::TTagSet tags;
    tags.AddRequiredTag({Format("%lv_cluster", Kind), row.Path.GetCluster().value()});

    if (options.EnablePathAggregation) {
        tags.AddTag({Format("%lv_path", Kind), pathTag}, /*parent*/ -1); // Parent is cluster.
        tags.AddTag({Format("%lv_tag", Kind), row.GetProfilingTag().value_or(NoneProfilingTag)}, /*parent*/ -2); // Parent is cluster.
        int parent = -3;
        if (options.Name) {
            tags.AddTag({Format("%lv_name", Kind), TrimProfilingTagValue(*options.Name)}, parent); // Parent is cluster.
            --parent;
        }
        if (options.AddObjectType) {
            tags.AddTag({"object_type", ToOptionalString(row.ObjectType).value_or(NoneObjectType)}, parent); // Parent is cluster.
        }
    } else {
        tags.AddRequiredTag({Format("%lv_path", Kind), pathTag});
        tags.AddRequiredTag({Format("%lv_tag", Kind), row.GetProfilingTag().value_or(NoneProfilingTag)});
        if (options.Name) {
            tags.AddRequiredTag({Format("%lv_name", Kind), TrimProfilingTagValue(*options.Name)});
        }
        if (options.AddObjectType) {
            tags.AddRequiredTag({"object_type", ToOptionalString(row.ObjectType).value_or(NoneObjectType)});
        }
    }

    if (options.Leading) {
        tags.AddRequiredTag({"leading", *options.Leading ? "true" : "false"});
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
