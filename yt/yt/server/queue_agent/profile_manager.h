#pragma once

#include "private.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EProfilerScope,
    (Object)
    (ObjectPartition)
    (ObjectPass)
    (AlertManager)
);

////////////////////////////////////////////////////////////////////////////////

template <typename TSnapshotPtr>
struct IProfileManager
    : public TRefCounted
{
    virtual void Profile(
        const TSnapshotPtr& previousQueueSnapshot,
        const TSnapshotPtr& currentQueueSnapshot) = 0;

    virtual const NProfiling::TProfiler& GetProfiler(EProfilerScope scope) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <EObjectKind Kind, typename TRow>
NProfiling::TTagSet CreateObjectProfilingTags(
    const TRow& row,
    bool enablePathAggregation = false,
    bool addObjectType = false,
    std::optional<bool> leading = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

template <typename TSnapshotPtr>
class TProfileManagerBase
    : public IProfileManager<TSnapshotPtr>
{
public:
    TProfileManagerBase(
        std::initializer_list<std::pair<EProfilerScope, NProfiling::TProfiler>> profilerByScope);

    const NProfiling::TProfiler& GetProfiler(EProfilerScope scope) const;

private:
    TEnumIndexedArray<EProfilerScope, NProfiling::TProfiler> ProfilerByScope_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

IQueueProfileManagerPtr CreateQueueProfileManager(
    const NProfiling::TProfiler& profiler,
    const NLogging::TLogger& logger,
    const NQueueClient::TQueueTableRow& row,
    bool leading);

////////////////////////////////////////////////////////////////////////////////

IConsumerProfileManagerPtr CreateConsumerProfileManager(
    const NProfiling::TProfiler& profiler,
    const NLogging::TLogger& logger,
    const NQueueClient::TConsumerTableRow& row,
    bool leading);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

#define PROFILE_MANAGER_INL_H_
#include "profile_manager-inl.h"
#undef PROFILE_MANAGER_INL_H_
