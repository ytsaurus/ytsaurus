#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/profiling/profiler.h>
#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class INodeMemoryTracker
    : public TRefCounted
{
public:
    using ECategory = NNodeTrackerClient::EMemoryCategory;
    using TPoolTag = TString;

    virtual i64 GetTotalLimit() const = 0;
    virtual i64 GetTotalUsed() const = 0;
    virtual i64 GetTotalFree() const = 0;
    virtual bool IsTotalExceeded() const = 0;

    virtual i64 GetExplicitLimit(ECategory category) const = 0;
    virtual i64 GetLimit(ECategory category, const std::optional<TPoolTag>& poolTag = {}) const = 0;
    virtual i64 GetUsed(ECategory category, const std::optional<TPoolTag>& poolTag = {}) const = 0;
    virtual i64 GetFree(ECategory category, const std::optional<TPoolTag>& poolTag = {}) const = 0;
    virtual bool IsExceeded(ECategory category, const std::optional<TPoolTag>& poolTag = {}) const = 0;

    virtual void SetTotalLimit(i64 newLimit) = 0;
    virtual void SetCategoryLimit(ECategory category, i64 newLimit) = 0;
    virtual void SetPoolWeight(const TPoolTag& poolTag, i64 newWeight) = 0;

    virtual void Acquire(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {}) = 0;
    virtual TError TryAcquire(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {}) = 0;
    virtual TError TryChange(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {}) = 0;
    virtual void Release(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {}) = 0;
    virtual i64 UpdateUsage(ECategory category, i64 newUsage) = 0;

    virtual IMemoryUsageTrackerPtr WithCategory(
        ECategory category,
        std::optional<TPoolTag> poolTag = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeMemoryTracker)

INodeMemoryTrackerPtr CreateNodeMemoryTracker(
    i64 totalLimit,
    const std::vector<std::pair<NNodeTrackerClient::EMemoryCategory, i64>>& limits,
    const NLogging::TLogger& logger = {},
    const NProfiling::TProfiler& profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
