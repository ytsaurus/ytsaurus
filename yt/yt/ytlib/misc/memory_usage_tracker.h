#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/error.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct INodeMemoryTracker
    : public TRefCounted
{
    using ECategory = EMemoryCategory;
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

    //! Returns true unless overcommit occurred.
    virtual bool Acquire(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {}) = 0;
    virtual TError TryAcquire(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {}) = 0;
    virtual TError TryChange(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {}) = 0;
    virtual void Release(ECategory category, i64 size, const std::optional<TPoolTag>& poolTag = {}) = 0;
    virtual i64 UpdateUsage(ECategory category, i64 newUsage) = 0;

    virtual TSharedRef Track(TSharedRef reference, EMemoryCategory category, bool keepExistingTracking) = 0;
    //! Returns an error if overcommit has occurred.
    virtual TErrorOr<TSharedRef> TryTrack(
        TSharedRef reference,
        EMemoryCategory category,
        bool keepExistingTracking) = 0;

    virtual IMemoryUsageTrackerPtr WithCategory(
        ECategory category,
        std::optional<TPoolTag> poolTag = {}) = 0;

    virtual void ClearTrackers() = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeMemoryTracker)

////////////////////////////////////////////////////////////////////////////////

TSharedRef WrapWithDelayedReferenceHolder(
    TSharedRef reference,
    TDuration delayBeforeFree,
    IInvokerPtr dtorInvoker);

/////////////////////////////////////////////////////////////////////////////

IMemoryUsageTrackerPtr WithCategory(
    const INodeMemoryTrackerPtr& memoryTracker,
    EMemoryCategory category,
    std::optional<INodeMemoryTracker::TPoolTag> poolTag = {});

INodeMemoryTrackerPtr CreateNodeMemoryTracker(
    i64 totalLimit,
    const std::vector<std::pair<EMemoryCategory, i64>>& limits = {},
    const NLogging::TLogger& logger = {},
    const NProfiling::TProfiler& profiler = {});

/////////////////////////////////////////////////////////////////////////////

TErrorOr<TSharedRef> TryTrackMemory(
    const INodeMemoryTrackerPtr& tracker,
    EMemoryCategory category,
    TSharedRef reference,
    bool keepExistingTracking = false);

TSharedRef TrackMemory(
    const INodeMemoryTrackerPtr& tracker,
    EMemoryCategory category,
    TSharedRef reference,
    bool keepExistingTracking = false);

TSharedRefArray TrackMemory(
    const INodeMemoryTrackerPtr& tracker,
    EMemoryCategory category,
    TSharedRefArray array,
    bool keepExistingTracking = false);

////////////////////////////////////////////////////////////////////////////////

IReservingMemoryUsageTrackerPtr CreateResevingMemoryUsageTracker(
    IMemoryUsageTrackerPtr underlying,
    NProfiling::TCounter memoryUsageCounter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
