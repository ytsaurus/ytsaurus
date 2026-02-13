#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct IHugePageManager
    : public TRefCounted
{
    virtual TErrorOr<TSharedMutableRef> ReserveHugePageBlob() = 0;

    virtual int GetUsedHugePageCount() const = 0;
    virtual i64 GetHugePageMemoryLimit() const = 0;
    virtual i64 GetHugePageBlobSize() const = 0;
    virtual i64 GetHugePageSize() const = 0;
    virtual bool IsEnabled() const = 0;

    virtual void Reconfigure(const THugePageManagerDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IHugePageManager)

////////////////////////////////////////////////////////////////////////////////

IHugePageManagerPtr CreateHugePageManager(
    THugePageManagerConfigPtr config,
    NProfiling::TProfiler profiler,
    IMemoryUsageTrackerPtr memoryUsageTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
