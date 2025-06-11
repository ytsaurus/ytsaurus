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

    virtual int GetHugePageCount() const = 0;
    virtual int GetHugePageSize() const = 0;
    virtual bool IsEnabled() const = 0;

    virtual void Reconfigure(const THugePageManagerDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IHugePageManager)

////////////////////////////////////////////////////////////////////////////////

IHugePageManagerPtr CreateHugePageManager(
    THugePageManagerConfigPtr config,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
