#pragma once

#include "tscp.h"

#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref_tracked.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/library/profiling/tag.h>

#include <atomic>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

TValue CpuDurationToValue(TCpuDuration duration);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

////////////////////////////////////////////////////////////////////////////////
