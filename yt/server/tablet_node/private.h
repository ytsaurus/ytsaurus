#pragma once

#include "public.h"

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

static const ui32 UncommittedRevision = 0;
static const ui32 InvalidRevision = 0;
static const ui32 MaxRevision = std::numeric_limits<ui32>::max();

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger TabletNodeLogger;
extern const NProfiling::TProfiler TabletNodeProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
