#pragma once

#include "public.h"

#include <yt/core/misc/small_vector.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

static const ui32 UncommittedRevision = 0;
static const ui32 InvalidRevision = std::numeric_limits<ui32>::max();
static const ui32 MaxRevision = std::numeric_limits<ui32>::max() - 1;

static const int TypicalStoreIdCount = 8;
using TStoreIdList = SmallVector<TStoreId, TypicalStoreIdCount>;

static const int InitialEditListCapacity = 2;
static const int EditListCapacityMultiplier = 2;
static const int MaxEditListCapacity = 256;

static const int MaxOrderedDynamicSegments = 32;
static const int InitialOrderedDynamicSegmentIndex = 10;

static const i64 MemoryUsageGranularity = (i64) 16 * 1024;

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger TabletNodeLogger;
extern const NProfiling::TProfiler TabletNodeProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
