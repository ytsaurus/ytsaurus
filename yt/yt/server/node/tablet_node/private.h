#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_node/private.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TWriteContext;

static constexpr ui32 UncommittedRevision = 0;
static constexpr ui32 InvalidRevision = std::numeric_limits<ui32>::max();
static constexpr ui32 MaxRevision = std::numeric_limits<ui32>::max() - 1;

static constexpr int InitialEditListCapacity = 2;
static constexpr int EditListCapacityMultiplier = 2;
static constexpr int MaxEditListCapacity = 256;

static constexpr int MaxOrderedDynamicSegments = 32;
static constexpr int InitialOrderedDynamicSegmentIndex = 10;

static constexpr i64 MemoryUsageGranularity = 16_KB;

static constexpr auto TabletStoresUpdateThrottlerRpcTimeout = TDuration::Minutes(10);
static constexpr auto LookupThrottlerRpcTimeout = TDuration::Seconds(15);
static constexpr auto SelectThrottlerRpcTimeout = TDuration::Seconds(15);
static constexpr auto CompactionReadThrottlerRpcTimeout = TDuration::Minutes(1);
static constexpr auto WriteThrottlerRpcTimeout = TDuration::Seconds(15);

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger LsmLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
