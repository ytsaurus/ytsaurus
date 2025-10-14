#pragma once

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TChunkLocationConfigBase)
DECLARE_REFCOUNTED_STRUCT(TChunkLocationDynamicConfigBase)

DECLARE_REFCOUNTED_CLASS(TLocationFairShareSlot)
DECLARE_REFCOUNTED_STRUCT(TLocationPerformanceCounters)
DECLARE_REFCOUNTED_CLASS(TChunkLocationBase)

////////////////////////////////////////////////////////////////////////////////

using NChunkClient::TChunkId;
using NChunkClient::ESessionType;
using NChunkClient::TChunkLocationUuid;
using NNodeTrackerClient::TChunkLocationIndex;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNode
