#pragma once

#include <yt/server/misc/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/misc/public.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

DECLARE_REFCOUNTED_CLASS(TResourceLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TCellNodeConfig)
DECLARE_REFCOUNTED_CLASS(TBatchingChunkServiceConfig)

using TNodeMemoryTracker = TMemoryUsageTracker<NNodeTrackerClient::EMemoryCategory>;
using TNodeMemoryTrackerGuard = TMemoryUsageTrackerGuard<NNodeTrackerClient::EMemoryCategory>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
