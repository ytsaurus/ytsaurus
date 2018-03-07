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

using TNodeMemoryTracker = NNodeTrackerClient::TNodeMemoryTracker;
using TNodeMemoryTrackerGuard = NNodeTrackerClient::TNodeMemoryTrackerGuard;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
