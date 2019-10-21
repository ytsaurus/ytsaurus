#pragma once

#include <yt/server/lib/misc/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/misc/public.h>

namespace NYT::NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

DECLARE_REFCOUNTED_CLASS(TResourceLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TCellNodeConfig)
DECLARE_REFCOUNTED_CLASS(TBatchingChunkServiceConfig)

using NNodeTrackerClient::TNodeMemoryTracker;
using NNodeTrackerClient::TNodeMemoryTrackerPtr;
using NNodeTrackerClient::TNodeMemoryTrackerGuard;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((UnrecognizedConfigOption)   (1400))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellNode
