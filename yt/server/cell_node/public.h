#pragma once

#include <core/misc/public.h>

#include <ytlib/node_tracker_client/public.h>

#include <server/misc/public.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

DECLARE_REFCOUNTED_CLASS(TResourceLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TCellNodeConfig)

using TNodeMemoryTracker = TMemoryUsageTracker<NNodeTrackerClient::EMemoryCategory>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
