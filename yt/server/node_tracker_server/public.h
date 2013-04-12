#pragma once

#include <ytlib/misc/common.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NNodeTrackerServer {

///////////////////////////////////////////////////////////////////////////////

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::TNodeDescriptor;

class TNodeTracker;
typedef TIntrusivePtr<TNodeTracker> TNodeTrackerPtr;

struct INodeAuthority;
typedef TIntrusivePtr<INodeAuthority> INodeAuthorityPtr;

class TNode;

class TNodeTrackerService;
typedef TIntrusivePtr<TNodeTrackerService> TNodeTrackerServicePtr;

class TNodeTrackerConfig;
typedef TIntrusivePtr<TNodeTrackerConfig> TNodeTrackerConfigPtr;

///////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
