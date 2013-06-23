#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/small_vector.h>
#include <ytlib/misc/small_set.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NNodeTrackerServer {

///////////////////////////////////////////////////////////////////////////////

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::TNodeDescriptor;

class TNodeTracker;
typedef TIntrusivePtr<TNodeTracker> TNodeTrackerPtr;

class TNode;
typedef TSmallVector<TNode*, NChunkClient::TypicalReplicaCount> TNodeList;
typedef TSmallSet<TNode*, NChunkClient::TypicalReplicaCount> TNodeSet;

class TNodeTrackerService;
typedef TIntrusivePtr<TNodeTrackerService> TNodeTrackerServicePtr;

class TNodeTrackerConfig;
typedef TIntrusivePtr<TNodeTrackerConfig> TNodeTrackerConfigPtr;

class TNodeConfig;
typedef TIntrusivePtr<TNodeConfig> TNodeConfigPtr;

///////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
