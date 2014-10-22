#pragma once

#include <core/misc/common.h>
#include <core/misc/small_vector.h>
#include <core/misc/small_set.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NNodeTrackerServer {

///////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqUnregisterNode;

} // namespace NProto

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::InvalidNodeId;

using NNodeTrackerClient::TNodeDescriptor;

class TNodeTracker;
typedef TIntrusivePtr<TNodeTracker> TNodeTrackerPtr;

class TNode;
typedef SmallVector<TNode*, NChunkClient::TypicalReplicaCount> TNodeList;
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
