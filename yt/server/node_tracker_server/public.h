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
class TReqRemoveNode;

} // namespace NProto

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::InvalidNodeId;

using NNodeTrackerClient::TNodeDescriptor;

DECLARE_REFCOUNTED_CLASS(TNodeTracker)

DECLARE_REFCOUNTED_CLASS(TNodeTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TNodeConfig)

class TNode;
typedef SmallVector<TNode*, NChunkClient::TypicalReplicaCount> TNodeList;
typedef TSmallSet<TNode*, NChunkClient::TypicalReplicaCount> TNodeSet;

///////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
