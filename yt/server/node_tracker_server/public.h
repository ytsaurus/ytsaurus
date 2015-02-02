#pragma once

#include <core/misc/small_vector.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NNodeTrackerServer {

///////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqUnregisterNode;
class TReqRemoveNode;

typedef NNodeTrackerClient::NProto::TReqRegisterNode TReqRegisterNode;
typedef NNodeTrackerClient::NProto::TReqIncrementalHeartbeat TReqIncrementalHeartbeat;
typedef NNodeTrackerClient::NProto::TReqFullHeartbeat TReqFullHeartbeat;

} // namespace NProto

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::InvalidNodeId;

using NNodeTrackerClient::TRackId;
using NNodeTrackerClient::NullRackId;

using NNodeTrackerClient::TNodeDescriptor;

DECLARE_REFCOUNTED_CLASS(TNodeTracker)

DECLARE_REFCOUNTED_CLASS(TNodeTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TNodeConfig)

class TNode;
typedef SmallVector<TNode*, NChunkClient::TypicalReplicaCount> TNodeList;
typedef TNodeList TSortedNodeList; // to clarify the semantics

class TRack;
typedef ui64 TRackSet;
const int MaxRackCount = 63;
const int NullRackIndex = 0;
const int NullRackMask = 1ULL;

///////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
