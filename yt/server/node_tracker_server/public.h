#pragma once

#include <yt/server/hydra/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/misc/small_vector.h>

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

///////////////////////////////////////////////////////////////////////////////

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::InvalidNodeId;

using NNodeTrackerClient::TRackId;
using NNodeTrackerClient::NullRackId;

using NNodeTrackerClient::TAddressMap;
using NNodeTrackerClient::TNodeDescriptor;

///////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNodeTracker)

DECLARE_REFCOUNTED_CLASS(TNodeTrackerConfig)

DECLARE_ENTITY_TYPE(TNode, NObjectClient::TObjectId, ::THash<NObjectClient::TObjectId>)
DECLARE_ENTITY_TYPE(TRack, TRackId, NObjectClient::TDirectObjectIdHash)

typedef SmallVector<TNode*, NChunkClient::TypicalReplicaCount> TNodeList;

typedef ui64 TRackSet;
const int MaxRackCount = 63;
const int NullRackIndex = 0;
const int NullRackMask = 1ULL;

///////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
