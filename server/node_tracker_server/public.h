#pragma once

#include <yt/server/hydra/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/misc/small_vector.h>

#include <bitset>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqRemoveNode;

typedef NNodeTrackerClient::NProto::TReqRegisterNode TReqRegisterNode;
typedef NNodeTrackerClient::NProto::TReqIncrementalHeartbeat TReqIncrementalHeartbeat;
typedef NNodeTrackerClient::NProto::TReqFullHeartbeat TReqFullHeartbeat;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::InvalidNodeId;

using NNodeTrackerClient::TRackId;

using NNodeTrackerClient::TDataCenterId;

using NNodeTrackerClient::TAddressMap;
using NNodeTrackerClient::TNodeAddressMap;
using NNodeTrackerClient::TNodeDescriptor;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNodeTracker)

DECLARE_REFCOUNTED_CLASS(TNodeTrackerConfig)

DECLARE_ENTITY_TYPE(TNode, NObjectClient::TObjectId, ::THash<NObjectClient::TObjectId>)
DECLARE_ENTITY_TYPE(TRack, TRackId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TDataCenter, TDataCenterId, NObjectClient::TDirectObjectIdHash)

using TNodeList = SmallVector<TNode*, NChunkClient::TypicalReplicaCount>;

constexpr int MaxRackCount = 255;
// NB: +1 is because of null rack.
constexpr int RackIndexBound = MaxRackCount + 1;
constexpr int NullRackIndex = 0;
using TRackSet = std::bitset<RackIndexBound>;

constexpr int MaxDataCenterCount = 16;
constexpr int NullDataCenterIndex = 0;
// NB: +1 is because of null dataCenter.
using TDataCenterSet = std::bitset<MaxDataCenterCount + 1>;

constexpr int TypicalInterDCEdgeCount = 9; // (2 DCs + null DC)^2
static_assert(
    TypicalInterDCEdgeCount <= NNodeTrackerServer::MaxDataCenterCount * NNodeTrackerServer::MaxDataCenterCount,
    "TypicalInterDCEdgeCount is too large.");

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
