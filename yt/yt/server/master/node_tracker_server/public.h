#pragma once

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <bitset>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqRemoveNode;

using TReqRegisterNode = NNodeTrackerClient::NProto::TReqRegisterNode;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::ENodeState;
using NNodeTrackerClient::ENodeFlavor;
using NNodeTrackerClient::InvalidNodeId;
using NNodeTrackerClient::THostId;
using NNodeTrackerClient::TRackId;
using NNodeTrackerClient::TDataCenterId;
using NNodeTrackerClient::TAddressMap;
using NNodeTrackerClient::TNodeAddressMap;
using NNodeTrackerClient::TNodeDescriptor;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(INodeTracker)
DECLARE_REFCOUNTED_STRUCT(INodeDisposalManager)

DECLARE_REFCOUNTED_CLASS(TNodeDiscoveryManager)

DECLARE_REFCOUNTED_CLASS(TNodeGroupConfig)
DECLARE_REFCOUNTED_CLASS(TNodeTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicNodeTrackerTestingConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicNodeTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TNodeDiscoveryManagerConfig)

DECLARE_REFCOUNTED_STRUCT(IExecNodeTracker)

DECLARE_ENTITY_TYPE(TNode, NObjectClient::TObjectId, ::THash<NObjectClient::TObjectId>)
DECLARE_ENTITY_TYPE(THost, THostId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TRack, TRackId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TDataCenter, TDataCenterId, NObjectClient::TDirectObjectIdHash)

DECLARE_MASTER_OBJECT_TYPE(TNode)

using TNodeList = TCompactVector<TNode*, NChunkClient::TypicalReplicaCount>;

class TNodeDirectoryBuilder;

constexpr int MaxRackCount = 255;
// NB: +1 is because of null rack.
constexpr int RackIndexBound = MaxRackCount + 1;
constexpr int NullRackIndex = 0;
using TRackSet = std::bitset<RackIndexBound>;

constexpr int MaxDataCenterCount = 16;
// NB: +1 is because of null data center.
constexpr int DataCenterIndexBound = MaxDataCenterCount + 1;
constexpr int NullDataCenterIndex = 0;
using TDataCenterSet = std::bitset<DataCenterIndexBound>;

////////////////////////////////////////////////////////////////////////////////

// COMPAT(savrus) Keep in sync with ENodeFlavor until 21.1 prevails.
DEFINE_ENUM(ENodeHeartbeatType,
    ((Cluster)      (0))
    ((Data)         (1))
    ((Exec)         (2))
    ((Tablet)       (3))
    ((Cellar)       (4))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
