#pragma once

#include "private.h"
#include "util/generic/fwd.h"

#include <yt/yt/server/master/cell_server/area.h>
#include <yt/yt/server/master/cell_server/cell_base.h>
#include <yt/yt/server/master/cell_server/cell_bundle.h>
#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/cell_server/cell_balancer.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>

#include <yt/yt/ytlib/cell_balancer/proto/cell_tracker_service.pb.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class TClusterStateProvider
    : public TRefCounted
{
public:
    TClusterStateProvider(NCellBalancerClient::NProto::TRspGetClusterState* response);

    std::vector<NCellServer::TNodeHolder> GetNodes(NCellarClient::ECellarType cellarType);
    bool IsPossibleHost(const NNodeTrackerServer::TNode* node, const NCellServer::TArea* area) const;

    DECLARE_ENTITY_MAP_ACCESSORS(CellBundle, NCellServer::TCellBundle);

    bool IsSlotWarmedUp(const NNodeTrackerServer::TNode::TCellSlot* slot) const;
    bool CheckIfNodeCanHostCells(const NNodeTrackerServer::TNode* node) const;
    const THashSet<NCellServer::TCellBase*>& Cells(NCellarClient::ECellarType cellarType);
    bool IsAlienPeer(const NCellServer::TCellBase* cell, NCellServer::TPeerId peerId) const;

    NNodeTrackerServer::TNode* FindNodeByAddress(const TString& address);

private:
    NHydra::TEntityMap<NCellServer::TCellBundle> CellBundleMap_;
    NHydra::TEntityMap<NCellServer::TCellBase> CellMap_;
    NHydra::TEntityMap<NCellServer::TArea> AreaMap_;

    THashMap<NNodeTrackerServer::TNodeId, std::unique_ptr<NNodeTrackerServer::TNode>> NodeMap_;
    THashMap<const NCellServer::TArea*, THashSet<const NNodeTrackerServer::TNode*>> AreaToNodesMap_;

    THashSet<const NNodeTrackerServer::TNode::TCellSlot*> WarmedUpSlots_;
    THashSet<const NNodeTrackerServer::TNode*> CellHostNodes_;
    THashMap<TString, NCellServer::TCellSet> AddressToCell_;
    THashMap<NCellarClient::ECellarType, THashSet<NCellServer::TCellBase*>> CellsPerTypeMap_;
    THashMap<const NCellServer::TCellBase*, THashSet<int>> AlienPeers_;
    THashMap<TString, NNodeTrackerServer::TNode*> AddressToNode_;

    const NCellServer::TCellSet* FindAssignedCells(const TString& address) const;
};

DEFINE_REFCOUNTED_TYPE(TClusterStateProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
