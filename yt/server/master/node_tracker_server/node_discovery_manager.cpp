#include "node_tracker.h"
#include "private.h"
#include "config.h"
#include "node.h"
#include "rack.h"
#include "node_discovery_manager.h"

#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/public.h>
#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NNodeTrackerServer {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCellMaster;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

void TNodeListForRole::UpdateAddresses()
{
    Addresses_.clear();
    for (const auto* node : Nodes_) {
        Addresses_.push_back(node->GetDefaultAddress());
    }
}

void TNodeListForRole::Clear()
{
    Nodes_.clear();
    Addresses_.clear();
}

void TNodeListForRole::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Nodes_);
}

void TNodeListForRole::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, Nodes_);
    UpdateAddresses();
}

////////////////////////////////////////////////////////////////////////////////

TNodeDiscoveryManagerConfigPtr GetConfigByNodeRole(
    const TDynamicNodeTrackerConfigPtr& config,
    ENodeRole nodeRole)
{
    switch (nodeRole) {
        case ENodeRole::MasterCache:
            return config->MasterCacheManager;
        case ENodeRole::TimestampProvider:
            return config->TimestampProviderManager;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

TNodeDiscoveryManager::TNodeDiscoveryManager(
    NCellMaster::TBootstrap* bootstrap,
    ENodeRole nodeRole)
    : Bootstrap_(bootstrap)
    , NodeRole_(nodeRole)
    , Logger(NYT::NLogging::TLogger(NodeTrackerServerLogger)
        .AddTag("NodeRole: %v", NodeRole_))
{
    Bootstrap_->GetConfigManager()->SubscribeConfigChanged(
        BIND(&TNodeDiscoveryManager::OnDynamicConfigChanged, MakeWeak(this)));
    Bootstrap_->GetHydraFacade()->GetHydraManager()->SubscribeLeaderActive(
        BIND(&TNodeDiscoveryManager::OnLeaderActive, MakeWeak(this)));
    Bootstrap_->GetHydraFacade()->GetHydraManager()->SubscribeStopLeading(
        BIND(&TNodeDiscoveryManager::OnStopLeading, MakeWeak(this)));
}

void TNodeDiscoveryManager::OnDynamicConfigChanged()
{
    Config_ = GetConfigByNodeRole(Bootstrap_->GetConfigManager()->GetConfig()->NodeTracker, NodeRole_);

    if (PeriodicExecutor_) {
        PeriodicExecutor_->SetPeriod(Config_->UpdatePeriod);
    }
}

void TNodeDiscoveryManager::OnLeaderActive()
{
    PeriodicExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Periodic),
        BIND(&TNodeDiscoveryManager::UpdateNodeList, MakeWeak(this)));
    PeriodicExecutor_->Start();
    OnDynamicConfigChanged();
}

void TNodeDiscoveryManager::OnStopLeading()
{
    if (PeriodicExecutor_) {
        PeriodicExecutor_->Stop();
        PeriodicExecutor_.Reset();
    }
}

bool TNodeDiscoveryManager::IsGoodNode(const TNode* node) const
{
    return node->GetAggregatedState() == ENodeState::Online &&
        IsObjectAlive(node) &&
        Config_->NodeTagFilter.IsSatisfiedBy(node->Tags());
}

void TNodeDiscoveryManager::UpdateNodeList()
{
    auto nodes = Bootstrap_->GetNodeTracker()->GetNodesForRole(NodeRole_);

    YT_LOG_INFO("Started updating nodes (OldNodes: %v)",
        MakeFormattableView(nodes, TNodePtrAddressFormatter()));

    THashSet<TNode*> selectedNodeSet(nodes.begin(), nodes.end());
    THashMap<TRack*, int> nodeCountPerRack;

    auto isRackLimitSatisfied = [&] (TNode* node, int rackLimit) {
        if (!IsGoodNode(node)) {
            return false;
        }

        auto* rack = node->GetRack();
        return !rack || nodeCountPerRack[rack] < rackLimit;
    };

    for (auto* node : nodes) {
        if (isRackLimitSatisfied(node, Config_->MaxPeersPerRack)) {
            ++nodeCountPerRack[node->GetRack()];
        } else {
            selectedNodeSet.erase(node);
        }
    }

    while (selectedNodeSet.size() > Config_->PeerCount) {
        auto it = selectedNodeSet.begin();
        --nodeCountPerRack[(*it)->GetRack()];
        selectedNodeSet.erase(it);
    }

    int nodesToReplaceCount = Config_->PeerCount - static_cast<int>(selectedNodeSet.size());
    if (nodesToReplaceCount == 0) {
        YT_LOG_INFO("No new nodes needed");
    } else {
        YT_LOG_INFO("New nodes needed (NodesToReplaceCount: %v)", nodesToReplaceCount);
    }

    auto updateSelectedNodeSet = [&] (int maxPeersPerRack) {
        for (auto [_, node] : Bootstrap_->GetNodeTracker()->Nodes()) {
            if (nodesToReplaceCount == 0) {
                break;
            }

            if (!selectedNodeSet.contains(node) && isRackLimitSatisfied(node, maxPeersPerRack)) {
                ++nodeCountPerRack[node->GetRack()];
                YT_VERIFY(selectedNodeSet.insert(node).second);
                --nodesToReplaceCount;
            }
        }
    };

    updateSelectedNodeSet(Config_->MaxPeersPerRack);

    int rackCount = static_cast<int>(nodeCountPerRack.size());
    if (nodesToReplaceCount > 0 && rackCount > 0) {
        YT_LOG_WARNING("There is not enough alive nodes satisfying rack awareness (MaxPeersPerRack: %v, NewNodesCount: %v, NeededNodesCount: %v)",
            Config_->MaxPeersPerRack,
            selectedNodeSet.size(),
            nodesToReplaceCount);
        auto nodeCount = (nodesToReplaceCount + rackCount - 1) / rackCount;
        updateSelectedNodeSet(nodeCount);
        updateSelectedNodeSet(Config_->PeerCount);
    }

    if (static_cast<int>(selectedNodeSet.size()) < Config_->PeerCount) {
        YT_LOG_WARNING("Failed to find enough alive nodes satisfying node tag filter (Filter: %v)",
            Config_->NodeTagFilter.GetFormula());
    }
    YT_LOG_INFO("Node list updated (NewNodeList: %v)",
        MakeFormattableView(selectedNodeSet, TNodePtrAddressFormatter()));

    CommitNewNodes(selectedNodeSet);
}

void TNodeDiscoveryManager::CommitNewNodes(const THashSet<TNode*>& nodes)
{
    NProto::TReqUpdateNodesForRole request;
    request.set_node_role(static_cast<int>(NodeRole_));
    for (auto* node : nodes) {
        request.add_node_ids(node->GetId());
    }

    CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
        ->CommitAndLog(Logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
