#include "node_tracker.h"
#include "private.h"
#include "config.h"
#include "node.h"
#include "rack.h"
#include "master_cache_manager.h"

#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NNodeTrackerServer {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NodeTrackerServerLogger;

////////////////////////////////////////////////////////////////////////////////

TMasterCacheManager::TMasterCacheManager(
    NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{
    Bootstrap_->GetConfigManager()->SubscribeConfigChanged(
        BIND(&TMasterCacheManager::OnDynamicConfigChanged, MakeWeak(this)));
    Bootstrap_->GetHydraFacade()->GetHydraManager()->SubscribeLeaderActive(
        BIND(&TMasterCacheManager::OnLeaderActive, MakeWeak(this)));
    Bootstrap_->GetHydraFacade()->GetHydraManager()->SubscribeStopLeading(
        BIND(&TMasterCacheManager::OnStopLeading, MakeWeak(this)));
}

void TMasterCacheManager::OnDynamicConfigChanged()
{
    Config_ = Bootstrap_->GetConfigManager()->GetConfig()->NodeTracker->MasterCacheManager;
    if (PeriodicExecutor_) {
        PeriodicExecutor_->SetPeriod(Config_->UpdatePeriod);
    }
}

void TMasterCacheManager::OnLeaderActive()
{
    PeriodicExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Periodic),
        BIND(&TMasterCacheManager::UpdateMasterCacheNodes, MakeWeak(this)));
    PeriodicExecutor_->Start();
    OnDynamicConfigChanged();
}

void TMasterCacheManager::OnStopLeading()
{
    if (PeriodicExecutor_) {
        PeriodicExecutor_->Stop();
        PeriodicExecutor_.Reset();
    }
}

bool TMasterCacheManager::IsGoodNode(const TNode* node) const
{
    return IsObjectAlive(node) && Config_->NodeTagFilter.IsSatisfiedBy(node->Tags());
}

THashMap<TRack*, int> TMasterCacheManager::CountNodesPerRack(const THashSet<TNode*>& nodes)
{
    THashMap<TRack*, int> result;
    for (auto* node : nodes) {
        if (IsObjectAlive(node)) {
            ++result[node->GetRack()];
        }
    }
    return result;
}

THashSet<TNode*> TMasterCacheManager::FindAppropriateNodes(const THashSet<TNode*>& selectedNodes, int count)
{
    auto nodeCountPerRack = CountNodesPerRack(selectedNodes);

    THashSet<TNode*> result;

    int maxPeersPerRack = Config_->MaxPeersPerRack;

    for (auto [_, node] : Bootstrap_->GetNodeTracker()->Nodes()) {
        if (count == 0) {
            break;
        }

        if (!IsGoodNode(node) || selectedNodes.contains(node)) {
            continue;
        }

        auto* rack = node->GetRack();
        if (!rack || nodeCountPerRack[rack] < maxPeersPerRack) {
            ++nodeCountPerRack[rack];
            YT_VERIFY(result.insert(node).second);
            --count;
        }
    }

    return result;
}

void TMasterCacheManager::UpdateMasterCacheNodes()
{
    auto nodes = Bootstrap_->GetNodeTracker()->GetMasterCacheNodes();
    YT_LOG_INFO("Started updating master cache nodes (OldNodes: %v)",
        MakeFormattableView(nodes, TNodePtrAddressFormatter()));

    for (auto* node : nodes) {
        if (!IsGoodNode(node)) {
            nodes.erase(node);
        }
    }

    int nodesToReplaceCount = Config_->PeerCount - static_cast<int>(nodes.size());
    if (nodesToReplaceCount == 0) {
        YT_LOG_INFO("No new master cache nodes needed");
    } else {
        YT_LOG_INFO("New master cache nodes needed (NodesToReplaceCount: %v)", nodesToReplaceCount);
    }

    auto newNodes = FindAppropriateNodes(nodes, nodesToReplaceCount);
    if (static_cast<int>(newNodes.size()) < nodesToReplaceCount) {
        YT_LOG_WARNING("Failed to find enough alive master cache nodes satisfying node tag filter (Filter: %v)",
            Config_->NodeTagFilter.GetFormula());
    }
    YT_LOG_INFO("Found new master cache nodes (FoundCount: %v, NewNodes: %v)",
         newNodes.size(),
         MakeFormattableView(newNodes, TNodePtrAddressFormatter()));

    nodes.insert(newNodes.begin(), newNodes.end());

    CommitMasterCacheNodes(nodes);
}

void TMasterCacheManager::CommitMasterCacheNodes(const THashSet<TNode*>& nodes)
{
    NProto::TReqUpdateMasterCacheNodes request;
    for (auto* node : nodes) {
        request.add_node_ids(node->GetId());
    }

    CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
        ->CommitAndLog(Logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
