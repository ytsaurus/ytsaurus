#include "bundle_node_tracker.h"

#include "area.h"
#include "private.h"
#include "tamed_cell_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/lib/hydra/hydra_manager.h>

namespace NYT::NCellServer {

using namespace NNodeTrackerServer;
using namespace NNodeTrackerServer::NProto;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

const auto static& Logger = CellServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TBundleNodeTracker::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void Initialize()
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->SubscribeNodeRegistered(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeOnline(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeUnregistered(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeDisposed(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeBanChanged(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeDecommissionChanged(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeDisableTabletCellsChanged(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeTagsChanged(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        cellManager->SubscribeAreaCreated(BIND(&TImpl::OnAreaCreated, MakeWeak(this)));
        cellManager->SubscribeAreaDestroyed(BIND(&TImpl::OnAreaRemoved, MakeWeak(this)));
        cellManager->SubscribeAreaNodeTagFilterChanged(BIND(&TImpl::OnAreaChanged, MakeWeak(this)));
        cellManager->SubscribeAfterSnapshotLoaded(BIND(&TImpl::OnAfterSnapshotLoaded, MakeWeak(this)));
    }

    void OnAfterSnapshotLoaded()
    {
        Clear();

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        for (auto [bundleId, bundle] : cellManager->CellBundles()) {
            for (const auto& [_, area] : bundle->Areas()) {
                YT_VERIFY(NodeMap_.emplace(area, TNodeSet()).second);
            }
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (auto [_, node] : nodeTracker->Nodes()) {
            OnNodeChanged(node);
        }
    }

    const TNodeSet& GetAreaNodes(const TArea* area) const
    {
        if (auto it = NodeMap_.find(area)) {
            return it->second;
        } else {
            return EmptyNodeSet;
        }
    }

    void Clear()
    {
        NodeMap_.clear();
    }

    DEFINE_SIGNAL(void(const TArea* area), AreaNodesChanged);

private:
    TBootstrap* const Bootstrap_;

    THashMap<const TArea*, TNodeSet> NodeMap_;
    static const TNodeSet EmptyNodeSet;

    void OnAreaCreated(TArea* area)
    {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Bundle node tracker caught area create signal (CellBundle: %v, Area: %v, AreaId: %v)",
            area->GetCellBundle()->GetName(),
            area->GetName(),
            area->GetId());

        auto result = NodeMap_.emplace(area, TNodeSet());
        YT_VERIFY(result.second);
        RevisitAreaNodes(&result.first->second, area);
    }

    void OnAreaChanged(TArea* area)
    {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Bundle node tracker caught area change signal (CellBundle: %v, Area: %v, AreaId: %v)",
            area->GetCellBundle()->GetName(),
            area->GetName(),
            area->GetId());

        RevisitAreaNodes(&GetOrCrash(NodeMap_, area), area);
    }

    void RevisitAreaNodes(TNodeSet* nodeSet, TArea* area)
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (auto [_, node] : nodeTracker->Nodes()) {
            AddOrRemoveNode(nodeSet, area, node);
        }
    }

    void OnAreaRemoved(TArea* area)
    {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Bundle node tracker caught area remove signal (CellBundle: %v, Area: %v, AreaId: %v)",
            area->GetCellBundle()->GetName(),
            area->GetName(),
            area->GetId());

        YT_VERIFY(NodeMap_.erase(area) > 0);
    }

    void OnNodeChanged(TNode* node)
    {
        YT_LOG_DEBUG("Bundle node tracker caught node change signal (NodeAddress: %v)",
            node->GetDefaultAddress());

        // TODO(gritukan): Ignore non-tablet nodes.

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        for (auto [bundleId, bundle] : cellManager->CellBundles()) {
            // TODO(savrus): Use hostility checker from cell tracker.
            if (!IsObjectAlive(bundle)) {
                continue;
            }
            for (const auto& [_, area] : bundle->Areas()) {
                AddOrRemoveNode(&GetOrCrash(NodeMap_, area), area, node);
            }
        }
    }

    void AddOrRemoveNode(TNodeSet* nodeSet, TArea* area, TNode* node)
    {
        bool good = CheckIfNodeCanHostCells(node);
        bool satisfy = area->NodeTagFilter().IsSatisfiedBy(node->Tags());

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Bundle node tracker is checking node (NodeAddress: %v, CellBundle: %v, Area: %v, AreaId: %v, "
            "State: %v, ReportedTabletNodeHeartbeat: %v, IsGood: %v, Satisfy: %v)",
            node->GetDefaultAddress(),
            area->GetCellBundle()->GetName(),
            area->GetName(),
            area->GetId(),
            node->GetLocalState(),
            node->ReportedTabletNodeHeartbeat(),
            good,
            satisfy);

        if (good & satisfy) {
            if (!nodeSet->contains(node)) {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Node added to area (NodeAddress: %v, CellBundle: %v, Area: %v, AreaId: %v)",
                    node->GetDefaultAddress(),
                    area->GetCellBundle()->GetName(),
                    area->GetName(),
                    area->GetId());
                YT_VERIFY(nodeSet->insert(node).second);
                AreaNodesChanged_.Fire(area);
            }
        } else {
            if (auto it = nodeSet->find(node); it != nodeSet->end()) {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Node removed from area (NodeAddress: %v, CellBundle: %v, Area: %v, AreaId: %v)",
                    node->GetDefaultAddress(),
                    area->GetCellBundle()->GetName(),
                    area->GetName(),
                    area->GetId());
                nodeSet->erase(it);
                AreaNodesChanged_.Fire(area);
            }
        }
    }

    bool IsMutationLoggingEnabled()
    {
        return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsMutationLoggingEnabled();
    }
};

const TBundleNodeTracker::TNodeSet  TBundleNodeTracker::TImpl::EmptyNodeSet;

////////////////////////////////////////////////////////////////////////////////

TBundleNodeTracker::TBundleNodeTracker(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TBundleNodeTracker::~TBundleNodeTracker()
{ }

void TBundleNodeTracker::Initialize()
{
    Impl_->Initialize();
}

void TBundleNodeTracker::Clear()
{
    Impl_->Clear();
}

const TBundleNodeTracker::TNodeSet& TBundleNodeTracker::GetAreaNodes(const TArea* area) const
{
    return Impl_->GetAreaNodes(area);
}

DELEGATE_SIGNAL(TBundleNodeTracker, void(const TArea*), AreaNodesChanged, *Impl_);

////////////////////////////////////////////////////////////////////////////////

bool CheckIfNodeCanHostCells(const TNode* node)
{
    if (!IsObjectAlive(node)) {
        return false;
    }

    if (!node->ReportedTabletNodeHeartbeat()) {
        return false;
    }

    if (node->GetBanned()) {
        return false;
    }

    if (node->GetDecommissioned()) {
        return false;
    }

    if (node->GetDisableTabletCells()) {
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
