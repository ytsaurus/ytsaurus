#include "public.h"
#include "private.h"
#include "bundle_node_tracker.h"
#include "tamed_cell_manager.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/node_tracker_server/node_tracker.h>

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
        nodeTracker->SubscribeNodeUnregistered(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeDisposed(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeBanChanged(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeDecommissionChanged(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeDisableTabletCellsChanged(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeTagsChanged(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeFullHeartbeat(BIND(&TImpl::OnNodeFullHeartbeat, MakeWeak(this)));

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        cellManager->SubscribeCellBundleCreated(BIND(&TImpl::OnCellBundleCreated, MakeWeak(this)));
        cellManager->SubscribeCellBundleDestroyed(BIND(&TImpl::OnCellBundleRemoved, MakeWeak(this)));
        cellManager->SubscribeCellBundleNodeTagFilterChanged(BIND(&TImpl::OnCellBundleChanged, MakeWeak(this)));
        cellManager->SubscribeAfterSnapshotLoaded(BIND(&TImpl::OnAfterSnapshotLoaded, MakeWeak(this)));
    }

    void OnAfterSnapshotLoaded()
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        for (const auto [bundleId, bundle] : cellManager->CellBundles()) {
            YT_VERIFY(NodeMap_.emplace(bundle, TNodeSet()).second);
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (const auto [nodeId, node] : nodeTracker->Nodes()) {
            OnNodeChanged(node);
        }
    }

    const TNodeSet& GetBundleNodes(const TCellBundle* bundle) const
    {
        if (auto it = NodeMap_.find(bundle)) {
            return it->second;
        } else {
            return EmptyNodeSet;
        }
    }

    void Clear()
    {
        NodeMap_.clear();
    }

    DEFINE_SIGNAL(void(const TCellBundle* bundle), BundleNodesChanged);

private:
    const TBootstrap* const Bootstrap_;
    THashMap<const TCellBundle*, TNodeSet> NodeMap_;
    static const TNodeSet EmptyNodeSet;

    void OnCellBundleCreated(TCellBundle* bundle)
    {
        YT_LOG_DEBUG("Bundle node tracker caught bundle create signal (BundleId: %v)",
            bundle->GetId());

        auto result = NodeMap_.emplace(bundle, TNodeSet());
        YT_VERIFY(result.second);
        RevisitCellBundleNodes(&result.first->second, bundle);
    }

    void OnCellBundleChanged(TCellBundle* bundle)
    {
        YT_LOG_DEBUG("Bundle node tracker caught bundle change signal (BundleId: %v)",
            bundle->GetId());

        RevisitCellBundleNodes(&NodeMap_[bundle], bundle);
    }

    void RevisitCellBundleNodes(TNodeSet* nodeSet, TCellBundle* bundle)
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (const auto [nodeId, node] : nodeTracker->Nodes()) {
            AddOrRemoveNode(nodeSet, bundle, node);
        }
    }

    void OnCellBundleRemoved(TCellBundle* bundle)
    {
        YT_LOG_DEBUG("Bundle node tracker caught bundle remove signal (BundleId: %v)",
            bundle->GetId());

        YT_VERIFY(NodeMap_.erase(bundle) > 0);
    }

    void OnNodeFullHeartbeat(TNode* node, TReqFullHeartbeat* /*request*/)
    {
        OnNodeChanged(node);
    }

    void OnNodeChanged(TNode* node)
    {
        YT_LOG_DEBUG("Bundle node tracker caught node change signal (NodeAddress: %v)",
            node->GetDefaultAddress());

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        for (const auto [bundleId, bundle] : cellManager->CellBundles()) {
            // TODO(savrus) Use hostility checker from cell tracker.
            AddOrRemoveNode(&NodeMap_[bundle], bundle, node);
        }
    }

    void AddOrRemoveNode(TNodeSet* nodeSet, TCellBundle* bundle, TNode* node)
    {
        bool good = CheckIfNodeCanHostCells(node);
        bool satisfy = bundle->NodeTagFilter().IsSatisfiedBy(node->Tags());

        YT_LOG_DEBUG("Bundle node tracker is checking node (NodeAddress: %v, BundleId: %v, State: %v, IsGood: %v, Satisfy: %v)",
            node->GetDefaultAddress(),
            bundle->GetId(),
            node->GetLocalState(),
            good,
            satisfy);

        if (good & satisfy) {
            if (nodeSet->find(node) == nodeSet->end()) {
                YT_LOG_DEBUG("Node added to bundle (NodeAddress: %v, BundleId: %v)",
                    node->GetDefaultAddress(),
                    bundle->GetId());
                YT_VERIFY(nodeSet->insert(node).second);
                BundleNodesChanged_.Fire(bundle);
            }
        } else {
            auto it = nodeSet->find(node);
            if (it != nodeSet->end()) {
                YT_LOG_DEBUG("Node removed from bundle (NodeAddress: %v, BundleId: %v)",
                    node->GetDefaultAddress(),
                    bundle->GetId());
                nodeSet->erase(it);
                BundleNodesChanged_.Fire(bundle);
            }
        }
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

const TBundleNodeTracker::TNodeSet& TBundleNodeTracker::GetBundleNodes(const TCellBundle* bundle) const
{
    return Impl_->GetBundleNodes(bundle);
}

DELEGATE_SIGNAL(TBundleNodeTracker, void(const TCellBundle*), BundleNodesChanged, *Impl_);

////////////////////////////////////////////////////////////////////////////////

bool CheckIfNodeCanHostCells(const TNode* node)
{
    if (!IsObjectAlive(node)) {
        return false;
    }

    if (node->GetLocalState() != ENodeState::Online) {
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
