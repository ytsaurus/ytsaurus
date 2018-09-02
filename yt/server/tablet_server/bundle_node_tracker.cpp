#include "public.h"
#include "private.h"
#include "bundle_node_tracker.h"
#include "tablet_manager.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/node_tracker_server/node_tracker.h>

namespace NYT {
namespace NTabletServer {

using namespace NNodeTrackerServer;
using namespace NNodeTrackerServer::NProto;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

const auto static& Logger = TabletServerLogger;

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

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->SubscribeTabletCellBundleCreated(BIND(&TImpl::OnTabletCellBundleCreated, MakeWeak(this)));
        tabletManager->SubscribeTabletCellBundleDestroyed(BIND(&TImpl::OnTabletCellBundleRemoved, MakeWeak(this)));
        tabletManager->SubscribeTabletCellBundleNodeTagFilterChanged(BIND(&TImpl::OnTabletCellBundleChanged, MakeWeak(this)));
    }

    void OnAfterSnapshotLoaded()
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        for (const auto& pair : tabletManager->TabletCellBundles()) {
            YCHECK(NodeMap_.emplace(pair.second, TNodeSet()).second);
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (const auto& pair : nodeTracker->Nodes()) {
            OnNodeChanged(pair.second);
        }
    }

    const TNodeSet& GetBundleNodes(const TTabletCellBundle* bundle) const
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

    DEFINE_SIGNAL(void(const TTabletCellBundle* bundle), BundleNodesChanged);

private:
    const TBootstrap* const Bootstrap_;
    THashMap<const TTabletCellBundle*, TNodeSet> NodeMap_;
    static const TNodeSet EmptyNodeSet;

    void OnTabletCellBundleCreated(TTabletCellBundle* bundle)
    {
        LOG_DEBUG("Bundle node tracker caught bundle create signal (BundleId: %v)",
            bundle->GetId());

        auto result = NodeMap_.emplace(bundle, TNodeSet());
        YCHECK(result.second);
        RevisitTabletCellBundleNodes(&result.first->second, bundle);
    }

    void OnTabletCellBundleChanged(TTabletCellBundle* bundle)
    {
        LOG_DEBUG("Bundle node tracker caught bundle change signal (BundleId: %v)",
            bundle->GetId());

        RevisitTabletCellBundleNodes(&NodeMap_[bundle], bundle);
    }

    void RevisitTabletCellBundleNodes(TNodeSet* nodeSet, TTabletCellBundle* bundle)
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (const auto& pair : nodeTracker->Nodes()) {
            AddOrRemoveNode(nodeSet, bundle, pair.second);
        }
    }

    void OnTabletCellBundleRemoved(TTabletCellBundle* bundle)
    {
        LOG_DEBUG("Bundle node tracker caught bundle remove signal (BundleId: %v)",
            bundle->GetId());

        YCHECK(NodeMap_.erase(bundle) > 0);
    }

    void OnNodeFullHeartbeat(TNode* node, TReqFullHeartbeat* /*request*/)
    {
        OnNodeChanged(node);
    }

    void OnNodeChanged(TNode* node)
    {
        LOG_DEBUG("Bundle node tracker caught node change signal (NodeAddress: %v)",
            node->GetDefaultAddress());

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        for (const auto& pair : tabletManager->TabletCellBundles()) {
            auto* bundle = pair.second;

            // TODO(savrus) Use hostility checker from tablet tracker.
            AddOrRemoveNode(&NodeMap_[bundle], bundle, node);
        }
    }

    void AddOrRemoveNode(TNodeSet* nodeSet, TTabletCellBundle* bundle, TNode* node)
    {
        bool good = CheckIfNodeCanHostTabletCells(node);
        bool satisfy = bundle->NodeTagFilter().IsSatisfiedBy(node->Tags());

        LOG_DEBUG("Bundle node tracker is checking node (NodeAddress: %v, BundleId: %v, State: %v, IsGood: %v, Satisfy: %v)",
            node->GetDefaultAddress(),
            bundle->GetId(),
            node->GetLocalState(),
            good,
            satisfy);

        if (good & satisfy) {
            if (nodeSet->find(node) == nodeSet->end()) {
                LOG_DEBUG("Node added to bundle (NodeAddress: %v, BundleId: %v)",
                    node->GetDefaultAddress(),
                    bundle->GetId());
                YCHECK(nodeSet->insert(node).second);
                BundleNodesChanged_.Fire(bundle);
            }
        } else {
            auto it = nodeSet->find(node);
            if (it != nodeSet->end()) {
                LOG_DEBUG("Node removed from bundle (NodeAddress: %v, BundleId: %v)",
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

TBundleNodeTracker::~TBundleNodeTracker() = default;

void TBundleNodeTracker::Initialize()
{
    Impl_->Initialize();
}

void TBundleNodeTracker::OnAfterSnapshotLoaded()
{
    Impl_->OnAfterSnapshotLoaded();
}

void TBundleNodeTracker::Clear()
{
    Impl_->Clear();
}

const TBundleNodeTracker::TNodeSet& TBundleNodeTracker::GetBundleNodes(const TTabletCellBundle* bundle) const
{
    return Impl_->GetBundleNodes(bundle);
}

DELEGATE_SIGNAL(TBundleNodeTracker, void(const TTabletCellBundle*), BundleNodesChanged, *Impl_);

////////////////////////////////////////////////////////////////////////////////

bool CheckIfNodeCanHostTabletCells(const TNode* node)
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

} // namespace NTabletServer
} // namespace NYT

