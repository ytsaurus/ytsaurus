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
            YCHECK(NodeMap_.emplace(pair.second, THashSet<TNode*>()).second);
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (const auto& pair : nodeTracker->Nodes()) {
            OnNodeChanged(pair.second);
        }
    }

    const THashSet<TNode*>& GetBundleNodes(const TTabletCellBundle* bundle) const
    {
        auto it = NodeMap_.find(bundle);
        YCHECK(it != NodeMap_.end());
        return it->second;
    }

private:
    const TBootstrap* const Bootstrap_;
    THashMap<const TTabletCellBundle*, THashSet<TNode*>> NodeMap_;

    void OnTabletCellBundleCreated(TTabletCellBundle* bundle)
    {
        auto result = NodeMap_.emplace(bundle, THashSet<TNode*>());
        YCHECK(result.second);
        RevisitTabletCellBundleNodes(&result.first->second, bundle);
    }

    void OnTabletCellBundleChanged(TTabletCellBundle* bundle)
    {
        RevisitTabletCellBundleNodes(&NodeMap_[bundle], bundle);
    }

    void RevisitTabletCellBundleNodes(THashSet<TNode*>* nodeSet, TTabletCellBundle* bundle)
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (const auto& pair : nodeTracker->Nodes()) {
            AddOrRemoveNode(nodeSet, bundle, pair.second);
        }
    }

    void OnTabletCellBundleRemoved(TTabletCellBundle* bundle)
    {
        YCHECK(NodeMap_.erase(bundle) > 0);
    }

    void OnNodeFullHeartbeat(TNode* node, TReqFullHeartbeat* /*request*/)
    {
        OnNodeChanged(node);
    }

    void OnNodeChanged(TNode* node)
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        for (const auto& pair : tabletManager->TabletCellBundles()) {
            auto* bundle = pair.second;

            // TODO(savrus) Use hostility checker from tablet tracker.
            AddOrRemoveNode(&NodeMap_[bundle], bundle, node);
        }
    }

    void AddOrRemoveNode(THashSet<TNode*>* nodeSet, TTabletCellBundle* bundle, TNode* node)
    {
        if (IsGood(node) && bundle->NodeTagFilter().IsSatisfiedBy(node->Tags())) {
            if (nodeSet->find(node) == nodeSet->end()) {
                LOG_DEBUG("Node added to bundle (NodeAddress: %v, Bundle: %v)",
                    node->GetDefaultAddress(),
                    bundle->GetName());
                YCHECK(nodeSet->insert(node).second);
            }
        } else {
            auto it = nodeSet->find(node);
            if (it != nodeSet->end()) {
                LOG_DEBUG("Node removed from bundle (NodeAddress: %v, Bundle: %v)",
                    node->GetDefaultAddress(),
                    bundle->GetName());
                nodeSet->erase(it);
            }
        }
    }

    static bool IsGood(const TNode* node)
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
};

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

const THashSet<TNode*>& TBundleNodeTracker::GetBundleNodes(const TTabletCellBundle* bundle) const
{
    return Impl_->GetBundleNodes(bundle);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

