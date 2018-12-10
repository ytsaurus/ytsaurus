#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/node_tracker_server/public.h>

#include <yt/core/actions/signal.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TBundleNodeTracker
    : public TRefCounted
{
public:
    using TNodeSet = THashSet<const NNodeTrackerServer::TNode*>;

    explicit TBundleNodeTracker(
        NCellMaster::TBootstrap* bootstrap);
    ~TBundleNodeTracker();

    void Initialize();
    void OnAfterSnapshotLoaded();
    void Clear();

    const TNodeSet& GetBundleNodes(const TTabletCellBundle* bundle) const;

    DECLARE_SIGNAL(void(const TTabletCellBundle*), BundleNodesChanged);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TBundleNodeTracker)

////////////////////////////////////////////////////////////////////////////////

bool CheckIfNodeCanHostTabletCells(const NNodeTrackerServer::TNode* node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

