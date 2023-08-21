#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/core/actions/signal.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

struct IBundleNodeTracker
    : public TRefCounted
{
public:
    using TNodeSet = THashSet<const NNodeTrackerServer::TNode*>;

    virtual void Initialize() = 0;
    virtual void Clear() = 0;

    virtual const TNodeSet& GetAreaNodes(const TArea* area) const = 0;

    DECLARE_INTERFACE_SIGNAL(void(const TArea*), AreaNodesChanged);
};

DEFINE_REFCOUNTED_TYPE(IBundleNodeTracker)

////////////////////////////////////////////////////////////////////////////////

IBundleNodeTrackerPtr CreateBundleNodeTracker(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

bool CheckIfNodeCanHostCells(const NNodeTrackerServer::TNode* node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
