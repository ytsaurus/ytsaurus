#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/node_tracker_server/public.h>

#include <yt/core/actions/signal.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

class TBundleNodeTracker
    : public TRefCounted
{
public:
    using TNodeSet = THashSet<const NNodeTrackerServer::TNode*>;

    explicit TBundleNodeTracker(NCellMaster::TBootstrap* bootstrap);

    ~TBundleNodeTracker();

    void Initialize();
    void Clear();

    const TNodeSet& GetBundleNodes(const TCellBundle* bundle) const;

    DECLARE_SIGNAL(void(const TCellBundle*), BundleNodesChanged);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TBundleNodeTracker)

////////////////////////////////////////////////////////////////////////////////

bool CheckIfNodeCanHostCells(const NNodeTrackerServer::TNode* node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
