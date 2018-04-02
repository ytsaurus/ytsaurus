#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/node_tracker_server/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TBundleNodeTracker
    : public TRefCounted
{
public:
    explicit TBundleNodeTracker(
        NCellMaster::TBootstrap* bootstrap);
    ~TBundleNodeTracker();

    void Initialize();
    void OnAfterSnapshotLoaded();

    const THashSet<NNodeTrackerServer::TNode*>& GetBundleNodes(const TTabletCellBundle* bundle) const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TBundleNodeTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

