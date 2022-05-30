#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

void ValidateTabletCellSnapshot(
    NClusterNode::IBootstrapBase* bootstrap,
    int snapshotId,
    const NHydra::TSnapshotParams& snapshotParams,
    const NConcurrency::IAsyncZeroCopyInputStreamPtr& reader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
