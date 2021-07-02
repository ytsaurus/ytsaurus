#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

void ValidateTabletCellSnapshot(
    NClusterNode::IBootstrapBase* bootstrap,
    const NConcurrency::IAsyncZeroCopyInputStreamPtr& reader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
