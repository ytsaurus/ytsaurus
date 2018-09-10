#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateInMemoryService(
    TInMemoryManagerConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
