#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateTabletService(
    TTabletSlotPtr slot,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
