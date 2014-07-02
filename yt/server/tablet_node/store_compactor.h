#pragma once

#include "public.h"

#include <server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

void StartStoreCompactor(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
