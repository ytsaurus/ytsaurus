#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

void StartStoreTrimmer(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
