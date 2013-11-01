#pragma once

#include "public.h"

#include <server/hydra/hydra_service.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTabletService
    : public NHydra::THydraServiceBase
{
public:
    TTabletService(
        TTabletSlot* slot,
        NCellNode::TBootstrap* bootstrap);

private:
    TTabletSlot* Slot;
    NCellNode::TBootstrap* Bootstrap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
