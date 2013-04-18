#pragma once

#include "public.h"

#include <server/cell_node/public.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

//! Controls acquisition and release of slots.
class TSlotManager
    : public TRefCounted
{
public:
    TSlotManager(
        TSlotManagerConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    //! Initializes slots etc.
    void Initialize(int slotCount);

    //! Returns a free slot or |nullptr| if there are none.
    TSlotPtr FindFreeSlot();

private:
    TSlotManagerConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;

    std::vector<TSlotPtr> Slots;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecNode
} // namespace NYT
