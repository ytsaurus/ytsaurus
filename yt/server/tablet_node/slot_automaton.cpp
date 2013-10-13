#include "stdafx.h"
#include "slot_automaton.h"
#include "tablet_slot.h"
#include "private.h"

#include <server/hydra/hydra_manager.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TSlotAutomaton::TSlotAutomaton(
    NCellNode::TBootstrap* bootstrap,
    TTabletSlot* slot)
    : Slot(slot)
{
    Logger.AddTag(Sprintf("CellGuid: %s",
        ~ToString(Slot->GetCellGuid())));
}

TSaveContext& TSlotAutomaton::SaveContext()
{
    return SaveContext_;
}

TLoadContext& TSlotAutomaton::LoadContext()
{
    return LoadContext_;
}

bool TSlotAutomaton::ValidateSnapshotVersion(int version)
{
    return NTabletNode::ValidateSnapshotVersion(version);
}

int TSlotAutomaton::GetCurrentSnapshotVersion()
{
    return NTabletNode::GetCurrentSnapshotVersion();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
