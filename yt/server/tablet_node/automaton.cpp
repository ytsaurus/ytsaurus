#include "stdafx.h"
#include "automaton.h"
#include "tablet_slot.h"
#include "serialize.h"
#include "private.h"

#include <server/hydra/hydra_manager.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NHydra;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

TTabletAutomaton::TTabletAutomaton(
    NCellNode::TBootstrap* bootstrap,
    TTabletSlot* slot)
    : Slot_(slot)
{
    Logger.AddTag(Sprintf("CellGuid: %s",
        ~ToString(Slot_->GetCellGuid())));
}

TSaveContext& TTabletAutomaton::SaveContext()
{
    return SaveContext_;
}

TLoadContext& TTabletAutomaton::LoadContext()
{
    return LoadContext_;
}

////////////////////////////////////////////////////////////////////////////////

TTabletAutomatonPart::TTabletAutomatonPart(
    TTabletSlot* slot,
    TBootstrap* bootstrap)
    : TCompositeAutomatonPart(
        slot->GetHydraManager(),
        slot->GetAutomaton())
    , Slot_(slot)
    , Bootstrap_(bootstrap)
{ }

bool TTabletAutomatonPart::ValidateSnapshotVersion(int version)
{
    return NTabletNode::ValidateSnapshotVersion(version);
}

int TTabletAutomatonPart::GetCurrentSnapshotVersion()
{
    return NTabletNode::GetCurrentSnapshotVersion();
}

void TTabletAutomatonPart::RegisterSaver(
    int priority,
    const Stroka& name,
    TCallback<void(TSaveContext&)> saver)
{
    TCompositeAutomatonPart::RegisterSaver(
        priority,
        name,
        BIND([=] () {
            auto& context = Slot_->GetAutomaton()->SaveContext();
            saver.Run(context);
         }));
}

void TTabletAutomatonPart::RegisterLoader(
    const Stroka& name,
    TCallback<void(TLoadContext&)> loader)
{
    TCompositeAutomatonPart::RegisterLoader(
        name,
        BIND([=] () {
            auto& context = Slot_->GetAutomaton()->LoadContext();
            loader.Run(context);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
