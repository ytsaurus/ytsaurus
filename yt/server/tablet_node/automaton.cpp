#include "stdafx.h"
#include "automaton.h"
#include "tablet_slot.h"
#include "serialize.h"
#include "private.h"

#include <core/misc/chunked_memory_pool.h>

#include <ytlib/table_client/unversioned_row.h>

#include <server/hydra/hydra_manager.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NHydra;
using namespace NTableClient;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

TTabletAutomaton::TTabletAutomaton(
    TTabletSlotPtr slot,
    IInvokerPtr snapshotInvoker)
    : TCompositeAutomaton(snapshotInvoker)
{
    Logger.AddTag("CellId: %v", slot->GetCellId());
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
    TTabletSlotPtr slot,
    TBootstrap* bootstrap)
    : TCompositeAutomatonPart(
        slot->GetHydraManager(),
        slot->GetAutomaton(),
        slot->GetAutomatonInvoker())
    , Slot_(slot)
    , Bootstrap_(bootstrap)
    , Logger(TabletNodeLogger)
{
    YCHECK(Slot_);
    YCHECK(Bootstrap_);

    Logger.AddTag("CellId: %v", Slot_->GetCellId());
}

bool TTabletAutomatonPart::ValidateSnapshotVersion(int version)
{
    return NTabletNode::ValidateSnapshotVersion(version);
}

int TTabletAutomatonPart::GetCurrentSnapshotVersion()
{
    return NTabletNode::GetCurrentSnapshotVersion();
}

void TTabletAutomatonPart::RegisterSaver(
    ESyncSerializationPriority priority,
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

void TTabletAutomatonPart::RegisterSaver(
    EAsyncSerializationPriority priority,
    const Stroka& name,
    TCallback<TCallback<void(TSaveContext&)>()> callback)
{
    TCompositeAutomatonPart::RegisterSaver(
        priority,
        name,
        BIND([=] () {
            auto continuation = callback.Run();
            return BIND([=] () {
                auto& context = Slot_->GetAutomaton()->SaveContext();
                continuation.Run(context);
            });
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
