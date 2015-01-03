#include "stdafx.h"
#include "automaton.h"
#include "tablet_slot.h"
#include "serialize.h"
#include "private.h"

#include <core/misc/chunked_memory_pool.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <server/hydra/hydra_manager.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NHydra;
using namespace NVersionedTableClient;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

struct TTempLoadPoolTag { };

TLoadContext::TLoadContext()
    : Slot_(nullptr)
    , TempPool_(std::make_unique<TChunkedMemoryPool>(TTempLoadPoolTag()))
    , RowBuilder_(new TUnversionedRowBuilder())
{ }

TChunkedMemoryPool* TLoadContext::GetTempPool() const
{
    return TempPool_.get();
}

TUnversionedRowBuilder* TLoadContext::GetRowBuilder() const
{
    return RowBuilder_.get();
}

////////////////////////////////////////////////////////////////////////////////

TTabletAutomaton::TTabletAutomaton(
    NCellNode::TBootstrap* bootstrap,
    TTabletSlot* slot)
    : Slot_(slot)
{
    Logger.AddTag("CellId: %v", Slot_->GetCellId());

    LoadContext_.SetSlot(Slot_);
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
    ESerializationPriority priority,
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
