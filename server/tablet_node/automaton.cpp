#include "automaton.h"
#include "private.h"
#include "serialize.h"
#include "tablet_slot.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/hydra/hydra_manager.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/misc/chunked_memory_pool.h>

namespace NYT::NTabletNode {

using namespace NHydra;
using namespace NTableClient;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

TTabletAutomaton::TTabletAutomaton(
    TTabletSlotPtr slot,
    IInvokerPtr snapshotInvoker)
    : TCompositeAutomaton(
        snapshotInvoker,
        slot->GetCellId(),
        slot->GetProfilingTagIds())
{ }

std::unique_ptr<NHydra::TSaveContext> TTabletAutomaton::CreateSaveContext(
    ICheckpointableOutputStream* output)
{
    auto context = std::make_unique<TSaveContext>();
    context->SetVersion(GetCurrentSnapshotVersion());
    TCompositeAutomaton::InitSaveContext(*context, output);
    return std::unique_ptr<NHydra::TSaveContext>(std::move(context));
}

std::unique_ptr<NHydra::TLoadContext> TTabletAutomaton::CreateLoadContext(
    ICheckpointableInputStream* input)
{
    auto context = std::make_unique<TLoadContext>();
    TCompositeAutomaton::InitLoadContext(*context, input);
    return std::unique_ptr<NHydra::TLoadContext>(std::move(context));
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
{
    YCHECK(Slot_);
    YCHECK(Bootstrap_);

    Logger = NLogging::TLogger(TabletNodeLogger)
        .AddTag("CellId: %v", Slot_->GetCellId());
}

bool TTabletAutomatonPart::ValidateSnapshotVersion(int version)
{
    return NTabletNode::ValidateSnapshotVersion(version);
}

int TTabletAutomatonPart::GetCurrentSnapshotVersion()
{
    return NTabletNode::GetCurrentSnapshotVersion();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
