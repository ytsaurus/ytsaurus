#include "automaton.h"
#include "private.h"
#include "serialize.h"
#include "tablet_slot.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/chunked_memory_pool.h>

namespace NYT::NTabletNode {

using namespace NHydra;
using namespace NTableClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

TTabletAutomaton::TTabletAutomaton(
    ITabletSlotPtr slot,
    IInvokerPtr snapshotInvoker)
    : TCompositeAutomaton(
        snapshotInvoker,
        slot->GetCellId())
{ }

std::unique_ptr<NHydra::TSaveContext> TTabletAutomaton::CreateSaveContext(
    ICheckpointableOutputStream* output)
{
    auto context = std::make_unique<TSaveContext>();
    context->SetVersion(GetCurrentReign());
    TCompositeAutomaton::InitSaveContext(*context, output);
    return context;
}

std::unique_ptr<NHydra::TLoadContext> TTabletAutomaton::CreateLoadContext(
    ICheckpointableInputStream* input)
{
    auto context = std::make_unique<TLoadContext>();
    TCompositeAutomaton::InitLoadContext(*context, input);
    return context;
}

TReign TTabletAutomaton::GetCurrentReign()
{
    return NTabletNode::GetCurrentReign();
}

EFinalRecoveryAction TTabletAutomaton::GetActionToRecoverFromReign(TReign reign)
{
    return NTabletNode::GetActionToRecoverFromReign(reign);
}

////////////////////////////////////////////////////////////////////////////////

TTabletAutomatonPart::TTabletAutomatonPart(
    ITabletSlotPtr slot,
    TBootstrap* bootstrap)
    : TCompositeAutomatonPart(
        slot->GetHydraManager(),
        slot->GetAutomaton(),
        slot->GetAutomatonInvoker())
    , Slot_(slot)
    , Bootstrap_(bootstrap)
{
    YT_VERIFY(Slot_);
    YT_VERIFY(Bootstrap_);

    Logger = TabletNodeLogger.WithTag("CellId: %v", Slot_->GetCellId());
}

bool TTabletAutomatonPart::ValidateSnapshotVersion(int version)
{
    return NTabletNode::ValidateSnapshotReign(version);
}

int TTabletAutomatonPart::GetCurrentSnapshotVersion()
{
    return NTabletNode::GetCurrentReign();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
