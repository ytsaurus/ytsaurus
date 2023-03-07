#include "automaton.h"
#include "private.h"
#include "serialize.h"
#include "tablet_slot.h"

#include <yt/server/node/cluster_node/bootstrap.h>

#include <yt/server/lib/hydra/hydra_manager.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/misc/chunked_memory_pool.h>

namespace NYT::NTabletNode {

using namespace NHydra;
using namespace NTableClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

ETabletReign TSaveContext::GetVersion() const
{
    return static_cast<ETabletReign>(NHydra::TSaveContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

ETabletReign TLoadContext::GetVersion() const
{
    return static_cast<ETabletReign>(NHydra::TLoadContext::GetVersion());
}

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
    TTabletSlotPtr slot,
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

    Logger = NLogging::TLogger(TabletNodeLogger)
        .AddTag("CellId: %v", Slot_->GetCellId());
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
