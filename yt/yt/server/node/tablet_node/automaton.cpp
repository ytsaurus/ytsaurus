#include "automaton.h"
#include "private.h"
#include "serialize.h"
#include "tablet_slot.h"

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/chunked_memory_pool.h>

namespace NYT::NTabletNode {

using namespace NHydra;
using namespace NTableClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

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
    TCellId cellId,
    ISimpleHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    IInvokerPtr automatonInvoker)
    : TCompositeAutomatonPart(
        std::move(hydraManager),
        std::move(automaton),
        std::move(automatonInvoker))
{
    Logger = TabletNodeLogger.WithTag("CellId: %v", cellId);
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
