#include "automaton.h"
#include "private.h"
#include "serialize.h"
#include "chaos_slot.h"

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/hydra_manager.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

namespace NYT::NChaosNode {

using namespace NHydra;
using namespace NTableClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

TChaosAutomaton::TChaosAutomaton(
    IChaosSlotPtr slot,
    IInvokerPtr snapshotInvoker)
    : TCompositeAutomaton(
        snapshotInvoker,
        slot->GetCellId())
{ }

std::unique_ptr<NHydra::TSaveContext> TChaosAutomaton::CreateSaveContext(
    ICheckpointableOutputStream* output)
{
    return std::make_unique<TSaveContext>(output);
}

std::unique_ptr<NHydra::TLoadContext> TChaosAutomaton::CreateLoadContext(
    ICheckpointableInputStream* input)
{
    auto context = std::make_unique<TLoadContext>(input);
    TCompositeAutomaton::SetupLoadContext(context.get());
    return context;
}

TReign TChaosAutomaton::GetCurrentReign()
{
    return NChaosNode::GetCurrentReign();
}

EFinalRecoveryAction TChaosAutomaton::GetActionToRecoverFromReign(TReign reign)
{
    return NChaosNode::GetActionToRecoverFromReign(reign);
}

////////////////////////////////////////////////////////////////////////////////

TChaosAutomatonPart::TChaosAutomatonPart(
    IChaosSlotPtr slot,
    IBootstrap* bootstrap)
    : TCompositeAutomatonPart(
        slot->GetHydraManager(),
        slot->GetAutomaton(),
        slot->GetAutomatonInvoker())
    , Slot_(slot)
    , Bootstrap_(bootstrap)
{
    YT_VERIFY(Slot_);
    YT_VERIFY(Bootstrap_);

    Logger = ChaosNodeLogger
        .WithTag("CellId: %v", Slot_->GetCellId());
}

bool TChaosAutomatonPart::ValidateSnapshotVersion(int version)
{
    return NChaosNode::ValidateSnapshotReign(version);
}

int TChaosAutomatonPart::GetCurrentSnapshotVersion()
{
    return NChaosNode::GetCurrentReign();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChaosNode::NYT
