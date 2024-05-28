#include "automaton.h"

#include "private.h"
#include "serialize.h"
#include "chaos_slot.h"

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/hydra_manager.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

namespace NYT::NChaosNode {

using namespace NHydra;
using namespace NTableClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

TChaosAutomaton::TChaosAutomaton(IChaosAutomatonHostPtr host)
    : TCompositeAutomaton(
        host->GetAsyncSnapshotInvoker(),
        host->GetCellId())
    , Host_(std::move(host))
{ }

std::unique_ptr<NHydra::TSaveContext> TChaosAutomaton::CreateSaveContext(
    ICheckpointableOutputStream* output,
    NLogging::TLogger logger)
{
    return std::make_unique<TSaveContext>(output, std::move(logger));
}

std::unique_ptr<NHydra::TLoadContext> TChaosAutomaton::CreateLoadContext(
    ICheckpointableInputStream* input)
{
    auto host = Host_.Lock();
    if (!host) {
        THROW_ERROR_EXCEPTION("Automaton host is destroyed");
    }
    auto context = std::make_unique<TLoadContext>(input);
    TCompositeAutomaton::SetupLoadContext(context.get());
    context->SetLeaseManager(host->GetLeaseManager());
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
    , Slot_(std::move(slot))
    , Bootstrap_(bootstrap)
{
    Logger = ChaosNodeLogger()
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
