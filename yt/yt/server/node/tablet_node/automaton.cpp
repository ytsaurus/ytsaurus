#include "automaton.h"

#include "private.h"
#include "serialize.h"
#include "tablet_slot.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

namespace NYT::NTabletNode {

using namespace NHydra;
using namespace NTableClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

TTabletAutomaton::TTabletAutomaton(ITabletAutomatonHostPtr host)
    : NHydra::TCompositeAutomaton(
        host->GetAsyncSnapshotInvoker(),
        host->GetCellId())
    , Host_(std::move(host))
{ }

std::unique_ptr<NHydra::TSaveContext> TTabletAutomaton::CreateSaveContext(
    ICheckpointableOutputStream* output,
    NLogging::TLogger logger)
{
    return std::make_unique<TSaveContext>(output, std::move(logger));
}

std::unique_ptr<NHydra::TLoadContext> TTabletAutomaton::CreateLoadContext(
    ICheckpointableInputStream* input)
{
    auto host = Host_.Lock();
    if (!host) {
        THROW_ERROR_EXCEPTION("Automaton host is destroyed");
    }
    auto context = std::make_unique<TLoadContext>(input);
    TCompositeAutomaton::SetupLoadContext(context.get());
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
    IInvokerPtr automatonInvoker,
    IMutationForwarderPtr mutationForwarder)
    : TCompositeAutomatonPart(
        std::move(hydraManager),
        std::move(automaton),
        std::move(automatonInvoker))
    , MutationForwarder_(std::move(mutationForwarder))
{
    Logger = TabletNodeLogger().WithTag("CellId: %v", cellId);
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

} // namespace NYT::NTabletNode
