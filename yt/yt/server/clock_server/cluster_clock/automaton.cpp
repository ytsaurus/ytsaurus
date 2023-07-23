#include "automaton.h"
#include "bootstrap.h"
#include "hydra_facade.h"
#include "serialize.h"

namespace NYT::NClusterClock {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TClockAutomaton::TClockAutomaton(TBootstrap* bootstrap)
    : TCompositeAutomaton(
        nullptr,
        bootstrap->GetCellId())
    , Bootstrap_(bootstrap)
{ }

std::unique_ptr<NHydra::TSaveContext> TClockAutomaton::CreateSaveContext(
    ICheckpointableOutputStream* output,
    NLogging::TLogger logger)
{
    return std::make_unique<TSaveContext>(output, std::move(logger));
}

std::unique_ptr<NHydra::TLoadContext> TClockAutomaton::CreateLoadContext(
    ICheckpointableInputStream* input)
{
    auto context = std::make_unique<TLoadContext>(Bootstrap_, input);
    TCompositeAutomaton::SetupLoadContext(context.get());
    return context;
}

NHydra::TReign TClockAutomaton::GetCurrentReign()
{
    return NClusterClock::GetCurrentReign();
}

NHydra::EFinalRecoveryAction TClockAutomaton::GetActionToRecoverFromReign(NHydra::TReign reign)
{
    return NClusterClock::GetActionToRecoverFromReign(reign);
}

////////////////////////////////////////////////////////////////////////////////

TClockAutomatonPart::TClockAutomatonPart(
    TBootstrap* bootstrap,
    EAutomatonThreadQueue queue)
    : TCompositeAutomatonPart(
        bootstrap->GetHydraFacade()->GetHydraManager(),
        bootstrap->GetHydraFacade()->GetAutomaton(),
        bootstrap->GetHydraFacade()->GetAutomatonInvoker(queue))
    , Bootstrap_(bootstrap)
{ }

bool TClockAutomatonPart::ValidateSnapshotVersion(int version)
{
    return NClusterClock::ValidateSnapshotReign(version);
}

int TClockAutomatonPart::GetCurrentSnapshotVersion()
{
    return NClusterClock::GetCurrentReign();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock

