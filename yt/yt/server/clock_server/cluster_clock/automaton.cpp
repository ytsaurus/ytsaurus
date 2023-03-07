#include "automaton.h"
#include "bootstrap.h"
#include "hydra_facade.h"
#include "serialize.h"

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

TClockAutomaton::TClockAutomaton(TBootstrap* bootstrap)
    : TCompositeAutomaton(
        nullptr,
        bootstrap->GetCellId())
    , Bootstrap_(bootstrap)
{ }

std::unique_ptr<NHydra::TSaveContext> TClockAutomaton::CreateSaveContext(
    ICheckpointableOutputStream* output)
{
    auto context = std::make_unique<TSaveContext>();
    context->SetVersion(GetCurrentReign());
    TCompositeAutomaton::InitSaveContext(*context, output);
    return context;
}

std::unique_ptr<NHydra::TLoadContext> TClockAutomaton::CreateLoadContext(
    ICheckpointableInputStream* input)
{
    auto context = std::make_unique<TLoadContext>(Bootstrap_);
    TCompositeAutomaton::InitLoadContext(*context, input);
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

EClockReign TSaveContext::GetVersion()
{
    return static_cast<EClockReign>(NHydra::TSaveContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

EClockReign TLoadContext::GetVersion()
{
    return static_cast<NClusterClock::EClockReign>(NHydra::TLoadContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock

