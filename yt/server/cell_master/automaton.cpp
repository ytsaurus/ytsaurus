#include "automaton.h"
#include "bootstrap.h"
#include "hydra_facade.h"
#include "serialize.h"

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

TMasterAutomaton::TMasterAutomaton(TBootstrap* bootstrap)
    : TCompositeAutomaton(nullptr)
    , Bootstrap_(bootstrap)
{ }

std::unique_ptr<NHydra::TSaveContext> TMasterAutomaton::CreateSaveContext(
    ICheckpointableOutputStream* output)
{
    auto context = std::make_unique<TSaveContext>();
    context->SetVersion(GetCurrentSnapshotVersion());
    TCompositeAutomaton::InitSaveContext(*context, output);
    return std::unique_ptr<NHydra::TSaveContext>(std::move(context));
}

std::unique_ptr<NHydra::TLoadContext> TMasterAutomaton::CreateLoadContext(
    ICheckpointableInputStream* input)
{
    auto context = std::make_unique<TLoadContext>(Bootstrap_);
    TCompositeAutomaton::InitLoadContext(*context, input);
    return std::unique_ptr<NHydra::TLoadContext>(std::move(context));
}

////////////////////////////////////////////////////////////////////////////////

TMasterAutomatonPart::TMasterAutomatonPart(
    TBootstrap* bootstrap,
    EAutomatonThreadQueue queue)
    : TCompositeAutomatonPart(
        bootstrap->GetHydraFacade()->GetHydraManager(),
        bootstrap->GetHydraFacade()->GetAutomaton(),
        bootstrap->GetHydraFacade()->GetAutomatonInvoker(queue))
    , Bootstrap_(bootstrap)
{ }

bool TMasterAutomatonPart::ValidateSnapshotVersion(int version)
{
    return NCellMaster::ValidateSnapshotVersion(version);
}

int TMasterAutomatonPart::GetCurrentSnapshotVersion()
{
    return NCellMaster::GetCurrentSnapshotVersion();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

