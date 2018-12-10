#include "automaton.h"
#include "bootstrap.h"
#include "hydra_facade.h"
#include "serialize.h"

#include <yt/server/object_server/object_manager.h>

namespace NYT::NCellMaster {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TMasterAutomaton::TMasterAutomaton(TBootstrap* bootstrap)
    : TCompositeAutomaton(
        nullptr,
        bootstrap->GetCellId())
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

TObjectBase* TLoadContext::GetWeakGhostObject(const TObjectId& id) const
{
    const auto& objectManager = Bootstrap_->GetObjectManager();
    return objectManager->GetWeakGhostObject(id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

