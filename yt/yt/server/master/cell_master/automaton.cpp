#include "automaton.h"
#include "bootstrap.h"
#include "hydra_facade.h"
#include "serialize.h"

#include <yt/server/master/object_server/object_manager.h>

#include <yt/server/master/security_server/security_manager.h>

namespace NYT::NCellMaster {

using namespace NObjectServer;
using namespace NSecurityServer;

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
    context->SetVersion(GetCurrentReign());
    TCompositeAutomaton::InitSaveContext(*context, output);
    return context;
}

std::unique_ptr<NHydra::TLoadContext> TMasterAutomaton::CreateLoadContext(
    ICheckpointableInputStream* input)
{
    auto context = std::make_unique<TLoadContext>(Bootstrap_);
    TCompositeAutomaton::InitLoadContext(*context, input);
    return context;
}

NHydra::TReign TMasterAutomaton::GetCurrentReign()
{
    return NCellMaster::GetCurrentReign();
}

NHydra::EFinalRecoveryAction TMasterAutomaton::GetActionToRecoverFromReign(NHydra::TReign reign)
{
    return NCellMaster::GetActionToRecoverFromReign(reign);
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
    return NCellMaster::ValidateSnapshotReign(version);
}

int TMasterAutomatonPart::GetCurrentSnapshotVersion()
{
    return NCellMaster::GetCurrentReign();
}

////////////////////////////////////////////////////////////////////////////////

EMasterReign TSaveContext::GetVersion()
{
    return static_cast<EMasterReign>(NHydra::TSaveContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

TObject* TLoadContext::GetWeakGhostObject(TObjectId id) const
{
    const auto& objectManager = Bootstrap_->GetObjectManager();
    return objectManager->GetWeakGhostObject(id);
}

template <>
const TSecurityTagsRegistryPtr& TLoadContext::GetInternRegistry() const
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    return securityManager->GetSecurityTagsRegistry();
}

EMasterReign TLoadContext::GetVersion()
{
    return static_cast<EMasterReign>(NHydra::TLoadContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

