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
    context->SetVersion(GetCurrentSnapshotVersion());
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

EMasterSnapshotVersion TSaveContext::GetVersion()
{
    return static_cast<NCellMaster::EMasterSnapshotVersion>(NHydra::TSaveContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

TObjectBase* TLoadContext::GetWeakGhostObject(TObjectId id) const
{
    const auto& objectManager = Bootstrap_->GetObjectManager();
    return objectManager->GetWeakGhostObject(id);
}

template<>
const TSecurityTagsRegistryPtr& TLoadContext::GetInternRegistry<TSecurityTags>() const
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    return securityManager->GetSecurityTagsRegistry();
}

EMasterSnapshotVersion TLoadContext::GetVersion()
{
    return static_cast<NCellMaster::EMasterSnapshotVersion>(NHydra::TLoadContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

