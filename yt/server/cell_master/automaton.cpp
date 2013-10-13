#include "stdafx.h"
#include "automaton.h"
#include "serialization_context.h"
#include "meta_state_facade.h"
#include "bootstrap.h"

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

TMasterAutomaton::TMasterAutomaton(TBootstrap* bootstrap)
    : SaveContext_(new TSaveContext())
    , LoadContext_(new TLoadContext(bootstrap))
{ }

TSaveContext& TMasterAutomaton::SaveContext()
{
    return *SaveContext_;
}

TLoadContext& TMasterAutomaton::LoadContext()
{
    return *LoadContext_;
}

bool TMasterAutomaton::ValidateSnapshotVersion(int version)
{
    return NCellMaster::ValidateSnapshotVersion(version);
}

int TMasterAutomaton::GetCurrentSnapshotVersion()
{
    return NCellMaster::GetCurrentSnapshotVersion();
}

////////////////////////////////////////////////////////////////////////////////

TMasterAutomatonPart::TMasterAutomatonPart(TBootstrap* bootstrap)
    : TCompositeAutomatonPart(
        bootstrap->GetMetaStateFacade()->GetManager(),
        bootstrap->GetMetaStateFacade()->GetAutomaton())
    , Bootstrap(bootstrap)
{ }

void TMasterAutomatonPart::RegisterSaver(
    int priority,
    const Stroka& name,
    TCallback<void(TSaveContext&)> saver)
{
    TCompositeAutomatonPart::RegisterSaver(
        priority,
        name,
        BIND([=] () {
            auto& context = Bootstrap->GetMetaStateFacade()->GetAutomaton()->SaveContext();
            saver.Run(context);
        }));
}

void TMasterAutomatonPart::RegisterLoader(
    const Stroka& name,
    TCallback<void(TLoadContext&)> loader)
{
    TCompositeAutomatonPart::RegisterLoader(
        name,
        BIND([=] () {
            auto& context = Bootstrap->GetMetaStateFacade()->GetAutomaton()->LoadContext();
            loader.Run(context);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

