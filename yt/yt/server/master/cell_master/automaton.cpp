#include "automaton.h"

#include "bootstrap.h"
#include "hydra_facade.h"
#include "serialize.h"
#include "config.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/finally.h>

namespace NYT::NCellMaster {

using namespace NHydra;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TMasterAutomaton::TMasterAutomaton(TBootstrap* bootstrap)
    : TCompositeAutomaton(
        nullptr,
        bootstrap->GetCellId())
    , Bootstrap_(bootstrap)
{ }

void TMasterAutomaton::ApplyMutation(NHydra::TMutationContext* context)
{
    NObjectServer::BeginMutation();
    auto guard = Finally(NObjectServer::EndMutation);
    TCompositeAutomaton::ApplyMutation(context);
}

void TMasterAutomaton::PrepareState()
{
    NObjectServer::BeginMutation();
    auto guard = Finally(NObjectServer::EndMutation);
    TCompositeAutomaton::PrepareState();
}

void TMasterAutomaton::Clear()
{
    NObjectServer::BeginTeardown();
    auto guard = Finally(NObjectServer::EndTeardown);
    TCompositeAutomaton::Clear();
}

void TMasterAutomaton::SetZeroState()
{
    NObjectServer::BeginMutation();
    auto guard = Finally(NObjectServer::EndMutation);
    TCompositeAutomaton::SetZeroState();
}

std::unique_ptr<NHydra::TSaveContext> TMasterAutomaton::CreateSaveContext(
    ICheckpointableOutputStream* output,
    NLogging::TLogger logger)
{
    return std::make_unique<TSaveContext>(
        output,
        std::move(logger),
        Bootstrap_->GetHydraFacade()->GetSnapshotSaveBackgroundThreadPool());
}

std::unique_ptr<NHydra::TLoadContext> TMasterAutomaton::CreateLoadContext(
    ICheckpointableInputStream* input)
{
    auto context = std::make_unique<TLoadContext>(
        Bootstrap_,
        input,
        Bootstrap_->GetHydraFacade()->GetSnapshotLoadBackgroundThreadPool());
    TCompositeAutomaton::SetupLoadContext(context.get());
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

TMasterAutomatonPart::TMasterAutomatonPart(
    TTestingTag tag,
    TBootstrap* bootstrap)
    : TCompositeAutomatonPart(tag)
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

} // namespace NYT::NCellMaster

