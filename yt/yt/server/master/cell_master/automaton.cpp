#include "automaton.h"

#include "bootstrap.h"
#include "hydra_facade.h"
#include "serialize.h"
#include "config.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/core/concurrency/thread_pool.h>

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
    TCompositeAutomaton::ApplyMutation(context);
    NObjectServer::EndMutation();
}

void TMasterAutomaton::PrepareState()
{
    NObjectServer::BeginMutation();
    TCompositeAutomaton::PrepareState();
    NObjectServer::EndMutation();
}

void TMasterAutomaton::Clear()
{
    NObjectServer::BeginTeardown();
    TCompositeAutomaton::Clear();
    NObjectServer::EndTeardown();
}

void TMasterAutomaton::SetZeroState()
{
    NObjectServer::BeginMutation();
    TCompositeAutomaton::SetZeroState();
    NObjectServer::EndMutation();
}

std::unique_ptr<NHydra::TSaveContext> TMasterAutomaton::CreateSaveContext(
    ICheckpointableOutputStream* output,
    NLogging::TLogger logger)
{
    auto backgroundThreadPool = Bootstrap_->GetConfig()->HydraManager->SnapshotBackgroundThreadCount > 0
        ? CreateThreadPool(Bootstrap_->GetConfig()->HydraManager->SnapshotBackgroundThreadCount, "SnapshotBack")
        : nullptr;
   return std::make_unique<TSaveContext>(
        output,
        std::move(logger),
        std::move(backgroundThreadPool));
}

std::unique_ptr<NHydra::TLoadContext> TMasterAutomaton::CreateLoadContext(
    ICheckpointableInputStream* input)
{
    auto context = std::make_unique<TLoadContext>(Bootstrap_, input);
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

