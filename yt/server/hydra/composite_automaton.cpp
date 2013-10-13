#include "stdafx.h"
#include "composite_automaton.h"
#include "hydra_manager.h"
#include "mutation_context.h"
#include "private.h"

#include <core/misc/serialize.h>

namespace NYT {
namespace NHydra {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext()
    : Version_(-1)
{ }

////////////////////////////////////////////////////////////////////////////////

TCompositeAutomatonPart::TCompositeAutomatonPart(
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton)
    : HydraManager(hydraManager)
    , Automaton(~automaton)
{
    YCHECK(HydraManager);
    YCHECK(Automaton);

    HydraManager->SubscribeStartLeading(BIND(
        &TThis::OnStartLeading,
        MakeWeak(this)));
    HydraManager->SubscribeStartLeading(BIND(
        &TThis::OnRecoveryStarted,
        MakeWeak(this)));
    HydraManager->SubscribeLeaderRecoveryComplete(BIND(
        &TThis::OnRecoveryComplete,
        MakeWeak(this)));
    HydraManager->SubscribeLeaderRecoveryComplete(BIND(
        &TThis::OnLeaderRecoveryComplete,
        MakeWeak(this)));
    HydraManager->SubscribeLeaderActive(BIND(
        &TThis::OnLeaderActive,
        MakeWeak(this)));
    HydraManager->SubscribeStopLeading(BIND(
        &TThis::OnStopLeading,
        MakeWeak(this)));

    HydraManager->SubscribeStartFollowing(BIND(
        &TThis::OnStartFollowing,
        MakeWeak(this)));
    HydraManager->SubscribeStartFollowing(BIND(
        &TThis::OnRecoveryStarted,
        MakeWeak(this)));
    HydraManager->SubscribeFollowerRecoveryComplete(BIND(
        &TThis::OnRecoveryComplete,
        MakeWeak(this)));
    HydraManager->SubscribeFollowerRecoveryComplete(BIND(
        &TThis::OnFollowerRecoveryComplete,
        MakeWeak(this)));
    HydraManager->SubscribeStopFollowing(BIND(
        &TThis::OnStopFollowing,
        MakeWeak(this)));

    Automaton->RegisterPart(this);
}

void TCompositeAutomatonPart::RegisterSaver(
    int priority,
    const Stroka& name,
    TClosure saver)
{
    TCompositeAutomaton::TSaverInfo info(priority, name, saver);
    YCHECK(Automaton->Savers.insert(std::make_pair(name, info)).second);
}

void TCompositeAutomatonPart::RegisterLoader(
    const Stroka& name,
    TClosure loader)
{
    TCompositeAutomaton::TLoaderInfo info(name, loader);
    YCHECK(Automaton->Loaders.insert(std::make_pair(name, info)).second);
}

void TCompositeAutomatonPart::RegisterSaver(
    int priority,
    const Stroka& name,
    TCallback<void(TSaveContext&)> saver)
{
    RegisterSaver(
        priority,
        name,
        BIND([=] () {
            auto& context = Automaton->SaveContext();
            saver.Run(context);
        }));
}

void TCompositeAutomatonPart::RegisterLoader(
    const Stroka& name,
    TCallback<void(TLoadContext&)> loader)
{
    TCompositeAutomatonPart::RegisterLoader(
        name,
        BIND([=] () {
            auto& context = Automaton->LoadContext();
            loader.Run(context);
        }));
}

void TCompositeAutomatonPart::Clear()
{ }

void TCompositeAutomatonPart::OnBeforeSnapshotLoaded()
{ }

void TCompositeAutomatonPart::OnAfterSnapshotLoaded()
{ }

bool TCompositeAutomatonPart::IsLeader() const
{
    return HydraManager->IsLeader();
}

bool TCompositeAutomatonPart::IsFollower() const
{
    return HydraManager->IsFollower();
}

bool TCompositeAutomatonPart::IsRecovery() const
{
    return HydraManager->IsRecovery();
}

void TCompositeAutomatonPart::OnStartLeading()
{ }

void TCompositeAutomatonPart::OnLeaderRecoveryComplete()
{ }

void TCompositeAutomatonPart::OnLeaderActive()
{ }

void TCompositeAutomatonPart::OnStopLeading()
{ }

void TCompositeAutomatonPart::OnStartFollowing()
{ }

void TCompositeAutomatonPart::OnFollowerRecoveryComplete()
{ }

void TCompositeAutomatonPart::OnStopFollowing()
{ }

void TCompositeAutomatonPart::OnRecoveryStarted()
{ }

void TCompositeAutomatonPart::OnRecoveryComplete()
{ }

////////////////////////////////////////////////////////////////////////////////

TCompositeAutomaton::TSaverInfo::TSaverInfo(
    int priority,
    const Stroka& name,
    TClosure saver)
    : Priority(priority)
    , Name(name)
    , Saver(saver)
{ }

TCompositeAutomaton::TLoaderInfo::TLoaderInfo(
    const Stroka& name,
    TClosure loader)
    : Name(name)
    , Loader(loader)
{ }

////////////////////////////////////////////////////////////////////////////////

TCompositeAutomaton::TCompositeAutomaton()
    : Logger(HydraLogger)
{ }

void TCompositeAutomaton::RegisterPart(TCompositeAutomatonPartPtr part)
{
    YCHECK(part);

    Parts.push_back(part);
}

void TCompositeAutomaton::SaveSnapshot(TOutputStream* output)
{
    using NYT::Save;

    std::vector<TSaverInfo> infos;
    FOREACH (const auto& pair, Savers) {
        infos.push_back(pair.second);
    }

    std::sort(
        infos.begin(),
        infos.end(),
        [] (const TSaverInfo& lhs, const TSaverInfo& rhs) {
            return
                (lhs.Priority < rhs.Priority) ||
                (lhs.Priority == rhs.Priority && lhs.Name < rhs.Name);
        });

    auto& context = SaveContext();
    context.SetOutput(output);

    Save(context, static_cast<i32>(GetCurrentSnapshotVersion()));
    Save(context, static_cast<i32>(infos.size()));

    FOREACH (const auto& info, infos) {
        Save(context, info.Name);
        info.Saver.Run();
    }
}

void TCompositeAutomaton::LoadSnapshot(TInputStream* input)
{
    using NYT::Load;

    auto& context = LoadContext();
    context.SetInput(input);

    int version = Load<i32>(context);
    int partCount = Load<i32>(context);

    context.SetVersion(version);

    LOG_INFO("Started loading composite automaton with %d parts",
        partCount);

    FOREACH (auto part, Parts) {
        part->OnBeforeSnapshotLoaded();
    }

    for (int partIndex = 0; partIndex < partCount; ++partIndex) {
        auto name = Load<Stroka>(context);
        
        auto it = Loaders.find(name);
        LOG_FATAL_IF(it == Loaders.end(), "Unknown snapshot part %s",
            ~name.Quote());

        LOG_INFO("Loading automaton part %s", ~name.Quote());

        const auto& info = it->second;
        info.Loader.Run();
    }

    FOREACH (auto part, Parts) {
        part->OnAfterSnapshotLoaded();
    }

    LOG_INFO("Completed loading composite automaton");
}

void TCompositeAutomaton::ApplyMutation(TMutationContext* context)
{
    if (context->GetType().empty()) {
        // Empty mutation. Typically appears as a tombstone after editing changelogs.
        return;
    }
    auto it = Methods.find(context->GetType());
    YCHECK(it != Methods.end());
    it->second.Run(context);
}

void TCompositeAutomaton::Clear()
{
    FOREACH (auto part, Parts) {
        part->Clear();
    }
}

bool TCompositeAutomaton::ValidateSnapshotVersion(int /*version*/)
{
    return true;
}

int TCompositeAutomaton::GetCurrentSnapshotVersion()
{
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
