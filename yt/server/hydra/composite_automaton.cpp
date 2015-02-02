#include "stdafx.h"
#include "composite_automaton.h"
#include "hydra_manager.h"
#include "mutation_context.h"
#include "private.h"

#include <core/misc/serialize.h>
#include <core/misc/checkpointable_stream.h>

#include <util/stream/buffered.h>

namespace NYT {
namespace NHydra {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static const size_t LoadBufferSize = 64 * 1024;
static const size_t SaveBufferSize = 64 * 1024;

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext()
    : Version_(-1)
{ }

void TLoadContext::Reset()
{
    Entities_.clear();
}

////////////////////////////////////////////////////////////////////////////////

TCompositeAutomatonPart::TCompositeAutomatonPart(
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton)
    : HydraManager(hydraManager)
    , Automaton(automaton.Get())
{
    YCHECK(HydraManager);
    YCHECK(Automaton);

    HydraManager->SubscribeStartLeading(BIND(&TThis::OnStartLeading, MakeWeak(this)));
    HydraManager->SubscribeStartLeading(BIND(&TThis::OnRecoveryStarted, MakeWeak(this)));
    HydraManager->SubscribeLeaderRecoveryComplete(BIND(&TThis::OnRecoveryComplete, MakeWeak(this)));
    HydraManager->SubscribeLeaderRecoveryComplete(BIND(&TThis::OnLeaderRecoveryComplete, MakeWeak(this)));
    HydraManager->SubscribeLeaderActive(BIND(&TThis::OnLeaderActive, MakeWeak(this)));
    HydraManager->SubscribeStopLeading(BIND(&TThis::OnStopLeading, MakeWeak(this)));

    HydraManager->SubscribeStartFollowing(BIND(&TThis::OnStartFollowing, MakeWeak(this)));
    HydraManager->SubscribeStartFollowing(BIND(&TThis::OnRecoveryStarted, MakeWeak(this)));
    HydraManager->SubscribeFollowerRecoveryComplete(BIND(&TThis::OnRecoveryComplete, MakeWeak(this)));
    HydraManager->SubscribeFollowerRecoveryComplete(BIND(&TThis::OnFollowerRecoveryComplete, MakeWeak(this)));
    HydraManager->SubscribeStopFollowing(BIND(&TThis::OnStopFollowing, MakeWeak(this)));

    Automaton->RegisterPart(this);
}

void TCompositeAutomatonPart::RegisterSaver(
    ESerializationPriority priority,
    const Stroka& name,
    TClosure saver)
{
    TCompositeAutomaton::TSaverInfo info(priority, name, saver, this);
    YCHECK(Automaton->Savers.insert(std::make_pair(name, info)).second);
}

void TCompositeAutomatonPart::RegisterLoader(
    const Stroka& name,
    TClosure loader)
{
    TCompositeAutomaton::TLoaderInfo info(name, loader, this);
    YCHECK(Automaton->Loaders.insert(std::make_pair(name, info)).second);
}

void TCompositeAutomatonPart::RegisterSaver(
    ESerializationPriority priority,
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

bool TCompositeAutomatonPart::ValidateSnapshotVersion(int /*version*/)
{
    return true;
}

int TCompositeAutomatonPart::GetCurrentSnapshotVersion()
{
    return 0;
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
    ESerializationPriority priority,
    const Stroka& name,
    TClosure saver,
    TCompositeAutomatonPart* part)
    : Priority(priority)
    , Name(name)
    , Saver(saver)
    , Part(part)
{ }

TCompositeAutomaton::TLoaderInfo::TLoaderInfo(
    const Stroka& name,
    TClosure loader,
    TCompositeAutomatonPart* part)
    : Name(name)
    , Loader(loader)
    , Part(part)
{ }

////////////////////////////////////////////////////////////////////////////////

TCompositeAutomaton::TCompositeAutomaton()
    : Logger(HydraLogger)
{ }

void TCompositeAutomaton::RegisterPart(TCompositeAutomatonPart* part)
{
    YCHECK(part);

    Parts.push_back(part);
}

void TCompositeAutomaton::SaveSnapshot(TOutputStream* output)
{
    using NYT::Save;

    std::vector<TSaverInfo> infos;
    for (const auto& pair : Savers) {
        infos.push_back(pair.second);
    }

    std::sort(
        infos.begin(),
        infos.end(),
        [] (const TSaverInfo& lhs, const TSaverInfo& rhs) {
            return
                lhs.Priority < rhs.Priority ||
                lhs.Priority == rhs.Priority && lhs.Name < rhs.Name;
        });

    auto checkpointableOutput = CreateCheckpointableOutputStream(output);
    auto bufferedCheckpointableOutput = CreateBufferedCheckpointableOutputStream(
        checkpointableOutput.get(),
        SaveBufferSize);

    auto& context = SaveContext();
    context.SetOutput(bufferedCheckpointableOutput.get());

    Save<i32>(context, infos.size());

    for (const auto& info : infos) {
        bufferedCheckpointableOutput->MakeCheckpoint();
        Save(context, info.Name);
        Save<i32>(context, info.Part->GetCurrentSnapshotVersion());
        info.Saver.Run();
    }
}

void TCompositeAutomaton::LoadSnapshot(TInputStream* input)
{
    using NYT::Load;

    TBufferedInput bufferedInput(input, LoadBufferSize);
    auto checkpointableInput = CreateCheckpointableInputStream(&bufferedInput);

    auto& context = LoadContext();
    context.Reset();
    context.SetInput(checkpointableInput.get());

    LOG_INFO("Started loading composite automaton");

    for (auto part : Parts) {
        part->OnBeforeSnapshotLoaded();
    }

    int partCount = Load<i32>(context);
    for (int partIndex = 0; partIndex < partCount; ++partIndex) {
        auto name = Load<Stroka>(context);
        int version = Load<i32>(context);
        auto it = Loaders.find(name);
        if (it == Loaders.end()) {
            LOG_INFO("Skipping unknown automaton part (Name: %v, Version: %v)",
                name,
                version);
        } else {
            LOG_INFO("Loading automaton part (Name: %v, Version: %v)",
                name,
                version);
            context.SetVersion(version);
            const auto& info = it->second;
            info.Loader.Run();
        }

        checkpointableInput->SkipToCheckpoint();
    }

    for (auto part : Parts) {
        part->OnAfterSnapshotLoaded();
    }

    LOG_INFO("Finished loading composite automaton");
}

void TCompositeAutomaton::ApplyMutation(TMutationContext* context)
{
    const auto& type = context->Request().Type;
    if (type.empty()) {
        // Empty mutation. Typically appears as a tombstone after editing changelogs.
        return;
    }
    auto it = Methods.find(type);
    YCHECK(it != Methods.end());
    it->second.Run(context);
}

void TCompositeAutomaton::Clear()
{
    for (auto part : Parts) {
        part->Clear();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
