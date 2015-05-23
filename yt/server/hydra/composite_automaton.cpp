#include "stdafx.h"
#include "composite_automaton.h"
#include "hydra_manager.h"
#include "mutation_context.h"
#include "private.h"

#include <core/misc/serialize.h>
#include <core/misc/checkpointable_stream.h>

#include <core/actions/cancelable_context.h>

#include <core/profiling/profile_manager.h>

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
    TCompositeAutomatonPtr automaton,
    IInvokerPtr automatonInvoker)
    : HydraManager_(std::move(hydraManager))
    , Automaton_(automaton.Get())
    , AutomatonInvoker_(std::move(automatonInvoker))
{
    YCHECK(HydraManager_);
    YCHECK(Automaton_);

    HydraManager_->SubscribeStartLeading(BIND(&TThis::OnStartLeading, MakeWeak(this)));
    HydraManager_->SubscribeStartLeading(BIND(&TThis::OnRecoveryStarted, MakeWeak(this)));
    HydraManager_->SubscribeLeaderRecoveryComplete(BIND(&TThis::OnRecoveryComplete, MakeWeak(this)));
    HydraManager_->SubscribeLeaderRecoveryComplete(BIND(&TThis::OnLeaderRecoveryComplete, MakeWeak(this)));
    HydraManager_->SubscribeLeaderActive(BIND(&TThis::OnLeaderActive, MakeWeak(this)));
    HydraManager_->SubscribeLeaderActive(BIND(&TThis::OnMyLeaderActive, MakeWeak(this)));
    HydraManager_->SubscribeStopLeading(BIND(&TThis::OnStopLeading, MakeWeak(this)));
    HydraManager_->SubscribeStopLeading(BIND(&TThis::OnMyStopLeading, MakeWeak(this)));

    HydraManager_->SubscribeStartFollowing(BIND(&TThis::OnStartFollowing, MakeWeak(this)));
    HydraManager_->SubscribeStartFollowing(BIND(&TThis::OnRecoveryStarted, MakeWeak(this)));
    HydraManager_->SubscribeFollowerRecoveryComplete(BIND(&TThis::OnRecoveryComplete, MakeWeak(this)));
    HydraManager_->SubscribeFollowerRecoveryComplete(BIND(&TThis::OnFollowerRecoveryComplete, MakeWeak(this)));
    HydraManager_->SubscribeStopFollowing(BIND(&TThis::OnStopFollowing, MakeWeak(this)));

    Automaton_->RegisterPart(this);
}

void TCompositeAutomatonPart::RegisterSaver(
    ESerializationPriority priority,
    const Stroka& name,
    TClosure saver)
{
    TCompositeAutomaton::TSaverInfo info(priority, name, saver, this);
    YCHECK(Automaton_->Savers_.insert(std::make_pair(name, info)).second);
}

void TCompositeAutomatonPart::RegisterLoader(
    const Stroka& name,
    TClosure loader)
{
    TCompositeAutomaton::TLoaderInfo info(name, loader, this);
    YCHECK(Automaton_->Loaders_.insert(std::make_pair(name, info)).second);
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
            auto& context = Automaton_->SaveContext();
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
            auto& context = Automaton_->LoadContext();
            loader.Run(context);
        }));
}

void TCompositeAutomatonPart::RegisterMethod(
    const Stroka& type,
    TCallback<void(TMutationContext*)> handler)
{
    TCompositeAutomaton::TMethodInfo info {
        handler,
        NProfiling::TProfileManager::Get()->RegisterTag("type", type)
    };
    YCHECK(Automaton_->Methods_.insert(std::make_pair(type, info)).second);
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
    return HydraManager_->IsLeader();
}

bool TCompositeAutomatonPart::IsFollower() const
{
    return HydraManager_->IsFollower();
}

bool TCompositeAutomatonPart::IsRecovery() const
{
    return HydraManager_->IsRecovery();
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

void TCompositeAutomatonPart::OnMyLeaderActive()
{
    EpochAutomatonInvoker_ = HydraManager_
        ->GetAutomatonCancelableContext()
        ->CreateInvoker(AutomatonInvoker_);
}

void TCompositeAutomatonPart::OnMyStopLeading()
{
    EpochAutomatonInvoker_.Reset();
}

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
    , Profiler(HydraProfiler)
{ }

void TCompositeAutomaton::RegisterPart(TCompositeAutomatonPart* part)
{
    YCHECK(part);

    Parts_.push_back(part);

    if (Parts_.size() == 1) {
        auto hydraManager = part->HydraManager_;

        hydraManager->SubscribeStartLeading(BIND(&TThis::OnRecoveryStarted, MakeWeak(this)));
        hydraManager->SubscribeLeaderRecoveryComplete(BIND(&TThis::OnRecoveryComplete, MakeWeak(this)));

        hydraManager->SubscribeStartFollowing(BIND(&TThis::OnRecoveryStarted, MakeWeak(this)));
        hydraManager->SubscribeFollowerRecoveryComplete(BIND(&TThis::OnRecoveryComplete, MakeWeak(this)));
    }
}

void TCompositeAutomaton::SetSerializationDumpEnabled(bool value)
{
    SerializationDumpEnabled_ = value;
}

void TCompositeAutomaton::SaveSnapshot(TOutputStream* output)
{
    using NYT::Save;

    std::vector<TSaverInfo> infos;
    for (const auto& pair : Savers_) {
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
    context.Dumper().SetEnabled(SerializationDumpEnabled_);

    LOG_INFO("Started loading composite automaton");

    for (auto part : Parts_) {
        part->OnBeforeSnapshotLoaded();
    }

    int partCount = LoadSuspended<i32>(context);
    SERIALIZATION_DUMP_WRITE(context, "parts[%v]", partCount);
    SERIALIZATION_DUMP_INDENT(context) {
        for (int partIndex = 0; partIndex < partCount; ++partIndex) {
            auto name = LoadSuspended<Stroka>(context);
            int version = LoadSuspended<i32>(context);

            SERIALIZATION_DUMP_WRITE(context, "%v@%v =>", name, version);
            SERIALIZATION_DUMP_INDENT(context) {
                auto it = Loaders_.find(name);
                if (it == Loaders_.end()) {
                    SERIALIZATION_DUMP_WRITE(context, "<skipped>");
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
            }

            checkpointableInput->SkipToCheckpoint();
        }
    }

    for (auto part : Parts_) {
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
    // COMPAT(babenko)
    if (type == "NYT.NHydra.NProto.TReqEvictExpiredResponses") {
        return;
    }

    auto it = Methods_.find(type);
    YCHECK(it != Methods_.end());
    const auto& info = it->second;

    NProfiling::TTagIdList tagIds;
    tagIds.push_back(info.TagId);
    static const auto profilingPath = NYTree::TYPath("/mutation_execute_time");
    PROFILE_TIMING (profilingPath, tagIds) {
        info.Callback.Run(context);
    }
}

void TCompositeAutomaton::Clear()
{
    for (auto part : Parts_) {
        part->Clear();
    }
}

void TCompositeAutomaton::OnRecoveryStarted()
{
    Profiler.SetEnabled(false);
}

void TCompositeAutomaton::OnRecoveryComplete()
{
    Profiler.SetEnabled(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
