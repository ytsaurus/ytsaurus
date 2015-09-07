#include "stdafx.h"
#include "composite_automaton.h"
#include "hydra_manager.h"
#include "mutation_context.h"
#include "snapshot.h"
#include "private.h"

#include <core/misc/serialize.h>
#include <core/misc/finally.h>

#include <core/concurrency/async_stream.h>

#include <core/actions/cancelable_context.h>

#include <core/profiling/profile_manager.h>

#include <util/stream/buffered.h>
#include <core/misc/finally.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

static const size_t SnapshotLoadBufferSize = 64 * 1024;
static const size_t SnapshotSaveBufferSize = 64 * 1024;
static const size_t SnapshotPrefetchWindowSize = 64 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TSaveContext::TSaveContext()
{
    Reset();
}

void TSaveContext::Reset()
{
    SerializationKeyIndex_ = 0;
    CheckpointableOutput_ = nullptr;
    Output_ = nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext()
{
    Reset();
}

void TLoadContext::Reset()
{
    Version_ = -1;
    CheckpointableInput_ = nullptr;
    Input_ = nullptr;
    Entities_.clear();
    Dumper_.SetEnabled(false);
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
    HydraManager_->SubscribeStopLeading(BIND(&TThis::OnStopLeading, MakeWeak(this)));

    HydraManager_->SubscribeStartFollowing(BIND(&TThis::OnStartFollowing, MakeWeak(this)));
    HydraManager_->SubscribeStartFollowing(BIND(&TThis::OnRecoveryStarted, MakeWeak(this)));
    HydraManager_->SubscribeFollowerRecoveryComplete(BIND(&TThis::OnRecoveryComplete, MakeWeak(this)));
    HydraManager_->SubscribeFollowerRecoveryComplete(BIND(&TThis::OnFollowerRecoveryComplete, MakeWeak(this)));
    HydraManager_->SubscribeStopFollowing(BIND(&TThis::OnStopFollowing, MakeWeak(this)));

    Automaton_->RegisterPart(this);
}

void TCompositeAutomatonPart::RegisterSaver(
    ESyncSerializationPriority priority,
    const Stroka& name,
    TCallback<void()> callback)
{
    // Check for duplicate part names.
    YCHECK(Automaton_->SaverPartNames_.insert(name).second);

    TCompositeAutomaton::TSyncSaverDescriptor descriptor;
    descriptor.Priority = priority;
    descriptor.Name = name;
    descriptor.Callback = callback;
    descriptor.Part = this;
    Automaton_->SyncSavers_.push_back(descriptor);
}

void TCompositeAutomatonPart::RegisterSaver(
    EAsyncSerializationPriority priority,
    const Stroka& name,
    TCallback<TCallback<void()>()> callback)
{
    // Check for duplicate part names.
    YCHECK(Automaton_->SaverPartNames_.insert(name).second);

    TCompositeAutomaton::TAsyncSaverDescriptor descriptor;
    descriptor.Priority = priority;
    descriptor.Name = name;
    descriptor.Callback = callback;
    descriptor.Part = this;
    Automaton_->AsyncSavers_.push_back(descriptor);
}

void TCompositeAutomatonPart::RegisterSaver(
    ESyncSerializationPriority priority,
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

void TCompositeAutomatonPart::RegisterSaver(
    EAsyncSerializationPriority priority,
    const Stroka& name,
    TCallback<TCallback<void(TSaveContext&)>()> callback)
{
    RegisterSaver(
        priority,
        name,
        BIND([=] () {
            auto continuation = callback.Run();
            return BIND([=] () {
                auto& context = Automaton_->SaveContext();
                return continuation.Run(context);
            });
        }));
}

void TCompositeAutomatonPart::RegisterLoader(
    const Stroka& name,
    TCallback<void()> callback)
{
    TCompositeAutomaton::TLoaderDescriptor descriptor;
    descriptor.Name = name;
    descriptor.Callback = BIND([=] () {
        const auto& context = Automaton_->LoadContext();
        if (!ValidateSnapshotVersion(context.GetVersion())) {
            THROW_ERROR_EXCEPTION("Unsupported snapshot version %v in part %v",
                context.GetVersion(),
                name);
        }
        callback.Run();
    });
    descriptor.Part = this;
    YCHECK(Automaton_->PartNameToLoaderDescriptor_.insert(std::make_pair(name, descriptor)).second);
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
    TCallback<void(TMutationContext*)> callback)
{
    TCompositeAutomaton::TMethodDescriptor descriptor{
        callback,
        NProfiling::TProfileManager::Get()->RegisterTag("type", type)
    };
    YCHECK(Automaton_->MethodNameToDescriptor_.insert(std::make_pair(type, descriptor)).second);
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
{
    StartEpoch();
}

void TCompositeAutomatonPart::OnLeaderRecoveryComplete()
{ }

void TCompositeAutomatonPart::OnLeaderActive()
{ }

void TCompositeAutomatonPart::OnStopLeading()
{
    StopEpoch();
}

void TCompositeAutomatonPart::OnStartFollowing()
{
    StartEpoch();
}

void TCompositeAutomatonPart::OnFollowerRecoveryComplete()
{ }

void TCompositeAutomatonPart::OnStopFollowing()
{
    StopEpoch();
}

void TCompositeAutomatonPart::OnRecoveryStarted()
{ }

void TCompositeAutomatonPart::OnRecoveryComplete()
{ }

void TCompositeAutomatonPart::StartEpoch()
{
    EpochAutomatonInvoker_ = HydraManager_
        ->GetAutomatonCancelableContext()
        ->CreateInvoker(AutomatonInvoker_);
}

void TCompositeAutomatonPart::StopEpoch()
{
    EpochAutomatonInvoker_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

TCompositeAutomaton::TCompositeAutomaton(IInvokerPtr asyncSnapshotInvoker)
    : Logger(HydraLogger)
    , Profiler(HydraProfiler)
    , AsyncSnapshotInvoker_(asyncSnapshotInvoker)
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

TFuture<void> TCompositeAutomaton::SaveSnapshot(IAsyncOutputStreamPtr writer)
{
    DoSaveSnapshot(
        writer,
        // NB: Do not yield in sync part.
        ESyncStreamAdapterStrategy::Get,
        [&] () {
            using NYT::Save;

            auto& context = SaveContext();

            int partCount = SyncSavers_.size() + AsyncSavers_.size();
            Save<i32>(context, partCount);

            // Sort by (priority, name).
            auto syncSavers = SyncSavers_;
            std::sort(
                syncSavers.begin(),
                syncSavers.end(),
                [ ](const TSyncSaverDescriptor& lhs, const TSyncSaverDescriptor& rhs) {
                    return
                        lhs.Priority < rhs.Priority ||
                        lhs.Priority == rhs.Priority && lhs.Name < rhs.Name;
                });

            for (const auto& descriptor : syncSavers) {
                WritePartHeader(descriptor);
                descriptor.Callback.Run();
            }
        });

    if (AsyncSavers_.empty()) {
        return VoidFuture;
    }

    YCHECK(AsyncSnapshotInvoker_);

    std::vector<TCallback<void()>> asyncCallbacks;

    // Sort by (priority, name).
    auto asyncSavers = AsyncSavers_;
    std::sort(
        asyncSavers.begin(),
        asyncSavers.end(),
        [ ](const TAsyncSaverDescriptor& lhs, const TAsyncSaverDescriptor& rhs) {
            return
                lhs.Priority < rhs.Priority ||
                lhs.Priority == rhs.Priority && lhs.Name < rhs.Name;
        });

    for (const auto& descriptor : asyncSavers) {
        asyncCallbacks.push_back(descriptor.Callback.Run());
    }

    // Hold the parts strongly during the async phase.
    std::vector<TCompositeAutomatonPartPtr> parts(Parts_.begin(), Parts_.end());
    return
        BIND([=, this_ = MakeStrong(this), parts_ = std::move(parts)] () {
            DoSaveSnapshot(
                writer,
                // NB: Can yield in async part.
                ESyncStreamAdapterStrategy::WaitFor,
                [&] () {
                    for (int index = 0; index < asyncSavers.size(); ++index) {
                        WritePartHeader(asyncSavers[index]);
                        asyncCallbacks[index].Run();
                    }
                });
        })
        .AsyncVia(AsyncSnapshotInvoker_)
        .Run();
}

void TCompositeAutomaton::LoadSnapshot(IAsyncZeroCopyInputStreamPtr reader)
{
    DoLoadSnapshot(
        reader,
        [&]() {
            using NYT::Load;

            auto& context = LoadContext();

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
                        auto it = PartNameToLoaderDescriptor_.find(name);
                        if (it == PartNameToLoaderDescriptor_.end()) {
                            SERIALIZATION_DUMP_WRITE(context, "<skipped>");
                            LOG_INFO("Skipping unknown automaton part (Name: %v, Version: %v)",
                                name,
                                version);
                        } else {
                            LOG_INFO("Loading automaton part (Name: %v, Version: %v)",
                                name,
                                version);
                            context.SetVersion(version);
                            const auto& descriptor = it->second;
                            descriptor.Callback.Run();
                        }
                    }

                    context.GetCheckpointableInput()->SkipToCheckpoint();
                }
            }

            for (auto part : Parts_) {
                part->OnAfterSnapshotLoaded();
            }
        });
}

void TCompositeAutomaton::ApplyMutation(TMutationContext* context)
{
    const auto& type = context->Request().Type;
    if (type.empty()) {
        // Empty mutation. Typically appears as a tombstone after editing changelogs.
        return;
    }

    auto it = MethodNameToDescriptor_.find(type);
    YCHECK(it != MethodNameToDescriptor_.end());
    const auto& descriptor = it->second;

    NProfiling::TTagIdList tagIds;
    tagIds.push_back(descriptor.TagId);
    static const auto profilingPath = NYTree::TYPath("/mutation_execute_time");
    PROFILE_TIMING (profilingPath, tagIds) {
        descriptor.Callback.Run(context);
    }
}

void TCompositeAutomaton::Clear()
{
    for (auto part : Parts_) {
        part->Clear();
    }
}

void TCompositeAutomaton::DoSaveSnapshot(
    NConcurrency::IAsyncOutputStreamPtr writer,
    ESyncStreamAdapterStrategy strategy,
    const std::function<void()>& callback)
{
    auto syncWriter = CreateSyncAdapter(writer, strategy);
    auto checkpointableOutput = CreateCheckpointableOutputStream(syncWriter.get());
    auto bufferedCheckpointableOutput = CreateBufferedCheckpointableOutputStream(
        checkpointableOutput.get(),
        SnapshotSaveBufferSize);

    auto& context = SaveContext();
    context.SetOutput(bufferedCheckpointableOutput.get());
    context.SetCheckpointableOutput(bufferedCheckpointableOutput.get());
    TFinallyGuard finallyGuard([&] () {
        context.Reset();
    });

    callback();
}

void TCompositeAutomaton::DoLoadSnapshot(
    IAsyncZeroCopyInputStreamPtr reader,
    const std::function<void()>& callback)
{
    auto prefetchingReader = CreatePrefetchingAdapter(reader, SnapshotPrefetchWindowSize);
    auto copyingReader = CreateCopyingAdapter(prefetchingReader);
    auto syncReader = CreateSyncAdapter(copyingReader, ESyncStreamAdapterStrategy::Get);
    TBufferedInput bufferedInput(syncReader.get(), SnapshotLoadBufferSize);
    auto checkpointableInput = CreateCheckpointableInputStream(&bufferedInput);

    auto& context = LoadContext();
    context.SetInput(checkpointableInput.get());
    context.SetCheckpointableInput(checkpointableInput.get());
    context.Dumper().SetEnabled(SerializationDumpEnabled_);
    TFinallyGuard finallyGuard([&] () {
        context.Reset();
    });

    callback();
}

void TCompositeAutomaton::WritePartHeader(const TSaverDescriptorBase& descriptor)
{
    auto& context = SaveContext();
    context.GetCheckpointableOutput()->MakeCheckpoint();

    int version = descriptor.Part->GetCurrentSnapshotVersion();
    LOG_INFO("Saving automaton part (Name: %v, Version: %v)",
        descriptor.Name,
        version);

    Save(context, descriptor.Name);
    Save<i32>(context, version);
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
