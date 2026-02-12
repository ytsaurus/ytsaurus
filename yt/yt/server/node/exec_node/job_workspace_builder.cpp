#include "job_workspace_builder.h"
#include "allocation.h"
#include "slot.h"

#include "job_gpu_checker.h"

#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/library/containers/cri/image_cache.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NExecNode {

using namespace NContainers::NCri;
using namespace NConcurrency;
using namespace NContainers;
using namespace NJobAgent;
using namespace NFS;

static const std::string SetupCommandsTag = "setup";

////////////////////////////////////////////////////////////////////////////////

TJobWorkspaceBuilder::TJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager)
    : Invoker_(std::move(invoker))
    , Context_(std::move(context))
    , DirectoryManager_(std::move(directoryManager))
    , Logger(Context_.Logger)
{
    YT_VERIFY(Context_.Slot);
    YT_VERIFY(Context_.Job);
    YT_VERIFY(DirectoryManager_);
}

template <TFuture<void>(TJobWorkspaceBuilder::*Step)()>
TFuture<void> TJobWorkspaceBuilder::GuardedAction()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto jobPhase = Context_.Job->GetPhase();

    switch (jobPhase) {
        case EJobPhase::WaitingForCleanup:
        case EJobPhase::Cleanup:
        case EJobPhase::Finished:
            YT_LOG_DEBUG(
                "Skip workspace building action (JobPhase: %v, ActionName: %v)",
                jobPhase,
                GetStepName<Step>());
            return OKFuture;

        case EJobPhase::Created:
            YT_VERIFY(Context_.Job->GetState() == EJobState::Waiting);
            break;

        default:
            YT_VERIFY(Context_.Job->GetState() == EJobState::Running);
            break;
    }

    TForbidContextSwitchGuard contextSwitchGuard;

    YT_LOG_DEBUG(
        "Run guarded workspace building action (JobPhase: %v, ActionName: %v)",
        jobPhase,
        GetStepName<Step>());

    return (*this.*Step)();
}

template <TFuture<void>(TJobWorkspaceBuilder::*Step)()>
constexpr const char* TJobWorkspaceBuilder::GetStepName()
{
    if (Step == &TJobWorkspaceBuilder::DoPrepareRootVolume) {
        return "DoPrepareRootVolume";
    } else if (Step == &TJobWorkspaceBuilder::DoPrepareTmpfsVolumes) {
        return "DoPrepareTmpfsVolumes";
    } else if (Step == &TJobWorkspaceBuilder::DoPrepareGpuCheckVolume) {
        return "DoPrepareGpuCheckVolume";
    } else if (Step == &TJobWorkspaceBuilder::DoBindRootVolume) {
        return "DoBindRootVolume";
    } else if (Step == &TJobWorkspaceBuilder::DoLinkTmpfsVolumes) {
        return "DoLinkTmpfsVolumes";
    } else if (Step == &TJobWorkspaceBuilder::DoValidateRootFS) {
        return "DoValidateRootFS";
    } else if (Step == &TJobWorkspaceBuilder::DoPrepareSandboxDirectories) {
        return "DoPrepareSandboxDirectories";
    } else if (Step == &TJobWorkspaceBuilder::DoRunSetupCommand) {
        return "DoRunSetupCommand";
    } else if (Step == &TJobWorkspaceBuilder::DoRunCustomPreparations) {
        return "DoRunCustomPreparations";
    } else if (Step == &TJobWorkspaceBuilder::DoRunGpuCheckCommand) {
        return "DoRunGpuCheckCommand";
    }
}

template <TFuture<void>(TJobWorkspaceBuilder::*Method)()>
TCallback<TFuture<void>()> TJobWorkspaceBuilder::MakeStep()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return BIND([this, this_ = MakeStrong(this)] {
        return GuardedAction<Method>();
    }).AsyncVia(Invoker_);
}

void TJobWorkspaceBuilder::ValidateJobPhase(EJobPhase expectedPhase) const
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto jobPhase = Context_.Job->GetPhase();
    if (jobPhase != expectedPhase) {
        // COMPAT(krasovav)
        if (expectedPhase == EJobPhase::CachingArtifacts && jobPhase == EJobPhase::DownloadingArtifacts) {
            return;
        }

        YT_LOG_DEBUG(
            "Unexpected job phase during workspace preparation (Actual: %v, Expected: %v)",
            jobPhase,
            expectedPhase);

        THROW_ERROR_EXCEPTION("Unexpected job phase")
            << TErrorAttribute("expected_phase", expectedPhase)
            << TErrorAttribute("actual_phase", jobPhase);
    }
}

void TJobWorkspaceBuilder::SetJobPhase(EJobPhase phase)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    UpdateBuilderPhase_.Fire(phase);
}

void TJobWorkspaceBuilder::UpdateArtifactStatistics(i64 compressedDataSize, bool cacheHit)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    UpdateArtifactStatistics_.Fire(compressedDataSize, cacheHit);
}

void TJobWorkspaceBuilder::MakeArtifactSymlinks()
{
    const auto& slot = Context_.Slot;

    YT_LOG_INFO(
        "Making artifact symlinks (ArtifactCount: %v)",
        std::size(Context_.Artifacts));

    for (const auto& artifact : Context_.Artifacts) {
        // Artifact is passed into the job via symlink.
        if (!artifact.BypassArtifactCache && !artifact.CopyFile) {
            YT_VERIFY(artifact.Artifact);

            YT_LOG_INFO(
                "Making symlink for artifact (FileName: %v, Executable: "
                "%v, SandboxKind: %v, CompressedDataSize: %v)",
                artifact.Name,
                artifact.Executable,
                artifact.SandboxKind,
                artifact.Key.GetCompressedDataSize());

            auto sandboxPath = slot->GetSandboxPath(artifact.SandboxKind, ResultHolder_.RootVolume, Context_.TestRootFS);
            // TODO(dgolear): Switch to std::string.
            TString symlinkPath = CombinePaths(sandboxPath, artifact.Name);

            WaitFor(slot->MakeLink(
                Context_.Job->GetId(),
                artifact.Name,
                artifact.SandboxKind,
                TString(artifact.Artifact->GetFileName()),
                symlinkPath,
                artifact.Executable))
                .ThrowOnError();

            YT_LOG_INFO(
                "Symlink for artifact is successfully made (FileName: %v, Executable: %v,"
                " SandboxKind: %v, CompressedDataSize: %v)",
                artifact.Name,
                artifact.Executable,
                artifact.SandboxKind,
                artifact.Key.GetCompressedDataSize());
        } else {
            YT_VERIFY(artifact.SandboxKind == ESandboxKind::User);
        }
    }

    YT_LOG_INFO("Artifact symlinks are made");
}

void TJobWorkspaceBuilder::MakeFilesForArtifactBinds()
{
    const auto& slot = Context_.Slot;

    YT_LOG_INFO(
        "Setting permissions for artifacts (ArtifactCount: %v)",
        std::size(Context_.Artifacts));

    std::vector<TFuture<void>> ioOperationFutures;
    ioOperationFutures.reserve(Context_.Artifacts.size());

    for (const auto& artifact : Context_.Artifacts) {
        if (artifact.AccessedViaBind) {
            YT_VERIFY(artifact.Artifact);

            auto sandboxPath = slot->GetSandboxPath(artifact.SandboxKind, ResultHolder_.RootVolume, Context_.TestRootFS);
            // TODO(dgolear): Swtich to std::string.
            TString artifactPath = CombinePaths(sandboxPath, artifact.Name);

            YT_LOG_INFO(
                "Set permissions for artifact (FileName: %v, Executable: "
                "%v, SandboxKind: %v, CompressedDataSize: %v)",
                artifact.Name,
                artifact.Executable,
                artifact.SandboxKind,
                artifact.Key.GetCompressedDataSize());

            ioOperationFutures.push_back(slot->MakeFileForSandboxBind(
                Context_.Job->GetId(),
                artifact.Name,
                artifact.SandboxKind,
                TString(artifact.Artifact->GetFileName()),
                artifactPath,
                artifact.Executable));
        } else {
            YT_VERIFY(artifact.SandboxKind == ESandboxKind::User);
        }
    }

    auto allSetFuture = AllSet(ioOperationFutures);
    auto errors = WaitFor(allSetFuture).ValueOrThrow();
    for (const auto& error : errors) {
        error.ThrowOnError();
    }

    YT_LOG_INFO("Permissions for artifacts set");
}

void TJobWorkspaceBuilder::SetNowTime(std::optional<TInstant>& timeField)
{
    timeField = TInstant::Now();
    UpdateTimePoints_.Fire(TimePoints_);
}

TFuture<void> TJobWorkspaceBuilder::Run()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto future = MakeStep<&TJobWorkspaceBuilder::DoPrepareRootVolume>()
        .Run()
        .Apply(MakeStep<&TJobWorkspaceBuilder::DoPrepareTmpfsVolumes>())
        .Apply(MakeStep<&TJobWorkspaceBuilder::DoPrepareGpuCheckVolume>())
        .Apply(MakeStep<&TJobWorkspaceBuilder::DoBindRootVolume>())
        .Apply(MakeStep<&TJobWorkspaceBuilder::DoLinkTmpfsVolumes>())
        .Apply(MakeStep<&TJobWorkspaceBuilder::DoValidateRootFS>())
        .Apply(MakeStep<&TJobWorkspaceBuilder::DoPrepareSandboxDirectories>())
        .Apply(MakeStep<&TJobWorkspaceBuilder::DoRunSetupCommand>())
        .Apply(MakeStep<&TJobWorkspaceBuilder::DoRunCustomPreparations>())
        .Apply(MakeStep<&TJobWorkspaceBuilder::DoRunGpuCheckCommand>())
        .Apply(BIND([this, this_ = MakeStrong(this)] (const TError& result) {
            YT_LOG_INFO(result, "Job workspace building finished");

            ResultHolder_.LastBuildError = result;
        }).AsyncVia(Invoker_));

    future.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError&) {
        // Drop reference to close race with check in TJob::Cleanup() on cancellation.
        Context_.Slot.Reset();
    }).Via(Invoker_));

    return future;
}

//! Extract and move the workspace building result.
//! Can only be called once after Run() completes.
TJobWorkspaceBuildingResult TJobWorkspaceBuilder::ExtractResult()
{
    if (std::exchange(ResultExtracted_, true)) {
        YT_LOG_FATAL("Result has already been extracted");
    }

    // It is expected that a situation where volumes are not linked will be triggered only when canceling job_workspace_builder.
    // The return of non-linked volumes is necessary in order to delete them correctly and set "disable" if an error occurs.
    if (ResultHolder_.TmpfsVolumes.empty()) {
        ResultHolder_.TmpfsVolumes = std::move(Context_.PreparedTmpfsVolumes);
    }

    return std::move(ResultHolder_);
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobWorkspaceBuilder
    : public TJobWorkspaceBuilder
{
public:
    TSimpleJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager)
        : TJobWorkspaceBuilder(
            std::move(invoker),
            std::move(context),
            std::move(directoryManager))
    { }

private:
    TRootFS MakeWritableRootFS()
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_VERIFY(ResultHolder_.RootVolume);

        auto binds = Context_.Binds;

        for (const auto& bind : ResultHolder_.RootBinds) {
            binds.push_back(bind);
        }

        return TRootFS{
            // TODO(dgolear): Switch to std::string.
            .RootPath = TString(ResultHolder_.RootVolume->GetPath()),
            .IsRootReadOnly = false,
            .Binds = std::move(binds),
        };
    }

    TFuture<void> DoPrepareRootVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("Root volume preparation is not supported in simple workspace");

        ValidateJobPhase(EJobPhase::CachingArtifacts);
        SetJobPhase(EJobPhase::PreparingRootVolume);

        if (!Context_.RootVolumeLayerArtifactKeys.empty()) {
            return MakeFuture(TError(
                NExecNode::EErrorCode::LayerUnpackingFailed,
                "Porto layers are not supported in simple job environment"));
        }

        if (Context_.DockerImage) {
            return MakeFuture(TError(
                NExecNode::EErrorCode::DockerImagePullingFailed,
                "External docker image is not supported in simple job environment"));
        }

        return OKFuture;
    }

    TFuture<void> DoPrepareTmpfsVolumes() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingRootVolume);
        SetJobPhase(EJobPhase::PreparingTmpfsVolumes);

        SetNowTime(TimePoints_.PrepareTmpfsVolumesStartTime);

        const auto& volumes = Context_.UserSandboxOptions.TmpfsVolumes;
        if (volumes.empty()) {
            SetNowTime(TimePoints_.PrepareTmpfsVolumesFinishTime);
            return OKFuture;
        }

        const auto& slot = Context_.Slot;
        return slot->PrepareTmpfsVolumes(ResultHolder_.RootVolume, volumes, Context_.TestRootFS)
            .AsUnique().Apply(BIND([slot, this, this_ = MakeStrong(this)] (TErrorOr<std::vector<TTmpfsVolumeResult>>&& volumeResultsOrError) {
                if (!volumeResultsOrError.IsOK()) {
                    THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::TmpfsVolumePreparationFailed, "Failed to prepare tmpfs volumes")
                        << volumeResultsOrError;
                }

                auto& volumeResults = volumeResultsOrError.Value();

                YT_LOG_DEBUG("Prepared tmpfs volumes (Volumes: %v)",
                    MakeFormattableView(volumeResults,
                        [] (auto* builder, const TTmpfsVolumeResult& result) {
                            builder->AppendFormat("{TmpfsPath: %v, VolumePath: %v}",
                                result.Path,
                                result.Volume->GetPath());
                        }));

                Context_.PreparedTmpfsVolumes = std::move(volumeResults);

                SetNowTime(TimePoints_.PrepareTmpfsVolumesFinishTime);
            }).AsyncVia(Invoker_));
    }

    TFuture<void> DoPrepareGpuCheckVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("GPU check volume preparation is not supported in simple workspace");

        ValidateJobPhase(EJobPhase::PreparingTmpfsVolumes);
        SetJobPhase(EJobPhase::PreparingGpuCheckVolume);

        return OKFuture;
    }

    TFuture<void> DoBindRootVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingGpuCheckVolume);
        SetJobPhase(EJobPhase::LinkingVolumes);

        YT_LOG_DEBUG("Root volume binding is not needed in simple workspace");

         ResultHolder_.RootVolume = std::move(Context_.RootVolume);

        return OKFuture;
    }

    TFuture<void> DoLinkTmpfsVolumes() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ResultHolder_.TmpfsVolumes = std::move(Context_.PreparedTmpfsVolumes);

        YT_LOG_DEBUG("Link tmpfs volumes is not needed in simple workspace");

        return OKFuture;
    }

    TFuture<void> DoValidateRootFS() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::LinkingVolumes);
        SetJobPhase(EJobPhase::ValidatingRootFS);

        return OKFuture;
    }

    TFuture<void> DoPrepareSandboxDirectories() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::ValidatingRootFS);
        SetJobPhase(EJobPhase::PreparingSandboxDirectories);

        YT_LOG_INFO("Started preparing sandbox directories");

        return Context_.Slot->PrepareSandboxDirectories(Context_.UserSandboxOptions)
            .Apply(BIND([this, this_ = MakeStrong(this)] {
                MakeArtifactSymlinks();

                YT_LOG_INFO("Finished preparing sandbox directories");
            }).AsyncVia(Invoker_));
    }

    TFuture<void> DoRunSetupCommand() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("Setup command is not supported in simple workspace");

        ValidateJobPhase(EJobPhase::PreparingSandboxDirectories);
        SetJobPhase(EJobPhase::RunningSetupCommands);

        return OKFuture;
    }

    TFuture<void> DoRunCustomPreparations() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("There are no custom preparations in simple workspace");

        ValidateJobPhase(EJobPhase::RunningSetupCommands);
        SetJobPhase(EJobPhase::RunningCustomPreparations);

        return OKFuture;
    }

    TFuture<void> DoRunGpuCheckCommand() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("GPU check is not supported in simple workspace");

        ValidateJobPhase(EJobPhase::RunningCustomPreparations);
        // NB: we intentionally not set running_gpu_check_command phase, since this phase is empty.

        return OKFuture;
    }
};

////////////////////////////////////////////////////////////////////////////////

TJobWorkspaceBuilderPtr CreateSimpleJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager)
{
    return New<TSimpleJobWorkspaceBuilder>(
        std::move(invoker),
        std::move(context),
        std::move(directoryManager));
}

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

class TPortoJobWorkspaceBuilder
    : public TJobWorkspaceBuilder
{
public:
    TPortoJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager,
        TGpuManagerPtr gpuManager)
        : TJobWorkspaceBuilder(
            std::move(invoker),
            std::move(context),
            std::move(directoryManager))
        , GpuManager_(std::move(gpuManager))
    { }

private:
    const TGpuManagerPtr GpuManager_;

    TFuture<void> DoPrepareRootVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::CachingArtifacts);
        SetJobPhase(EJobPhase::PreparingRootVolume);

        const auto& slot = Context_.Slot;
        const auto& layerArtifactKeys = Context_.RootVolumeLayerArtifactKeys;

        if (Context_.DockerImage && layerArtifactKeys.empty()) {
            return MakeFuture(TError(
                NExecNode::EErrorCode::DockerImagePullingFailed,
                "External docker image is not supported in Porto job environment"));
        }

        if (!layerArtifactKeys.empty()) {
            SetNowTime(TimePoints_.PrepareRootVolumeStartTime);

            YT_LOG_INFO("Preparing root volume (LayerCount: %v, HasVirtualSandbox: %v)",
                layerArtifactKeys.size(),
                Context_.UserSandboxOptions.VirtualSandboxData.has_value());

            for (const auto& layer : layerArtifactKeys) {
                i64 layerSize = layer.GetCompressedDataSize();
                UpdateArtifactStatistics(layerSize, slot->IsLayerCached(layer));
            }

            TVolumePreparationOptions options;
            options.JobId = Context_.Job->GetId();
            options.ArtifactDownloadOptions = Context_.ArtifactDownloadOptions;
            options.UserSandboxOptions = Context_.UserSandboxOptions;

            return slot->PrepareRootVolume(
                layerArtifactKeys,
                options)
                    .Apply(
                        BIND([slot, this, this_ = MakeStrong(this)] (const TErrorOr<IVolumePtr>& volumeOrError) {
                            if (!volumeOrError.IsOK()) {
                                YT_LOG_WARNING(volumeOrError, "Failed to prepare root volume");

                                THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::RootVolumePreparationFailed, "Failed to prepare root volume")
                                    << volumeOrError;
                            }

                            auto rootVolume = std::move(volumeOrError.Value());
                            return slot->CreateSlotDirectories(
                                rootVolume,
                                Context_.UserSandboxOptions.UserId)
                                    .Apply(
                                        BIND([rootVolume, this, this_ = MakeStrong(this)] {
                                            Context_.RootVolume = rootVolume;
                                            YT_LOG_DEBUG("Root volume prepared");
                                            SetNowTime(TimePoints_.PrepareRootVolumeFinishTime);
                                        })
                                        .AsyncVia(Invoker_))
                                    .ToUncancelable();

                        })
                        .AsyncVia(Invoker_));
        } else {
            YT_LOG_DEBUG("Root volume preparation is not needed");
            return OKFuture;
        }
    }

    TFuture<void> DoPrepareTmpfsVolumes() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingRootVolume);
        SetJobPhase(EJobPhase::PreparingTmpfsVolumes);

        SetNowTime(TimePoints_.PrepareTmpfsVolumesStartTime);

        const auto& volumes = Context_.UserSandboxOptions.TmpfsVolumes;
        if (volumes.empty()) {
            SetNowTime(TimePoints_.PrepareTmpfsVolumesFinishTime);
            return OKFuture;
        }

        const auto& slot = Context_.Slot;
        return slot->PrepareTmpfsVolumes(ResultHolder_.RootVolume, volumes, Context_.TestRootFS)
            .AsUnique()
            .Apply(
                BIND([slot, this, this_ = MakeStrong(this)] (TErrorOr<std::vector<TTmpfsVolumeResult>>&& volumeResultsOrError) {
                    if (!volumeResultsOrError.IsOK()) {
                        THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::TmpfsVolumePreparationFailed, "Failed to prepare tmpfs volumes")
                            << volumeResultsOrError;
                    }

                    Context_.PreparedTmpfsVolumes = std::move(volumeResultsOrError.Value());

                    YT_LOG_DEBUG("Prepared tmpfs volumes (Volumes: %v)",
                    MakeFormattableView(Context_.PreparedTmpfsVolumes,
                        [] (auto* builder, const TTmpfsVolumeResult& result) {
                            builder->AppendFormat("{TmpfsPath: %v, VolumePath: %v}",
                                result.Path,
                                result.Volume->GetPath());
                        }));
                    SetNowTime(TimePoints_.PrepareTmpfsVolumesFinishTime);
                })
                .AsyncVia(Invoker_))
            .ToUncancelable();
    }

    TFuture<void> DoPrepareGpuCheckVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingTmpfsVolumes);
        SetJobPhase(EJobPhase::PreparingGpuCheckVolume);

        const auto& slot = Context_.Slot;
        const auto& layerArtifactKeys = Context_.GpuCheckVolumeLayerArtifactKeys;

        if (!layerArtifactKeys.empty()) {
            SetNowTime(TimePoints_.PrepareGpuCheckVolumeStartTime);

            YT_LOG_INFO("Preparing GPU check volume (LayerCount: %v)",
                layerArtifactKeys.size());

            for (const auto& layer : layerArtifactKeys) {
                i64 layerSize = layer.GetCompressedDataSize();
                UpdateArtifactStatistics(layerSize, slot->IsLayerCached(layer));
            }

            TVolumePreparationOptions options;
            options.JobId = Context_.Job->GetId();
            options.ArtifactDownloadOptions = Context_.ArtifactDownloadOptions;

            return slot->PrepareGpuCheckVolume(
                layerArtifactKeys,
                options)
                .Apply(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<IVolumePtr>& volumeOrError) {
                    if (!volumeOrError.IsOK()) {
                        YT_LOG_WARNING(volumeOrError, "Failed to prepare GPU check volume");

                        THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::RootVolumePreparationFailed, "Failed to prepare GPU check volume")
                            << volumeOrError;
                    }

                    YT_LOG_DEBUG("GPU check volume prepared");

                    ResultHolder_.GpuCheckVolume = volumeOrError.Value();

                    SetNowTime(TimePoints_.PrepareGpuCheckVolumeFinishTime);
                }));
        } else {
            YT_LOG_DEBUG("GPU check volume preparation is not needed");
            return OKFuture;
        }

        return OKFuture;
    }

    TFuture<void> DoBindRootVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingGpuCheckVolume);
        SetJobPhase(EJobPhase::LinkingVolumes);

        auto slot = Context_.Slot;
        auto slotPath = slot->GetSlotPath();
        if (Context_.RootVolume && !Context_.UserSandboxOptions.EnableRootVolumeDiskQuota) {
            return slot->RbindRootVolume(Context_.RootVolume, slotPath)
                .Apply(BIND(
                    [
                        slot,
                        this,
                        this_ = MakeStrong(this)
                    ] (const TErrorOr<IVolumePtr>& volumeOrError) {
                        Context_.RootVolume.Reset();

                        if (!volumeOrError.IsOK()) {
                            YT_LOG_WARNING(volumeOrError, "Failed to prepare root volume");

                            THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::RootVolumePreparationFailed, "Failed to prepare root volume")
                                << volumeOrError;
                        }

                        ResultHolder_.RootVolume = std::move(volumeOrError.Value());
                    })
                    .AsyncVia(Invoker_))
                .ToImmediatelyCancelable();
        } else {
            ResultHolder_.RootVolume = std::move(Context_.RootVolume);
            YT_LOG_DEBUG("Root volume binding is not needed");
        }

        return OKFuture;
    }

    TFuture<void> DoLinkTmpfsVolumes() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::LinkingVolumes);

        SetNowTime(TimePoints_.LinkTmpfsVolumesStartTime);

        const auto& volumes = Context_.PreparedTmpfsVolumes;
        const auto& slot = Context_.Slot;
        return slot->LinkTmpfsVolumes(ResultHolder_.RootVolume, volumes, Context_.TestRootFS)
            .Apply(BIND([volumeResults = volumes, this, this_ = MakeStrong(this)](const TErrorOr<void>& error) {
                if (!error.IsOK()) {
                    THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::TmpfsVolumeLinkingFailed, "Failed to link tmpfs volumes")
                        << error;
                }

                YT_LOG_DEBUG("Linked tmpfs volumes (Volumes: %v)",
                    MakeFormattableView(volumeResults,
                        [] (auto* builder, const TTmpfsVolumeResult& result) {
                            builder->AppendFormat("{TmpfsPath: %v, VolumePath: %v}",
                                result.Path,
                                result.Volume->GetPath());
                    }));

                ResultHolder_.TmpfsVolumes = std::move(volumeResults);
                Context_.PreparedTmpfsVolumes.clear();

                SetNowTime(TimePoints_.LinkTmpfsVolumesFinishTime);
            }).AsyncVia(Invoker_))
            .ToUncancelable();
    }

    TFuture<void> DoValidateRootFS() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::LinkingVolumes);
        SetJobPhase(EJobPhase::ValidatingRootFS);

        if (!ResultHolder_.RootVolume || Context_.TestRootFS) {
            return OKFuture;
        }

        SetNowTime(TimePoints_.ValidateRootFSStartTime);
        const auto& slot = Context_.Slot;
        return slot->ValidateRootFS(ResultHolder_.RootVolume)
            .Apply(BIND([this, this_ = MakeStrong(this)] () {
                SetNowTime(TimePoints_.ValidateRootFSFinishTime);
            })
            .AsyncVia(Invoker_));
    }

    TFuture<void> DoPrepareSandboxDirectories() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::ValidatingRootFS);
        SetJobPhase(EJobPhase::PreparingSandboxDirectories);

        YT_LOG_INFO("Started preparing sandbox directories");

        // NB: If EnableRootVolumeDiskQuota is set and we have RootVolume, then we have already
        // applied a quota to root volume and should not set it again within sandbox preparation.
        bool ignoreQuota = Context_.UserSandboxOptions.EnableRootVolumeDiskQuota && ResultHolder_.RootVolume;

        return Context_.Slot->PrepareSandboxDirectories(Context_.UserSandboxOptions, ignoreQuota)
            .Apply(BIND([this, this_ = MakeStrong(this)] {
                if (ResultHolder_.RootVolume && !Context_.TestRootFS) {
                    MakeFilesForArtifactBinds();
                } else {
                    MakeArtifactSymlinks();
                }

                YT_LOG_INFO("Finished preparing sandbox directories");
            }).AsyncVia(Invoker_));
    }

    TRootFS MakeWritableRootFS()
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_VERIFY(ResultHolder_.RootVolume);

        auto binds = Context_.Binds;

        for (const auto& bind : ResultHolder_.RootBinds) {
            binds.push_back(bind);
        }

        return TRootFS{
            .RootPath = TString(ResultHolder_.RootVolume->GetPath()),
            .IsRootReadOnly = false,
            .Binds = std::move(binds),
        };
    }

    TRootFS MakeWritableGpuCheckRootFS()
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_VERIFY(ResultHolder_.GpuCheckVolume);

        return TRootFS{
            .RootPath = TString(ResultHolder_.GpuCheckVolume->GetPath()),
            .IsRootReadOnly = false,
            .Binds = {},
        };
    }

    TFuture<void> DoRunSetupCommand() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingSandboxDirectories);
        SetJobPhase(EJobPhase::RunningSetupCommands);

        if (!ResultHolder_.RootVolume) {
            return OKFuture;
        }

        const auto& slot = Context_.Slot;

        const auto& commands = Context_.SetupCommands;
        ResultHolder_.SetupCommandCount = commands.size();

        if (commands.empty()) {
            YT_LOG_DEBUG("No setup command is needed");

            return OKFuture;
        }

        YT_LOG_INFO("Running setup commands");

        return slot->RunPreparationCommands(
            Context_.Job->GetId(),
            commands,
            MakeWritableRootFS(),
            Context_.CommandUser,
            /*devices*/ std::nullopt,
            /*hostName*/ std::nullopt,
            /*ipAddresses*/ {},
            /*tag*/ SetupCommandsTag,
            /*throwOnFailedCommand*/ true)
            .AsVoid();
    }

    TFuture<void> DoRunCustomPreparations() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("Running custom preparations");

        ValidateJobPhase(EJobPhase::RunningSetupCommands);
        SetJobPhase(EJobPhase::RunningCustomPreparations);

        if (!Context_.NeedGpu) {
            return OKFuture;
        }

        auto networkPriority = Context_.Job->GetAllocation()->GetNetworkPriority();

        return BIND([this, this_ = MakeStrong(this), networkPriority] {
            GpuManager_->ApplyNetworkPriority(networkPriority);
        })
            .AsyncVia(Invoker_)
            .Run();
    }

    TFuture<void> DoRunGpuCheckCommand() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::RunningCustomPreparations);

        if (Context_.GpuCheckOptions) {
            SetJobPhase(EJobPhase::RunningGpuCheckCommand);

            YT_VERIFY(ResultHolder_.GpuCheckVolume);

            auto options = *Context_.GpuCheckOptions;

            auto context = TJobGpuCheckerContext{
                .Slot = Context_.Slot,
                .Job = Context_.Job,
                .RootFS = MakeWritableGpuCheckRootFS(),
                .CommandUser = Context_.CommandUser,
                .Type = EGpuCheckType::Preliminary,
                .Options = options,
                .CurrentStartIndex = ResultHolder_.SetupCommandCount,
                // It is preliminary (not extra) GPU check.
                .TestExtraGpuCheckCommandFailure = false,
            };

            auto checker = New<TJobGpuChecker>(std::move(context), Logger);

            checker->SubscribeRunCheck(BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] {
                SetNowTime(TimePoints_.GpuCheckStartTime);
            }));

            checker->SubscribeFinishCheck(BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] {
                SetNowTime(TimePoints_.GpuCheckFinishTime);
            }));

            YT_LOG_INFO("Starting preliminary GPU check");

            return BIND(&TJobGpuChecker::RunGpuCheck, std::move(checker))
                .AsyncVia(Invoker_)
                .Run()
                .Apply(BIND([this, this_ = MakeStrong(this)] (const TError& result) {
                    ValidateJobPhase(EJobPhase::RunningGpuCheckCommand);
                    if (!result.IsOK()) {
                        auto checkError = TError(NExecNode::EErrorCode::GpuCheckCommandFailed, "Preliminary GPU check command failed")
                            << std::move(result);
                        THROW_ERROR checkError;
                    }

                    YT_LOG_INFO("Preliminary GPU check command finished");
                }).AsyncVia(Invoker_));
        } else {
            // NB: we intentionally not set running_gpu_check_command phase, since this phase is empty.
            YT_LOG_INFO("No preliminary GPU check is needed");

            return OKFuture;
        }
    }
};

TJobWorkspaceBuilderPtr CreatePortoJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager,
    TGpuManagerPtr gpuManager)
{
    return New<TPortoJobWorkspaceBuilder>(
        std::move(invoker),
        std::move(context),
        std::move(directoryManager),
        std::move(gpuManager));
}

#endif

////////////////////////////////////////////////////////////////////////////////

class TCriJobWorkspaceBuilder
    : public TJobWorkspaceBuilder
{
public:
    TCriJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildingContext context,
        IJobDirectoryManagerPtr directoryManager,
        ICriImageCachePtr imageCache)
        : TJobWorkspaceBuilder(
            std::move(invoker),
            std::move(context),
            std::move(directoryManager))
        , ImageCache_(std::move(imageCache))
    { }

private:
    TFuture<void> DoPrepareRootVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::CachingArtifacts);
        SetJobPhase(EJobPhase::PreparingRootVolume);

        const auto& dockerImage = Context_.DockerImage;

        if (!dockerImage && !Context_.RootVolumeLayerArtifactKeys.empty()) {
            return MakeFuture(TError(
                NExecNode::EErrorCode::LayerUnpackingFailed,
                "Porto layers are not supported in CRI job environment"));
        }

        if (dockerImage) {
            SetNowTime(TimePoints_.PrepareRootVolumeStartTime);

            TCriImageDescriptor imageDescriptor {
                .Image = *dockerImage,
            };

            YT_LOG_INFO("Preparing root volume (Image: %v)", imageDescriptor);

            return ImageCache_->PullImage(
                imageDescriptor,
                Context_.DockerAuth)
                .Apply(BIND([
                    =,
                    this,
                    this_ = MakeStrong(this),
                    authenticated = bool(Context_.DockerAuth)
                ] (const TErrorOr<TCriImageCacheEntryPtr>& imageOrError) {
                    if (!imageOrError.IsOK()) {
                        YT_LOG_WARNING(imageOrError, "Failed to prepare root volume (Image: %v)", imageDescriptor);

                        THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::DockerImagePullingFailed, "Failed to pull docker image")
                            << TErrorAttribute("docker_image", imageDescriptor.Image)
                            << TErrorAttribute("authenticated", authenticated)
                            << imageOrError;
                    }

                    const auto& cachedImage = imageOrError.Value()->Image();
                    YT_LOG_INFO("Root volume prepared (Image: %v)", cachedImage);

                    ResultHolder_.DockerImage = cachedImage.Image;
                    ResultHolder_.DockerImageId = cachedImage.Id;

                    SetNowTime(TimePoints_.PrepareRootVolumeFinishTime);
                }));
        } else {
            YT_LOG_DEBUG("Root volume preparation is not needed");
            return OKFuture;
        }
    }

    TFuture<void> DoPrepareTmpfsVolumes() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingRootVolume);
        SetJobPhase(EJobPhase::PreparingTmpfsVolumes);

        SetNowTime(TimePoints_.PrepareTmpfsVolumesStartTime);

        const auto& volumes = Context_.UserSandboxOptions.TmpfsVolumes;
        if (volumes.empty()) {
            SetNowTime(TimePoints_.PrepareTmpfsVolumesFinishTime);
            return OKFuture;
        }

        const auto& slot = Context_.Slot;
        return slot->PrepareTmpfsVolumes(ResultHolder_.RootVolume, volumes, Context_.TestRootFS)
            .AsUnique().Apply(BIND([slot, this, this_ = MakeStrong(this)] (TErrorOr<std::vector<TTmpfsVolumeResult>>&& volumeResultsOrError) {
                if (!volumeResultsOrError.IsOK()) {
                    THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::TmpfsVolumePreparationFailed, "Failed to prepare tmpfs volumes")
                        << volumeResultsOrError;
                }

                auto& volumeResults = volumeResultsOrError.Value();

                YT_LOG_DEBUG("Prepared tmpfs volumes (Volumes: %v)",
                    MakeFormattableView(volumeResults,
                        [] (auto* builder, const TTmpfsVolumeResult& result) {
                            builder->AppendFormat("{TmpfsPath: %v, VolumePath: %v}",
                                result.Path,
                                result.Volume->GetPath());
                        }));

                Context_.PreparedTmpfsVolumes = std::move(volumeResults);

                SetNowTime(TimePoints_.PrepareTmpfsVolumesFinishTime);
            }).AsyncVia(Invoker_));
    }

    TFuture<void> DoPrepareGpuCheckVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG_IF(Context_.GpuCheckOptions, "Skip preparing GPU check volume since GPU check is not support in CRI environment");

        ValidateJobPhase(EJobPhase::PreparingTmpfsVolumes);
        SetJobPhase(EJobPhase::PreparingGpuCheckVolume);

        return OKFuture;
    }

    TFuture<void> DoBindRootVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingGpuCheckVolume);
        SetJobPhase(EJobPhase::LinkingVolumes);

        YT_LOG_DEBUG("Root volume binding is not needed in cri workspace");

        ResultHolder_.RootVolume = Context_.RootVolume;

        return OKFuture;
    }

    TFuture<void> DoLinkTmpfsVolumes() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::LinkingVolumes);

        YT_LOG_DEBUG("Link tmpfs volumes is not supported in cri workspace");

        ResultHolder_.TmpfsVolumes = std::move(Context_.PreparedTmpfsVolumes);

        return OKFuture;
    }

    TFuture<void> DoValidateRootFS() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::LinkingVolumes);
        SetJobPhase(EJobPhase::ValidatingRootFS);

        return OKFuture;
    }

    TFuture<void> DoPrepareSandboxDirectories() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::ValidatingRootFS);
        SetJobPhase(EJobPhase::PreparingSandboxDirectories);

        YT_LOG_INFO("Started preparing sandbox directories");

        return Context_.Slot->PrepareSandboxDirectories(Context_.UserSandboxOptions)
            .Apply(BIND([this, this_ = MakeStrong(this)] {
                MakeFilesForArtifactBinds();

                YT_LOG_INFO("Finished preparing sandbox directories");
            }).AsyncVia(Invoker_));
    }

    TFuture<void> DoRunSetupCommand() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingSandboxDirectories);
        SetJobPhase(EJobPhase::RunningSetupCommands);

        if (Context_.SetupCommands.empty()) {
            YT_LOG_DEBUG("No setup command is needed");
            return OKFuture;
        }

        YT_LOG_INFO("Running setup commands");

        TRootFS rootFS{
            .Binds = Context_.Binds,
        };

        rootFS.Binds.push_back(TBind{
            .SourcePath = Context_.Slot->GetSlotPath(),
            .TargetPath = "/slot",
            .ReadOnly = false,
        });

        ResultHolder_.SetupCommandCount = Context_.SetupCommands.size();
        return Context_.Slot->RunPreparationCommands(
            Context_.Job->GetId(),
            Context_.SetupCommands,
            rootFS,
            Context_.CommandUser,
            /*devices*/ std::nullopt,
            /*hostName*/ std::nullopt,
            /*ipAddresses*/ {},
            /*tag*/ SetupCommandsTag,
            /*throwOnFailedCommand*/ true)
            .AsVoid();
    }

    TFuture<void> DoRunCustomPreparations() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("There are no custom preparations in CRI workspace");

        ValidateJobPhase(EJobPhase::RunningSetupCommands);
        SetJobPhase(EJobPhase::RunningCustomPreparations);

        return OKFuture;
    }

    TFuture<void> DoRunGpuCheckCommand() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG_IF(Context_.GpuCheckOptions, "GPU check is not supported in CRI workspace");

        ValidateJobPhase(EJobPhase::RunningCustomPreparations);
        // NB: we intentionally not set running_gpu_check_command phase, since this phase is empty.

        return OKFuture;
    }

private:
    const ICriImageCachePtr ImageCache_;
};

////////////////////////////////////////////////////////////////////////////////

TJobWorkspaceBuilderPtr CreateCriJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager,
    ICriImageCachePtr imageCache)
{
    return New<TCriJobWorkspaceBuilder>(
        std::move(invoker),
        std::move(context),
        std::move(directoryManager),
        std::move(imageCache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
