#include "job_workspace_builder.h"

#include "job_gpu_checker.h"
#include "job_directory_manager.h"

#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/library/containers/cri/cri_executor.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NExecNode
{

using namespace NContainers::NCri;
using namespace NConcurrency;
using namespace NContainers;
using namespace NJobAgent;
using namespace NFS;

////////////////////////////////////////////////////////////////////////////////

static const TString MountSuffix = "mount";

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

    if (Context_.NeedGpuCheck) {
        YT_VERIFY(Context_.GpuCheckBinaryPath);
        YT_VERIFY(Context_.GpuCheckBinaryArgs);
    }
}

template<TFuture<void>(TJobWorkspaceBuilder::*Step)()>
TFuture<void> TJobWorkspaceBuilder::GuardedAction()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto jobPhase = Context_.Job->GetPhase();

    switch (jobPhase) {
        case EJobPhase::WaitingForCleanup:
        case EJobPhase::Cleanup:
        case EJobPhase::Finished:
            YT_LOG_DEBUG(
                "Skip workspace building action (JobPhase: %v, ActionName: %v)",
                jobPhase,
                GetStepName<Step>());
            return VoidFuture;

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

template<TFuture<void>(TJobWorkspaceBuilder::*Step)()>
constexpr const char* TJobWorkspaceBuilder::GetStepName()
{
    if (Step == &TJobWorkspaceBuilder::DoPrepareRootVolume) {
        return "DoPrepareRootVolume";
    } else if (Step == &TJobWorkspaceBuilder::DoRunSetupCommand) {
        return "DoRunSetupCommand";
    } else if (Step == &TJobWorkspaceBuilder::DoRunGpuCheckCommand) {
        return "DoRunGpuCheckCommand";
    }
}

template<TFuture<void>(TJobWorkspaceBuilder::*Method)()>
TCallback<TFuture<void>()> TJobWorkspaceBuilder::MakeStep()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return BIND([this, this_ = MakeStrong(this)] () {
        return GuardedAction<Method>();
    }).AsyncVia(Invoker_);
}

void TJobWorkspaceBuilder::ValidateJobPhase(EJobPhase expectedPhase) const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto jobPhase = Context_.Job->GetPhase();
    if (jobPhase != expectedPhase) {
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
    VERIFY_THREAD_AFFINITY(JobThread);

    UpdateBuilderPhase_.Fire(phase);
}

void TJobWorkspaceBuilder::UpdateArtifactStatistics(i64 compressedDataSize, bool cacheHit)
{
    VERIFY_THREAD_AFFINITY(JobThread);

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
            YT_VERIFY(artifact.Chunk);

            YT_LOG_INFO(
                "Making symlink for artifact (FileName: %v, Executable: "
                "%v, SandboxKind: %v, CompressedDataSize: %v)",
                artifact.Name,
                artifact.Executable,
                artifact.SandboxKind,
                artifact.Key.GetCompressedDataSize());

            auto sandboxPath = slot->GetSandboxPath(artifact.SandboxKind);
            auto symlinkPath =
                CombinePaths(sandboxPath, artifact.Name);

            WaitFor(slot->MakeLink(
                Context_.Job->GetId(),
                artifact.Name,
                artifact.SandboxKind,
                artifact.Chunk->GetFileName(),
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

void TJobWorkspaceBuilder::PrepareArtifactBinds()
{
    const auto& slot = Context_.Slot;

    YT_LOG_INFO(
        "Setting permissions for artifacts (ArtifactCount: %v)",
        std::size(Context_.Artifacts));

    std::vector<TFuture<void>> ioOperationFutures;
    ioOperationFutures.reserve(Context_.Artifacts.size());

    for (const auto& artifact : Context_.Artifacts) {
        if (!artifact.BypassArtifactCache && !artifact.CopyFile) {
            YT_VERIFY(artifact.Chunk);

            auto sandboxPath = slot->GetSandboxPath(artifact.SandboxKind);
            auto artifactPath = CombinePaths(sandboxPath, artifact.Name);

            YT_LOG_INFO(
                "Set permissions for artifact (FileName: %v, Executable: "
                "%v, SandboxKind: %v, CompressedDataSize: %v)",
                artifact.Name,
                artifact.Executable,
                artifact.SandboxKind,
                artifact.Key.GetCompressedDataSize());

            ioOperationFutures.push_back(slot->MakeSandboxBind(
                Context_.Job->GetId(),
                artifact.Name,
                artifact.SandboxKind,
                artifact.Chunk->GetFileName(),
                artifactPath,
                artifact.Executable));
        } else {
            YT_VERIFY(artifact.SandboxKind == ESandboxKind::User);
        }
    }

    WaitFor(AllSucceeded(ioOperationFutures))
        .ThrowOnError();
    YT_LOG_INFO("Permissions for artifacts set");
}

TFuture<TJobWorkspaceBuildingResult> TJobWorkspaceBuilder::Run()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto future = BIND(&TJobWorkspaceBuilder::DoPrepareSandboxDirectories, MakeStrong(this))
        .AsyncVia(Invoker_)
        .Run()
        .Apply(MakeStep<&TJobWorkspaceBuilder::DoPrepareRootVolume>())
        .Apply(MakeStep<&TJobWorkspaceBuilder::DoRunSetupCommand>())
        .Apply(MakeStep<&TJobWorkspaceBuilder::DoRunGpuCheckCommand>())
        .Apply(BIND([this, this_ = MakeStrong(this)] (const TError& result) -> TJobWorkspaceBuildingResult {
            YT_LOG_INFO(result, "Job workspace building finished");

            ResultHolder_.LastBuildError = result;
            return std::move(ResultHolder_);
        }).AsyncVia(Invoker_));

    future.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TJobWorkspaceBuildingResult>&) {
        // Drop reference to close race with check in TJob::Cleanup() on cancellation.
        Context_.Slot.Reset();
    }).Via(Invoker_));

    return future;
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
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_VERIFY(ResultHolder_.RootVolume);

        auto binds = Context_.Binds;

        for (const auto& bind : ResultHolder_.RootBinds) {
            binds.push_back(bind);
        }

        return TRootFS {
            .RootPath = ResultHolder_.RootVolume->GetPath(),
            .IsRootReadOnly = false,
            .Binds = std::move(binds),
        };
    }

    TFuture<void> DoPrepareSandboxDirectories() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::DownloadingArtifacts);
        SetJobPhase(EJobPhase::PreparingSandboxDirectories);

        YT_LOG_INFO("Started preparing sandbox directories");

        const auto& slot = Context_.Slot;

        ResultHolder_.TmpfsPaths = WaitFor(slot->PrepareSandboxDirectories(Context_.UserSandboxOptions))
            .ValueOrThrow();

        MakeArtifactSymlinks();

        YT_LOG_INFO("Finished preparing sandbox directories");

        return VoidFuture;
    }

    TFuture<void> DoPrepareRootVolume() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("Root volume preparation is not supported in simple workspace");

        ValidateJobPhase(EJobPhase::PreparingSandboxDirectories);
        SetJobPhase(EJobPhase::PreparingRootVolume);

        return VoidFuture;
    }

    TFuture<void> DoRunSetupCommand() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("Setup command is not supported in simple workspace");

        ValidateJobPhase(EJobPhase::PreparingRootVolume);
        SetJobPhase(EJobPhase::RunningSetupCommands);

        return VoidFuture;
    }

    TFuture<void> DoRunGpuCheckCommand() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG("GPU check is not supported in simple workspace");

        ValidateJobPhase(EJobPhase::RunningSetupCommands);
        SetJobPhase(EJobPhase::RunningGpuCheckCommand);

        return VoidFuture;
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
        IJobDirectoryManagerPtr directoryManager)
        : TJobWorkspaceBuilder(
            std::move(invoker),
            std::move(context),
            std::move(directoryManager))
    { }

private:
    TFuture<void> DoPrepareSandboxDirectories() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::DownloadingArtifacts);
        SetJobPhase(EJobPhase::PreparingSandboxDirectories);

        YT_LOG_INFO("Started preparing sandbox directories");

        const auto& slot = Context_.Slot;
        ResultHolder_.TmpfsPaths = WaitFor(slot->PrepareSandboxDirectories(Context_.UserSandboxOptions))
            .ValueOrThrow();

        if (!Context_.LayerArtifactKeys.empty()) {
            PrepareArtifactBinds();
        } else {
            MakeArtifactSymlinks();
        }

        YT_LOG_INFO("Finished preparing sandbox directories");

        return VoidFuture;
    }

    TRootFS MakeWritableRootFS()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_VERIFY(ResultHolder_.RootVolume);

        auto binds = Context_.Binds;

        for (const auto& bind : ResultHolder_.RootBinds) {
            binds.push_back(bind);
        }

        return TRootFS{
            .RootPath = ResultHolder_.RootVolume->GetPath(),
            .IsRootReadOnly = false,
            .Binds = binds
        };
    }

    TFuture<void> DoPrepareRootVolume() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingSandboxDirectories);
        SetJobPhase(EJobPhase::PreparingRootVolume);

        if (Context_.DockerImage) {
            return MakeFuture(TError(
                EErrorCode::DockerImagePullingFailed,
                "External docker image is not supported in Porto job environment"));
        }

        const auto& slot = Context_.Slot;
        const auto& layerArtifactKeys = Context_.LayerArtifactKeys;

        if (!layerArtifactKeys.empty()) {
            VolumePrepareStartTime_ = TInstant::Now();
            UpdateTimers_.Fire(MakeStrong(this));

            YT_LOG_INFO("Preparing root volume (LayerCount: %v)", layerArtifactKeys.size());

            for (const auto& layer : layerArtifactKeys) {
                i64 layerSize = layer.GetCompressedDataSize();
                UpdateArtifactStatistics(layerSize, slot->IsLayerCached(layer));
            }

            return slot->PrepareRootVolume(
                layerArtifactKeys,
                Context_.ArtifactDownloadOptions,
                Context_.UserSandboxOptions)
                .Apply(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<IVolumePtr>& volumeOrError) {
                    if (!volumeOrError.IsOK()) {
                        YT_LOG_WARNING(volumeOrError, "Failed to prepare root volume");

                        THROW_ERROR_EXCEPTION(EErrorCode::RootVolumePreparationFailed, "Failed to prepare root volume")
                            << volumeOrError;
                    }

                    YT_LOG_DEBUG("Root volume prepared");

                    VolumePrepareFinishTime_ = TInstant::Now();
                    UpdateTimers_.Fire(MakeStrong(this));
                    ResultHolder_.RootVolume = volumeOrError.Value();
                }));
        } else {
            YT_LOG_DEBUG("Root volume preparation is not needed");
            return VoidFuture;
        }
    }

    TFuture<void> DoRunSetupCommand() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingRootVolume);
        SetJobPhase(EJobPhase::RunningSetupCommands);

        if (Context_.LayerArtifactKeys.empty()) {
            return VoidFuture;
        }

        const auto &slot = Context_.Slot;

        const auto& commands = Context_.SetupCommands;
        ResultHolder_.SetupCommandCount = commands.size();

        if (commands.empty()) {
            YT_LOG_DEBUG("No setup command is needed");

            return VoidFuture;
        }

        YT_LOG_INFO("Running setup commands");

        return slot->RunSetupCommands(
            Context_.Job->GetId(),
            commands,
            MakeWritableRootFS(),
            Context_.CommandUser,
            /*devices*/ std::nullopt,
            /*startIndex*/ 0);
    }

    TFuture<void> DoRunGpuCheckCommand() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::RunningSetupCommands);
        SetJobPhase(EJobPhase::RunningGpuCheckCommand);

        if (Context_.NeedGpuCheck) {
            TJobGpuCheckerContext settings {
                .Slot = Context_.Slot,
                .Job = Context_.Job,
                .RootFS = MakeWritableRootFS(),
                .CommandUser = Context_.CommandUser,

                .GpuCheckBinaryPath = *Context_.GpuCheckBinaryPath,
                .GpuCheckBinaryArgs = *Context_.GpuCheckBinaryArgs,
                .GpuCheckType = Context_.GpuCheckType,
                .CurrentStartIndex = ResultHolder_.SetupCommandCount,
                // It is preliminary (not extra) GPU check.
                .TestExtraGpuCheckCommandFailure = false,
                .GpuDevices = Context_.GpuDevices
            };

            auto checker = New<TJobGpuChecker>(std::move(settings), Logger);

            checker->SubscribeRunCheck(BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] () {
                GpuCheckStartTime_ = TInstant::Now();
                UpdateTimers_.Fire(MakeStrong(this));
            }));

            checker->SubscribeFinishCheck(BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] () {
                GpuCheckFinishTime_ = TInstant::Now();
                UpdateTimers_.Fire(MakeStrong(this));
            }));

            YT_LOG_INFO("Starting preliminary GPU check");

            return BIND(&TJobGpuChecker::RunGpuCheck, std::move(checker))
                .AsyncVia(Invoker_)
                .Run()
                .Apply(BIND([this, this_ = MakeStrong(this)] (const TError& result) {
                    ValidateJobPhase(EJobPhase::RunningGpuCheckCommand);
                    if (!result.IsOK()) {
                        YT_LOG_WARNING("Preliminary GPU check command failed");

                        auto checkError = TError(EErrorCode::GpuCheckCommandFailed, "GPU check command failed")
                            << result;
                        THROW_ERROR checkError;
                    }

                    YT_LOG_INFO("Preliminary GPU check command finished");
                }).AsyncVia(Invoker_));
        } else {
            YT_LOG_INFO("No preliminary GPU check is needed");

            return VoidFuture;
        }
    }
};

TJobWorkspaceBuilderPtr CreatePortoJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager)
{
    return New<TPortoJobWorkspaceBuilder>(
        std::move(invoker),
        std::move(context),
        std::move(directoryManager));
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
        ICriExecutorPtr executor)
        : TJobWorkspaceBuilder(
            std::move(invoker),
            std::move(context),
            std::move(directoryManager))
        , Executor_(std::move(executor))
    { }

private:
    TFuture<void> DoPrepareSandboxDirectories() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::DownloadingArtifacts);
        SetJobPhase(EJobPhase::PreparingSandboxDirectories);

        YT_LOG_INFO("Started preparing sandbox directories");

        const auto& slot = Context_.Slot;

        ResultHolder_.TmpfsPaths = WaitFor(slot->PrepareSandboxDirectories(Context_.UserSandboxOptions))
            .ValueOrThrow();

        PrepareArtifactBinds();

        YT_LOG_INFO("Finished preparing sandbox directories");

        return VoidFuture;
    }

    TFuture<void> DoPrepareRootVolume() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingSandboxDirectories);
        SetJobPhase(EJobPhase::PreparingRootVolume);

        if (!Context_.LayerArtifactKeys.empty()) {
            return MakeFuture(TError(
                EErrorCode::LayerUnpackingFailed,
                "Porto layers are not supported in CRI job environment"));
        }

        if (const auto& dockerImage = Context_.DockerImage) {
            VolumePrepareStartTime_ = TInstant::Now();
            UpdateTimers_.Fire(MakeStrong(this));

            TCriImageDescriptor imageDescriptor {
                .Image = *dockerImage,
            };

            YT_LOG_INFO("Preparing root volume (Image: %v)", imageDescriptor);

            return Executor_->PullImage(imageDescriptor, /*always*/ false, Context_.DockerAuth)
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TCriImageDescriptor>& imageOrError) {
                    if (!imageOrError.IsOK()) {
                        YT_LOG_WARNING(imageOrError, "Failed to prepare root volume (Image: %v)", imageDescriptor);

                        THROW_ERROR_EXCEPTION(EErrorCode::DockerImagePullingFailed, "Failed to pull docker image")
                            << TErrorAttribute("docker_image", *dockerImage)
                            << imageOrError;
                    }

                    // TODO(khlebnikov) Result image may differ from requested?
                    const auto& imageResult = imageOrError.Value();
                    YT_LOG_INFO("Root volume prepared (Image: %v)", imageResult);

                    ResultHolder_.DockerImage = imageResult.Image;
                    VolumePrepareFinishTime_ = TInstant::Now();
                    UpdateTimers_.Fire(MakeStrong(this));
                }));
        } else {
            YT_LOG_DEBUG("Root volume preparation is not needed");
            return VoidFuture;
        }
    }

    TFuture<void> DoRunSetupCommand() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG_UNLESS(Context_.SetupCommands.empty(), "Setup command is not supported in CRI workspace");

        ValidateJobPhase(EJobPhase::PreparingRootVolume);
        SetJobPhase(EJobPhase::RunningSetupCommands);

        return VoidFuture;
    }

    TFuture<void> DoRunGpuCheckCommand() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_DEBUG_IF(Context_.NeedGpuCheck, "GPU check is not supported in CRI workspace");

        ValidateJobPhase(EJobPhase::RunningSetupCommands);
        SetJobPhase(EJobPhase::RunningGpuCheckCommand);

        return VoidFuture;
    }

private:
    const ICriExecutorPtr Executor_;
};

////////////////////////////////////////////////////////////////////////////////

TJobWorkspaceBuilderPtr CreateCriJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context,
    IJobDirectoryManagerPtr directoryManager,
    ICriExecutorPtr executor)
{
    return New<TCriJobWorkspaceBuilder>(
        std::move(invoker),
        std::move(context),
        std::move(directoryManager),
        std::move(executor));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
