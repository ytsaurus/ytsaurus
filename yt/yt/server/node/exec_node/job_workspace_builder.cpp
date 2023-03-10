#include "job_workspace_builder.h"

#include "job_gpu_checker.h"
#include "job_directory_manager.h"

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

namespace NYT::NExecNode
{

using namespace NConcurrency;
using namespace NContainers;
using namespace NJobAgent;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;
static const TString MountSuffix = "mount";

////////////////////////////////////////////////////////////////////////////////

TJobWorkspaceBuilder::TJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildSettings settings,
    IJobDirectoryManagerPtr directoryManager)
    : Invoker_(std::move(invoker))
    , Settings_(std::move(settings))
    , DirectoryManager_(std::move(directoryManager))
{
    YT_VERIFY(Settings_.Slot);
    YT_VERIFY(Settings_.Job);
    YT_VERIFY(DirectoryManager_);

    if (Settings_.NeedGpuCheck) {
        YT_VERIFY(Settings_.GpuCheckBinaryPath);
        YT_VERIFY(Settings_.GpuCheckBinaryArgs);
    }
}

TFuture<void> TJobWorkspaceBuilder::GuardedAction(const std::function<TFuture<void>()>& action)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    switch (Settings_.Job->GetPhase()) {
        case EJobPhase::WaitingAbort:
        case EJobPhase::Cleanup:
        case EJobPhase::Finished:
            return VoidFuture;

        case EJobPhase::Created:
            YT_VERIFY(Settings_.Job->GetState() == EJobState::Waiting);
            break;

        default:
            YT_VERIFY(Settings_.Job->GetState() == EJobState::Running);
            break;
    }

    TForbidContextSwitchGuard contextSwitchGuard;
    return action();
}

void TJobWorkspaceBuilder::ValidateJobPhase(EJobPhase expectedPhase) const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto jobPhase = Settings_.Job->GetPhase();
    if (jobPhase != expectedPhase) {
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

TFuture<void> TJobWorkspaceBuilder::PrepareSandboxDirectories()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return DoPrepareSandboxDirectories();
}

TFuture<void> TJobWorkspaceBuilder::PrepareRootVolume()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return GuardedAction([=, this, this_ = MakeStrong(this)] () {
        return DoPrepareRootVolume();
    });
}

TFuture<void> TJobWorkspaceBuilder::RunSetupCommand()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return GuardedAction([=, this, this_ = MakeStrong(this)] () {
        return DoRunSetupCommand();
    });
}

TFuture<void> TJobWorkspaceBuilder::RunGpuCheckCommand()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return GuardedAction([=, this, this_ = MakeStrong(this)] () {
        return DoRunGpuCheckCommand();
    });
}

TFuture<TJobWorkspaceBuildResult> TJobWorkspaceBuilder::Run()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return BIND(&TJobWorkspaceBuilder::PrepareSandboxDirectories, MakeStrong(this))
        .AsyncVia(Invoker_)
        .Run()
        .Apply(BIND(&TJobWorkspaceBuilder::PrepareRootVolume, MakeStrong(this))
            .AsyncVia(Invoker_))
        .Apply(BIND(&TJobWorkspaceBuilder::RunSetupCommand, MakeStrong(this))
            .AsyncVia(Invoker_))
        .Apply(BIND(&TJobWorkspaceBuilder::RunGpuCheckCommand, MakeStrong(this))
            .AsyncVia(Invoker_))
        .Apply(BIND([=, this, this_ = MakeStrong(this)] () {
            return ResultHolder_;
        }).AsyncVia(Invoker_));
}

TRootFS TJobWorkspaceBuilder::MakeWritableRootFS()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(ResultHolder_.RootVolume);

    std::vector<TBind> binds = Settings_.Binds;

    for (const auto& bind : ResultHolder_.RootBinds) {
        binds.push_back(bind);
    }

    return TRootFS {
        .RootPath = ResultHolder_.RootVolume->GetPath(),
        .IsRootReadOnly = false,
        .Binds = binds
    };
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobWorkspaceBuilder
    : public TJobWorkspaceBuilder
{
public:
    TSimpleJobWorkspaceBuilder(
        IInvokerPtr invoker,
        TJobWorkspaceBuildSettings settings,
        IJobDirectoryManagerPtr directoryManager)
        : TJobWorkspaceBuilder(
            std::move(invoker),
            std::move(settings),
            std::move(directoryManager))
    { }

private:

    void MakeArtifactSymlinks()
    {
        auto slot = Settings_.Slot;

        for (const auto& artifact : Settings_.Artifacts) {
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
                    NFS::CombinePaths(sandboxPath, artifact.Name);

                WaitFor(slot->MakeLink(
                    Settings_.Job->GetId(),
                    artifact.Name,
                    artifact.SandboxKind,
                    artifact.Chunk->GetFileName(),
                    symlinkPath,
                    artifact.Executable))
                    .ThrowOnError();
            } else {
                YT_VERIFY(artifact.SandboxKind == ESandboxKind::User);
            }
        }
    }

    TFuture<void> DoPrepareSandboxDirectories()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_INFO("Started preparing sandbox directories");

        auto slot = Settings_.Slot;

        ResultHolder_.TmpfsPaths = WaitFor(slot->PrepareSandboxDirectories(Settings_.UserSandboxOptions))
            .ValueOrThrow();

        MakeArtifactSymlinks();

        YT_LOG_INFO("Finished preparing sandbox directories");

        return VoidFuture;
    }

    TFuture<void> DoPrepareRootVolume()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingSandboxDirectories);
        SetJobPhase(EJobPhase::PreparingRootVolume);

        VolumePrepareStartTime_ = TInstant::Now();
        UpdateTimers_.Fire(MakeStrong(this));

        if (Settings_.UserSandboxOptions.EnableDiskQuota &&
            Settings_.UserSandboxOptions.HasRootFsQuota &&
            (Settings_.UserSandboxOptions.DiskSpaceLimit || Settings_.UserSandboxOptions.InodeLimit)) {
            return DirectoryManager_->ApplyQuota(
                Settings_.Slot->GetSlotPath(),
                TJobDirectoryProperties{
                    .DiskSpaceLimit = Settings_.UserSandboxOptions.DiskSpaceLimit,
                    .InodeLimit = Settings_.UserSandboxOptions.InodeLimit,
                    .UserId = Settings_.UserSandboxOptions.UserId
                })
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<void>& volumeOrError) {
                    if (!volumeOrError.IsOK()) {
                        THROW_ERROR_EXCEPTION(TError(EErrorCode::RootVolumePreparationFailed, "Failed to set quotas")
                            << volumeOrError);
                    }

                    VolumePrepareFinishTime_ = TInstant::Now();
                    UpdateTimers_.Fire(MakeStrong(this));
                }));
        } else {
            return VoidFuture;
        }
    }

    TFuture<void> DoRunSetupCommand()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingRootVolume);
        SetJobPhase(EJobPhase::RunningSetupCommands);

        return VoidFuture;
    }

    TFuture<void> DoRunGpuCheckCommand()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::RunningSetupCommands);
        SetJobPhase(EJobPhase::RunningGpuCheckCommand);

        return VoidFuture;
    }
};

////////////////////////////////////////////////////////////////////////////////

TJobWorkspaceBuilderPtr CreateSimpleJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildSettings settings,
    IJobDirectoryManagerPtr directoryManager)
{
    return New<TSimpleJobWorkspaceBuilder>(
        std::move(invoker),
        std::move(settings),
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
        TJobWorkspaceBuildSettings settings,
        IJobDirectoryManagerPtr directoryManager)
        : TJobWorkspaceBuilder(
            std::move(invoker),
            std::move(settings),
            std::move(directoryManager))
    { }

private:

    void MakeArtifactSymlinks()
    {
        auto slot = Settings_.Slot;

        for (const auto& artifact : Settings_.Artifacts) {
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
                    NFS::CombinePaths(sandboxPath, artifact.Name);

                WaitFor(slot->MakeLink(
                    Settings_.Job->GetId(),
                    artifact.Name,
                    artifact.SandboxKind,
                    artifact.Chunk->GetFileName(),
                    symlinkPath,
                    artifact.Executable))
                    .ThrowOnError();
            } else {
                YT_VERIFY(artifact.SandboxKind == ESandboxKind::User);
            }
        }
    }

    TFuture<void> DoPrepareSandboxDirectories()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_INFO("Started preparing sandbox directories");

        auto slot = Settings_.Slot;

        ResultHolder_.TmpfsPaths = WaitFor(slot->PrepareSandboxDirectories(Settings_.UserSandboxOptions))
            .ValueOrThrow();

        MakeArtifactSymlinks();

        YT_LOG_INFO("Finished preparing sandbox directories");

        return VoidFuture;
    }

    TFuture<void> DoPrepareRootVolume()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingSandboxDirectories);
        SetJobPhase(EJobPhase::PreparingRootVolume);

        auto slot = Settings_.Slot;
        auto layerArtifactKeys = Settings_.LayerArtifactKeys;

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
                Settings_.ArtifactDownloadOptions,
                Settings_.UserSandboxOptions)
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<IVolumePtr>& volumeOrError) {
                    if (!volumeOrError.IsOK()) {
                        THROW_ERROR_EXCEPTION(TError(EErrorCode::RootVolumePreparationFailed, "Failed to prepare artifacts")
                            << volumeOrError);
                    }

                    VolumePrepareFinishTime_ = TInstant::Now();
                    UpdateTimers_.Fire(MakeStrong(this));
                    ResultHolder_.RootVolume = volumeOrError.Value();
                }));
        } else if (Settings_.UserSandboxOptions.EnableDiskQuota &&
            Settings_.UserSandboxOptions.HasRootFsQuota &&
            (Settings_.UserSandboxOptions.DiskSpaceLimit || Settings_.UserSandboxOptions.InodeLimit)) {

            VolumePrepareStartTime_ = TInstant::Now();
            UpdateTimers_.Fire(MakeStrong(this));

            return DirectoryManager_->ApplyQuota(
                Settings_.Slot->GetSlotPath(),
                TJobDirectoryProperties{
                    .DiskSpaceLimit = Settings_.UserSandboxOptions.DiskSpaceLimit,
                    .InodeLimit = Settings_.UserSandboxOptions.InodeLimit,
                    .UserId = Settings_.UserSandboxOptions.UserId
                })
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<void>& volumeOrError) {
                    if (!volumeOrError.IsOK()) {
                        THROW_ERROR_EXCEPTION(TError(EErrorCode::RootVolumePreparationFailed, "Failed to set quotas")
                            << volumeOrError);
                    }

                    VolumePrepareFinishTime_ = TInstant::Now();
                    UpdateTimers_.Fire(MakeStrong(this));
                }));
        } else {
            return VoidFuture;
        }
    }

    TFuture<void> DoRunSetupCommand()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingRootVolume);
        SetJobPhase(EJobPhase::RunningSetupCommands);

        if (Settings_.LayerArtifactKeys.empty()) {
            return VoidFuture;
        }

        auto slot = Settings_.Slot;

        auto commands = Settings_.SetupCommands;
        ResultHolder_.SetupCommandCount = commands.size();

        if (commands.empty()) {
            return VoidFuture;
        }

        YT_LOG_INFO("Running setup commands");

        return slot->RunSetupCommands(
            Settings_.Job->GetId(),
            commands,
            MakeWritableRootFS(),
            Settings_.CommandUser,
            /*devices*/ std::nullopt,
            /*startIndex*/ 0);
    }

    TFuture<void> DoRunGpuCheckCommand()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::RunningSetupCommands);
        SetJobPhase(EJobPhase::RunningGpuCheckCommand);

        if (Settings_.NeedGpuCheck) {
            TJobGpuCheckerSettings settings {
                .Slot = Settings_.Slot,
                .Job = Settings_.Job,
                .RootFs = MakeWritableRootFS(),
                .CommandUser = Settings_.CommandUser,

                .GpuCheckBinaryPath = *Settings_.GpuCheckBinaryPath,
                .GpuCheckBinaryArgs = *Settings_.GpuCheckBinaryArgs,
                .GpuCheckType = Settings_.GpuCheckType,
                .CurrentStartIndex = ResultHolder_.SetupCommandCount,
                .TestExtraGpuCheckCommandFailure = Settings_.TestExtraGpuCheckCommandFailure,
                .GpuDevices = Settings_.GpuDevices
            };

            auto checker = New<TJobGpuChecker>(std::move(settings));

            checker->SubscribeRunCheck(BIND_NO_PROPAGATE([=, this, this_ = MakeStrong(this)] () {
                GpuCheckStartTime_ = TInstant::Now();
                UpdateTimers_.Fire(MakeStrong(this));
            }));

            checker->SubscribeRunCheck(BIND_NO_PROPAGATE([=, this, this_ = MakeStrong(this)] () {
                GpuCheckFinishTime_ = TInstant::Now();
                UpdateTimers_.Fire(MakeStrong(this));
            }));

            return BIND(&TJobGpuChecker::RunGpuCheck, checker)
                .AsyncVia(Invoker_)
                .Run()
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<void>& result) {
                    ValidateJobPhase(EJobPhase::RunningGpuCheckCommand);
                    if (!result.IsOK()) {
                        auto checkError = TError(EErrorCode::GpuCheckCommandFailed, "Preliminary GPU check command failed")
                            << result;
                        THROW_ERROR checkError;
                    }
                }).AsyncVia(Invoker_));
        } else {
            return VoidFuture;
        }
    }
};

TJobWorkspaceBuilderPtr CreatePortoJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildSettings settings,
    IJobDirectoryManagerPtr directoryManager)
{
    return New<TPortoJobWorkspaceBuilder>(
        std::move(invoker),
        std::move(settings),
        std::move(directoryManager));
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
