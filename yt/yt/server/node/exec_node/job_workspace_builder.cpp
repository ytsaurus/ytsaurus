#include "job_workspace_builder.h"

#include "job_fs_secretary.h"
#include "slot.h"
#include "volume.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/context_switch.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/fs.h>

#include <library/cpp/yt/string/enum.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NContainers;
using namespace NJobAgent;
using namespace NFS;

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
    YT_VERIFY(Context_.FSSecretary);
    YT_VERIFY(Context_.Job);
    YT_VERIFY(DirectoryManager_);
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

        YT_TLOG_DEBUG("Unexpected job phase during workspace preparation")
            .With("Actual", jobPhase)
            .With("Expected", expectedPhase);

        THROW_ERROR_EXCEPTION("Unexpected job phase")
            .With("expected_phase", expectedPhase)
            .With("actual_phase", jobPhase);
    }
}

void TJobWorkspaceBuilder::SetJobPhase(EJobPhase phase)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    UpdateBuilderPhase_.Fire(phase);
}

TFuture<void> TJobWorkspaceBuilder::DoBuildSlotRootDirectory()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    ValidateJobPhase(EJobPhase::PreparingLayers);
    SetJobPhase(EJobPhase::PreparingSlotDirectories);

    return Context_.Slot->BuildSlotRootDirectory();
}

TFuture<void> TJobWorkspaceBuilder::DoPrepareGpuCheckVolume()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return OKFuture;
}

TFuture<void> TJobWorkspaceBuilder::DoApplyNetworkPriority()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    return OKFuture;
}

void TJobWorkspaceBuilder::MakeArtifactSymlinks()
{
    const auto& slot = Context_.Slot;

    YT_TLOG_INFO("Making artifact symlinks")
        .With("ArtifactCount", std::size(Context_.FSSecretary->GetArtifactDescriptors()));

    for (const auto& artifact : Context_.FSSecretary->GetArtifactDescriptors()) {
        // Artifact is passed into the job via symlink.
        if (!artifact.BypassArtifactCache && !artifact.CopyFile) {
            const auto& preparedArtifact = Context_.FSSecretary->GetArtifactByName(artifact.Name);

            YT_TLOG_INFO("Making symlink for artifact")
                .With("FileName", artifact.Name)
                .With("Executable", artifact.Executable)
                .With("SandboxKind", artifact.SandboxKind)
                .With("CompressedDataSize", artifact.Key.GetCompressedDataSize());

            auto sandboxPath = slot->GetSandboxPath(artifact.SandboxKind, ResultHolder_.RootVolume);
            auto symlinkPath = CombinePaths(sandboxPath, artifact.Name);

            WaitFor(slot->MakeLink(
                Context_.Job->GetId(),
                artifact.Name,
                artifact.SandboxKind,
                preparedArtifact->GetFileName(),
                symlinkPath,
                artifact.Executable))
                .ThrowOnError();

            YT_TLOG_INFO("Symlink for artifact is successfully made")
                .With("FileName", artifact.Name)
                .With("Executable", artifact.Executable)
                .With("SandboxKind", artifact.SandboxKind)
                .With("CompressedDataSize", artifact.Key.GetCompressedDataSize());
        } else {
            YT_VERIFY(artifact.SandboxKind == ESandboxKind::User);
        }
    }

    YT_TLOG_INFO("Artifact symlinks are made");
}

void TJobWorkspaceBuilder::MakeFilesForArtifactBinds()
{
    const auto& slot = Context_.Slot;

    const auto& artifacts = Context_.FSSecretary->GetArtifactDescriptors();

    YT_TLOG_INFO("Setting permissions for artifacts")
        .With("ArtifactCount", std::size(artifacts));

    std::vector<TFuture<void>> ioOperationFutures;
    ioOperationFutures.reserve(size(artifacts));

    for (const auto& artifact : artifacts) {
        if (artifact.AccessedViaBind) {
            const auto& preparedArtifact = Context_.FSSecretary->GetArtifactByName(artifact.Name);

            auto sandboxPath = slot->GetSandboxPath(artifact.SandboxKind, ResultHolder_.RootVolume);
            auto artifactPath = CombinePaths(sandboxPath, artifact.Name);

            YT_TLOG_INFO("Set permissions for artifact")
                .With("FileName", artifact.Name)
                .With("Executable", artifact.Executable)
                .With("SandboxKind", artifact.SandboxKind)
                .With("CompressedDataSize", artifact.Key.GetCompressedDataSize());

            ioOperationFutures.push_back(slot->MakeFileForSandboxBind(
                Context_.Job->GetId(),
                artifact.Name,
                artifact.SandboxKind,
                preparedArtifact->GetFileName(),
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

    YT_TLOG_INFO("Permissions for artifacts set");
}

void TJobWorkspaceBuilder::SetNowTime(std::optional<TInstant>& timeField)
{
    timeField = TInstant::Now();
    UpdateTimePoints_.Fire(TimePoints_);
}

TFuture<void> TJobWorkspaceBuilder::DoValidateRootFS()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    ValidateJobPhase(EJobPhase::LinkingVolumes);
    SetJobPhase(EJobPhase::ValidatingRootFS);

    if (!ResultHolder_.RootVolume) {
        return OKFuture;
    }

    SetNowTime(TimePoints_.ValidateRootFSStartTime);
    auto slot = Context_.Slot;
    return slot->ValidateRootFS(ResultHolder_.RootVolume)
        .Apply(BIND([this, this_ = MakeStrong(this)] () {
            SetNowTime(TimePoints_.ValidateRootFSFinishTime);
        })
        .AsyncVia(Invoker_));
}

TFuture<void> TJobWorkspaceBuilder::GuardedAction(EStep step)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto jobPhase = Context_.Job->GetPhase();

    switch (jobPhase) {
        case EJobPhase::WaitingForCleanup:
        case EJobPhase::Cleanup:
        case EJobPhase::Finished:
            YT_TLOG_DEBUG("Skip workspace building action")
                .With("JobPhase", jobPhase)
                .With("ActionName", FormatEnum(step));
            return OKFuture;

        case EJobPhase::Created:
            YT_VERIFY(Context_.Job->GetState() == EJobState::Waiting);
            break;

        default:
            YT_VERIFY(Context_.Job->GetState() == EJobState::Running);
            break;
    }

    TForbidContextSwitchGuard contextSwitchGuard;

    YT_TLOG_DEBUG("Run guarded workspace building action")
        .With("JobPhase", jobPhase)
        .With("ActionName", FormatEnum(step));

    switch (step) {
        case EStep::PrepareLayers:
            return DoPrepareLayers();
        case EStep::BuildSlotRootDirectory:
            return DoBuildSlotRootDirectory();
        case EStep::PrepareRootVolume:
            return DoPrepareRootVolume();
        case EStep::PrepareNonRootVolumes:
            return DoPrepareNonRootVolumes();
        case EStep::PrepareGpuCheckVolume:
            return DoPrepareGpuCheckVolume();
        case EStep::BindRootVolume:
            return DoBindRootVolume();
        case EStep::LinkVolumes:
            return DoLinkVolumes();
        case EStep::ValidateRootFS:
            return DoValidateRootFS();
        case EStep::PrepareSandboxDirectories:
            return DoPrepareSandboxDirectories();
        case EStep::RunSetupCommand:
            return DoRunSetupCommand();
        case EStep::RunCustomPreparations:
            return DoRunCustomPreparations();
        case EStep::ApplyNetworkPriority:
            return DoApplyNetworkPriority();
        case EStep::RunGpuCheckCommand:
            return DoRunGpuCheckCommand();
    }
    YT_ABORT();
}

TFuture<void> TJobWorkspaceBuilder::Run()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto future = OKFuture
        .Apply(MakeStep<EStep::PrepareLayers>())
        .Apply(MakeStep<EStep::BuildSlotRootDirectory>())
        .Apply(MakeStep<EStep::PrepareRootVolume>())
        .Apply(MakeStep<EStep::PrepareNonRootVolumes>())
        .Apply(MakeStep<EStep::BindRootVolume>())
        .Apply(MakeStep<EStep::LinkVolumes>())
        .Apply(MakeStep<EStep::ValidateRootFS>())
        .Apply(MakeStep<EStep::PrepareSandboxDirectories>())
        .Apply(MakeStep<EStep::RunSetupCommand>())
        .Apply(MakeStep<EStep::RunCustomPreparations>())
        .Apply(MakeStep<EStep::RunGpuCheckCommand>());

    return FinishRun(std::move(future));
}

TFuture<void> TJobWorkspaceBuilder::FinishRun(TFuture<void> future)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    future = future.Apply(BIND([this, this_ = MakeStrong(this)] (const TError& result) {
        YT_TLOG_INFO("Job workspace building finished")
            .With(result);

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
        YT_TLOG_FATAL("Result has already been extracted");
    }

    // It is expected that a situation where volumes are not linked will be triggered only when canceling job_workspace_builder.
    // The return of non-linked volumes is necessary in order to delete them correctly and set "disable" if an error occurs.
    if (ResultHolder_.PreparedNonRootVolumes.empty()) {
        ResultHolder_.PreparedNonRootVolumes = std::move(Context_.PreparedNonRootVolumes);
    }

    return std::move(ResultHolder_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
