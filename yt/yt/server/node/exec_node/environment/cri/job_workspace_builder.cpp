#include "job_workspace_builder.h"

#include <yt/yt/server/node/exec_node/environment/common/host_slot.h>

#include <yt/yt/server/node/exec_node/allocation.h>
#include <yt/yt/server/node/exec_node/job_fs_secretary.h>
#include <yt/yt/server/node/exec_node/slot.h>
#include <yt/yt/server/node/exec_node/volume.h>

#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/library/containers/cri/image_cache.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/fs.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NContainers;
using namespace NJobAgent;
using namespace NFS;
using namespace NContainers::NCri;

////////////////////////////////////////////////////////////////////////////////

static const std::string SetupCommandsTag = "setup";

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
    THostUserSlotPtr GetSlot() const
    {
        return StaticPointerCast<THostUserSlot>(Context_.Slot);
    }

    TFuture<void> DoPrepareLayers() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_TLOG_DEBUG("Layer preparation is not needed in CRI job environment");

        ValidateJobPhase(EJobPhase::CachingArtifacts);
        SetJobPhase(EJobPhase::PreparingLayers);

        return OKFuture;
    }

    TFuture<void> DoPrepareRootVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingSlotDirectories);
        SetJobPhase(EJobPhase::PreparingVolumes);

        const auto& dockerImage = Context_.FSSecretary->GetDockerImage();

        if (!dockerImage && Context_.FSSecretary->GetRootVolumeParams()) {
            return MakeFuture(TError(
                NExecNode::EErrorCode::LayerUnpackingFailed,
                "Layers are not supported in CRI job environment"));
        }

        if (dockerImage) {
            SetNowTime(TimePoints_.PrepareRootVolumeStartTime);

            TCriImageDescriptor imageDescriptor {
                .Image = *dockerImage,
            };

            YT_TLOG_INFO("Preparing root volume")
                .With("Image", imageDescriptor);

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
                        YT_TLOG_WARNING("Failed to prepare root volume")
                            .With("Image", imageDescriptor)
                            .With(imageOrError);

                        THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::DockerImagePullingFailed, "Failed to pull docker image")
                            .With("docker_image", imageDescriptor.Image)
                            .With("authenticated", authenticated)
                            .With(imageOrError);
                    }

                    const auto& cachedImage = imageOrError.Value()->Image();
                    YT_TLOG_INFO("Root volume prepared")
                        .With("Image", cachedImage);

                    ResultHolder_.DockerImage = cachedImage.Image;
                    ResultHolder_.DockerImageId = cachedImage.Id;

                    SetNowTime(TimePoints_.PrepareRootVolumeFinishTime);
                }));
        } else {
            YT_TLOG_DEBUG("Root volume preparation is not needed");
            return OKFuture;
        }
    }

    TFuture<void> DoPrepareNonRootVolumes() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        SetNowTime(TimePoints_.PrepareNonRootVolumesStartTime);

        const auto& volumes = Context_.FSSecretary->GetNonRootVolumesToPrepare();
        if (volumes.empty()) {
            SetNowTime(TimePoints_.PrepareNonRootVolumesFinishTime);
            return OKFuture;
        }

        auto slot = GetSlot();

        return slot->PrepareNonRootVolumes(
            Context_.Job->GetId(),
            ResultHolder_.RootVolume,
            volumes,
            Context_.UserSandboxOptions.JobVolumeMounts)
            .AsUnique()
            .Apply(BIND([
                jobId = Context_.Job->GetId(),
                slot,
                this,
                this_ = MakeStrong(this)
            ] (TErrorOr<std::vector<TVolumeResultPtr>>&& volumeResultsOrError) {
                if (!volumeResultsOrError.IsOK()) {
                    THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::NonRootVolumePreparationFailed, "Failed to prepare non-root volumes")
                        .With(volumeResultsOrError);
                }

                auto& volumeResults = volumeResultsOrError.Value();

                YT_TLOG_DEBUG("Prepared non-root volumes")
                    .With("Volumes", MakeFormattableView(
                        volumeResults,
                        [] (TNonNullPtr<TStringBuilderBase> builder, const TVolumeResultPtr& result) {
                            builder->AppendFormat("{VolumeId: %v, VolumePath: %v}",
                                result->VolumeId,
                                result->Volume->GetPath());
                        }));

                Context_.PreparedNonRootVolumes = std::move(volumeResults);

                SetNowTime(TimePoints_.PrepareNonRootVolumesFinishTime);
            }).AsyncVia(Invoker_));
    }

    TFuture<void> DoBindRootVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingVolumes);
        SetJobPhase(EJobPhase::LinkingVolumes);

        YT_TLOG_DEBUG("Root volume binding is not needed in cri workspace");

        return OKFuture;
    }

    TFuture<void> DoLinkVolumes() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::LinkingVolumes);

        YT_TLOG_DEBUG("Link volumes is not supported in cri workspace");

        ResultHolder_.PreparedNonRootVolumes = std::move(Context_.PreparedNonRootVolumes);

        return OKFuture;
    }

    TFuture<void> DoPrepareSandboxDirectories() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::ValidatingRootFS);
        SetJobPhase(EJobPhase::PreparingSandboxDirectories);

        YT_TLOG_INFO("Started preparing sandbox directories");

        return Context_.Slot->PrepareSandboxDirectories(Context_.UserSandboxOptions)
            .Apply(BIND([this, this_ = MakeStrong(this)] {
                MakeFilesForArtifactBinds();

                YT_TLOG_INFO("Finished preparing sandbox directories");
            }).AsyncVia(Invoker_));
    }

    TFuture<void> DoRunSetupCommand() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingSandboxDirectories);
        SetJobPhase(EJobPhase::RunningSetupCommands);

        if (Context_.SetupCommands.empty()) {
            YT_TLOG_DEBUG("No setup command is needed");
            return OKFuture;
        }

        YT_TLOG_INFO("Running setup commands");

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

        YT_TLOG_DEBUG("There are no custom preparations in CRI workspace");

        ValidateJobPhase(EJobPhase::RunningSetupCommands);
        SetJobPhase(EJobPhase::RunningCustomPreparations);

        return OKFuture;
    }

    TFuture<void> DoRunGpuCheckCommand() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_TLOG_DEBUG_IF(Context_.GpuCheckOptions, "GPU check is not supported in CRI workspace");

        ValidateJobPhase(EJobPhase::RunningCustomPreparations);

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
