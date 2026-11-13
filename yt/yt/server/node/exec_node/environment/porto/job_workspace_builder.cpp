#include "job_workspace_builder.h"

#include "job_fs_secretary.h"
#include "slot.h"

#include <yt/yt/server/node/exec_node/allocation.h>
#include <yt/yt/server/node/exec_node/job_fs_secretary.h>
#include <yt/yt/server/node/exec_node/job_gpu_checker.h>
#include <yt/yt/server/node/exec_node/slot.h>
#include <yt/yt/server/node/exec_node/volume.h>

#include <yt/yt/server/lib/exec_node/helpers.h>

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

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

static const std::string SetupCommandsTag = "setup";

////////////////////////////////////////////////////////////////////////////////

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

    TPortoJobFSSecretary& GetFSSecretary() const
    {
        return static_cast<TPortoJobFSSecretary&>(*Context_.FSSecretary);
    }

    TPortoUserSlotPtr GetSlot() const
    {
        return StaticPointerCast<TPortoUserSlot>(Context_.Slot);
    }

    TFuture<void> DoPrepareLayers() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::CachingArtifacts);
        SetJobPhase(EJobPhase::PreparingLayers);

        auto slot = GetSlot();
        const auto& fsSecretary = Context_.FSSecretary;
        auto rootVolumeParams = fsSecretary->GetRootVolumeParams();

        if (fsSecretary->GetDockerImage() && !rootVolumeParams) {
            return MakeFuture(TError(
                NExecNode::EErrorCode::DockerImagePullingFailed,
                "External docker image is not supported in Porto job environment"));
        }

        if (Context_.Job->HasUserJobSpec() &&
            (!rootVolumeParams ||
             rootVolumeParams->LayerArtifactKeys.GetAll().empty() ||
             !Context_.Job->HasRootVolumeLayers()))
        {
            return MakeFuture(TError(
                NExecNode::EErrorCode::UserJobWithoutLayers,
                "User job in Porto environment requires at least one root filesystem layer"));
        }

        THashSet<TArtifactKey> uniqueLayers;
        auto tryInsertLayerKey = [&] (const TArtifactKey& key) {
            if (!GetFSSecretary().HasPreparedLayer(key)) {
                uniqueLayers.insert(key);
            }
        };

        if (!fsSecretary->GetRootVolume()) {
            if (rootVolumeParams) {
                for (const auto& key : rootVolumeParams->LayerArtifactKeys.GetAll()) {
                    tryInsertLayerKey(key);
                }
            }
        }

        if (auto gpuVolumeParams = fsSecretary->GetGpuCheckVolumeParams(); gpuVolumeParams) {
            for (const auto& key : gpuVolumeParams->LayerArtifactKeys.GetAll()) {
                tryInsertLayerKey(key);
            }
        }

        for (const auto& params : fsSecretary->GetNonRootVolumesToPrepare()) {
            for (const auto& key : params->LayerArtifactKeys.GetAll()) {
                tryInsertLayerKey(key);
            }
        }

        const TVirtualSandboxOptions* virtualSandboxOptions = nullptr;
        if (!fsSecretary->GetRootVolume()) {
            if (const auto& data = fsSecretary->GetVirtualSandboxOptions();
                data && !GetFSSecretary().HasPreparedLayer(data->ArtifactKey))
            {
                virtualSandboxOptions = &*data;
            }
        }

        if (uniqueLayers.empty() && !virtualSandboxOptions) {
            YT_TLOG_DEBUG("Layer preparation is not needed");
            return OKFuture;
        }

        SetNowTime(TimePoints_.PrepareLayersStartTime);

        auto totalLayerCount = uniqueLayers.size() + (virtualSandboxOptions ? 1 : 0);
        YT_TLOG_INFO("Preparing layers")
            .With("LayerCount", totalLayerCount);

        std::vector<TOverlayLayerPreparationOptions> layerOptions;
        layerOptions.reserve(totalLayerCount);
        for (const auto& key : uniqueLayers) {
            layerOptions.push_back(TOverlayLayerPreparationOptions{
                .ArtifactKey = key,
            });
        }
        if (virtualSandboxOptions) {
            layerOptions.push_back(TOverlayLayerPreparationOptions{
                .ArtifactKey = virtualSandboxOptions->ArtifactKey,
                .ImageReader = virtualSandboxOptions->Reader,
            });
        }

        auto layerFutures = slot->PrepareLayers(
            Context_.Job->GetId(),
            layerOptions,
            Context_.ArtifactDownloadContext);
        YT_VERIFY(layerFutures.size() == layerOptions.size());

        return AllSucceeded(std::move(layerFutures))
            .ToImmediatelyCancelable()
            .AsUnique()
            .Apply(BIND([
                this,
                this_ = MakeStrong(this),
                layerOptions = std::move(layerOptions)
            ] (TErrorOr<std::vector<TOverlayData>>&& overlayDataArrayOrError) mutable {
                if (!overlayDataArrayOrError.IsOK()) {
                    YT_TLOG_WARNING("Failed to prepare overlay layers")
                        .With(overlayDataArrayOrError);

                    THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::OverlayLayerPreparationFailed, "Failed to prepare overlay layers")
                        .With(overlayDataArrayOrError);
                }

                auto& overlayDataArray = overlayDataArrayOrError.Value();
                YT_VERIFY(overlayDataArray.size() == layerOptions.size());
                TPreparedLayers preparedLayers;
                for (int i = 0; i < std::ssize(layerOptions); ++i) {
                    EmplaceOrCrash(
                        preparedLayers.ArtifactKeyToOverlayData,
                        std::move(layerOptions[i].ArtifactKey),
                        std::move(overlayDataArray[i]));
                }
                GetFSSecretary().AddPreparedLayers(std::move(preparedLayers));

                YT_TLOG_INFO("All layers prepared");
                SetNowTime(TimePoints_.PrepareLayersFinishTime);
            }).AsyncVia(Invoker_));
    }

    TFuture<void> DoPrepareRootVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingSlotDirectories);
        SetJobPhase(EJobPhase::PreparingVolumes);

        auto slot = GetSlot();
        auto rootVolumeParams = Context_.FSSecretary->GetRootVolumeParams();

        // Check if root volume can be reused from previous job in the allocation.
        if (const auto& existingRootVolume = Context_.FSSecretary->GetRootVolume()) {
            YT_VERIFY(Context_.FSSecretary->IsRootVolumeReusable());

            YT_TLOG_INFO("Reusing root volume from previous job")
                .With("VolumePath", existingRootVolume->GetPath());

            SetNowTime(TimePoints_.PrepareRootVolumeStartTime);
            ResultHolder_.RootVolume = existingRootVolume;
            SetNowTime(TimePoints_.PrepareRootVolumeFinishTime);
            return OKFuture;
        }

        if (rootVolumeParams) {
            SetNowTime(TimePoints_.PrepareRootVolumeStartTime);

            YT_TLOG_INFO("Preparing root volume")
                .With("LayerCount", rootVolumeParams->LayerArtifactKeys.GetAll().size())
                .With("HasVirtualSandbox", Context_.UserSandboxOptions.VirtualSandboxOptions.has_value());

            TVolumePreparationOptions options;
            options.JobId = Context_.Job->GetId();
            options.ArtifactDownloadContext = Context_.ArtifactDownloadContext;
            options.UserSandboxOptions = Context_.UserSandboxOptions;

            return slot->PrepareRootVolume(
                GetFSSecretary().GetPreparedRootVolumeOverlayData(),
                rootVolumeParams,
                options)
                    .Apply(
                        BIND([slot, this, this_ = MakeStrong(this)] (const TErrorOr<IVolumePtr>& volumeOrError) {
                            if (!volumeOrError.IsOK()) {
                                YT_TLOG_WARNING("Failed to prepare root volume")
                                    .With(volumeOrError);

                                THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::RootVolumePreparationFailed, "Failed to prepare root volume")
                                    .With(volumeOrError);
                            }

                            ResultHolder_.RootVolume = volumeOrError.Value();

                            return slot->CreateRootVolumeDirectories(
                                std::move(volumeOrError.Value()),
                                Context_.UserSandboxOptions.UserId)
                                    .Apply(
                                        BIND([this, this_ = MakeStrong(this)] {
                                            YT_TLOG_DEBUG("Root volume prepared");
                                            SetNowTime(TimePoints_.PrepareRootVolumeFinishTime);
                                        })
                                        .AsyncVia(Invoker_));

                        })
                        .AsyncVia(Invoker_));
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

        std::vector<std::vector<TOverlayData>> perVolumeOverlayData;
        perVolumeOverlayData.reserve(volumes.size());
        for (const auto& volume : volumes) {
            perVolumeOverlayData.push_back(GetFSSecretary().GetPreparedNonRootVolumeOverlayData(*volume));
        }

        return slot->PrepareNonRootVolumes(
            Context_.Job->GetId(),
            ResultHolder_.RootVolume,
            volumes,
            std::move(perVolumeOverlayData),
            Context_.UserSandboxOptions.JobVolumeMounts)
            .AsUnique()
            .Apply(
                BIND([slot, this, this_ = MakeStrong(this)] (TErrorOr<std::vector<TVolumeResultPtr>>&& volumeResultsOrError) {
                    if (!volumeResultsOrError.IsOK()) {
                        THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::NonRootVolumePreparationFailed, "Failed to prepare non-root volumes")
                            .With(volumeResultsOrError);
                    }

                    Context_.PreparedNonRootVolumes = std::move(volumeResultsOrError.Value());

                    YT_TLOG_DEBUG("Prepared non-root volumes")
                        .With("Volumes", MakeFormattableView(
                            Context_.PreparedNonRootVolumes,
                            [] (TNonNullPtr<TStringBuilderBase> builder, const TVolumeResultPtr& result) {
                                builder->AppendFormat("{VolumeId: %v, VolumePath: %v}",
                                    result->VolumeId,
                                    result->Volume->GetPath());
                            }));
                    SetNowTime(TimePoints_.PrepareNonRootVolumesFinishTime);
                })
                .AsyncVia(Invoker_))
            // TODO(krasovav): Remove it (YT-27698)
            .ToUncancelable();
    }

    TFuture<void> DoPrepareGpuCheckVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto slot = GetSlot();
        const auto gpuVolumeParams = Context_.FSSecretary->GetGpuCheckVolumeParams();

        if (gpuVolumeParams) {
            SetNowTime(TimePoints_.PrepareGpuCheckVolumeStartTime);

            YT_TLOG_INFO("Preparing GPU check volume")
                .With("Volume", gpuVolumeParams);

            TVolumePreparationOptions options;
            options.JobId = Context_.Job->GetId();
            options.ArtifactDownloadContext = Context_.ArtifactDownloadContext;

            return slot->PrepareGpuCheckVolume(
                GetFSSecretary().GetPreparedGpuCheckVolumeOverlayData(),
                gpuVolumeParams,
                options)
                .Apply(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<IVolumePtr>& volumeOrError) {
                    if (!volumeOrError.IsOK()) {
                        YT_TLOG_WARNING("Failed to prepare GPU check volume")
                            .With(volumeOrError);

                        THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::RootVolumePreparationFailed, "Failed to prepare GPU check volume")
                            .With(volumeOrError);
                    }

                    YT_TLOG_DEBUG("GPU check volume prepared");

                    ResultHolder_.GpuCheckVolume = volumeOrError.Value();

                    SetNowTime(TimePoints_.PrepareGpuCheckVolumeFinishTime);
                }).AsyncVia(Invoker_));
        } else {
            YT_TLOG_DEBUG("GPU check volume preparation is not needed");
            return OKFuture;
        }

        return OKFuture;
    }

    TFuture<void> DoBindRootVolume() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::PreparingVolumes);
        SetJobPhase(EJobPhase::LinkingVolumes);

        auto slot = GetSlot();
        if (ResultHolder_.RootVolume && !Context_.UserSandboxOptions.DisableRbindRootVolume) {
            return slot->RbindRootVolume(ResultHolder_.RootVolume)
                .Apply(BIND(
                    [
                        this,
                        this_ = MakeStrong(this)
                    ] (const TErrorOr<IVolumePtr>& volumeOrError) {
                        if (!volumeOrError.IsOK()) {
                            YT_TLOG_WARNING("Failed to prepare root volume")
                                .With(volumeOrError);

                            THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::RootVolumePreparationFailed, "Failed to prepare root volume")
                                .With(volumeOrError);
                        }

                        ResultHolder_.RootVolume = std::move(volumeOrError.Value());
                    })
                    .AsyncVia(Invoker_))
                .ToImmediatelyCancelable();
        } else {
            YT_TLOG_DEBUG("Root volume binding is not needed");
        }

        return OKFuture;
    }

    TFuture<void> DoLinkVolumes() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::LinkingVolumes);

        SetNowTime(TimePoints_.LinkVolumesStartTime);

        // Combine newly prepared volumes with reused volumes.
        std::vector<TVolumeResultPtr> allVolumes;
        allVolumes.reserve(Context_.PreparedNonRootVolumes.size() + Context_.ReusedNonRootVolumes.size());
        allVolumes.insert(allVolumes.end(), Context_.PreparedNonRootVolumes.begin(), Context_.PreparedNonRootVolumes.end());
        allVolumes.insert(allVolumes.end(), Context_.ReusedNonRootVolumes.begin(), Context_.ReusedNonRootVolumes.end());

        auto slot = GetSlot();
        return slot->LinkVolumes(ResultHolder_.RootVolume, allVolumes, Context_.UserSandboxOptions.JobVolumeMounts)
            .Apply(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<void>& error) mutable {
                if (!error.IsOK()) {
                    THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::NonRootVolumeLinkingFailed, "Failed to link non-root volumes")
                        .With(error);
                }

                YT_TLOG_DEBUG("Linked non-root volumes")
                    .With("PreparedVolumes", MakeFormattableView(
                        Context_.PreparedNonRootVolumes,
                        [] (TNonNullPtr<TStringBuilderBase> builder, const TVolumeResultPtr& result) {
                            builder->AppendFormat(
                                "{VolumeId: %v, VolumePath: %v}",
                                result->VolumeId,
                                result->Volume->GetPath());
                        }))
                    .With("ReusedVolumes", MakeFormattableView(
                        Context_.ReusedNonRootVolumes,
                        [] (TNonNullPtr<TStringBuilderBase> builder, const TVolumeResultPtr& result) {
                            builder->AppendFormat(
                                "{VolumeId: %v, VolumePath: %v}",
                                result->VolumeId,
                                result->Volume->GetPath());
                        }));

                // Only pass newly prepared volumes to ResultHolder_.
                // Reused volumes are already in FSSecretary's NonRootVolumes_ map.
                ResultHolder_.PreparedNonRootVolumes = std::move(Context_.PreparedNonRootVolumes);
                Context_.ReusedNonRootVolumes.clear();

                SetNowTime(TimePoints_.LinkVolumesFinishTime);
            }).AsyncVia(Invoker_))
            .ToUncancelable();
    }

    TFuture<void> DoPrepareSandboxDirectories() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        ValidateJobPhase(EJobPhase::ValidatingRootFS);
        SetJobPhase(EJobPhase::PreparingSandboxDirectories);

        YT_TLOG_INFO("Started preparing sandbox directories");

        return Context_.Slot->PrepareSandboxDirectories(Context_.UserSandboxOptions)
            .Apply(BIND([this, this_ = MakeStrong(this)] {
                if (ResultHolder_.RootVolume) {
                    MakeFilesForArtifactBinds();
                } else {
                    MakeArtifactSymlinks();
                }

                YT_TLOG_INFO("Finished preparing sandbox directories");
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
            .RootPath = ResultHolder_.RootVolume->GetPath(),
            .IsRootReadOnly = false,
            .Binds = std::move(binds),
        };
    }

    TRootFS MakeWritableGpuCheckRootFS()
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        YT_VERIFY(ResultHolder_.GpuCheckVolume);

        std::vector<NContainers::TBind> binds;
        for (const auto& path : GpuManager_->GetRequiredHostPaths()) {
            binds.push_back(NContainers::TBind{
                .SourcePath = path,
                .TargetPath = path,
                .ReadOnly = true,
            });
        }

        return TRootFS{
            .RootPath = ResultHolder_.GpuCheckVolume->GetPath(),
            .IsRootReadOnly = false,
            .Binds = std::move(binds),
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

        auto slot = GetSlot();

        const auto& commands = Context_.SetupCommands;
        ResultHolder_.SetupCommandCount = commands.size();

        if (commands.empty()) {
            YT_TLOG_DEBUG("No setup command is needed");

            return OKFuture;
        }

        YT_TLOG_INFO("Running setup commands");

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

        YT_TLOG_DEBUG("Running custom preparations");

        ValidateJobPhase(EJobPhase::RunningSetupCommands);
        SetJobPhase(EJobPhase::RunningCustomPreparations);

        return OKFuture;
    }

    TFuture<void> DoApplyNetworkPriority() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

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

        if (Context_.GpuCheckOptions) {
            // NB: The GPU chain does not drive job phases.
            YT_VERIFY(ResultHolder_.GpuCheckVolume);

            auto options = *Context_.GpuCheckOptions;

            auto context = TJobGpuCheckerContext{
                .Slot = Context_.Slot,
                .Job = Context_.Job,
                .RootFS = MakeWritableGpuCheckRootFS(),
                .CommandUser = Context_.CommandUser,
                .Type = EGpuCheckType::Preliminary,
                .Options = options,
                .CurrentStartIndex = 0,
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

            YT_TLOG_INFO("Starting preliminary GPU check");

            return BIND(&TJobGpuChecker::RunGpuCheck, std::move(checker))
                .AsyncVia(Invoker_)
                .Run()
                .Apply(BIND([this, this_ = MakeStrong(this)] (const TError& result) {
                    if (!result.IsOK()) {
                        auto checkError = TError(NExecNode::EErrorCode::GpuCheckCommandFailed, "Preliminary GPU check command failed")
                            .With(std::move(result));
                        THROW_ERROR checkError;
                    }

                    YT_TLOG_INFO("Preliminary GPU check command finished");
                }).AsyncVia(Invoker_));
        } else {
            YT_TLOG_INFO("No preliminary GPU check is needed");

            return OKFuture;
        }
    }

    TFuture<void> Run() override
    {
        YT_ASSERT_THREAD_AFFINITY(JobThread);

        auto layersFuture = MakeStep<EStep::PrepareLayers>()
            .Run();

        // Both chains create volumes in the porto place, so the slot root directory
        // must be rebuilt before the chains fork. Apply network priority before the
        // GPU check, which may use RDMA.
        auto networkPriorityFuture = layersFuture
            .Apply(MakeStep<EStep::BuildSlotRootDirectory>())
            .Apply(MakeStep<EStep::ApplyNetworkPriority>());

        // This chain owns all job phase transitions.
        auto rootChainFuture = networkPriorityFuture
            .Apply(MakeStep<EStep::PrepareRootVolume>())
            .Apply(MakeStep<EStep::PrepareNonRootVolumes>())
            .Apply(MakeStep<EStep::BindRootVolume>())
            .Apply(MakeStep<EStep::LinkVolumes>())
            .Apply(MakeStep<EStep::ValidateRootFS>())
            .Apply(MakeStep<EStep::PrepareSandboxDirectories>())
            .Apply(MakeStep<EStep::RunSetupCommand>())
            .Apply(MakeStep<EStep::RunCustomPreparations>());

        auto gpuChainFuture = networkPriorityFuture
            .Apply(MakeStep<EStep::PrepareGpuCheckVolume>())
            .Apply(MakeStep<EStep::RunGpuCheckCommand>());

        // Wait for both chains so that every volume they create reaches FSSecretary for cleanup.
        auto combinedFuture = AllSet(std::vector<TFuture<void>>{
            std::move(rootChainFuture),
            std::move(gpuChainFuture),
        }).Apply(BIND([networkPriorityFuture] (const std::vector<TErrorOr<void>>& results) {
            // Both chains are forked off networkPriorityFuture, so it is always set here.
            const auto& prefixResult = networkPriorityFuture.GetOrCrash();
            if (!prefixResult.IsOK()) {
                THROW_ERROR prefixResult;
            }

            std::vector<TError> errors;
            for (const auto& result : results) {
                if (!result.IsOK()) {
                    errors.push_back(result);
                }
            }

            if (errors.size() == 1) {
                THROW_ERROR errors[0];
            }

            if (!errors.empty()) {
                THROW_ERROR_EXCEPTION("Failed to build job workspace")
                    .With(errors);
            }
        }));

        return FinishRun(std::move(combinedFuture));
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

} // namespace NYT::NExecNode
