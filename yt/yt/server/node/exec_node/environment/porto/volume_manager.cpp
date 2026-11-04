#include "volume_manager.h"

#include <yt/yt/server/node/exec_node/artifact.h>
#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/helpers.h>
#include <yt/yt/server/node/exec_node/private.h>
#include <yt/yt/server/node/exec_node/volume.h>
#include <yt/yt/server/node/exec_node/volume_artifact.h>
#include <yt/yt/server/node/exec_node/volume_counters.h>

#include "slot_location.h"
#include "volume_options.h"
#include "layer_location.h"
#include "porto_volume.h"
#include "volume_cache.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/server/lib/misc/disk_health_checker.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/library/containers/instance.h>
#include <yt/yt/library/containers/porto_executor.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <library/cpp/yt/misc/variant.h>

#include <util/string/vector.h>

#include <util/system/fs.h>

namespace NYT::NExecNode {

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NContainers;
using namespace NLogging;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NTools;
using namespace NYson;
using namespace NYTree;

using NYT::FromProto;

using NControllerAgent::ELayerAccessMethod;
using NControllerAgent::ELayerFilesystem;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TPortoVolumeManager)

class TPortoVolumeManager
    : public IPortoVolumeManager
{
public:
    TPortoVolumeManager(
        NDataNode::TDataNodeConfigPtr config,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        IVolumeArtifactCachePtr artifactCache,
        IInvokerPtr controlInvoker,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        IBootstrap* const bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , DynamicConfigManager_(std::move(dynamicConfigManager))
        , ArtifactCache_(std::move(artifactCache))
        , ControlInvoker_(std::move(controlInvoker))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
    { }

    TFuture<void> Initialize()
    {
        auto dynamicConfig = DynamicConfigManager_->GetConfig()->ExecNode->SlotManager->VolumeManager;
        DynamicConfig_.Store(dynamicConfig);

        // Create locations.

        std::vector<TFuture<void>> initLocationResults;
        std::vector<TLayerLocationPtr> locations;
        for (int index = 0; index < std::ssize(Config_->VolumeManager->LayerLocations); ++index) {
            const auto& locationConfig = Config_->VolumeManager->LayerLocations[index];
            auto id = Format("layer%v", index);

            auto layerImportProfiler = ExecNodeProfiler()
                .WithPrefix("/location_import_layers/porto")
                .WithTag("location_id", id);
            auto layerImportExecutor = CreatePortoExecutor(
                dynamicConfig->LayerCache->LayerPortoExecutor,
                Format("layer_import%v", index),
                layerImportProfiler,
                locationConfig->LayerImportLocationConcurrency);

            auto layerExecutor = CreatePortoExecutor(
                dynamicConfig->LayerCache->LayerPortoExecutor,
                Format("layer%v", index),
                ExecNodeProfiler().WithPrefix("/location_layers/porto").WithTag("location_id", id),
                /*apiThreadCount*/ 1);

            // Imports go through the import executor, removals through the layer one;
            // the per-location gauge aggregates the surplus of both.
            layerImportProfiler
                .AddFuncGauge("/layer_surplus", MakeStrong(this), [layerExecutor, layerImportExecutor] {
                    return layerExecutor->GetLayerSurplus() + layerImportExecutor->GetLayerSurplus();
                });

            auto fastLayerProfiler = ExecNodeProfiler()
                .WithPrefix("/location_fast_layers/porto")
                .WithTag("location_id", id);
            auto fastLayerExecutor = CreatePortoExecutor(
                dynamicConfig->LayerCache->LayerPortoExecutor,
                Format("fast_layer%v", index),
                fastLayerProfiler);
            fastLayerProfiler
                .AddFuncGauge("/layer_surplus", MakeStrong(this), [fastLayerExecutor] {
                    return fastLayerExecutor->GetLayerSurplus();
                });

            auto location = New<TLayerLocation>(
                locationConfig,
                DynamicConfigManager_,
                locationConfig->DiskHealthChecker,
                CreatePortoExecutor(
                    dynamicConfig->LayerCache->VolumePortoExecutor,
                    Format("volume%v", index),
                    ExecNodeProfiler().WithPrefix("/location_volumes/porto").WithTag("location_id", id)),
                layerExecutor,
                fastLayerExecutor,
                std::move(layerImportExecutor),
                id);
            initLocationResults.push_back(location->Initialize());
            locations.push_back(std::move(location));
        }

        auto results = WaitFor(AllSet(initLocationResults))
            .ValueOrCrash();
        for (const auto& error : results) {
            YT_TLOG_WARNING_UNLESS(error.IsOK(), "Proceeding with partially initialized layer locations")
                .With(error);
        }

        auto tmpfsProfiler = ExecNodeProfiler().WithPrefix("/tmpfs_layers/porto");
        auto tmpfsExecutor = CreatePortoExecutor(
            dynamicConfig->LayerCache->TmpfsCache->PortoExecutor,
            "tmpfs_layer",
            tmpfsProfiler);
        tmpfsProfiler
            .AddFuncGauge("/layer_surplus", MakeStrong(this), [tmpfsExecutor] {
                return tmpfsExecutor->GetLayerSurplus();
            });
        LayerCache_ = New<TLayerCache>(
            Config_->VolumeManager,
            DynamicConfigManager_,
            locations,
            tmpfsExecutor,
            ArtifactCache_,
            ControlInvoker_,
            MemoryUsageTracker_,
            Bootstrap_);

        // Subscribe to PopulateAlerts only after LayerCache_ is created to avoid data race.
        if (Bootstrap_) {
            Bootstrap_->SubscribePopulateAlerts(BIND(&TPortoVolumeManager::PopulateAlerts, MakeWeak(this)));
        }

        SquashFSVolumeCache_ = New<TSquashFSVolumeCache>(
            Bootstrap_,
            locations,
            ArtifactCache_);

        NbdVolumeFactory_ = New<TNbdVolumeFactory>(
            Bootstrap_,
            DynamicConfigManager_,
            locations);

        return LayerCache_->Initialize();
    }

    TFuture<void> GetVolumeReleaseEvent() override
    {
        return LayerCache_->GetVolumeReleaseEvent();
    }

    void InitializeSlot(TNonNullPtr<TSlotLocation> location, int slotIndex) override
    {
        CleanPortoSlotResources(
            location,
            slotIndex,
            this,
            *DynamicConfigManager_->GetConfig()->ExecNode->SlotManager,
            /*preservedVolumePaths*/ {},
            /*initializing*/ true);
    }

    void PrepareForReplacement() const override
    {
        MarkLayersAsNotRemovable();
        ClearCaches();
    }

    TFuture<void> Disable(const TError& reason) override
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        return LayerCache_->Disable(reason);
    }

    TError GetHealthCheckError() const override
    {
        return IsEnabled()
            ? TError()
            : TError(EErrorCode::PortoVolumeManagerFailure, "Layer cache is disabled");
    }

    bool IsEnabled() const override
    {
        return LayerCache_->IsEnabled();
    }

    std::vector<TFuture<TOverlayData>> PrepareOverlayLayers(
        TJobId jobId,
        std::vector<TOverlayLayerPreparationOptions> layerOptions,
        const TArtifactDownloadContext& artifactDownloadContext) override
    {
        auto tag = TGuid::Create();

        YT_TLOG_DEBUG("Preparing layers")
            .With("Tag", tag)
            .With("JobId", jobId)
            .With("LayerCount", layerOptions.size());

        if (DynamicConfig_.Acquire()->ThrowOnPrepareLayers) {
            std::vector<TFuture<TOverlayData>> errorFutures;
            errorFutures.reserve(layerOptions.size());
            for (const auto& layerOption : layerOptions) {
                errorFutures.push_back(MakeFuture<TOverlayData>(TError("Throw on prepare layers")
                    .With("artifact_key", ToString(layerOption.ArtifactKey))));
            }
            return errorFutures;
        }

        std::vector<TFuture<TOverlayData>> overlayDataFutures;
        overlayDataFutures.reserve(layerOptions.size());

        for (auto& layerOption : layerOptions) {
            const auto& artifactKey = layerOption.ArtifactKey;
            if (FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Nbd) {
                overlayDataFutures.push_back(GetOrCreateRONbdVolume(
                    tag,
                    TPrepareRONbdVolumeOptions{
                        .JobId = jobId,
                        .ArtifactKey = std::move(layerOption.ArtifactKey),
                        .ImageReader = std::move(layerOption.ImageReader),
                    }));
            } else if (FromProto<ELayerFilesystem>(artifactKey.filesystem()) == ELayerFilesystem::SquashFS) {
                overlayDataFutures.push_back(GetOrCreateSquashFSVolume(
                    tag,
                    TPrepareSquashFSVolumeOptions{
                        .JobId = jobId,
                        .ArtifactKey = std::move(layerOption.ArtifactKey),
                        .ArtifactDownloadContext = artifactDownloadContext,
                    }));
            } else {
                overlayDataFutures.push_back(GetOrCreateLayer(
                    tag,
                    TPrepareLayerOptions{
                        .JobId = jobId,
                        .ArtifactKey = std::move(layerOption.ArtifactKey),
                        .ArtifactDownloadContext = artifactDownloadContext,
                    }));
            }
        }

        return overlayDataFutures;
    }

    //! Create overlayfs volume from pre-prepared overlay layer data.
    TFuture<IVolumePtr> PrepareVolume(
        std::vector<TOverlayData> overlayDataArray,
        TBaseVolumeParamsPtr volumeParams,
        const TVolumePreparationContext& context,
        const TVolumePreparationOptions& options) override
    {
        auto tag = TGuid::Create();

        auto userSandboxOptions = options.UserSandboxOptions;

        auto Logger = ExecNodeLogger()
            .WithTag("Tag", tag)
            .WithTag("JobId", options.JobId)
            .WithTag("LayerCount", overlayDataArray.size());

        YT_TLOG_DEBUG("Preparing root volume");

        if (DynamicConfig_.Acquire()->ThrowOnPrepareVolume) {
            auto error = TError(NExecNode::EErrorCode::RootVolumePreparationFailed, "Throw on prepare volume");
            YT_TLOG_INFO("Failed to prepare root volume")
                .With(error);
            THROW_ERROR(error);
        }

        auto jobId = options.JobId;
        if (volumeParams->VolumeType == EVolumeType::Nbd) {
            auto nbdVolumeParams = StaticPointerCast<TNbdDiskVolumeParams>(std::move(volumeParams));
            const auto& sandboxNbdRootVolumeSpec = nbdVolumeParams->SandboxNbdRootVolumeSpec;
            // Create NBD root volume separately and use it as the upper layer,
            // the same way tmpfs/disk upper layers are handled in PrepareNonRootVolumes().
            return CreateRWNbdVolume(
                tag,
                context,
                TPrepareRWNbdVolumeOptions{
                    .JobId = jobId,
                    .DeviceId = sandboxNbdRootVolumeSpec.DeviceId,
                    .DeviceSize = sandboxNbdRootVolumeSpec.DeviceSize,
                    .FilesystemType = sandboxNbdRootVolumeSpec.FilesystemType,
                    .BackendOptions = Visit(sandboxNbdRootVolumeSpec.BackendSpec,
                        [] (const TChunkNbdVolumeSpec& spec) -> TRWNbdVolumeBackendOptions {
                            return TChunkNbdVolumeOptions{.Spec = spec};
                        },
                        [] (const TJournalNbdVolumeSpec& spec) -> TRWNbdVolumeBackendOptions {
                            return TJournalNbdVolumeOptions{.Spec = spec};
                        }),
                })
                .AsUnique()
                .Apply(BIND(
                    [
                        tag,
                        jobId,
                        Logger,
                        userSandboxOptions = std::move(userSandboxOptions),
                        overlayDataArray = std::move(overlayDataArray),
                        this,
                        this_ = MakeStrong(this)
                    ] (IVolumePtr&& nbdVolume) mutable -> TFuture<TOverlayVolumePtr> {
                        // See PORTO-460 for "//" prefix.
                        TString placePath = "//" + nbdVolume->GetPath();
                        YT_TLOG_DEBUG("Place overlay volume in NBD volume")
                            .With("PortoPlace", placePath);
                        return DoCreateOverlayVolume(
                            tag,
                            jobId,
                            userSandboxOptions.UserId,
                            placePath,
                            std::move(overlayDataArray),
                            /*volumeForUpperLayer*/ std::move(nbdVolume))
                            .ToUncancelable();
                    })
                    .AsyncVia(GetCurrentInvoker()))
                .As<IVolumePtr>();
        } else {
            // Now we are ready to create overlay volume. It is a light
            // operation so we are allowed to make it uncancelable.
            return CreateRootOverlayVolume(
                tag,
                TPrepareRootOverlayVolumeOptions{
                    .JobId = jobId,
                    .UserSandboxOptions = std::move(userSandboxOptions),
                    .OverlayDataArray = std::move(overlayDataArray),
                    .RootVolumeParams = volumeParams
                })
                .ToUncancelable()
                .As<IVolumePtr>();
        }
    }

    TFuture<std::vector<TVolumeResultPtr>> PrepareNonRootVolumes(
        const std::optional<std::string>& /*sandboxPath*/,
        const TJobId& jobId,
        const std::vector<TBaseVolumeParamsPtr>& volumes,
        std::vector<std::vector<TOverlayData>> perVolumeOverlayData,
        const std::vector<TVolumeMountPtr>& /*volumeMounts*/,
        const TVolumePreparationContext& /*context*/) override
    {
        YT_VERIFY(volumes.size() == perVolumeOverlayData.size());

        // Create debug tag.
        auto tag = TGuid::Create();

        std::vector<TFuture<TVolumeResultPtr>> futures;
        futures.reserve(volumes.size());
        for (int i = 0; i < std::ssize(volumes); ++i) {
            const auto& volume = volumes[i];
            auto overlayDataArray = std::move(perVolumeOverlayData[i]);

            auto volumeFuture = [&] {
                switch (volume->VolumeType) {
                    case EVolumeType::Tmpfs:
                        return CreateTmpfsVolume(tag, StaticPointerCast<TTmpfsVolumeParams>(volume));
                    case EVolumeType::LocalDisk:
                        return CreateLoopVolume(tag, StaticPointerCast<TLocalDiskVolumeParams>(volume));
                    default:
                        YT_ABORT();
                }
            }();

            futures.push_back(volumeFuture
                .AsUnique()
                .Apply(BIND(
                    [
                        tag,
                        jobId,
                        overlayDataArray = std::move(overlayDataArray),
                        volumeParams = volume,
                        this,
                        this_ = MakeStrong(this)
                    ] (TVolumeResultPtr&& result) {
                        if (overlayDataArray.empty()) {
                            return MakeFuture(result);
                        }

                        auto placePath = "//" + NFS::JoinPaths(result->Volume->GetPath(), "place");
                        // TODO(krasovav): If an exception is thrown here, then all volumes must be properly cleaned up.
                        return DoCreateOverlayVolume(
                            tag,
                            jobId,
                            volumeParams->UserId,
                            placePath,
                            overlayDataArray,
                            /*volumeForUpperLayer*/ std::move(result->Volume))
                            .AsUnique()
                            .Apply(BIND([result = std::move(result)] (TOverlayVolumePtr&& volume) mutable -> TVolumeResultPtr {
                                result->Volume = StaticPointerCast<IVolume>(std::move(volume));
                                return result;
                            }))
                            .ToUncancelable();
                    }))
                .ToUncancelable());
        }
        return AllSucceeded(std::move(futures));
    }

    TFuture<void> LinkVolumes(
        const std::string& destinationDirectory,
        const std::vector<TVolumeResultPtr>& volumes,
        const std::vector<TVolumeMountPtr>& volumeMounts) override
    {
        // Create debug tag.
        auto tag = TGuid::Create();

        TFuture<void> future = OKFuture;
        for (const auto& volumeMount : volumeMounts) {
            if (volumeMount->MountPath == TAbsoluteNormalizedPath("/")) {
                continue;
            }
            auto volume = GetNonRootVolumeResultByVolumeId(volumeMount->VolumeId, volumes);
            auto target = NFS::GetRealPath(NFS::JoinPaths(destinationDirectory, volumeMount->MountPath.Path().string()));

            future = future
                .Apply(
                    BIND([tag, volume, target] {
                        NFS::MakeDirRecursive(target);
                        return volume->Volume->Link(tag, target);
                    })
                    .AsyncVia(GetCurrentInvoker())
                );
        }
        return future;
    }

    void ClearCaches() const override
    {
        for (const auto& layer : LayerCache_->GetAll()) {
            LayerCache_->TryRemoveValue(layer);
        }
    }

    void MarkLayersAsNotRemovable() const override
    {
        for (const auto& layer : LayerCache_->GetAll()) {
            layer->SetLayerRemovalNotNeeded();
        }
    }

    void OnDynamicConfigChanged(
        const TVolumeManagerDynamicConfigPtr& oldConfig,
        const TVolumeManagerDynamicConfigPtr& newConfig) override
    {
        if (*newConfig == *oldConfig) {
            return;
        }

        DynamicConfig_.Store(newConfig);

        LayerCache_->OnDynamicConfigChanged(oldConfig->LayerCache, newConfig->LayerCache);
    }

    // TODO(yuryalekseev): Remove me when slot rbind is removed.
    TFuture<IVolumePtr> RbindRootVolume(
        const IVolumePtr& volume,
        const std::string& slotPath) override
    {
        auto location = LayerCache_->PickVolumeLocation();
        return location->RbindRootVolume(volume, slotPath);
    }

    //! Remove volumes planted at a given place, excluding the given porto mount paths.
    TFuture<void> RemoveVolumes(
        const std::string& place,
        TDuration timeout,
        const THashSet<std::string>& excludedVolumePaths) override
    {
        auto location = LayerCache_->PickRandomLocation();
        return BIND(
            [
                location,
                place,
                timeout,
                excludedVolumePaths
            ] {
                return location->RemoveVolumes(
                    place,
                    timeout,
                    excludedVolumePaths);
            })
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    //! Remove layers planted at a given place.
    TFuture<void> RemoveLayers(const std::string& place, TDuration timeout) override
    {
        auto location = LayerCache_->PickRandomLocation();
        return BIND(
            [
                location,
                place,
                timeout
            ] {
                return location->RemoveLayers(
                    place,
                    timeout);
            })
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

private:
    IBootstrap* const Bootstrap_;
    const NDataNode::TDataNodeConfigPtr Config_;
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    TAtomicIntrusivePtr<TVolumeManagerDynamicConfig> DynamicConfig_;
    const IVolumeArtifactCachePtr ArtifactCache_;
    const IInvokerPtr ControlInvoker_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    TLayerCachePtr LayerCache_;
    TSquashFSVolumeCachePtr SquashFSVolumeCache_;
    TNbdVolumeFactoryPtr NbdVolumeFactory_;

    void BuildOrchid(NYTree::TFluentAny fluent) const override
    {
        LayerCache_->BuildOrchid(fluent);
    }

    TFuture<TOverlayData> GetOrCreateLayer(
        TGuid tag,
        TPrepareLayerOptions options)
    {
        return LayerCache_->GetOrCreateLayer(tag, std::move(options))
            .As<TOverlayData>();
    }

    TFuture<TOverlayData> GetOrCreateRONbdVolume(
        TGuid tag,
        TPrepareRONbdVolumeOptions options)
    {
        return NbdVolumeFactory_->GetOrCreateVolume(tag, std::move(options))
            .As<TOverlayData>();
    }

    TFuture<IVolumePtr> CreateRWNbdVolume(
        TGuid tag,
        const TVolumePreparationContext& context,
        TPrepareRWNbdVolumeOptions options)
    {
        return NbdVolumeFactory_->CreateVolume(tag, context, std::move(options));
    }

    TFuture<TOverlayData> GetOrCreateSquashFSVolume(
        TGuid tag,
        TPrepareSquashFSVolumeOptions options)
    {
        return SquashFSVolumeCache_->GetOrCreateVolume(tag, std::move(options))
            .As<TOverlayData>();
    }

    TFuture<TVolumeResultPtr> CreateTmpfsVolume(
        TGuid tag,
        const TTmpfsVolumeParamsPtr& volumeParams)
    {
        YT_TLOG_INFO("Creating tmpfs volume")
            .With("Tag", tag)
            .With("VolumeId", volumeParams->VolumeId)
            .With("Size", volumeParams->Size)
            .With("UserId", volumeParams->UserId);

        auto tagSet = TVolumeProfilerCounters::MakeTagSet(
            /*volume type*/ "tmpfs",
            /*Cypress path*/ "n/a");
        TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

        auto location = LayerCache_->PickVolumeLocation();
        auto future = location->CreateTmpfsVolume(
            tag,
            tagSet,
            std::move(volumeCreateTimeGuard),
            volumeParams);

        return future
            .AsUnique()
            .Apply(BIND(
                [
                    tagSet = std::move(tagSet),
                    location = std::move(location),
                    volumeParams
                ] (TVolumeMeta&& volumeMeta) mutable {
                    auto result = New<TTmpfsVolumeResult>(
                        volumeParams->VolumeId,
                        EVolumeType::Tmpfs,
                        New<TTmpfsVolume>(
                            std::move(tagSet),
                            std::move(volumeMeta),
                            std::move(location)),
                        volumeParams->Index);
                    return StaticPointerCast<TVolumeResult>(result);
                }))
            .ToUncancelable();
    }

    TFuture<TVolumeResultPtr> CreateLoopVolume(
        TGuid tag,
        const TLocalDiskVolumeParamsPtr& volumeParams)
    {
        YT_TLOG_INFO("Creating loop volume")
            .With("Tag", tag)
            .With("VolumeId", volumeParams->VolumeId)
            .With("Size", volumeParams->Size)
            .With("InodeLimit", volumeParams->InodeLimit)
            .With("UserId", volumeParams->UserId);

        auto tagSet = TVolumeProfilerCounters::MakeTagSet(
            /*volume type*/ "loop",
            /*Cypress path*/ "n/a");
        TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

        auto location = LayerCache_->PickVolumeLocation();
        auto future = location->CreateLoopVolume(
            tag,
            tagSet,
            std::move(volumeCreateTimeGuard),
            volumeParams);

        return future
            .AsUnique()
            .Apply(BIND(
                [
                    tagSet = std::move(tagSet),
                    location = std::move(location),
                    volumeId = volumeParams->VolumeId
                ] (TVolumeMeta&& volumeMeta) mutable {
                    auto result = New<TVolumeResult>(
                        std::move(volumeId),
                        EVolumeType::LocalDisk,
                        New<TLoopVolume>(
                            std::move(tagSet),
                            std::move(volumeMeta),
                            std::move(location)));
                    return result;
                }))
            .ToUncancelable();
    }

    TFuture<TOverlayVolumePtr> DoCreateOverlayVolume(
        TGuid tag,
        TJobId jobId,
        int userId,
        std::optional<std::string> placePath,
        std::vector<TOverlayData> overlayDataArray,
        IVolumePtr volumeForUpperLayer = nullptr,
        std::optional<i64> diskSpaceLimit = std::nullopt,
        std::optional<i64> inodeLimit = std::nullopt,
        bool placeInUserSlot = false)
    {
        auto tagSet = TVolumeProfilerCounters::MakeTagSet(
            /*volume type*/ "overlay",
            /*Cypress path*/ "n/a");
        TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

        auto Logger = ExecNodeLogger()
            .WithTag("Tag", tag)
            .WithTag("JobId", jobId)
            .WithTag("OverlayDataArraySize", overlayDataArray.size());

        YT_TLOG_DEBUG("Creating overlay volume");

        for (const auto& volumeOrLayer : overlayDataArray) {
            if (volumeOrLayer.IsLayer()) {
                LayerCache_->Touch(volumeOrLayer.GetLayer());

                YT_TLOG_DEBUG("Using layer to create overlay volume")
                    .With("LayerId", volumeOrLayer.GetLayer()->GetMeta().Id);
            } else {
                YT_TLOG_DEBUG("Using volume to create overlay volume")
                    .With("VolumeId", volumeOrLayer.GetVolume()->GetId());
            }
        }

        auto location = LayerCache_->PickVolumeLocation();
        auto volumeMetaFuture = location->CreateOverlayVolume(
            tag,
            tagSet,
            std::move(volumeCreateTimeGuard),
            userId,
            placePath,
            diskSpaceLimit,
            inodeLimit,
            overlayDataArray,
            placeInUserSlot);

        // This future is intentionally uncancellable: we don't want to interrupt volume creation.
        return volumeMetaFuture
            .AsUnique()
            .Apply(BIND(
                [
                    Logger,
                    tagSet = std::move(tagSet),
                    location = std::move(location),
                    overlayDataArray = std::move(overlayDataArray),
                    volumeForUpperLayer = std::move(volumeForUpperLayer)
                ] (TVolumeMeta&& volumeMeta) {
                    YT_TLOG_DEBUG("Created overlay volume");
                    return New<TOverlayVolume>(
                        std::move(tagSet),
                        std::move(volumeMeta),
                        std::move(location),
                        std::move(overlayDataArray),
                        std::move(volumeForUpperLayer));
                }))
            .ToUncancelable();
    }

    //! Create rootfs overlay volume.
    TFuture<TOverlayVolumePtr> CreateRootOverlayVolume(
        TGuid tag,
        TPrepareRootOverlayVolumeOptions options)
    {
        bool placeInUserSlot = false;

        std::optional<std::string> placePath;

        const auto& userSandboxOptions = options.UserSandboxOptions;
        if (userSandboxOptions.DisableRbindRootVolume && !userSandboxOptions.SlotPath.empty()) {
            // Plant porto place for overlay volume in user slot.
            placePath = NFS::CombinePaths(
                userSandboxOptions.SlotPath,
                GetSandboxRelPath(ESandboxKind::PortoPlace));

            YT_TLOG_DEBUG("Place overlay volume in user slot")
                .With("PortoPlace", placePath);

            placeInUserSlot = true;
        }

        YT_VERIFY(options.RootVolumeParams->VolumeType == EVolumeType::LocalDisk);
        auto rootLocalVolumeParams = StaticPointerCast<TLocalDiskVolumeParams>(options.RootVolumeParams);
        std::optional<i64> diskSpaceLimit;
        std::optional<i64> inodeLimit;

        if (userSandboxOptions.EnableDiskQuota) {
            diskSpaceLimit = rootLocalVolumeParams->Size;
            inodeLimit = rootLocalVolumeParams->InodeLimit;
        }

        return DoCreateOverlayVolume(
            tag,
            options.JobId,
            userSandboxOptions.UserId,
            placePath,
            std::move(options.OverlayDataArray),
            /*volumeForUpperLayer*/ nullptr,
            diskSpaceLimit,
            inodeLimit,
            placeInUserSlot);
    }

    void PopulateAlerts(std::vector<TError>* alerts)
    {
        if (LayerCache_) {
            LayerCache_->PopulateAlerts(alerts);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TPortoVolumeManager)

////////////////////////////////////////////////////////////////////////////////

TFuture<IPortoVolumeManagerPtr> CreatePortoVolumeManager(
    NDataNode::TDataNodeConfigPtr config,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    IVolumeArtifactCachePtr artifactCache,
    IInvokerPtr controlInvoker,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    IBootstrap* bootstrap)
{
    auto volumeManager = New<TPortoVolumeManager>(
        std::move(config),
        std::move(dynamicConfigManager),
        std::move(artifactCache),
        std::move(controlInvoker),
        std::move(memoryUsageTracker),
        bootstrap);

    return volumeManager->Initialize()
        .Apply(BIND([volumeManager = std::move(volumeManager)] () mutable {
            return StaticPointerCast<IPortoVolumeManager>(std::move(volumeManager));
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
