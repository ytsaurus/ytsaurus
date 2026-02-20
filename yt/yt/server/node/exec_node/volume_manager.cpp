#include "volume_manager.h"

#include "artifact.h"
#include "bootstrap.h"
#include "layer_location.h"
#include "porto_volume.h"
#include "private.h"
#include "volume.h"
#include "volume_artifact.h"
#include "volume_cache.h"
#include "volume_counters.h"
#include "volume_options.h"
#include "helpers.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/misc/disk_health_checker.h>

#include <yt/yt/server/tools/tools.h>
#include <yt/yt/server/tools/proc.h>

#include <yt/yt/library/containers/instance.h>
#include <yt/yt/library/containers/porto_executor.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

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

DECLARE_REFCOUNTED_CLASS(TSimpleVolumeManager)

class TSimpleVolumeManager
    : public IVolumeManager
{
public:
    TSimpleVolumeManager(
        IInvokerPtr invoker,
        bool detachedTmpfsUmount)
        : Invoker_(std::move(invoker))
        , DetachUnmount_(detachedTmpfsUmount)
    { }

    //! Prepare root overlayfs volume.
    TFuture<IVolumePtr> PrepareVolume(
        const std::vector<TArtifactKey>&,
        const TVolumePreparationOptions&) override
    {
        YT_UNIMPLEMENTED("PrepareVolume is not implemented for SimpleVolumeManager");
    }

    //! Prepare tmpfs volumes.
    TFuture<std::vector<TTmpfsVolumeResult>> PrepareTmpfsVolumes(
        const std::optional<TString>& sandboxPath,
        const std::vector<TTmpfsVolumeParams>& volumes,
        const std::vector<NScheduler::TVolumeMountPtr>& volumeMounts) override
    {
        YT_VERIFY(sandboxPath);
        // Create debug tag.
        auto tag = TGuid::Create();

        std::vector<TFuture<TTmpfsVolumeResult>> futures;
        futures.reserve(volumes.size());
        for (const auto& volume : volumes) {
            auto mountPath = GetVolumeMountPathByVolumeId(volume.VolumeId, volumeMounts);
            futures.push_back(CreateTmpfsVolume(tag, *sandboxPath, volume, mountPath));
        }
        return AllSucceeded(std::move(futures));
    }

    TFuture<IVolumePtr> RbindRootVolume(
        const IVolumePtr&,
        const TString&) override
    {
        YT_UNIMPLEMENTED("RbindRootVolume is not implemented for SimpleVolumeManager");
    }

    TFuture<void> LinkTmpfsVolumes(
        const TString&,
        const std::vector<TTmpfsVolumeResult>&,
        const std::vector<NScheduler::TVolumeMountPtr>&) override
    {
        YT_UNIMPLEMENTED("LinkTmpfsVolumes is not implemented for SimpleVolumeManager");
    }

    TFuture<void> Initialize(const std::vector<TSlotLocationConfigPtr>& locations)
    {
        // NB: Iterating over /proc/mounts is not reliable,
        // see https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=593516.
        // To avoid problems with undeleting tmpfs ordered by user in sandbox
        // we always try to remove it several times.
        for (int attempt = 0; attempt < TmpfsRemoveAttemptCount; ++attempt) {
            std::vector<std::string> mountPaths;
            for (const auto& location : locations) {
                FindTmpfsMountPathsInLocation(location->Path, mountPaths);
            }

            // Sort from longest paths, to shortest.
            std::sort(mountPaths.begin(), mountPaths.end(), [] (const std::string& lhs, const std::string& rhs) {
                return StringSplitter(lhs).Split('/').Count() > StringSplitter(rhs).Split('/').Count();
            });

            auto error = WaitFor(CleanupTmpfsMountPaths(std::move(mountPaths)));
            if (!error.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to initialize simple volume manager")
                    << error;
            }
        }

        return OKFuture;
    }

    bool IsLayerCached(const TArtifactKey&) const override
    {
        return false;
    }

    void BuildOrchid(NYTree::TFluentAny fluent) const override
    {
        fluent
            .BeginMap()
            .EndMap();
    }

    void ClearCaches() const override
    { }

    void MarkLayersAsNotRemovable() const override
    { }

    TFuture<void> GetVolumeReleaseEvent() override
    {
        return OKFuture;
    }

    TFuture<void> DisableLayerCache(const TError&) override
    {
        return OKFuture;
    }

    bool IsEnabled() const override
    {
        return true;
    }

    void OnDynamicConfigChanged(
        const TVolumeManagerDynamicConfigPtr&,
        const TVolumeManagerDynamicConfigPtr&) override
    { }

private:
    const IInvokerPtr Invoker_;
    const bool DetachUnmount_;

    TFuture<TTmpfsVolumeResult> CreateTmpfsVolume(
        TGuid tag,
        const TString& sandboxPath,
        const TTmpfsVolumeParams& volume,
        const std::string& mountPath)
    {
        YT_VERIFY(sandboxPath);

        auto tagSet = TVolumeProfilerCounters::MakeTagSet(
            /*volume type*/ "tmpfs",
            /*Cypress path*/ "n/a");
        TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

        // TODO(dgolear): Switch to std::string.
        TString path = NFS::GetRealPath(NFS::CombinePaths(sandboxPath, mountPath));

        auto config = New<TMountTmpfsConfig>();
        config->Path = path;
        config->Size = volume.Size;
        config->UserId = volume.UserId;

        YT_LOG_DEBUG("Creating tmpfs volume (Tag: %v, Config: %v)",
            tag,
            ConvertToYsonString(config, EYsonFormat::Text));

        return BIND(
            [
                tagSet,
                volumeId = volume.VolumeId,
                volumeCreateTimeGuard = std::move(volumeCreateTimeGuard),
                config = std::move(config),
                tmpfsIndex = volume.Index,
                this,
                this_ = MakeStrong(this)
            ] {
                try {
                    RunTool<TMountTmpfsAsRootTool>(config);

                    TVolumeProfilerCounters::Get()->GetGauge(tagSet, "/count")
                        .Update(VolumeCounters().Increment(tagSet));
                    TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/created").Increment(1);

                    return TTmpfsVolumeResult{
                        .Volume = New<TSimpleTmpfsVolume>(
                            tagSet,
                            config->Path,
                            Invoker_,
                            DetachUnmount_),
                        .VolumeId = volumeId,
                        .Index = tmpfsIndex
                    };
                } catch (const std::exception& ex) {
                    TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/create_errors").Increment(1);
                    throw;
                }
            })
            .AsyncVia(Invoker_)
            .Run()
            .ToUncancelable();
    }

    void FindTmpfsMountPathsInLocation(const std::string& locationPath, std::vector<std::string>& mountPaths)
    {
        auto mountPoints = NFS::GetMountPoints("/proc/mounts");
        for (const auto& mountPoint : mountPoints) {
            if (mountPoint.Path.starts_with(locationPath + "/")) {
                mountPaths.push_back(mountPoint.Path);
            }
        }
    }

    TFuture<void> CleanupTmpfsMountPaths(std::vector<std::string>&& mountPaths) const
    {
        return BIND([mountPaths = std::move(mountPaths), detachUnmount = DetachUnmount_] {
            for (const auto& path : mountPaths) {
                YT_LOG_DEBUG("Removing mount point (Path: %v)",
                    path);
                try {
                    // Due to bug in the kernel, this can sometimes fail with "Directory is not empty" error.
                    // More info: https://bugzilla.redhat.com/show_bug.cgi?id=1066751
                    RunTool<TRemoveDirContentAsRootTool>(path);
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Failed to remove mount point (Path: %v)",
                        path);
                }

                auto config = New<TUmountConfig>();
                config->Path = path;
                config->Detach = detachUnmount;
                RunTool<TUmountAsRootTool>(config);
            }
        })
        .AsyncVia(Invoker_)
        .Run();
    }
};

DEFINE_REFCOUNTED_TYPE(TSimpleVolumeManager)

////////////////////////////////////////////////////////////////////////////////

TFuture<IVolumeManagerPtr> CreateSimpleVolumeManager(
    const std::vector<TSlotLocationConfigPtr>& locations,
    IInvokerPtr invoker,
    bool detachedTmpfsUmount)
{
    auto volumeManager = New<TSimpleVolumeManager>(
        std::move(invoker),
        detachedTmpfsUmount);

    return volumeManager->Initialize(locations).Apply(BIND([=] {
        return static_cast<IVolumeManagerPtr>(volumeManager);
    }));
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TPortoVolumeManager)

class TPortoVolumeManager
    : public IVolumeManager
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

        if (Bootstrap_) {
            Bootstrap_->SubscribePopulateAlerts(BIND(&TPortoVolumeManager::PopulateAlerts, MakeWeak(this)));
        }
        // Create locations.

        std::vector<TFuture<void>> initLocationResults;
        std::vector<TLayerLocationPtr> locations;
        for (int index = 0; index < std::ssize(Config_->VolumeManager->LayerLocations); ++index) {
            const auto& locationConfig = Config_->VolumeManager->LayerLocations[index];
            auto id = Format("layer%v", index);
            auto location = New<TLayerLocation>(
                locationConfig,
                DynamicConfigManager_,
                locationConfig->DiskHealthChecker,
                CreatePortoExecutor(
                    dynamicConfig->LayerCache->VolumePortoExecutor,
                    Format("volume%v", index),
                    ExecNodeProfiler().WithPrefix("/location_volumes/porto").WithTag("location_id", id)),
                CreatePortoExecutor(
                    dynamicConfig->LayerCache->LayerPortoExecutor,
                    Format("layer%v", index),
                    ExecNodeProfiler().WithPrefix("/location_layers/porto").WithTag("location_id", id)),
                id);
            initLocationResults.push_back(location->Initialize());
            locations.push_back(std::move(location));
        }

        auto errorOrResults = WaitFor(AllSet(initLocationResults));

        if (!errorOrResults.IsOK()) {
            auto wrappedError = TError("Failed to initialize layer locations") << errorOrResults;
            YT_LOG_WARNING(wrappedError);
        }

        auto tmpfsExecutor = CreatePortoExecutor(
            dynamicConfig->LayerCache->TmpfsCache->PortoExecutor,
            "tmpfs_layer",
            ExecNodeProfiler().WithPrefix("/tmpfs_layers/porto"));
        LayerCache_ = New<TLayerCache>(
            Config_->VolumeManager,
            DynamicConfigManager_,
            locations,
            tmpfsExecutor,
            ArtifactCache_,
            ControlInvoker_,
            MemoryUsageTracker_,
            Bootstrap_);

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

    TFuture<void> DisableLayerCache(const TError& reason) override
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        return LayerCache_->Disable(reason);
    }

    bool IsEnabled() const override
    {
        return LayerCache_->IsEnabled();
    }

    //! Prepare rootfs volume.
    TFuture<IVolumePtr> PrepareVolume(
        const std::vector<TArtifactKey>& artifactKeys,
        const TVolumePreparationOptions& options) override
    {
        YT_VERIFY(!artifactKeys.empty());

        auto tag = TGuid::Create();

        const auto& userSandboxOptions = options.UserSandboxOptions;

        YT_LOG_DEBUG(
            "Prepare volume (Tag: %v, ArtifactCount: %v, HasVirtualSandbox: %v, HasSandboxRootVolumeData: %v)",
            tag,
            artifactKeys.size(),
            userSandboxOptions.VirtualSandboxData.has_value(),
            userSandboxOptions.SandboxNbdRootVolumeData.has_value());

        if (DynamicConfig_.Acquire()->ThrowOnPrepareVolume) {
            auto error = TError(NExecNode::EErrorCode::RootVolumePreparationFailed, "Throw on prepare volume");
            YT_LOG_DEBUG(
                error,
                "Prepare volume (Tag: %v, ArtifactCount: %v, HasVirtualSandbox: %v, HasSandboxRootVolumeData: %v)",
                tag,
                artifactKeys.size(),
                userSandboxOptions.VirtualSandboxData.has_value(),
                userSandboxOptions.SandboxNbdRootVolumeData.has_value());
            THROW_ERROR(error);
        }

        std::vector<TFuture<TOverlayData>> overlayDataFutures;

        for (const auto& artifactKey : artifactKeys) {
            if (FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Nbd) {
                overlayDataFutures.push_back(GetOrCreateRONbdVolume(
                    tag,
                    TPrepareRONbdVolumeOptions{
                        .JobId = options.JobId,
                        .ArtifactKey = artifactKey,
                        .ImageReader = nullptr, // Create image reader if necessary.
                    }));
            } else if (FromProto<ELayerFilesystem>(artifactKey.filesystem()) == ELayerFilesystem::SquashFS) {
                overlayDataFutures.push_back(GetOrCreateSquashFSVolume(
                    tag,
                    TPrepareSquashFSVolumeOptions{
                        .JobId = options.JobId,
                        .ArtifactKey = artifactKey,
                        .ArtifactDownloadOptions = options.ArtifactDownloadOptions,
                    }));
            } else {
                overlayDataFutures.push_back(GetOrCreateLayer(
                    tag,
                    TPrepareLayerOptions{
                        .JobId = options.JobId,
                        .ArtifactKey = artifactKey,
                        .ArtifactDownloadOptions = options.ArtifactDownloadOptions,
                    }));
            }
        }

        if (auto data = userSandboxOptions.VirtualSandboxData) {
            overlayDataFutures.push_back(GetOrCreateRONbdVolume(
                tag,
                TPrepareRONbdVolumeOptions{
                    .JobId = options.JobId,
                    .ArtifactKey = data->ArtifactKey,
                    .ImageReader = data->Reader,
                }));
        }

        if (auto data = userSandboxOptions.SandboxNbdRootVolumeData) {
            overlayDataFutures.push_back(GetOrCreateRWNbdVolume(
                tag,
                TPrepareRWNbdVolumeOptions{
                    .JobId = options.JobId,
                    .Size = data->Size,
                    .MediumIndex = data->MediumIndex,
                    .Filesystem = data->FsType,
                    .DeviceId = data->DeviceId,
                    .DataNodeChannel = {/*Fill in channel later on.*/},
                    .SessionId = {/*Fill in session id later on.*/},
                    .DataNodeRpcTimeout = data->DataNodeRpcTimeout,
                    .DataNodeAddress = data->DataNodeAddress,
                    .DataNodeNbdServiceRpcTimeout = data->DataNodeNbdServiceRpcTimeout,
                    .DataNodeNbdServiceMakeTimeout = data->DataNodeNbdServiceMakeTimeout,
                    .MasterRpcTimeout = data->MasterRpcTimeout,
                    .MinDataNodeCount = data->MinDataNodeCount,
                    .MaxDataNodeCount = data->MaxDataNodeCount,
                }));
        }

        // ToDo(psushin): choose proper invoker.
        // Avoid sync calls to WaitFor, to respect job preparation context switch guards.
        return AllSucceeded(std::move(overlayDataFutures))
            .AsUnique()
            .Apply(BIND(
                [
                    tag,
                    jobId = options.JobId,
                    userSandboxOptions,
                    this,
                    this_ = MakeStrong(this)
                ] (std::vector<TOverlayData>&& overlayDataArray) {
                    return CreateOverlayVolume(
                        tag,
                        TPrepareOverlayVolumeOptions{
                            .JobId = jobId,
                            .UserSandboxOptions = std::move(userSandboxOptions),
                            .OverlayDataArray = std::move(overlayDataArray)
                        });
                })
                .AsyncVia(GetCurrentInvoker()))
            .ToImmediatelyCancelable()
            .As<IVolumePtr>();
    }

    //! Prepare tmpfs volumes.
    TFuture<std::vector<TTmpfsVolumeResult>> PrepareTmpfsVolumes(
        const std::optional<TString>&,
        const std::vector<TTmpfsVolumeParams>& volumes,
        const std::vector<NScheduler::TVolumeMountPtr>&) override
    {
        // Create debug tag.
        auto tag = TGuid::Create();

        std::vector<TFuture<TTmpfsVolumeResult>> futures;
        futures.reserve(volumes.size());
        for (const auto& volume : volumes) {
            futures.push_back(CreateTmpfsVolume(tag, volume));
        }
        return AllSucceeded(std::move(futures));
    }

    TFuture<void> LinkTmpfsVolumes(
        const TString& destinationDirectory,
        const std::vector<TTmpfsVolumeResult>& volumes,
        const std::vector<NScheduler::TVolumeMountPtr>& volumeMounts) override
    {
        // Create debug tag.
        auto tag = TGuid::Create();

        std::vector<TFuture<void>> futures;
        futures.reserve(volumes.size());
        for (const auto& volume : volumes) {
            auto mountPath = GetVolumeMountPathByVolumeId(volume.VolumeId, volumeMounts);
            TString target = NFS::GetRealPath(NFS::CombinePaths(destinationDirectory, mountPath));
            futures.push_back(volume.Volume->Link(tag, target));
        }

        return AllSucceeded(std::move(futures))
            .ToUncancelable();
    }

    bool IsLayerCached(const TArtifactKey& artifactKey) const override
    {
        return LayerCache_->IsLayerCached(artifactKey);
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
        const TString& slotPath) override
    {
        auto location = LayerCache_->PickLocation();
        return location->RbindRootVolume(volume, slotPath);
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

    TFuture<TOverlayData> GetOrCreateRWNbdVolume(
        TGuid tag,
        TPrepareRWNbdVolumeOptions options)
    {
        return NbdVolumeFactory_->GetOrCreateVolume(tag, std::move(options))
            .As<TOverlayData>();
    }

    TFuture<TOverlayData> GetOrCreateSquashFSVolume(
        TGuid tag,
        TPrepareSquashFSVolumeOptions options)
    {
        return SquashFSVolumeCache_->GetOrCreateVolume(tag, std::move(options))
            .As<TOverlayData>();
    }

    TFuture<TTmpfsVolumeResult> CreateTmpfsVolume(
        TGuid tag,
        const TTmpfsVolumeParams& volumeParams)
    {
        YT_LOG_INFO(
            "Creating tmpfs volume (Tag: %v, VolumeId: %v, Size: %v, UserId: %v)",
            tag,
            volumeParams.VolumeId,
            volumeParams.Size,
            volumeParams.UserId);

        auto tagSet = TVolumeProfilerCounters::MakeTagSet(
            /*volume type*/ "tmpfs",
            /*Cypress path*/ "n/a");
        TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

        auto location = LayerCache_->PickLocation();
        auto future = location->CreateTmpfsVolume(
            tag,
            tagSet,
            std::move(volumeCreateTimeGuard),
            volumeParams);

        return future.AsUnique()
            .Apply(BIND(
                [
                    volumeId = volumeParams.VolumeId,
                    index = volumeParams.Index,
                    tagSet = std::move(tagSet),
                    location = std::move(location)
                ] (TVolumeMeta&& volumeMeta) mutable {
                    TTmpfsVolumeResult result;
                    result.VolumeId = std::move(volumeId);
                    result.Volume = New<TTmpfsVolume>(
                        std::move(tagSet),
                        std::move(volumeMeta),
                        std::move(location));
                    result.Index = index;
                    return result;
                }))
            .ToUncancelable();
    }

    //! Create rootfs overlay volume.
    TFuture<TOverlayVolumePtr> CreateOverlayVolume(
        TGuid tag,
        TPrepareOverlayVolumeOptions options)
    {
        auto tagSet = TVolumeProfilerCounters::MakeTagSet(
            /*volume type*/ "overlay",
            /*Cypress path*/ "n/a");
        TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

        auto Logger = ExecNodeLogger()
            .WithTag("Tag: %v, JobId: %v, OverlayDataArraySize: %v",
                tag,
                options.JobId,
                options.OverlayDataArray.size());

        YT_LOG_DEBUG("Creating overlay volume");

        for (const auto& volumeOrLayer : options.OverlayDataArray) {
            if (volumeOrLayer.IsLayer()) {
                LayerCache_->Touch(volumeOrLayer.GetLayer());

                YT_LOG_DEBUG(
                    "Using layer to create overlay volume (LayerId: %v)",
                    volumeOrLayer.GetLayer()->GetMeta().Id);
            } else {
                YT_LOG_DEBUG(
                    "Using volume to create overlay volume (VolumeId: %v)",
                    volumeOrLayer.GetVolume()->GetId());
            }
        }

        auto location = LayerCache_->PickLocation();
        auto volumeMetaFuture = location->CreateOverlayVolume(
            tag,
            tagSet,
            std::move(volumeCreateTimeGuard),
            options.UserSandboxOptions,
            options.OverlayDataArray);

        // This future is intentionally uncancellable: we don't want to interrupt volume creation.
        return volumeMetaFuture
            .AsUnique()
            .Apply(BIND(
                [
                    Logger,
                    tagSet = std::move(tagSet),
                    location = std::move(location),
                    overlayDataArray = std::move(options.OverlayDataArray)
                ] (TVolumeMeta&& volumeMeta) {
                    YT_LOG_DEBUG("Created overlay volume");
                    return New<TOverlayVolume>(
                        std::move(tagSet),
                        std::move(volumeMeta),
                        std::move(location),
                        std::move(overlayDataArray));
                }))
            .ToUncancelable();
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

TFuture<IVolumeManagerPtr> CreatePortoVolumeManager(
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
            return StaticPointerCast<IVolumeManager>(std::move(volumeManager));
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
