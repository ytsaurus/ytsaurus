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

#include <yt/yt/server/lib/exec_node/helpers.h>

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
        const TJobId& jobId,
        const std::vector<TTmpfsVolumeParams>& volumes,
        const std::vector<NScheduler::TVolumeMountPtr>& volumeMounts,
        const TArtifactDownloadOptions&) override
    {
        YT_VERIFY(sandboxPath);
        // Create debug tag.
        auto tag = TGuid::Create();

        std::vector<TFuture<TTmpfsVolumeResult>> futures;
        futures.reserve(volumes.size());
        for (const auto& volume : volumes) {
            auto mountPath = GetVolumeMountPathByVolumeId(volume.VolumeId, volumeMounts);
            futures.push_back(CreateTmpfsVolume(tag, jobId, *sandboxPath, volume, mountPath));
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

    TFuture<void> RemoveVolumes(const TString& place, TDuration timeout) override
    {
        YT_LOG_DEBUG("RemoveVolumes is empty in SimpleVolumeManager (Place: %v, Timeout: %v)",
            place,
            timeout);
        return OKFuture;
    }

    TFuture<void> RemoveLayers(const TString& place, TDuration timeout) override
    {
        YT_LOG_DEBUG("RemoveLayers is empty in SimpleVolumeManager (Place: %v, Timeout: %v)",
            place,
            timeout);
        return OKFuture;
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
        const TJobId& jobId,
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

        YT_LOG_DEBUG(
            "Creating tmpfs volume (Tag: %v, JobId: %v, Config: %v)",
            tag,
            jobId,
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

    void FindTmpfsMountPathsInLocation(const std::string& locationPath, std::vector<std::string>& mountPaths) const
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

    TFuture<void> DisableLayerCache(const TError& reason) override
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        return LayerCache_->Disable(reason);
    }

    bool IsEnabled() const override
    {
        return LayerCache_->IsEnabled();
    }

    std::vector<TFuture<TOverlayData>> PrepareOverlayLayers(
        const std::vector<TArtifactKey>& artifactKeys,
        TGuid tag,
        TJobId jobId,
        const TArtifactDownloadOptions& artifactDownloadOptions)
    {
        std::vector<TFuture<TOverlayData>> overlayDataFutures;
        overlayDataFutures.reserve(artifactKeys.size());

        for (const auto& artifactKey : artifactKeys) {
            if (FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Nbd) {
                overlayDataFutures.push_back(GetOrCreateRONbdVolume(
                    tag,
                    TPrepareRONbdVolumeOptions{
                        .JobId = jobId,
                        .ArtifactKey = artifactKey,
                        .ImageReader = nullptr, // Create image reader if necessary.
                    }));
            } else if (FromProto<ELayerFilesystem>(artifactKey.filesystem()) == ELayerFilesystem::SquashFS) {
                overlayDataFutures.push_back(GetOrCreateSquashFSVolume(
                    tag,
                    TPrepareSquashFSVolumeOptions{
                        .JobId = jobId,
                        .ArtifactKey = artifactKey,
                        .ArtifactDownloadOptions = artifactDownloadOptions,
                    }));
            } else {
                overlayDataFutures.push_back(GetOrCreateLayer(
                    tag,
                    TPrepareLayerOptions{
                        .JobId = jobId,
                        .ArtifactKey = artifactKey,
                        .ArtifactDownloadOptions = artifactDownloadOptions,
                    }));
            }
        }

        return overlayDataFutures;
    }

    //! Prepare rootfs volume.
    TFuture<IVolumePtr> PrepareVolume(
        const std::vector<TArtifactKey>& artifactKeys,
        const TVolumePreparationOptions& options) override
    {
        YT_VERIFY(!artifactKeys.empty());

        auto tag = TGuid::Create();

        const auto& userSandboxOptions = options.UserSandboxOptions;

        auto Logger = ExecNodeLogger()
            .WithTag("Tag: %v, JobId: %v, ArtifactCount: %v",
                tag,
                options.JobId,
                artifactKeys.size());

        YT_LOG_DEBUG("Preparing root volume");

        if (DynamicConfig_.Acquire()->ThrowOnPrepareVolume) {
            auto error = TError(NExecNode::EErrorCode::RootVolumePreparationFailed, "Throw on prepare volume");
            YT_LOG_INFO(
                error,
                "Failed to prepare root volume");
            THROW_ERROR(error);
        }

        auto overlayDataFutures = PrepareOverlayLayers(artifactKeys, tag, options.JobId, options.ArtifactDownloadOptions);

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
            overlayDataFutures.push_back(CreateRWNbdVolume(
                tag,
                TPrepareRWNbdVolumeOptions{
                    .JobId = options.JobId,
                    .Size = data->Size,
                    .MediumIndex = data->MediumIndex,
                    .Filesystem = data->FsType,
                    .DeviceId = data->DeviceId,
                    .DataNodeChannel = {/*Channel will be filled later on.*/},
                    .SessionId = {/*SessionId will be filled later on.*/},
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
            .ToImmediatelyCancelable()
            .AsUnique()
            .Apply(BIND(
                [
                    tag,
                    jobId = options.JobId,
                    userSandboxOptions,
                    this,
                    this_ = MakeStrong(this)
                ] (std::vector<TOverlayData>&& overlayDataArray) {
                    // Now we are ready to create overlay volume. It is a light
                    // operation so we are allowed to make it uncancelable.
                    return CreateRootOverlayVolume(
                        tag,
                        TPrepareOverlayVolumeOptions{
                            .JobId = jobId,
                            .UserSandboxOptions = std::move(userSandboxOptions),
                            .OverlayDataArray = std::move(overlayDataArray)
                        })
                        .ToUncancelable();
                })
                .AsyncVia(GetCurrentInvoker()))
            .As<IVolumePtr>();
    }

    //! Prepare tmpfs volumes.
    TFuture<std::vector<TTmpfsVolumeResult>> PrepareTmpfsVolumes(
        const std::optional<TString>&,
        const TJobId& jobId,
        const std::vector<TTmpfsVolumeParams>& volumes,
        const std::vector<NScheduler::TVolumeMountPtr>&,
        const TArtifactDownloadOptions& artifactDownloadOptions) override
    {
        // Create debug tag.
        auto tag = TGuid::Create();

        std::vector<TFuture<TTmpfsVolumeResult>> futures;
        futures.reserve(volumes.size());
        for (const auto& volume : volumes) {
            // TODO: Remove call PrepareOverlayLayers (YT-27698)
            futures.push_back(AllSucceeded(PrepareOverlayLayers(
                    volume.LayerArtifactKeys,
                    tag,
                    jobId,
                    artifactDownloadOptions))
                .AsUnique()
                .Apply(BIND(
                    [
                        tag,
                        jobId,
                        volume,
                        this,
                        this_ = MakeStrong(this)
                    ] (std::vector<TOverlayData>&& overlayDataArray) mutable {
                        return CreateTmpfsVolume(tag, volume)
                            .AsUnique()
                            .Apply(BIND(
                                [
                                    tag,
                                    jobId,
                                    overlayDataArray = std::move(overlayDataArray),
                                    volumeParams = std::move(volume),
                                    this,
                                    this_ = MakeStrong(this)
                                ] (TTmpfsVolumePtr&& volume) {
                                    TTmpfsVolumeResult result;
                                    result.VolumeId = std::move(volumeParams.VolumeId);
                                    result.Index = volumeParams.Index;
                                    if (overlayDataArray.empty()) {
                                        result.Volume = std::move(volume);
                                        return MakeFuture(result);
                                    }

                                    TString placePath = NFS::JoinPaths(volume->GetPath(), "place");
                                    // TODO If an exception is thrown here, then all volumes must be properly cleaned up.
                                    return DoCreateOverlayVolume(
                                        tag,
                                        jobId,
                                        volumeParams.UserId,
                                        placePath,
                                        overlayDataArray,
                                        /* volumeForUpperLayer */ std::move(volume)
                                    )
                                        .AsUnique()
                                        .Apply(BIND([result = std::move(result)] (TOverlayVolumePtr&& volume) mutable -> TTmpfsVolumeResult {
                                            result.Volume = std::move(static_cast<IVolumePtr>(volume));
                                            return result;
                                        }))
                                        .ToUncancelable();
                                })
                            )
                            .ToUncancelable();
                    })
                    .AsyncVia(GetCurrentInvoker()))
                .ToUncancelable());
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
        auto location = LayerCache_->PickVolumeLocation();
        return location->RbindRootVolume(volume, slotPath);
    }

    //! Remove volumes planted at a given place.
    TFuture<void> RemoveVolumes(const TString& place, TDuration timeout) override
    {
        auto location = LayerCache_->PickRandomVolumeLocation();
        return BIND(
            [
                location,
                place,
                timeout
            ] {
                return location->RemoveVolumes(
                    place,
                    timeout);
            })
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    //! Remove layers planted at a given place.
    TFuture<void> RemoveLayers(const TString& place, TDuration timeout) override
    {
        auto location = LayerCache_->PickRandomLayerLocation();
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

    TFuture<TOverlayData> CreateRWNbdVolume(
        TGuid tag,
        TPrepareRWNbdVolumeOptions options)
    {
        return NbdVolumeFactory_->CreateVolume(tag, std::move(options))
            .As<TOverlayData>();
    }

    TFuture<TOverlayData> GetOrCreateSquashFSVolume(
        TGuid tag,
        TPrepareSquashFSVolumeOptions options)
    {
        return SquashFSVolumeCache_->GetOrCreateVolume(tag, std::move(options))
            .As<TOverlayData>();
    }

    TFuture<TTmpfsVolumePtr> CreateTmpfsVolume(
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
                    location = std::move(location)
                ] (TVolumeMeta&& volumeMeta) mutable {
                    return New<TTmpfsVolume>(
                        std::move(tagSet),
                        std::move(volumeMeta),
                        std::move(location));
                }))
            .ToUncancelable();
    }

    TFuture<TOverlayVolumePtr> DoCreateOverlayVolume(
        TGuid tag,
        TJobId jobId,
        int userId,
        std::optional<TString> placePath,
        std::vector<TOverlayData> overlayDataArray,
        IVolumePtr volumeForUpperLayer = nullptr,
        std::optional<int> diskSpaceLimit = std::nullopt,
        std::optional<int> inodeLimit = std::nullopt,
        bool placeInUserSlot = false)
    {
        auto tagSet = TVolumeProfilerCounters::MakeTagSet(
            /*volume type*/ "overlay",
            /*Cypress path*/ "n/a");
        TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

        auto Logger = ExecNodeLogger()
            .WithTag("Tag: %v, JobId: %v, OverlayDataArraySize: %v",
                tag,
                jobId,
                overlayDataArray.size());

        YT_LOG_DEBUG("Creating overlay volume");

        for (const auto& volumeOrLayer : overlayDataArray) {
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
                    YT_LOG_DEBUG("Created overlay volume");
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
        TPrepareOverlayVolumeOptions options)
    {
        bool placeInUserSlot = false;

        // Place overlayfs (upper and work directories) in root volume, if it is present.
        std::optional<TString> placePath;
        for (const auto& overlayData : options.OverlayDataArray) {
            if (overlayData.IsVolume() && overlayData.GetVolume()->IsRootVolume()) {
                if (placePath) {
                    THROW_ERROR_EXCEPTION("Can not have multiple root volumes in overlay volume")
                        << TErrorAttribute("first_root_volume", placePath)
                        << TErrorAttribute("second_root_volume", overlayData.GetPath());
                }
                // See PORTO-460 for "//" prefix.
           //     placePath = "//" + overlayData.GetPath();

                YT_LOG_DEBUG("Place overlay volume in NBD volume (PortoPlace: %v)",
                    placePath);
            }
        }

        const auto& userSandboxOptions = options.UserSandboxOptions;
        if (userSandboxOptions.EnableRootVolumeDiskQuota && !userSandboxOptions.SlotPath.empty() && !placePath) {
            // Plant porto place for overlay volume in user slot.
            placePath = NFS::CombinePaths(
                userSandboxOptions.SlotPath,
                GetSandboxRelPath(ESandboxKind::PortoPlace));

            YT_LOG_DEBUG("Place overlay volume in user slot (PortoPlace: %v)",
                placePath);

            placeInUserSlot = true;
            // See PORTO-460 for "//" prefix.
            //placePath = (Config_->LocationIsAbsolute ? "" : "//") + placePath.value();
        }

        std::optional<int> diskSpaceLimit;
        std::optional<int> inodeLimit;

        if (userSandboxOptions.EnableDiskQuota && userSandboxOptions.EnableRootVolumeDiskQuota) {
            if (userSandboxOptions.DiskSpaceLimit) {
                diskSpaceLimit = *userSandboxOptions.DiskSpaceLimit;
            }

            if (userSandboxOptions.InodeLimit) {
                inodeLimit = *userSandboxOptions.InodeLimit;
            }
        }

        return DoCreateOverlayVolume(
            tag,
            options.JobId,
            userSandboxOptions.UserId,
            placePath,
            std::move(options.OverlayDataArray),
            /* volumeForUpperLayer */ nullptr,
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
