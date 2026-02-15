#include "volume_manager.h"

#include "artifact.h"
#include "artifact_cache.h"
#include "volume_artifact.h"
#include "bootstrap.h"
#include "helpers.h"
#include "layer_location.h"
#include "porto_volume.h"
#include "private.h"
#include "tmpfs_layer_cache.h"
#include "volume.h"
#include "volume_cache.h"
#include "volume_counters.h"
#include "volume_options.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/private.h>
#include <yt/yt/server/node/data_node/chunk.h>
#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/node/exec_node/volume.pb.h>

#include <yt/yt/server/lib/nbd/block_device.h>
#include <yt/yt/server/lib/nbd/file_system_block_device.h>
#include <yt/yt/server/lib/nbd/image_reader.h>
#include <yt/yt/server/lib/nbd/chunk_block_device.h>
#include <yt/yt/server/lib/nbd/chunk_handler.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/misc/disk_health_checker.h>
#include <yt/yt/server/lib/misc/private.h>

#include <yt/yt/server/tools/tools.h>
#include <yt/yt/server/tools/proc.h>

#include <yt/yt/library/containers/instance.h>
#include <yt/yt/library/containers/porto_executor.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/data_node_nbd_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/chunk_client/proto/data_source.pb.h>

#include <yt/yt/ytlib/exec_node/public.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/library/profiling/tagged_counters.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/net/connection.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <library/cpp/resource/resource.h>

#include <library/cpp/yt/string/string.h>

#include <util/digest/city.h>

#include <util/string/vector.h>

#include <util/system/fs.h>

namespace NYT::NExecNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NContainers;
using namespace NLogging;
using namespace NYT::NNbd;
using namespace NNode;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NRpc;
using namespace NScheduler;
using namespace NServer;
using namespace NTools;
using namespace NYson;
using namespace NYTree;

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
        const std::vector<TTmpfsVolumeParams>& volumes) override
    {
        YT_VERIFY(sandboxPath);
        // Create debug tag.
        auto tag = TGuid::Create();

        std::vector<TFuture<TTmpfsVolumeResult>> futures;
        futures.reserve(volumes.size());
        for (const auto& volume : volumes) {
            futures.push_back(CreateTmpfsVolume(tag, *sandboxPath, volume));
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
        const std::vector<TTmpfsVolumeResult>&) override
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
        const TTmpfsVolumeParams& volume)
    {
        YT_VERIFY(sandboxPath);

        auto tagSet = TVolumeProfilerCounters::MakeTagSet(
            /*volume type*/ "tmpfs",
            /*Cypress path*/ "n/a");
        TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

        // TODO(dgolear): Switch to std::string.
        TString path = NFS::GetRealPath(NFS::CombinePaths(sandboxPath, volume.Path));

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
                volumeCreateTimeGuard = std::move(volumeCreateTimeGuard),
                config = std::move(config),
                this,
                this_ = MakeStrong(this)
            ] {
                try {
                    RunTool<TMountTmpfsAsRootTool>(config);

                    TVolumeProfilerCounters::Get()->GetGauge(tagSet, "/count")
                        .Update(VolumeCounters().Increment(tagSet));
                    TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/created").Increment(1);

                    return TTmpfsVolumeResult{
                        .Path = config->Path,
                        .Volume = New<TSimpleTmpfsVolume>(
                            tagSet,
                            config->Path,
                            Invoker_,
                            DetachUnmount_)
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

        RONbdVolumeCache_ = New<TRONbdVolumeCache>(
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
                        .ImageReader = nullptr,
                    }));
            } else if (FromProto<ELayerFilesystem>(artifactKey.filesystem()) == ELayerFilesystem::SquashFS) {
                overlayDataFutures.push_back(GetOrCreateSquashFSVolume(
                    tag,
                    artifactKey,
                    options.ArtifactDownloadOptions));
            } else {
                overlayDataFutures.push_back(PrepareLayer(
                    tag,
                    artifactKey,
                    options.ArtifactDownloadOptions));
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

        if (userSandboxOptions.SandboxNbdRootVolumeData) {
            auto future = PrepareNbdSession(*userSandboxOptions.SandboxNbdRootVolumeData)
                .Apply(BIND(
                    [
                        tag = tag,
                        jobId = options.JobId,
                        data = *userSandboxOptions.SandboxNbdRootVolumeData,
                        this,
                        this_ = MakeStrong(this)
                    ] (const TErrorOr<std::optional<std::tuple<IChannelPtr, TSessionId>>>& rspOrError) {
                        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

                        const auto& response = rspOrError.Value();
                        if (!response) {
                            THROW_ERROR_EXCEPTION("Could not find suitable data node to host NBD disk")
                                << TErrorAttribute("medium_index", data.MediumIndex)
                                << TErrorAttribute("size", data.Size)
                                << TErrorAttribute("fs_type", data.FsType);
                        }

                        const auto& [channel, sessionId] = *response;

                        YT_LOG_DEBUG(
                            "Prepared NBD session (SessionId: %v, MediumIndex: %v, Size: %v, FsType: %v, DeviceId: %v)",
                            sessionId,
                            data.MediumIndex,
                            data.Size,
                            data.FsType,
                            data.DeviceId);

                        return PrepareRWNbdVolume(
                            tag,
                            TPrepareRWNbdVolumeOptions{
                                .JobId = jobId,
                                .Size = data.Size,
                                .MediumIndex = data.MediumIndex,
                                .Filesystem = data.FsType,
                                .DeviceId = data.DeviceId,
                                .DataNodeChannel = channel,
                                .SessionId = sessionId,
                                .DataNodeNbdServiceRpcTimeout = data.DataNodeNbdServiceRpcTimeout,
                                .DataNodeNbdServiceMakeTimeout = data.DataNodeNbdServiceMakeTimeout,
                            });
                    }))
                .Apply(BIND(
                    [
                        jobId = options.JobId,
                        data = *userSandboxOptions.SandboxNbdRootVolumeData,
                        this,
                        this_ = MakeStrong(this)
                    ] (const TErrorOr<TRWNbdVolumePtr>& errorOrVolume) {
                        if (!errorOrVolume.IsOK()) {
                            THROW_ERROR_EXCEPTION("Failed to find RW NBD volume")
                                << TErrorAttribute("job_id", jobId)
                                << TErrorAttribute("device_id", data.DeviceId);
                        }

                        auto device = Bootstrap_->GetNbdServer()->FindDevice(data.DeviceId);
                        if (!device) {
                            THROW_ERROR_EXCEPTION("Failed to find RW NBD device")
                                << TErrorAttribute("job_id", jobId)
                                << TErrorAttribute("device_id", data.DeviceId);
                        }

                        YT_LOG_DEBUG("Subscribing job for RW NBD device errors");
                        auto res = device->SubscribeForErrors(
                            jobId.Underlying(),
                            MakeJobInterrupter(jobId, Bootstrap_));
                        if (!res) {
                            THROW_ERROR_EXCEPTION("Failed to subscribe job for RW NBD device errors")
                                << TErrorAttribute("job_id", jobId)
                                << TErrorAttribute("device_id", data.DeviceId);
                        }
                        YT_LOG_DEBUG("Subscribed job for RW NBD device errors");
                        return errorOrVolume.Value();
                    }))
                .As<TOverlayData>();

            overlayDataFutures.push_back(std::move(future));
        }

        // ToDo(psushin): choose proper invoker.
        // Avoid sync calls to WaitFor, to respect job preparation context switch guards.
        auto future = AllSucceeded(std::move(overlayDataFutures))
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const std::vector<TOverlayData>& overlayDataArray) {
                auto tagSet = TVolumeProfilerCounters::MakeTagSet(
                    /*volume type*/ "overlay",
                    /*Cypress path*/ "n/a");
                TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));
                return CreateOverlayVolume(
                    tag,
                    std::move(tagSet),
                    std::move(volumeCreateTimeGuard),
                    userSandboxOptions,
                    overlayDataArray);
            }).AsyncVia(GetCurrentInvoker()))
            .ToImmediatelyCancelable()
            .As<IVolumePtr>();

        return future;
    }

    //! Prepare tmpfs volumes.
    TFuture<std::vector<TTmpfsVolumeResult>> PrepareTmpfsVolumes(
        const std::optional<TString>&,
        const std::vector<TTmpfsVolumeParams>& volumes) override
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
        const std::vector<TTmpfsVolumeResult>& volumes) override
    {
         // Create debug tag.
        auto tag = TGuid::Create();

        std::vector<TFuture<void>> futures;
        futures.reserve(volumes.size());
        for (const auto& volume : volumes) {
            TString target = NFS::GetRealPath(NFS::CombinePaths(destinationDirectory, volume.Path));
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

    //! TODO(yuryalekseev): Remove me when slot rbind is removed.
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
    TRONbdVolumeCachePtr RONbdVolumeCache_;

    void BuildOrchid(NYTree::TFluentAny fluent) const override
    {
        LayerCache_->BuildOrchid(fluent);
    }

    TFuture<IBlockDevicePtr> CreateRWNbdDevice(
        TGuid tag,
        TPrepareRWNbdVolumeOptions options)
    {
        auto Logger = ExecNodeLogger()
            .WithTag("Tag: %v, JobId: %v, DeviceId: %v, DiskSize: %v, DiskMediumIndex: %v, DiskFilesystem: %v",
                tag,
                options.JobId,
                options.DeviceId,
                options.Size,
                options.MediumIndex,
                options.Filesystem);

        auto config = New<TChunkBlockDeviceConfig>();
        config->Size = options.Size;
        config->MediumIndex = options.MediumIndex;
        config->FsType = options.Filesystem;
        config->DataNodeNbdServiceRpcTimeout = options.DataNodeNbdServiceRpcTimeout;
        config->DataNodeNbdServiceMakeTimeout = options.DataNodeNbdServiceMakeTimeout;

        YT_LOG_DEBUG("Creating RW NBD device");

        auto device = CreateChunkBlockDevice(
            std::move(options.DeviceId),
            std::move(config),
            Bootstrap_->GetDefaultInThrottler(),
            Bootstrap_->GetDefaultOutThrottler(),
            Bootstrap_->GetNbdServer()->GetInvoker(),
            std::move(options.DataNodeChannel),
            std::move(options.SessionId),
            Bootstrap_->GetNbdServer()->GetLogger());

        return device->Initialize()
            .Apply(BIND(
                [
                    Logger,
                    device
                ] (const TError& error) {
                    if (!error.IsOK()) {
                        YT_UNUSED_FUTURE(device->Finalize());
                        THROW_ERROR_EXCEPTION("Failed to create RW NBD device")
                            << error;
                    } else {
                        YT_LOG_DEBUG("Created RW NBD device");
                        return device;
                    }
                })
                .AsyncVia(Bootstrap_->GetNbdServer()->GetInvoker()))
            .ToUncancelable();
    }

    //! Download and extract tar archive (tar layer).
    TFuture<TOverlayData> PrepareLayer(
        TGuid tag,
        const TArtifactKey& artifactKey,
        const TArtifactDownloadOptions& downloadOptions)
    {
        YT_LOG_DEBUG(
            "Prepare layer (Tag: %v, CypressPath: %v)",
            tag,
            artifactKey.data_source().path());

        YT_VERIFY(!artifactKey.has_access_method() || FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Local);
        YT_VERIFY(!artifactKey.has_filesystem() || FromProto<ELayerFilesystem>(artifactKey.filesystem()) == ELayerFilesystem::Archive);

        return LayerCache_->PrepareLayer(artifactKey, downloadOptions, tag).As<TOverlayData>();
    }

    //! Create RW NBD volume. The order of creation is as follows:
    //! 1. Create RW NBD device.
    //! 2. Register RW NBD device with NBD server.
    //! 3. Create RW NBD porto volume connected to RW NBD device.
    TFuture<TRWNbdVolumePtr> PrepareRWNbdVolume(
        TGuid tag,
        TPrepareRWNbdVolumeOptions options)
    {
        const auto jobId = options.JobId;
        const auto deviceId = options.DeviceId;
        const auto filesystem = options.Filesystem;
        auto nbdServer = Bootstrap_->GetNbdServer();

        auto Logger = ExecNodeLogger()
            .WithTag("Tag: %v, JobId: %v, DeviceId: %v, VolumeSize: %v, VolumeMediumIndex: %v, VolumeFilesystem: %v",
                tag,
                options.JobId,
                options.DeviceId,
                options.Size,
                options.MediumIndex,
                options.Filesystem);

        YT_LOG_DEBUG("Preparing RW NBD volume");

        auto tagSet = TTagSet({{"type", "nbd"}});
        TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

        return CreateRWNbdDevice(tag, std::move(options))
            .Apply(BIND(
                [
                    tag,
                    tagSet,
                    jobId,
                    deviceId,
                    filesystem = filesystem,
                    this,
                    this_ = MakeStrong(this)
                ] (const TErrorOr<IBlockDevicePtr>& errorOrDevice) {
                    if (!errorOrDevice.IsOK()) {
                        THROW_ERROR_EXCEPTION("Failed to prepare RW NBD volume")
                            << errorOrDevice;
                    }

                    Bootstrap_->GetNbdServer()->RegisterDevice(deviceId, errorOrDevice.Value());

                    return CreateRWNbdVolume(
                        tag,
                        std::move(tagSet),
                        TCreateNbdVolumeOptions{
                            .JobId = jobId,
                            .DeviceId = deviceId,
                            .Filesystem = ToString(filesystem),
                            .IsReadOnly = false
                        });
                })
                .AsyncVia(nbdServer->GetInvoker()))
            .Apply(BIND(
                [
                    Logger,
                    tagSet,
                    nbdServer,
                    deviceId,
                    volumeCreateTimeGuard = std::move(volumeCreateTimeGuard)
                ] (const TErrorOr<TRWNbdVolumePtr>& errorOrVolume) {
                    if (!errorOrVolume.IsOK()) {
                        if (auto device = nbdServer->TryUnregisterDevice(deviceId)) {
                            YT_LOG_DEBUG("Finalizing RW NBD device");
                            YT_UNUSED_FUTURE(device->Finalize());
                        } else {
                            YT_LOG_WARNING("Failed to unregister RW NBD device");
                        }

                        THROW_ERROR_EXCEPTION("Failed to prepare RW NBD volume")
                            << errorOrVolume;
                    }

                    YT_LOG_DEBUG("Prepared RW NBD volume");

                    return errorOrVolume.Value();
                })
                .AsyncVia(Bootstrap_->GetNbdServer()->GetInvoker()))
            .ToUncancelable();
    }

    TFuture<TOverlayData> GetOrCreateRONbdVolume(
        TGuid tag,
        TPrepareRONbdVolumeOptions options)
    {
        return RONbdVolumeCache_->GetOrCreateVolume(tag, std::move(options))
            .As<TOverlayData>();
    }

    //! Download SquashFS file and create volume from it.
    TFuture<TOverlayData> GetOrCreateSquashFSVolume(
        TGuid tag,
        const TArtifactKey& artifactKey,
        const TArtifactDownloadOptions& downloadOptions)
    {
        YT_VERIFY(!artifactKey.has_access_method() || FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Local);
        YT_VERIFY(FromProto<ELayerFilesystem>(artifactKey.filesystem()) == ELayerFilesystem::SquashFS);

        return SquashFSVolumeCache_->GetOrCreateVolume(tag, artifactKey, downloadOptions)
            .As<TOverlayData>();
    }

    TFuture<TTmpfsVolumeResult> CreateTmpfsVolume(
        TGuid tag,
        const TTmpfsVolumeParams& volumeParams)
    {
        YT_LOG_INFO(
            "Creating tmpfs volume (Tag: %v, Path: %v, Size: %v, UserId: %v)",
            tag,
            volumeParams.Path,
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
                    tmpfsPath = volumeParams.Path,
                    tagSet = std::move(tagSet),
                    location = std::move(location)
                ] (TVolumeMeta&& volumeMeta) mutable {
                    TTmpfsVolumeResult result;
                    result.Path = std::move(tmpfsPath);
                    result.Volume = New<TTmpfsVolume>(
                        std::move(tagSet),
                        std::move(volumeMeta),
                        std::move(location));
                    return result;
                }))
            .ToUncancelable();
    }

    TFuture<TRWNbdVolumePtr> CreateRWNbdVolume(
        TGuid tag,
        TTagSet tagSet,
        TCreateNbdVolumeOptions options)
    {
        auto Logger = ExecNodeLogger()
            .WithTag("Tag: %v, JobId: %v, DeviceId: %v, Filesystem: %v",
                tag,
                options.JobId,
                options.DeviceId,
                options.Filesystem);

        YT_LOG_DEBUG("Creating RW NBD volume");

        auto nbdServer = Bootstrap_->GetNbdServer();

        auto location = LayerCache_->PickLocation();
        auto volumeMetaFuture = location->CreateNbdVolume(
            tag,
            tagSet,
            DynamicConfigManager_->GetConfig()->ExecNode->Nbd,
            options);

        return volumeMetaFuture
            .Apply(BIND(
                [
                    Logger,
                    tagSet = std::move(tagSet),
                    location = std::move(location),
                    deviceId = options.DeviceId,
                    nbdServer = nbdServer
                ] (const TErrorOr<TVolumeMeta>& errorOrVolumeMeta) mutable {
                    if (!errorOrVolumeMeta.IsOK()) {
                        THROW_ERROR_EXCEPTION("Failed to create RW NBD volume")
                            << errorOrVolumeMeta;
                    }

                    YT_LOG_DEBUG("Created RW NBD volume");

                    return New<TRWNbdVolume>(
                        std::move(tagSet),
                        errorOrVolumeMeta.Value(),
                        std::move(location),
                        std::move(deviceId),
                        std::move(nbdServer));
                })
                .AsyncVia(nbdServer->GetInvoker()))
            .ToUncancelable();
        // NB. ToUncancelable is needed to make sure that object owning
        // the volume will be created so there is no porto volume leak.
    }

    TOverlayVolumePtr CreateOverlayVolume(
        TGuid tag,
        TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TUserSandboxOptions& options,
        const std::vector<TOverlayData>& overlayDataArray)
    {
        YT_LOG_INFO(
            "All layers and volumes have been prepared (Tag: %v, OverlayDataArraySize: %v)",
            tag,
            overlayDataArray.size());

        YT_LOG_DEBUG(
            "Creating overlay volume (Tag: %v, OverlayDataArraySize: %v)",
            tag,
            overlayDataArray.size());

        for (const auto& volumeOrLayer : overlayDataArray) {
            if (volumeOrLayer.IsLayer()) {
                LayerCache_->Touch(volumeOrLayer.GetLayer());

                YT_LOG_DEBUG(
                    "Using layer to create new overlay volume (Tag: %v, LayerId: %v)",
                    tag,
                    volumeOrLayer.GetLayer()->GetMeta().Id);
            } else {
                YT_LOG_DEBUG(
                    "Using volume to create new overlay volume (Tag: %v, VolumeId: %v)",
                    tag,
                    volumeOrLayer.GetVolume()->GetId());
            }
        }

        auto location = LayerCache_->PickLocation();
        auto volumeMetaFuture = location->CreateOverlayVolume(
            tag,
            tagSet,
            std::move(volumeCreateTimeGuard),
            options,
            overlayDataArray);

        // This future is intentionally uncancellable: we don't want to interrupt invoked volume creation,
        // until it is completed and the OverlayVolume object is fully created.
        auto volumeFuture = volumeMetaFuture.AsUnique().Apply(BIND(
            [
                location = std::move(location),
                tagSet = std::move(tagSet),
                overlayDataArray = std::move(overlayDataArray)
            ] (TVolumeMeta&& volumeMeta) {
                return New<TOverlayVolume>(
                    std::move(tagSet),
                    std::move(volumeMeta),
                    std::move(location),
                    std::move(overlayDataArray));
            })).ToUncancelable();

        auto volume = WaitFor(volumeFuture)
            .ValueOrThrow();

        YT_LOG_DEBUG(
            "Created overlay volume (Tag: %v, VolumeId: %v)",
            tag,
            volume->GetId());

        return volume;
    }

    void PopulateAlerts(std::vector<TError>* alerts)
    {
        if (LayerCache_) {
            LayerCache_->PopulateAlerts(alerts);
        }
    }

    TFuture<std::vector<std::string>> FindDataNodesWithMedium(const TSessionId& sessionId, const TSandboxNbdRootVolumeData& data)
    {
        if (data.DataNodeAddress) {
            return MakeFuture<std::vector<std::string>>({*data.DataNodeAddress});
        }

        // Create AllocateWriteTargets request.
        auto cellTag = Bootstrap_->GetConnection()->GetRandomMasterCellTagWithRoleOrThrow(NCellMasterClient::EMasterCellRole::ChunkHost);
        auto channel = Bootstrap_->GetMasterChannel(std::move(cellTag));
        TChunkServiceProxy proxy(channel);
        auto req = proxy.AllocateWriteTargets();
        req->SetTimeout(data.MasterRpcTimeout);
        auto* subrequest = req->add_subrequests();
        ToProto(subrequest->mutable_session_id(), sessionId);
        subrequest->set_min_target_count(data.MinDataNodeCount);
        subrequest->set_desired_target_count(data.MaxDataNodeCount);
        subrequest->set_is_nbd_chunk(true);

        // Invoke AllocateWriteTargets request and process response.
        return req->Invoke().Apply(BIND([this, this_ = MakeStrong(this), mediumIndex = data.MediumIndex] (const TErrorOr<TChunkServiceProxy::TRspAllocateWriteTargetsPtr>& rspOrError) {
            if (!rspOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to find suitable data nodes")
                    << TErrorAttribute("medium_index", mediumIndex)
                    << TErrorAttribute("error", rspOrError);
            }

            const auto& rsp = rspOrError.Value();
            const auto& subresponse = rsp->subresponses(0);
            if (subresponse.has_error()) {
                THROW_ERROR_EXCEPTION("Failed to find suitable data nodes")
                    << TErrorAttribute("medium_index", mediumIndex)
                    << TErrorAttribute("error", FromProto<TError>(subresponse.error()));
            }

            // TODO(yuryalekseev): nodeDirectory->MergeFrom(response->node_directory()); ?

            auto replicas = FromProto<TChunkReplicaWithMediumList>(subresponse.replicas());
            std::vector<std::string> result;
            result.reserve(replicas.size());
            for (auto replica : replicas) {
                auto desc = Bootstrap_->GetConnection()->GetNodeDirectory()->FindDescriptor(replica.GetNodeId());
                if (!desc) {
                    continue;
                }

                result.push_back(desc->GetDefaultAddress());
            }

            return result;
        }));
    }

    //! Open NBD session on data node that can host NBD disk.
    std::optional<std::tuple<IChannelPtr, TSessionId>> TryOpenNbdSession(
        TSessionId sessionId,
        std::vector<std::string> addresses,
        TSandboxNbdRootVolumeData data)
    {
        YT_LOG_DEBUG(
            "Trying to open NBD session on any suitable data node (SessionId: %v, DataNodeAddresses: %v, MediumIndex: %v, Size: %v, FsType: %v, DataNodeRpcTimeout: %v)",
            sessionId,
            addresses,
            data.MediumIndex,
            data.Size,
            data.FsType,
            data.DataNodeRpcTimeout);

        for (const auto& address : addresses) {
            auto channel = Bootstrap_->GetConnection()->GetChannelFactory()->CreateChannel(address);
            if (!channel) {
                YT_LOG_DEBUG(
                    "Failed to create channel to data node (Address: %v)",
                    address);
                continue;
            }

            TDataNodeNbdServiceProxy proxy(channel);
            auto req = proxy.OpenSession();
            req->SetTimeout(data.DataNodeRpcTimeout);
            ToProto(req->mutable_session_id(), sessionId);
            req->set_size(data.Size);
            req->set_fs_type(ToProto(data.FsType));

            auto rspOrError = WaitFor(req->Invoke());

            if (!rspOrError.IsOK()) {
                YT_LOG_INFO(
                    rspOrError,
                    "Failed to open NBD session, skip data node (Address: %v)",
                    address);
                continue;
            }

            YT_LOG_INFO(
                "Opened NBD session (SessionId: %v, DataNodeAddress: %v, MediumIndex: %v, Size: %v, FsType: %v)",
                sessionId,
                address,
                data.MediumIndex,
                data.Size,
                data.FsType);

            return std::make_tuple(std::move(channel), sessionId);
        }

        return std::nullopt;
    }

    //! Find data node suitable to host NBD disk and open NBD session.
    TFuture<std::optional<std::tuple<IChannelPtr, TSessionId>>> PrepareNbdSession(
        const TSandboxNbdRootVolumeData& data)
    {
        auto sessionId = GenerateSessionId(data.MediumIndex);

        YT_LOG_DEBUG(
            "Prepare NBD session (SessionId: %v, MediumIndex: %v, Size: %v, FsType: %v, DeviceId: %v)",
            sessionId,
            data.MediumIndex,
            data.Size,
            data.FsType,
            data.DeviceId);

        return FindDataNodesWithMedium(sessionId, data).Apply(BIND(
            [
                this,
                this_ = MakeStrong(this),
                sessionId = sessionId,
                data = data
            ] (const TErrorOr<std::vector<std::string>>& rspOrError) mutable {
                THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

                auto dataNodeAddresses = rspOrError.Value();
                if (dataNodeAddresses.empty()) {
                    THROW_ERROR_EXCEPTION("No data node address suitable for NBD disk has been found")
                        << TErrorAttribute("medium_index", data.MediumIndex)
                        << TErrorAttribute("size", data.Size)
                        << TErrorAttribute("fs_type", data.FsType);
                }

                return BIND(
                    &TPortoVolumeManager::TryOpenNbdSession,
                    MakeStrong(this),
                    sessionId,
                    Passed(std::move(dataNodeAddresses)),
                    Passed(std::move(data)))
                    // TODO(yuryalekseev): use more appropriate invoker.
                    .AsyncVia(ControlInvoker_)
                    .Run();
        }));
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
