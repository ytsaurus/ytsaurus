#include "volume_cache.h"

#include "bootstrap.h"
#include "helpers.h"
#include "layer_location.h"
#include "private.h"
#include "volume_counters.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/nbd/image_reader.h>
#include <yt/yt/server/lib/nbd/file_system_block_device.h>

#include <yt/yt/library/containers/porto_executor.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NExecNode {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NContainers;
using namespace NControllerAgent;
using namespace NDataNode;
using namespace NLogging;
using namespace NNbd;
using namespace NProfiling;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;
static const auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

i64 GetCacheCapacity(const std::vector<TLayerLocationPtr>& layerLocations)
{
    i64 result = 0;
    for (const auto& location : layerLocations) {
        result += location->GetCapacity();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TLayerLocationPtr DoPickLocation(
    const std::vector<TLayerLocationPtr> locations,
    std::function<bool(const TLayerLocationPtr&, const TLayerLocationPtr&)> isBetter)
{
    TLayerLocationPtr location;
    for (const auto& candidate : locations) {
        if (!candidate->IsEnabled()) {
            continue;
        }

        if (!location) {
            location = candidate;
            continue;
        }

        if (!candidate->IsFull() && isBetter(candidate, location)) {
            location = candidate;
        }
    }

    if (!location) {
        THROW_ERROR_EXCEPTION(
            NExecNode::EErrorCode::NoLayerLocationAvailable,
            "Failed to get layer location; all locations are disabled");
    }

    return location;
}

////////////////////////////////////////////////////////////////////////////////

TSquashFSVolumeCache::TSquashFSVolumeCache(
    IBootstrap* const bootstrap,
    std::vector<TLayerLocationPtr> layerLocations,
    IVolumeArtifactCachePtr artifactCache)
    : TVolumeCacheBase(
        ExecNodeProfiler().WithPrefix("/squashfs_volume_cache"),
        bootstrap,
        std::move(layerLocations))
    , ArtifactCache_(std::move(artifactCache))
{ }

TFuture<IVolumePtr> TSquashFSVolumeCache::GetOrCreateVolume(
    TGuid tag,
    const TArtifactKey& artifactKey,
    const TArtifactDownloadOptions& downloadOptions)
{
    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, CypressPath: %v",
            tag,
            artifactKey.data_source().path());

    auto cookie = BeginInsert(artifactKey);
    auto value = cookie.GetValue();
    if (cookie.IsActive()) {
        DownloadAndPrepareVolume(artifactKey, downloadOptions, tag)
            .Subscribe(BIND([=, cookie = std::move(cookie)] (const TErrorOr<TIntrusivePtr<TCachedVolume<TArtifactKey>>>& volumeOrError) mutable {
                if (volumeOrError.IsOK()) {
                    YT_LOG_DEBUG(
                        "Squashfs volume has been inserted into cache (VolumeId: %v)",
                        volumeOrError.Value()->GetId());
                    cookie.EndInsert(volumeOrError.Value());
                } else {
                    YT_LOG_DEBUG(
                        volumeOrError,
                        "Canceling insertion of Squashfs volume into cache");
                    cookie.Cancel(volumeOrError);
                }
            })
            .Via(GetCurrentInvoker()));
    } else {
        YT_LOG_DEBUG(
            "Squashfs volume is either already in the cache or is being inserted (VolumeId: %v)",
            value.IsSet() && value.Get().IsOK() ? ToString(value.Get().Value()->GetId()) : "<importing>");
    }

    return value.As<IVolumePtr>();
}

TFuture<TSquashFSVolumePtr> TSquashFSVolumeCache::DownloadAndPrepareVolume(
    const TArtifactKey& artifactKey,
    const TArtifactDownloadOptions& downloadOptions,
    TGuid tag)
{
    YT_VERIFY(!artifactKey.has_access_method() || FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Local);
    YT_VERIFY(FromProto<ELayerFilesystem>(artifactKey.filesystem()) == ELayerFilesystem::SquashFS);

    YT_LOG_DEBUG(
        "Downloading and preparing squashfs volume (Tag: %v, CypressPath: %v)",
        tag,
        artifactKey.data_source().path());

    return ArtifactCache_->DownloadArtifact(artifactKey, downloadOptions)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const IVolumeArtifactPtr& artifact) {
            auto tagSet = TVolumeProfilerCounters::MakeTagSet(
                /*volume type*/ "squashfs",
                /*Cypress path*/ "n/a");
            TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

            // We pass artifact here to later save it in SquashFS volume so that SquashFS file outlives SquashFS volume.
            return CreateSquashFSVolume(
                tag,
                std::move(tagSet),
                std::move(volumeCreateTimeGuard),
                artifactKey,
                artifact);
        }).AsyncVia(GetCurrentInvoker()));
}

TSquashFSVolumePtr TSquashFSVolumeCache::CreateSquashFSVolume(
    TGuid tag,
    TTagSet tagSet,
    TEventTimerGuard volumeCreateTimeGuard,
    const TArtifactKey& artifactKey,
    IVolumeArtifactPtr artifact)
{
    auto squashFSFilePath = artifact->GetFileName();

    YT_LOG_DEBUG(
        "Creating squashfs volume (Tag: %v, SquashFSFilePath: %v)",
        tag,
        squashFSFilePath);

    auto location = PickLocation();
    auto volumeMetaFuture = location->CreateSquashFSVolume(tag, tagSet, std::move(volumeCreateTimeGuard), artifactKey, squashFSFilePath);
    auto volumeFuture = volumeMetaFuture.AsUnique().Apply(BIND(
        [
            tagSet = std::move(tagSet),
            artifactKey,
            artifact = std::move(artifact),
            location = std::move(location)
        ] (TVolumeMeta&& volumeMeta) mutable {
        return New<TSquashFSVolume>(
            std::move(tagSet),
            std::move(volumeMeta),
            std::move(artifact),
            std::move(location),
            std::move(artifactKey));
    })).ToUncancelable();
    // This uncancelable future ensures that TSquashFSVolume object owning the volume will be created
    // and protects from Porto volume leak.

    auto volume = WaitFor(volumeFuture)
        .ValueOrThrow();

    YT_LOG_INFO(
        "Created squashfs volume (Tag: %v, VolumeId: %v, SquashFSFilePath: %v)",
        tag,
        volume->GetId(),
        squashFSFilePath);

    return volume;
}

DEFINE_REFCOUNTED_TYPE(TSquashFSVolumeCache)

////////////////////////////////////////////////////////////////////////////////

TRONbdVolumeCache::TRONbdVolumeCache(
    IBootstrap* const bootstrap,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    std::vector<TLayerLocationPtr> layerLocations)
    : TVolumeCacheBase(
        ExecNodeProfiler().WithPrefix("/ronbd_volume_cache"),
        bootstrap,
        std::move(layerLocations))
    , DynamicConfigManager_(std::move(dynamicConfigManager))
{ }

TFuture<IVolumePtr> TRONbdVolumeCache::GetOrCreateVolume(
    TGuid tag,
    TPrepareRONbdVolumeOptions options)
{
    ValidatePrepareNbdVolumeOptions(options);

    const auto artifactKey = options.ArtifactKey;
    const auto deviceId = artifactKey.nbd_device_id();
    const auto jobId = options.JobId;

    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, JobId: %v, DeviceId: %v, CypressPath: %v",
            tag,
            jobId,
            deviceId,
            artifactKey.data_source().path());

    YT_LOG_DEBUG("Getting RO NBD volume");

    auto nbdConfig = DynamicConfigManager_->GetConfig()->ExecNode->Nbd;
    auto nbdServer = Bootstrap_->GetNbdServer();
    if (!nbdServer || !nbdConfig || !nbdConfig->Enabled) {
        auto error = TError("Nbd server is not present")
            << TErrorAttribute("device_id", deviceId)
            << TErrorAttribute("job_id", jobId)
            << TErrorAttribute("path", artifactKey.data_source().path())
            << TErrorAttribute("filesystem", FromProto<ELayerFilesystem>(artifactKey.filesystem()));

        YT_LOG_ERROR(error, "Failed to get RO NBD volume");
        return MakeFuture<IVolumePtr>(std::move(error));
    }

    auto cookie = GetInsertCookie(deviceId, nbdServer);
    auto value = cookie.GetValue();

    if (cookie.IsActive()) {
        PrepareRONbdVolume(tag, std::move(options))
            .Subscribe(BIND(
                [
                    Logger = Logger,
                    cookie = std::move(cookie)
                ] (const TErrorOr<TVolumePtr>& volumeOrError) mutable {
                    if (volumeOrError.IsOK()) {
                        YT_LOG_DEBUG(
                            "RO NBD volume has been inserted into cache (VolumeId: %v)",
                            volumeOrError.Value()->GetId());
                        cookie.EndInsert(volumeOrError.Value());
                    } else {
                        YT_LOG_WARNING(
                            volumeOrError,
                            "Canceling insertion of RO NBD volume into cache");
                        cookie.Cancel(volumeOrError);
                    }
                })
                .Via(nbdServer->GetInvoker()));
    } else {
        YT_LOG_DEBUG(
            "RO NBD volume is either already in the cache or is being inserted (VolumeId: %v)",
            value.IsSet() && value.Get().IsOK() ? ToString(value.Get().Value()->GetId()) : "<importing>");
    }

    // Subscribe job for NBD device errors.
    return value
        .Apply(
            MakeJobSubscriberForDeviceErrors(
                jobId,
                deviceId,
                nbdServer,
                Logger)
            .AsyncVia(nbdServer->GetInvoker()))
        .As<IVolumePtr>();
}

void TRONbdVolumeCache::ValidatePrepareNbdVolumeOptions(const TPrepareRONbdVolumeOptions& options)
{
    const auto& artifactKey = options.ArtifactKey;
    YT_VERIFY(artifactKey.has_access_method());
    YT_VERIFY(FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Nbd);
    YT_VERIFY(artifactKey.has_filesystem());
    YT_VERIFY(artifactKey.has_nbd_device_id());
    const auto& deviceId = artifactKey.nbd_device_id();
    YT_VERIFY(!deviceId.empty());
}

TRONbdVolumeCache::TInsertCookie TRONbdVolumeCache::GetInsertCookie(const TString& deviceId, const INbdServerPtr& nbdServer)
{
    auto guard = TGuard(InsertLock_);

    auto cookie = BeginInsert(deviceId);
    if (!cookie.IsActive()) {
        // This is either a cached or a being inserted volume.
        if (auto device = nbdServer->FindDevice(deviceId)) {
            // Remove volume from cache if its device has any errors.
            if (auto error = device->GetError(); !error.IsOK()) {
                YT_LOG_WARNING(
                    error,
                    "Cached RO NBD device has errors, removing it from cache and recreating it");
                // Remove volume from cache.
                TryRemove(deviceId, /*forbidResurrection*/ true);
                // Start a new insertion.
                cookie = BeginInsert(deviceId);
            }
        }
    }

    return cookie;
}

TExtendedCallback<TRONbdVolumeCache::TVolumePtr(const TErrorOr<TRONbdVolumeCache::TVolumePtr>&)> TRONbdVolumeCache::MakeJobSubscriberForDeviceErrors(
    TJobId jobId,
    const TString& deviceId,
    const INbdServerPtr& nbdServer,
    const TLogger& Logger)
{
    return BIND_NO_PROPAGATE(
        [
            Logger,
            nbdServer,
            deviceId,
            jobId,
            this,
            this_ = MakeStrong(this)
        ] (const TErrorOr<TVolumePtr>& volumeOrError) {
            if (!volumeOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to prepare RO NBD volume")
                    << TErrorAttribute("job_id", jobId)
                    << TErrorAttribute("device_id", deviceId)
                    << volumeOrError;
            }

            auto device = nbdServer->FindDevice(deviceId);
            if (!device) {
                THROW_ERROR_EXCEPTION("Failed to find RO NBD device")
                    << TErrorAttribute("job_id", jobId)
                    << TErrorAttribute("device_id", deviceId);
            }

            YT_LOG_DEBUG("Subscribing job for NBD device errors");
            auto res = device->SubscribeForErrors(
                jobId.Underlying(),
                MakeJobInterrupter(jobId, Bootstrap_));
            if (!res) {
                THROW_ERROR_EXCEPTION("Failed to subscribe job for NBD device errors")
                    << TErrorAttribute("job_id", jobId)
                    << TErrorAttribute("device_id", deviceId);
            } else {
                YT_LOG_DEBUG("Subscribed job for NBD device errors");
            }

            return volumeOrError.Value();
    });
}

IImageReaderPtr TRONbdVolumeCache::CreateArtifactReader(
    const TLogger& Logger,
    const TArtifactKey& artifactKey)
{
    YT_VERIFY(artifactKey.has_filesystem());

    auto path = NYPath::TYPath(artifactKey.data_source().path());

    YT_LOG_DEBUG("Creating NBD artifact reader (Path: %v)",
        path);

    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs(
        artifactKey.chunk_specs().begin(),
        artifactKey.chunk_specs().end());

    auto fileReader = CreateRandomAccessFileReader(
        std::move(chunkSpecs),
        std::move(path),
        Bootstrap_->GetLayerReaderHost(),
        Bootstrap_->GetNbdServer()->GetInvoker(),
        Bootstrap_->GetNbdServer()->GetLogger());

    return CreateCypressFileImageReader(
        std::move(fileReader),
        Bootstrap_->GetNbdServer()->GetLogger());
}

TFuture<IBlockDevicePtr> TRONbdVolumeCache::CreateRONbdDevice(
    TGuid tag,
    TPrepareRONbdVolumeOptions options)
{
    const auto& artifactKey = options.ArtifactKey;
    const auto& deviceId = artifactKey.nbd_device_id();

    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, JobId: %v, DeviceId: %v, CypressPath: %v, Filesystem: %v",
            tag,
            options.JobId,
            deviceId,
            artifactKey.data_source().path(),
            FromProto<ELayerFilesystem>(artifactKey.filesystem()));

    YT_LOG_DEBUG("Creating RO NBD device");

    auto device = CreateFileSystemBlockDevice(
        deviceId,
        New<TFileSystemBlockDeviceConfig>(),
        options.ImageReader,
        Bootstrap_->GetNbdServer()->GetInvoker(),
        Bootstrap_->GetNbdServer()->GetLogger());

    return device->Initialize()
        .Apply(BIND(
            [
                Logger,
                device
            ] (const TError& error) {
                if (!error.IsOK()) {
                    YT_UNUSED_FUTURE(device->Finalize());
                    THROW_ERROR_EXCEPTION("Failed to create RO NBD device")
                        << error;
                } else {
                    YT_LOG_DEBUG("Created RO NBD device");
                    return device;
                }
            })
            .AsyncVia(Bootstrap_->GetNbdServer()->GetInvoker()))
        .ToUncancelable();
}

TFuture<TRONbdVolumePtr> TRONbdVolumeCache::CreateRONbdVolume(
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

    YT_LOG_DEBUG("Creating RO NBD volume");

    auto nbdServer = Bootstrap_->GetNbdServer();

    auto location = PickLocation();
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
                    THROW_ERROR_EXCEPTION("Failed to create RO NBD volume")
                        << errorOrVolumeMeta;
                }

                YT_LOG_DEBUG("Created RO NBD volume");

                return New<TRONbdVolume>(
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

TFuture<TRONbdVolumePtr> TRONbdVolumeCache::PrepareRONbdVolume(
    TGuid tag,
    TPrepareRONbdVolumeOptions options)
{
    auto nbdServer = Bootstrap_->GetNbdServer();
    const auto artifactKey = options.ArtifactKey;
    const auto jobId = options.JobId;

    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, JobId: %v, DeviceId: %v, CypressPath: %v",
            tag,
            jobId,
            artifactKey.nbd_device_id(),
            artifactKey.data_source().path());

    YT_LOG_DEBUG("Preparing RO NBD volume");

    if (!options.ImageReader) {
        options.ImageReader = CreateArtifactReader(
            Logger,
            artifactKey);
    }

    auto tagSet = TVolumeProfilerCounters::MakeTagSet(
        /*volume type*/ "nbd",
        /*Cypress path*/ artifactKey.data_source().path());
    TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

    return CreateRONbdDevice(tag, std::move(options))
        .Apply(BIND(
            [
                tag,
                tagSet,
                jobId,
                deviceId = artifactKey.nbd_device_id(),
                filesystem = FromProto<ELayerFilesystem>(artifactKey.filesystem()),
                this,
                this_ = MakeStrong(this)
            ] (const TErrorOr<IBlockDevicePtr>& errorOrDevice) {
                if (!errorOrDevice.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to prepare RO NBD volume")
                        << errorOrDevice;
                }

                Bootstrap_->GetNbdServer()->RegisterDevice(deviceId, errorOrDevice.Value());

                return CreateRONbdVolume(
                    tag,
                    std::move(tagSet),
                    TCreateNbdVolumeOptions{
                        .JobId = jobId,
                        .DeviceId = deviceId,
                        .Filesystem = ToString(filesystem),
                        .IsReadOnly = true
                    });
            })
            .AsyncVia(nbdServer->GetInvoker()))
        .Apply(BIND(
            [
                Logger,
                tagSet,
                nbdServer,
                deviceId = artifactKey.nbd_device_id(),
                volumeCreateTimeGuard = std::move(volumeCreateTimeGuard)
            ] (const TErrorOr<TRONbdVolumePtr>& errorOrVolume) {
                if (!errorOrVolume.IsOK()) {
                    if (auto device = nbdServer->TryUnregisterDevice(deviceId)) {
                        YT_LOG_DEBUG("Finalizing RO NBD device");
                        YT_UNUSED_FUTURE(device->Finalize());
                    } else {
                        YT_LOG_WARNING("Failed to unregister RO NBD device");
                    }

                    THROW_ERROR_EXCEPTION("Failed to prepare RO NBD volume")
                        << errorOrVolume;
                }

                YT_LOG_DEBUG("Prepared RO NBD volume");

                return errorOrVolume.Value();
            })
            .AsyncVia(nbdServer->GetInvoker()))
        .ToUncancelable();
}

DEFINE_REFCOUNTED_TYPE(TRONbdVolumeCache)

////////////////////////////////////////////////////////////////////////////////

TLayerCache::TLayerCache(
    const NDataNode::TVolumeManagerConfigPtr& config,
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr& dynamicConfigManager,
    std::vector<TLayerLocationPtr> layerLocations,
    IPortoExecutorPtr tmpfsExecutor,
    IVolumeArtifactCachePtr artifactCache,
    IInvokerPtr controlInvoker,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    IBootstrap* bootstrap)
    : TAsyncSlruCacheBase(
        CreateCacheConfig(config, layerLocations),
        ExecNodeProfiler().WithPrefix("/layer_cache"))
    , DynamicConfigManager_(dynamicConfigManager)
    , ArtifactCache_(std::move(artifactCache))
    , ControlInvoker_(std::move(controlInvoker))
    , LayerLocations_(std::move(layerLocations))
    , TmpfsExecutor_(std::move(tmpfsExecutor))
    , ProfilingExecutor_(New<NConcurrency::TPeriodicExecutor>(
        ControlInvoker_,
        BIND_NO_PROPAGATE(&TLayerCache::OnProfiling, MakeWeak(this)),
        ProfilingPeriod))
{
    auto absorbLayer = BIND(
        [=, this, this_ = MakeWeak(this)] (
            const TArtifactKey& artifactKey,
            const TArtifactDownloadOptions& downloadOptions,
            TGuid tag,
            TLayerLocationPtr location)
        {
            if (auto cache = this_.Lock()) {
                return DownloadAndImportLayer(artifactKey, downloadOptions, tag, std::move(location));
            } else {
                THROW_ERROR_EXCEPTION("Layer cache has been destroyed");
            }
        });

    RegularTmpfsLayerCache_ = New<TTmpfsLayerCache>(
        bootstrap,
        config->RegularTmpfsLayerCache,
        DynamicConfigManager_,
        ControlInvoker_,
        memoryUsageTracker,
        "regular",
        TmpfsExecutor_,
        absorbLayer);

    NirvanaTmpfsLayerCache_ = New<TTmpfsLayerCache>(
        bootstrap,
        config->NirvanaTmpfsLayerCache,
        DynamicConfigManager_,
        ControlInvoker_,
        memoryUsageTracker,
        "nirvana",
        TmpfsExecutor_,
        absorbLayer);
}

TFuture<void> TLayerCache::Initialize()
{
    Semaphore_ = New<NConcurrency::TAsyncSemaphore>(
        DynamicConfigManager_->GetConfig()->ExecNode->SlotManager->VolumeManager->LayerCache->LayerImportConcurrency);
    for (const auto& location : LayerLocations_) {
        for (const auto& layerMeta : location->GetAllLayers()) {
            TArtifactKey key;
            key.MergeFrom(layerMeta.artifact_key());

            YT_LOG_DEBUG(
                "Loading existing cached Porto layer (LayerId: %v, ArtifactPath: %v)",
                layerMeta.Id,
                layerMeta.artifact_key().data_source().path());

            auto layer = New<TLayer>(layerMeta, key, location);
            auto cookie = BeginInsert(layer->GetKey());
            if (cookie.IsActive()) {
                cookie.EndInsert(layer);
            } else {
                YT_LOG_DEBUG(
                    "Failed to insert cached Porto layer (LayerId: %v, ArtifactPath: %v)",
                    layerMeta.Id,
                    layerMeta.artifact_key().data_source().path());
            }
        }
    }

    ProfilingExecutor_->Start();

    return AllSucceeded(std::vector<TFuture<void>>{
        RegularTmpfsLayerCache_->Initialize(),
        NirvanaTmpfsLayerCache_->Initialize()
    });
}

bool TLayerCache::IsEnabled() const
{
    for (const auto& location : LayerLocations_) {
        if (location->IsEnabled()) {
            return true;
        }
    }

    return false;
}

TLayerLocationPtr TLayerCache::PickLocation()
{
    return DoPickLocation(LayerLocations_, [] (const TLayerLocationPtr& candidate, const TLayerLocationPtr& current) {
        return candidate->GetVolumeCount() < current->GetVolumeCount();
    });
}

void TLayerCache::PopulateAlerts(std::vector<TError>* alerts)
{
    for (const auto& location : LayerLocations_) {
        auto error = location->GetAlert();

        if (!error.IsOK()) {
            alerts->push_back(std::move(error));
        }
    }

    if (!IsEnabled()) {
        alerts->push_back(
            TError(
                NExecNode::EErrorCode::NoLayerLocationAvailable,
                "Layer cache is disabled"));
    }
}

TFuture<void> TLayerCache::Disable(const TError& reason)
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_WARNING(reason, "Layer cache is disabled");

    for (const auto& location : LayerLocations_) {
        location->Disable(reason, false);
    }

    return AllSucceeded(std::vector<TFuture<void>>{
        ProfilingExecutor_->Stop(),
        RegularTmpfsLayerCache_->Disable(reason, /*persistentDisable*/ false),
        NirvanaTmpfsLayerCache_->Disable(reason, /*persistentDisable*/ false)
    }).Apply(BIND([=, this, this_ = MakeStrong(this)] {
        OnProfiling();
    }));
}

TFuture<TLayerPtr> TLayerCache::PrepareLayer(
    TArtifactKey artifactKey,
    const TArtifactDownloadOptions& downloadOptions,
    TGuid tag)
{
    auto layer = FindLayerInTmpfs(artifactKey, tag);
    if (layer) {
        return MakeFuture(layer);
    }

    auto cookie = BeginInsert(artifactKey);
    auto value = cookie.GetValue();
    if (cookie.IsActive()) {
        DownloadAndImportLayer(artifactKey, downloadOptions, tag, nullptr)
            .Subscribe(BIND([=, cookie = std::move(cookie)] (const TErrorOr<TLayerPtr>& layerOrError) mutable {
                if (layerOrError.IsOK()) {
                    YT_LOG_DEBUG(
                        "Layer has been inserted into cache (Tag: %v, ArtifactPath: %v, LayerId: %v)",
                        tag,
                        artifactKey.data_source().path(),
                        layerOrError.Value()->GetMeta().Id);
                    cookie.EndInsert(layerOrError.Value());
                } else {
                    YT_LOG_DEBUG(
                        layerOrError,
                        "Insert layer into cache canceled (Tag: %v, ArtifactPath: %v)",
                        tag,
                        artifactKey.data_source().path());
                    cookie.Cancel(layerOrError);
                }
            })
            .Via(GetCurrentInvoker()));
    } else {
        YT_LOG_DEBUG(
            "Layer is already being loaded into cache (Tag: %v, ArtifactPath: %v, LayerId: %v)",
            tag,
            artifactKey.data_source().path(),
            value.IsSet() && value.Get().IsOK() ? ToString(value.Get().Value()->GetMeta().Id) : "<importing>");
    }

    return value;
}

TFuture<void> TLayerCache::GetVolumeReleaseEvent()
{
    std::vector<TFuture<void>> futures;
    for (const auto& location : LayerLocations_) {
        futures.push_back(location->GetVolumeReleaseEvent());
    }

    return AllSet(std::move(futures))
        .AsVoid()
        .ToUncancelable();
}

bool TLayerCache::IsLayerCached(const TArtifactKey& artifactKey)
{
    auto layer = FindLayerInTmpfs(artifactKey);
    if (layer) {
        return true;
    }

    return Find(artifactKey) != nullptr;
}

void TLayerCache::Touch(const TLayerPtr& layer)
{
    layer->IncreaseHitCount();
    Find(layer->GetKey());
}

void TLayerCache::BuildOrchid(NYTree::TFluentAny fluent) const
{
    fluent.BeginMap()
        .Item("cached_layer_count").Value(GetSize())
        .Item("regular_tmpfs_cache").DoMap([&] (auto fluentMap) {
            RegularTmpfsLayerCache_->BuildOrchid(fluentMap);
        })
        .Item("nirvana_tmpfs_cache").DoMap([&] (auto fluentMap) {
            NirvanaTmpfsLayerCache_->BuildOrchid(fluentMap);
        })
    .EndMap();
}

void TLayerCache::OnDynamicConfigChanged(
    const TLayerCacheDynamicConfigPtr& oldConfig,
    const TLayerCacheDynamicConfigPtr& newConfig)
{
    if (*newConfig == *oldConfig) {
        return;
    }

    Semaphore_->SetTotal(newConfig->LayerImportConcurrency);

    for (const auto& location : LayerLocations_) {
        location->OnDynamicConfigChanged(oldConfig, newConfig);
    }

    TmpfsExecutor_->OnDynamicConfigChanged(newConfig->TmpfsCache->PortoExecutor);
}

TSlruCacheConfigPtr TLayerCache::CreateCacheConfig(
    const NDataNode::TVolumeManagerConfigPtr& config,
    const std::vector<TLayerLocationPtr>& layerLocations)
{
    auto cacheConfig = TSlruCacheConfig::CreateWithCapacity(
        config->EnableLayersCache
        ? static_cast<i64>(GetCacheCapacity(layerLocations) * config->CacheCapacityFraction)
        : 0,
        /*shardCount*/ 1);
    return cacheConfig;
}

i64 TLayerCache::GetWeight(const TLayerPtr& layer) const
{
    return layer->GetSize();
}

void TLayerCache::OnAdded(const TLayerPtr& layer)
{
    YT_LOG_DEBUG(
        "Layer added to cache (LayerId: %v, ArtifactPath: %v, Size: %v)",
        layer->GetMeta().Id,
        layer->GetCypressPath(),
        layer->GetSize());
}

void TLayerCache::OnRemoved(const TLayerPtr& layer)
{
    YT_LOG_DEBUG(
        "Layer removed from cache (LayerId: %v, ArtifactPath: %v, Size: %v)",
        layer->GetMeta().Id,
        layer->GetCypressPath(),
        layer->GetSize());
}

void TLayerCache::OnWeightUpdated(i64 weightDelta)
{
    YT_LOG_DEBUG("Layer cache weight updated (WeightDelta: %v)", weightDelta);
}

void TLayerCache::ProfileLocation(const TLayerLocationPtr& location)
{
    auto& performanceCounters = location->GetPerformanceCounters();

    performanceCounters.AvailableSpace.Update(location->GetAvailableSpace());
    performanceCounters.UsedSpace.Update(location->GetUsedSpace());
    performanceCounters.TotalSpace.Update(location->GetCapacity());
    performanceCounters.Full.Update(location->IsFull() ? 1 : 0);
    performanceCounters.LayerCount.Update(location->GetLayerCount());
    performanceCounters.VolumeCount.Update(location->GetVolumeCount());
}

TLayerPtr TLayerCache::FindLayerInTmpfs(const TArtifactKey& artifactKey, const TGuid& tag)
{
    auto findLayer = [&] (TTmpfsLayerCachePtr& tmpfsCache, const TString& cacheName) -> TLayerPtr {
        auto tmpfsLayer = tmpfsCache->FindLayer(artifactKey);
        if (tmpfsLayer) {
            YT_LOG_DEBUG_IF(
                tag,
                "Found layer in %v tmpfs cache (LayerId: %v, ArtifactPath: %v, Tag: %v)",
                cacheName,
                tmpfsLayer->GetMeta().Id,
                artifactKey.data_source().path(),
                tag);
            return tmpfsLayer;
        }
        return nullptr;
    };

    auto regularLayer = findLayer(RegularTmpfsLayerCache_, "regular");
    return regularLayer
        ? regularLayer
        : findLayer(NirvanaTmpfsLayerCache_, "nirvana");
}

TFuture<TLayerPtr> TLayerCache::DownloadAndImportLayer(
    const TArtifactKey& artifactKey,
    const TArtifactDownloadOptions& downloadOptions,
    TGuid tag,
    TLayerLocationPtr location)
{
    auto layerId = TLayerId::Create();

    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, LayerId: %v, ArtifactPath: %v",
            tag,
            layerId,
            artifactKey.data_source().path());

    YT_LOG_DEBUG(
        "Start loading layer into cache (HasTargetLocation: %v)",
        static_cast<bool>(location));

    return ArtifactCache_->DownloadArtifact(artifactKey, downloadOptions)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const IVolumeArtifactPtr& artifactChunk) mutable {
            YT_LOG_DEBUG("Layer artifact loaded, starting import");

            // NB(psushin): we limit number of concurrently imported layers, since this is heavy operation
            // which may delay light operations performed in the same IO thread pool inside Porto daemon.
            // PORTO-518
            NConcurrency::TAsyncSemaphoreGuard guard;
            while (!(guard = NConcurrency::TAsyncSemaphoreGuard::TryAcquire(Semaphore_))) {
                WaitFor(Semaphore_->GetReadyEvent())
                    .ThrowOnError();
            }

            if (!location) {
                location = PickLocation();
            }

            // Import layer in context of container, i.e. account memory allocations to container, e.g.
            // "self" container. If container is empty, memory allocations are accounted to Porto daemon.
            TString container;
            if (location->ResidesOnTmpfs()) {
                container = "self";
            }

            auto layerMeta = WaitFor(location->ImportLayer(artifactKey, TString(artifactChunk->GetFileName()), container, layerId, tag))
                .ValueOrThrow();
            return New<TLayer>(layerMeta, artifactKey, location);
        })
        // We must pass this action through invoker to avoid synchronous execution.
        // WaitFor calls inside this action can ruin context-switch-free handlers inside TJob.
        .AsyncVia(GetCurrentInvoker()));
}

TLayerLocationPtr TLayerCache::PickLocation() const
{
    return DoPickLocation(LayerLocations_, [] (const TLayerLocationPtr& candidate, const TLayerLocationPtr& current) {
        if (!candidate->IsLayerImportInProgress() && current->IsLayerImportInProgress()) {
            // Always prefer candidate which is not doing import right now.
            return true;
        } else if (candidate->IsLayerImportInProgress() && !current->IsLayerImportInProgress()) {
            return false;
        }

        return candidate->GetAvailableSpace() > current->GetAvailableSpace();
    });
}

void TLayerCache::OnProfiling()
{
    if (auto location = RegularTmpfsLayerCache_->GetLocation()) {
        ProfileLocation(location);
    }

    if (auto location = NirvanaTmpfsLayerCache_->GetLocation()) {
        ProfileLocation(location);
    }

    for (const auto& location : LayerLocations_) {
        ProfileLocation(location);
    }
}

DEFINE_REFCOUNTED_TYPE(TLayerCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
