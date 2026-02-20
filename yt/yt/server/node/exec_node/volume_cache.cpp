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
#include <yt/yt/server/lib/nbd/chunk_block_device.h>

#include <yt/yt/library/containers/porto_executor.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/data_node_nbd_service_proxy.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/client/cell_master_client/public.h>

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
using namespace NYT::NNbd;
using namespace NProfiling;
using namespace NYTree;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;
static const auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

static i64 GetCapacity(const std::vector<TLayerLocationPtr>& layerLocations)
{
    i64 result = 0;
    for (const auto& location : layerLocations) {
        result += location->GetCapacity();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TLayerLocationPtr DoPickLocation(
    const std::vector<TLayerLocationPtr>& locations,
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
    TPrepareSquashFSVolumeOptions options)
{
    const auto& artifactKey = options.ArtifactKey;
    const auto& downloadOptions = options.ArtifactDownloadOptions;

    YT_VERIFY(!artifactKey.has_access_method() || FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Local);
    YT_VERIFY(FromProto<ELayerFilesystem>(artifactKey.filesystem()) == ELayerFilesystem::SquashFS);

    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, JobId: %v, CypressPath: %v",
            tag,
            options.JobId,
            options.ArtifactKey.data_source().path());

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

TNbdVolumeFactory::TNbdVolumeFactory(
    IBootstrap* const bootstrap,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    std::vector<TLayerLocationPtr> layerLocations)
    : TVolumeCacheBase(
        ExecNodeProfiler().WithPrefix("/ronbd_volume_cache"),
        bootstrap,
        std::move(layerLocations))
    , DynamicConfigManager_(std::move(dynamicConfigManager))
{ }

TFuture<IVolumePtr> TNbdVolumeFactory::GetOrCreateVolume(
    TGuid tag,
    TPrepareRONbdVolumeOptions options)
{
    ValidatePrepareRONbdVolumeOptions(options);

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
                ] (const TErrorOr<IVolumePtr>& volumeOrError) mutable {
                    if (volumeOrError.IsOK()) {
                        YT_LOG_DEBUG(
                            "RO NBD volume has been inserted into cache (VolumeId: %v)",
                            volumeOrError.Value()->GetId());
                        auto volume = DynamicPointerCast<TVolume>(volumeOrError.Value());
                        cookie.EndInsert(volume);
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

//! This method creates RW NBD volumes.
TFuture<IVolumePtr> TNbdVolumeFactory::GetOrCreateVolume(
    TGuid tag,
    TPrepareRWNbdVolumeOptions options)
{
    ValidatePrepareRWNbdVolumeOptions(options);

    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, JobId: %v, DeviceId: %v",
            tag,
            options.JobId,
            options.DeviceId);

    // NB. RW NBD volumes are not cached.
    YT_LOG_DEBUG("Creating RW NBD volume");

    return PrepareNbdSession(options)
        .Apply(BIND(
            [
                tag,
                Logger,
                options,
                this,
                this_ = MakeStrong(this)
            ] (const TErrorOr<std::optional<std::tuple<NRpc::IChannelPtr, TSessionId>>>& rspOrError) mutable {
                THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

                const auto& response = rspOrError.Value();
                if (!response) {
                    THROW_ERROR_EXCEPTION("Could not find suitable data node to host NBD disk")
                        << TErrorAttribute("medium_index", options.MediumIndex)
                        << TErrorAttribute("size", options.Size)
                        << TErrorAttribute("fs_type", options.Filesystem);
                }

                const auto& [channel, sessionId] = *response;
                options.DataNodeChannel = channel;
                options.SessionId = sessionId;

                YT_LOG_DEBUG(
                    "Prepared NBD session (SessionId: %v, MediumIndex: %v, Size: %v, FsType: %v)",
                    sessionId,
                    options.MediumIndex,
                    options.Size,
                    options.Filesystem);

                return PrepareRWNbdVolume(tag, options);
            }))
        .Apply(BIND(
            [
                Logger,
                options,
                this,
                this_ = MakeStrong(this)
            ] (const TErrorOr<IVolumePtr>& errorOrVolume) {
                if (!errorOrVolume.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to find RW NBD volume")
                        << TErrorAttribute("job_id", options.JobId)
                        << TErrorAttribute("device_id", options.DeviceId);
                }

                auto device = Bootstrap_->GetNbdServer()->FindDevice(options.DeviceId);
                if (!device) {
                    THROW_ERROR_EXCEPTION("Failed to find RW NBD device")
                        << TErrorAttribute("job_id", options.JobId)
                        << TErrorAttribute("device_id", options.DeviceId);
                }

                YT_LOG_DEBUG("Subscribing job for RW NBD device errors");
                auto res = device->SubscribeForErrors(
                    options.JobId.Underlying(),
                    MakeJobInterrupter(options.JobId, Bootstrap_));
                if (!res) {
                    THROW_ERROR_EXCEPTION("Failed to subscribe job for RW NBD device errors")
                        << TErrorAttribute("job_id", options.JobId)
                        << TErrorAttribute("device_id", options.DeviceId);
                }
                YT_LOG_DEBUG("Subscribed job for RW NBD device errors");
                return errorOrVolume.Value();
            })
        ).As<IVolumePtr>();
}

void TNbdVolumeFactory::ValidatePrepareRONbdVolumeOptions(const TPrepareRONbdVolumeOptions& options)
{
    const auto& artifactKey = options.ArtifactKey;
    YT_VERIFY(artifactKey.has_access_method());
    YT_VERIFY(FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Nbd);
    YT_VERIFY(artifactKey.has_filesystem());
    YT_VERIFY(artifactKey.has_nbd_device_id());
    const auto& deviceId = artifactKey.nbd_device_id();
    YT_VERIFY(!deviceId.empty());
}

void TNbdVolumeFactory::ValidatePrepareRWNbdVolumeOptions(const TPrepareRWNbdVolumeOptions&)
{ }

TNbdVolumeFactory::TInsertCookie TNbdVolumeFactory::GetInsertCookie(const TString& deviceId, const INbdServerPtr& nbdServer)
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

TExtendedCallback<TNbdVolumeFactory::TVolumePtr(const TErrorOr<TNbdVolumeFactory::TVolumePtr>&)> TNbdVolumeFactory::MakeJobSubscriberForDeviceErrors(
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

TFuture<IBlockDevicePtr> TNbdVolumeFactory::InitializeNbdDevice(
    const IBlockDevicePtr& device,
    const NLogging::TLogger& Logger) const
{
    YT_LOG_DEBUG("Initializing NBD device");

    return device->Initialize()
        .Apply(BIND(
            [
                Logger,
                device
            ] (const TError& error) {
                if (!error.IsOK()) {
                    YT_UNUSED_FUTURE(device->Finalize());
                    THROW_ERROR_EXCEPTION("Failed to initialize NBD device")
                        << error;
                } else {
                    YT_LOG_DEBUG("Initialized NBD device");
                    return device;
                }
            })
            .AsyncVia(Bootstrap_->GetNbdServer()->GetInvoker()))
        .ToUncancelable();
}

TFuture<IVolumePtr> TNbdVolumeFactory::CreateNbdVolume(
    TGuid tag,
    TTagSet tagSet,
    TCreateNbdVolumeOptions options,
    TVolumeFactory volumeFactory)
{
    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, JobId: %v, DeviceId: %v, IsReadOnly: %v, Filesystem: %v",
            tag,
            options.JobId,
            options.DeviceId,
            options.IsReadOnly,
            options.Filesystem);

    YT_LOG_DEBUG("Creating NBD volume");

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
                volumeFactory = std::move(volumeFactory),
                tagSet = std::move(tagSet),
                location = std::move(location),
                deviceId = options.DeviceId,
                nbdServer = nbdServer
            ] (const TErrorOr<TVolumeMeta>& errorOrVolumeMeta) mutable {
                if (!errorOrVolumeMeta.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to create NBD volume")
                        << errorOrVolumeMeta;
                }

                YT_LOG_DEBUG("Created NBD volume");

                return volumeFactory(
                    std::move(tagSet),
                    errorOrVolumeMeta.Value(),
                    std::move(location),
                    std::move(deviceId),
                    std::move(nbdServer));
            })
            .AsyncVia(nbdServer->GetInvoker()))
        .ToUncancelable()
        .As<IVolumePtr>();
    // NB. ToUncancelable is needed to make sure that object owning
    // the volume will be created so there is no porto volume leak.
}

TFuture<IVolumePtr> TNbdVolumeFactory::PrepareNbdVolume(
    const TLogger& Logger,
    TGuid tag,
    TTagSet tagSet,
    TFuture<IBlockDevicePtr> deviceFuture,
    TCreateNbdVolumeOptions options,
    TVolumeFactory volumeFactory)
{
    auto nbdServer = Bootstrap_->GetNbdServer();

    YT_LOG_DEBUG("Preparing NBD volume");

    TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

    return deviceFuture
        .Apply(BIND(
            [
                Logger,
                tag,
                tagSet,
                options,
                volumeFactory = std::move(volumeFactory),
                this,
                this_ = MakeStrong(this)
            ] (const TErrorOr<IBlockDevicePtr>& errorOrDevice) {
                if (!errorOrDevice.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to prepare NBD volume")
                        << errorOrDevice;
                }

                Bootstrap_->GetNbdServer()->RegisterDevice(options.DeviceId, errorOrDevice.Value());

                return CreateNbdVolume(
                    tag,
                    std::move(tagSet),
                    std::move(options),
                    std::move(volumeFactory));
            })
            .AsyncVia(nbdServer->GetInvoker()))
        .Apply(BIND(
            [
                Logger,
                tagSet,
                nbdServer,
                options,
                volumeCreateTimeGuard = std::move(volumeCreateTimeGuard)
            ] (const TErrorOr<IVolumePtr>& errorOrVolume) {
                if (!errorOrVolume.IsOK()) {
                    if (auto device = nbdServer->TryUnregisterDevice(options.DeviceId)) {
                        YT_LOG_DEBUG("Finalizing NBD device");
                        YT_UNUSED_FUTURE(device->Finalize());
                    } else {
                        YT_LOG_WARNING("Failed to unregister NBD device");
                    }

                    THROW_ERROR_EXCEPTION("Failed to prepare NBD volume")
                        << errorOrVolume;
                }

                YT_LOG_DEBUG("Prepared NBD volume");

                return errorOrVolume.Value();
            })
            .AsyncVia(nbdServer->GetInvoker()))
        .ToUncancelable();
}

// RO NBD volumes.

IImageReaderPtr TNbdVolumeFactory::CreateArtifactReader(
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

TFuture<IBlockDevicePtr> TNbdVolumeFactory::CreateRONbdDevice(
    TGuid tag,
    TPrepareRONbdVolumeOptions options)
{
    const auto& artifactKey = options.ArtifactKey;
    const auto& deviceId = artifactKey.nbd_device_id();

    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, JobId: %v, DeviceId: %v, Type: %v, CypressPath: %v, Filesystem: %v",
            tag,
            options.JobId,
            deviceId,
            "RO",
            artifactKey.data_source().path(),
            FromProto<ELayerFilesystem>(artifactKey.filesystem()));

    YT_LOG_DEBUG("Creating NBD device");

    auto device = CreateFileSystemBlockDevice(
        deviceId,
        New<TFileSystemBlockDeviceConfig>(),
        options.ImageReader,
        Bootstrap_->GetNbdServer()->GetInvoker(),
        Bootstrap_->GetNbdServer()->GetLogger());

    YT_LOG_DEBUG("Created NBD device");

    return InitializeNbdDevice(device, Logger);
}

TFuture<IVolumePtr> TNbdVolumeFactory::PrepareRONbdVolume(
    TGuid tag,
    TPrepareRONbdVolumeOptions options)
{
    const auto artifactKey = options.ArtifactKey;
    const auto jobId = options.JobId;

    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, JobId: %v, DeviceId: %v, Type: %v, CypressPath: %v",
            tag,
            jobId,
            artifactKey.nbd_device_id(),
            "RO",
            artifactKey.data_source().path());

    if (!options.ImageReader) {
        options.ImageReader = CreateArtifactReader(
            Logger,
            artifactKey);
    }

    auto tagSet = TVolumeProfilerCounters::MakeTagSet(
        /*volume type*/ "nbd",
        /*Cypress path*/ artifactKey.data_source().path());

    auto deviceFuture = CreateRONbdDevice(tag, std::move(options));

    return PrepareNbdVolume(
        Logger,
        tag,
        tagSet,
        std::move(deviceFuture),
        TCreateNbdVolumeOptions{
            .JobId = jobId,
            .DeviceId = artifactKey.nbd_device_id(),
            .Filesystem = ToString(FromProto<ELayerFilesystem>(artifactKey.filesystem())),
            .IsReadOnly = true,
        },
        MakeVolumeFactory<TRONbdVolume>());
}

// RW NBD volumes.

TFuture<IBlockDevicePtr> TNbdVolumeFactory::CreateRWNbdDevice(
    TGuid tag,
    TPrepareRWNbdVolumeOptions options)
{
    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, JobId: %v, DeviceId: %v, Type: %v, DiskSize: %v, DiskMediumIndex: %v, DiskFilesystem: %v",
            tag,
            options.JobId,
            options.DeviceId,
            "RW",
            options.Size,
            options.MediumIndex,
            options.Filesystem);

    auto config = New<TChunkBlockDeviceConfig>();
    config->Size = options.Size;
    config->MediumIndex = options.MediumIndex;
    config->FsType = options.Filesystem;
    config->DataNodeNbdServiceRpcTimeout = options.DataNodeNbdServiceRpcTimeout;
    config->DataNodeNbdServiceMakeTimeout = options.DataNodeNbdServiceMakeTimeout;

    YT_LOG_DEBUG("Creating NBD device");

    auto device = CreateChunkBlockDevice(
        std::move(options.DeviceId),
        std::move(config),
        Bootstrap_->GetDefaultInThrottler(),
        Bootstrap_->GetDefaultOutThrottler(),
        Bootstrap_->GetNbdServer()->GetInvoker(),
        std::move(options.DataNodeChannel),
        std::move(options.SessionId),
        Bootstrap_->GetNbdServer()->GetLogger());

    YT_LOG_DEBUG("Created NBD device");

    return InitializeNbdDevice(device, Logger);
}

TFuture<IVolumePtr> TNbdVolumeFactory::PrepareRWNbdVolume(
    TGuid tag,
    TPrepareRWNbdVolumeOptions options)
{
    const auto jobId = options.JobId;
    const auto deviceId = options.DeviceId;
    const auto filesystem = options.Filesystem;

    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, JobId: %v, DeviceId: %v, Type: %v, VolumeSize: %v, VolumeMediumIndex: %v, VolumeFilesystem: %v",
            tag,
            options.JobId,
            options.DeviceId,
            "RW",
            options.Size,
            options.MediumIndex,
            options.Filesystem);

    auto tagSet = TTagSet({{"type", "nbd"}});

    auto deviceFuture = CreateRWNbdDevice(tag, std::move(options));

    return PrepareNbdVolume(
        Logger,
        tag,
        tagSet,
        std::move(deviceFuture),
        TCreateNbdVolumeOptions{
            .JobId = jobId,
            .DeviceId = deviceId,
            .Filesystem = ToString(filesystem),
            .IsReadOnly = false,
        },
        MakeVolumeFactory<TRWNbdVolume>());
}


TFuture<std::vector<std::string>> TNbdVolumeFactory::FindDataNodesWithMedium(
    const TSessionId& sessionId,
    const TPrepareRWNbdVolumeOptions& options)
{
    if (options.DataNodeAddress) {
        return MakeFuture<std::vector<std::string>>({*options.DataNodeAddress});
    }

    // Create AllocateWriteTargets request.
    auto cellTag = Bootstrap_->GetConnection()->GetRandomMasterCellTagWithRoleOrThrow(NCellMasterClient::EMasterCellRole::ChunkHost);
    auto channel = Bootstrap_->GetMasterChannel(std::move(cellTag));
    TChunkServiceProxy proxy(channel);
    auto req = proxy.AllocateWriteTargets();
    req->SetTimeout(options.MasterRpcTimeout);
    auto* subrequest = req->add_subrequests();
    ToProto(subrequest->mutable_session_id(), sessionId);
    subrequest->set_min_target_count(options.MinDataNodeCount);
    subrequest->set_desired_target_count(options.MaxDataNodeCount);
    subrequest->set_is_nbd_chunk(true);

    // Invoke AllocateWriteTargets request and process response.
    return req->Invoke().Apply(BIND([this, this_ = MakeStrong(this), mediumIndex = options.MediumIndex] (const TErrorOr<TChunkServiceProxy::TRspAllocateWriteTargetsPtr>& rspOrError) {
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

std::optional<std::tuple<NRpc::IChannelPtr, NYT::NChunkClient::TSessionId>> TNbdVolumeFactory::TryOpenNbdSession(
    NYT::NChunkClient::TSessionId sessionId,
    std::vector<std::string> addresses,
    TPrepareRWNbdVolumeOptions options)
{
    YT_LOG_DEBUG(
        "Trying to open NBD session on any suitable data node (SessionId: %v, DataNodeAddresses: %v, MediumIndex: %v, Size: %v, FsType: %v, DataNodeRpcTimeout: %v)",
        sessionId,
        addresses,
        options.MediumIndex,
        options.Size,
        options.Filesystem,
        options.DataNodeRpcTimeout);

    for (const auto& address : addresses) {
        auto channel = Bootstrap_->GetConnection()->GetChannelFactory()->CreateChannel(address);
        if (!channel) {
            YT_LOG_DEBUG(
                "Failed to create channel to data node (Address: %v)",
                address);
            continue;
        }

        NChunkClient::TDataNodeNbdServiceProxy proxy(channel);
        auto req = proxy.OpenSession();
        req->SetTimeout(options.DataNodeRpcTimeout);
        ToProto(req->mutable_session_id(), sessionId);
        req->set_size(options.Size);
        req->set_fs_type(ToProto(options.Filesystem));

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
            options.MediumIndex,
            options.Size,
            options.Filesystem);

        return std::make_tuple(std::move(channel), sessionId);
    }

    return std::nullopt;
}

TFuture<std::optional<std::tuple<NRpc::IChannelPtr, NYT::NChunkClient::TSessionId>>> TNbdVolumeFactory::PrepareNbdSession(
    const TPrepareRWNbdVolumeOptions& options)
{
    auto sessionId = GenerateSessionId(options.MediumIndex);

    YT_LOG_DEBUG(
        "Prepare NBD session (SessionId: %v, MediumIndex: %v, Size: %v, FsType: %v, DeviceId: %v)",
        sessionId,
        options.MediumIndex,
        options.Size,
        options.Filesystem,
        options.DeviceId);

        return FindDataNodesWithMedium(sessionId, options)
            .Apply(BIND(
                [
                    this,
                    this_ = MakeStrong(this),
                    sessionId,
                    options
                ] (const TErrorOr<std::vector<std::string>>& rspOrError) mutable {
                    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

                    auto dataNodeAddresses = rspOrError.Value();
                    if (dataNodeAddresses.empty()) {
                        THROW_ERROR_EXCEPTION("No data node address suitable for NBD disk has been found")
                            << TErrorAttribute("medium_index", options.MediumIndex)
                            << TErrorAttribute("size", options.Size)
                            << TErrorAttribute("fs_type", options.Filesystem);
                    }

                    return BIND(
                        &TNbdVolumeFactory::TryOpenNbdSession,
                        MakeStrong(this),
                        sessionId,
                        Passed(std::move(dataNodeAddresses)),
                        options)
                    .AsyncVia(Bootstrap_->GetNbdServer()->GetInvoker())
                    .Run();
                })
                .AsyncVia(Bootstrap_->GetNbdServer()->GetInvoker()));
}

DEFINE_REFCOUNTED_TYPE(TNbdVolumeFactory)

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

void TLayerCache::ValidateTPrepareLayerOptions(const TPrepareLayerOptions& options)
{
    const auto& artifactKey = options.ArtifactKey;
    YT_VERIFY(!artifactKey.has_access_method() || FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Local);
    YT_VERIFY(!artifactKey.has_filesystem() || FromProto<ELayerFilesystem>(artifactKey.filesystem()) == ELayerFilesystem::Archive);
}

TFuture<TLayerPtr> TLayerCache::GetOrCreateLayer(
    TGuid tag,
    TPrepareLayerOptions options)
{
    ValidateTPrepareLayerOptions(options);

    const auto& artifactKey = options.ArtifactKey;
    const auto& downloadOptions = options.ArtifactDownloadOptions;

    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, JobId: %v, CypressPath: %v",
            tag,
            options.JobId,
            options.ArtifactKey.data_source().path());

    YT_LOG_DEBUG("Getting layer");

    auto layer = FindLayerInTmpfs(artifactKey, tag);
    if (layer) {
        YT_LOG_DEBUG("Found layer in tmpfs cache");
        return MakeFuture(layer);
    }

    auto cookie = BeginInsert(artifactKey);
    auto value = cookie.GetValue();
    if (cookie.IsActive()) {
        DownloadAndImportLayer(artifactKey, downloadOptions, tag, nullptr)
            .Subscribe(BIND([=, cookie = std::move(cookie)] (const TErrorOr<TLayerPtr>& layerOrError) mutable {
                if (layerOrError.IsOK()) {
                    YT_LOG_DEBUG(
                        "Layer has been inserted into cache (LayerId: %v)",
                        layerOrError.Value()->GetMeta().Id);
                    cookie.EndInsert(layerOrError.Value());
                } else {
                    YT_LOG_DEBUG(
                        layerOrError,
                        "Canceling insertion of layer into cache");
                    cookie.Cancel(layerOrError);
                }
            })
            .Via(GetCurrentInvoker()));
    } else {
        YT_LOG_DEBUG(
            "Layer is either already in the cache or is being inserted (LayerId: %v)",
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
        ? static_cast<i64>(NExecNode::GetCapacity(layerLocations) * config->CacheCapacityFraction)
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
