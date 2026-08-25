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

#include <yt/yt/server/lib/nbd/block_device.h>

#include <yt/yt/server/lib/nbd/chunk/chunk_block_device.h>
#include <yt/yt/server/lib/nbd/chunk/chunk_handler.h>
#include <yt/yt/server/lib/nbd/chunk/config.h>

#include <yt/yt/server/lib/nbd/image/config.h>
#include <yt/yt/server/lib/nbd/image/image_block_device.h>
#include <yt/yt/server/lib/nbd/image/image_reader.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/data_node_nbd_service_proxy.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/library/containers/porto_executor.h>

#include <yt/yt/client/cell_master_client/public.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/cpu_clock/clock.h>

namespace NYT::NExecNode {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NContainers;
using namespace NControllerAgent;
using namespace NDataNode;
using namespace NLogging;
using namespace NYT::NNbd;
using namespace NYT::NNbd::NChunk;
using namespace NYT::NNbd::NImage;
using namespace NProfiling;
using namespace NYTree;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;
static constexpr auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

namespace {

i64 GetCapacity(const std::vector<TLayerLocationPtr>& layerLocations)
{
    i64 result = 0;
    for (const auto& location : layerLocations) {
        result += location->GetCapacity();
    }
    return result;
}

} // namespace anonymous

////////////////////////////////////////////////////////////////////////////////

TLayerLocationPtr PickLocation(
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
        .WithTag("Tag", tag)
        .WithTag("JobId", options.JobId)
        .WithTag("CypressPath", options.ArtifactKey.data_source().path());

    auto cookie = BeginInsert(artifactKey);
    auto value = cookie.GetValue();
    if (cookie.IsActive()) {
        DownloadAndPrepareVolume(artifactKey, downloadOptions, tag)
            .Subscribe(BIND([=, cookie = std::move(cookie)] (const TErrorOr<TIntrusivePtr<TCachedVolume<TArtifactKey>>>& volumeOrError) mutable {
                if (volumeOrError.IsOK()) {
                    YT_TLOG_DEBUG("Squashfs volume has been inserted into cache")
                        .With("VolumeId", volumeOrError.Value()->GetId());
                    cookie.EndInsert(volumeOrError.Value());
                } else {
                    YT_TLOG_DEBUG("Canceling insertion of Squashfs volume into cache")
                        .With(volumeOrError);
                    cookie.Cancel(volumeOrError);
                }
            })
            .Via(GetCurrentInvoker()));
    } else {
        YT_TLOG_DEBUG("Squashfs volume is either already in the cache or is being inserted")
            .With("VolumeId", value.IsSet() && value.GetOrCrash().IsOK() ? ToString(value.GetOrCrash().Value()->GetId()) : "<importing>");
    }

    return value.As<IVolumePtr>();
}

TFuture<TSquashFSVolumePtr> TSquashFSVolumeCache::DownloadAndPrepareVolume(
    const TArtifactKey& artifactKey,
    const TArtifactDownloadOptions& downloadOptions,
    TGuid tag)
{
    YT_TLOG_DEBUG("Downloading and preparing squashfs volume")
        .With("Tag", tag)
        .With("CypressPath", artifactKey.data_source().path());

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

    YT_TLOG_DEBUG("Creating squashfs volume")
        .With("Tag", tag)
        .With("SquashFSFilePath", squashFSFilePath);

    auto location = PickVolumeLocation();
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

    YT_TLOG_INFO("Created squashfs volume")
        .With("Tag", tag)
        .With("VolumeId", volume->GetId())
        .With("SquashFSFilePath", squashFSFilePath);

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
        .WithTag("Tag", tag)
        .WithTag("JobId", jobId)
        .WithTag("DeviceId", deviceId)
        .WithTag("CypressPath", artifactKey.data_source().path());

    YT_TLOG_DEBUG("Getting RO NBD volume");

    auto nbdConfig = DynamicConfigManager_->GetConfig()->ExecNode->Nbd;
    auto nbdServer = Bootstrap_->GetNbdServer();
    if (!nbdServer || !nbdConfig || !nbdConfig->Enabled) {
        auto error = TError("NBD server is not present")
            .With("device_id", deviceId)
            .With("job_id", jobId)
            .With("path", artifactKey.data_source().path())
            .With("filesystem", FromProto<ELayerFilesystem>(artifactKey.filesystem()));

        YT_TLOG_ERROR("Failed to get or create RO NBD volume")
            .With(error);
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
                        YT_TLOG_DEBUG("RO NBD volume has been inserted into cache")
                            .With("VolumeId", volumeOrError.Value()->GetId());
                        auto volume = DynamicPointerCast<TVolume>(volumeOrError.Value());
                        cookie.EndInsert(volume);
                    } else {
                        YT_TLOG_WARNING("Canceling insertion of RO NBD volume into cache")
                            .With(volumeOrError);
                        cookie.Cancel(volumeOrError);
                    }
                })
                .Via(nbdServer->GetInvoker()));
    } else {
        YT_TLOG_DEBUG("RO NBD volume is either already in the cache or is being inserted")
            .With("VolumeId", value.IsSet() && value.GetOrCrash().IsOK() ? ToString(value.GetOrCrash().Value()->GetId()) : "<importing>");
    }

    return value
        .Apply(BIND(
            [jobId, deviceId] (const TErrorOr<TVolumePtr>& volumeOrError) {
                if (!volumeOrError.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to prepare RO NBD volume")
                        .With("job_id", jobId)
                        .With("device_id", deviceId)
                        .With(volumeOrError);
                }

                return volumeOrError.Value();
            }))
        .As<IVolumePtr>();
}

TFuture<IVolumePtr> TNbdVolumeFactory::CreateVolume(
    TGuid tag,
    TPrepareRWNbdVolumeOptions options)
{
    ValidatePrepareRWNbdVolumeOptions(options);

    auto Logger = ExecNodeLogger()
        .WithTag("Tag", tag)
        .WithTag("JobId", options.JobId)
        .WithTag("DeviceId", options.DeviceId);

    // NB. RW NBD volumes are not cached.
    YT_TLOG_DEBUG("Creating RW NBD volume");

    auto volumeFuture = PrepareRWChunkNbdSession(options)
        .Apply(BIND(
            [
                tag,
                Logger,
                options,
                this,
                this_ = MakeStrong(this)
            ] (const TErrorOr<std::optional<std::tuple<NRpc::IChannelPtr, TSessionId>>>& rspOrError) mutable {
                THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

                auto& chunkOptions = GetOrCrash<TChunkNbdVolumeOptions>(options.BackendOptions);

                const auto& response = rspOrError.Value();
                if (!response) {
                    THROW_ERROR_EXCEPTION("Could not find suitable data node to host NBD disk")
                        .With("medium_index", chunkOptions.Spec.MediumIndex)
                        .With("size", options.DeviceSize)
                        .With("filesystem_type", options.FilesystemType);
                }

                const auto& [channel, sessionId] = *response;
                chunkOptions.DataNodeChannel = channel;
                chunkOptions.SessionId = sessionId;

                YT_TLOG_DEBUG("Prepared NBD session")
                    .With("SessionId", sessionId)
                    .With("MediumIndex", chunkOptions.Spec.MediumIndex)
                    .With("Size", options.DeviceSize)
                    .With("FilesystemType", options.FilesystemType);

                return PrepareRWChunkNbdVolume(tag, options);
            })).As<IVolumePtr>();

    return volumeFuture
        .Apply(BIND(
            [options] (const TErrorOr<IVolumePtr>& errorOrVolume) {
                if (!errorOrVolume.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to create RW NBD volume")
                        .With("job_id", options.JobId)
                        .With("device_id", options.DeviceId)
                        .With(errorOrVolume);
                }

                return errorOrVolume.Value();
            }));
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

template <typename TNbdVolume>
static TNbdVolumeFactory::TVolumeFactory MakeVolumeFactory()
{
    return BIND(
        [] (
            NProfiling::TTagSet tagSet,
            TVolumeMeta volumeMeta,
            TLayerLocationPtr layerLocation,
            std::string nbdDeviceId,
            INbdServerPtr nbdServer) -> IVolumePtr {

        return New<TNbdVolume>(
            std::move(tagSet),
            std::move(volumeMeta),
            std::move(layerLocation),
            std::move(nbdDeviceId),
            std::move(nbdServer));
    });
}

TNbdVolumeFactory::TInsertCookie TNbdVolumeFactory::GetInsertCookie(const std::string& deviceId, const INbdServerPtr& nbdServer)
{
    auto guard = TGuard(InsertLock_);

    auto cookie = BeginInsert(deviceId);
    if (!cookie.IsActive()) {
        // This is either a cached or a being inserted volume.
        if (auto device = nbdServer->FindDevice(deviceId)) {
            // Remove volume from cache if its device has any errors.
            if (auto error = device->GetError(); !error.IsOK()) {
                YT_TLOG_WARNING("Cached RO NBD device has errors, removing it from cache and recreating it")
                    .With(error);
                // Remove volume from cache.
                TryRemove(deviceId, /*forbidResurrection*/ true);
                // Start a new insertion.
                cookie = BeginInsert(deviceId);
            }
        }
    }

    return cookie;
}

TFuture<IBlockDevicePtr> TNbdVolumeFactory::InitializeNbdDevice(
    const IBlockDevicePtr& device,
    const NLogging::TLogger& Logger) const
{
    YT_TLOG_DEBUG("Initializing NBD device");

    return device->Initialize()
        .Apply(BIND(
            [
                Logger,
                device
            ] (const TError& error) {
                if (!error.IsOK()) {
                    // Failed to initialize device, finalize it in background.
                    YT_UNUSED_FUTURE(device->Finalize());
                    THROW_ERROR_EXCEPTION("Failed to initialize NBD device")
                        .With(error);
                }
                YT_TLOG_DEBUG("Initialized NBD device");
                return device;
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
        .WithTag("Tag", tag)
        .WithTag("JobId", options.JobId)
        .WithTag("DeviceId", options.DeviceId)
        .WithTag("IsReadOnly", options.IsReadOnly)
        .WithTag("FilesystemType", options.FilesystemType);

    YT_TLOG_DEBUG("Creating NBD volume");

    auto nbdServer = Bootstrap_->GetNbdServer();

    auto location = PickVolumeLocation();
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
                        .With(errorOrVolumeMeta);
                }

                YT_TLOG_DEBUG("Created NBD volume");

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

    YT_TLOG_DEBUG("Preparing NBD volume");

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
                        .With(errorOrDevice);
                }

                const auto& device = errorOrDevice.Value();
                Bootstrap_->GetNbdServer()->RegisterDevice(options.DeviceId, device);

                auto adjustedOptions = options;
                adjustedOptions.BlockSize = device->GetBlockSize();

                return CreateNbdVolume(
                    tag,
                    std::move(tagSet),
                    std::move(adjustedOptions),
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
                        YT_TLOG_DEBUG("Finalizing RO NBD device");
                        YT_UNUSED_FUTURE(device->Finalize());
                    } else {
                        YT_TLOG_WARNING("Failed to unregister NBD device");
                    }

                    THROW_ERROR_EXCEPTION("Failed to prepare NBD volume")
                        .With(errorOrVolume);
                }

                YT_TLOG_DEBUG("Prepared NBD volume");

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

    YT_TLOG_DEBUG("Creating NBD artifact reader")
        .With("Path", path);

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
        .WithTag("Tag", tag)
        .WithTag("JobId", options.JobId)
        .WithTag("DeviceId", deviceId)
        .WithTag("Type", "RO")
        .WithTag("CypressPath", artifactKey.data_source().path())
        .WithTag("Filesystem", FromProto<ELayerFilesystem>(artifactKey.filesystem()));

    YT_TLOG_DEBUG("Creating NBD device");

    auto device = CreateImageBlockDevice(
        deviceId,
        New<TImageBlockDeviceConfig>(),
        options.ImageReader,
        Bootstrap_->GetNbdServer()->GetInvoker(),
        Bootstrap_->GetNbdServer()->GetLogger());

    YT_TLOG_DEBUG("Created NBD device");

    return InitializeNbdDevice(device, Logger);
}

TFuture<IVolumePtr> TNbdVolumeFactory::PrepareRONbdVolume(
    TGuid tag,
    TPrepareRONbdVolumeOptions options)
{
    const auto artifactKey = options.ArtifactKey;
    const auto jobId = options.JobId;

    auto Logger = ExecNodeLogger()
        .WithTag("Tag", tag)
        .WithTag("JobId", jobId)
        .WithTag("DeviceId", artifactKey.nbd_device_id())
        .WithTag("Type", "RO")
        .WithTag("CypressPath", artifactKey.data_source().path());

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
            .FilesystemType = ToString(FromProto<ELayerFilesystem>(artifactKey.filesystem())),
            .IsReadOnly = true,
        },
        MakeVolumeFactory<TRONbdVolume>());
}

// RW NBD volumes.

TFuture<IBlockDevicePtr> TNbdVolumeFactory::CreateRWChunkNbdDevice(
    TGuid tag,
    TPrepareRWNbdVolumeOptions options)
{
    auto& chunkOptions = GetOrCrash<TChunkNbdVolumeOptions>(options.BackendOptions);

    auto Logger = ExecNodeLogger()
        .WithTag("Tag", tag)
        .WithTag("JobId", options.JobId)
        .WithTag("DeviceId", options.DeviceId)
        .WithTag("Type", "RW")
        .WithTag("DiskSize", options.DeviceSize)
        .WithTag("DiskMediumIndex", chunkOptions.Spec.MediumIndex)
        .WithTag("DiskFilesystemType", options.FilesystemType);

    auto nbdConfig = DynamicConfigManager_->GetConfig()->ExecNode->Nbd;
    if (!nbdConfig || !nbdConfig->Enabled || !nbdConfig->ReadWriteEnabled) {
        auto error = TError("RW NBD disks are disabled")
            .With("device_id", options.DeviceId)
            .With("job_id", options.JobId)
            .With("size", options.DeviceSize);

        YT_TLOG_ERROR("Failed to create RW NBD volume")
            .With(error);
        return MakeFuture<IBlockDevicePtr>(std::move(error));
    }

    auto config = New<TChunkBlockDeviceConfig>();
    config->Size = options.DeviceSize;
    config->MediumIndex = chunkOptions.Spec.MediumIndex;
    config->FsType = options.FilesystemType;
    config->DataNodeNbdServiceRpcTimeout = chunkOptions.Spec.DataNodeNbdServiceRpcTimeout;
    config->DataNodeNbdServiceMakeTimeout = chunkOptions.Spec.DataNodeNbdServiceMakeTimeout;
    config->MultiplexingParallelism = chunkOptions.Spec.MultiplexingParallelism;

    YT_TLOG_DEBUG("Creating NBD device");

    auto device = CreateChunkBlockDevice(
        std::move(options.DeviceId),
        std::move(config),
        Bootstrap_->GetDefaultInThrottler(),
        Bootstrap_->GetDefaultOutThrottler(),
        Bootstrap_->GetNbdServer()->GetInvoker(),
        std::move(chunkOptions.DataNodeChannel),
        std::move(chunkOptions.SessionId),
        Bootstrap_->GetNbdServer()->GetLogger());

    YT_TLOG_DEBUG("Created NBD device");

    return InitializeNbdDevice(device, Logger);
}

TFuture<IVolumePtr> TNbdVolumeFactory::PrepareRWChunkNbdVolume(
    TGuid tag,
    TPrepareRWNbdVolumeOptions options)
{
    const auto jobId = options.JobId;
    const auto deviceId = options.DeviceId;
    const auto filesystemType = options.FilesystemType;
    const auto& chunkOptions = GetOrCrash<TChunkNbdVolumeOptions>(options.BackendOptions);

    auto Logger = ExecNodeLogger()
        .WithTag("Tag", tag)
        .WithTag("JobId", options.JobId)
        .WithTag("DeviceId", options.DeviceId)
        .WithTag("Type", "RW")
        .WithTag("VolumeSize", options.DeviceSize)
        .WithTag("VolumeMediumIndex", chunkOptions.Spec.MediumIndex)
        .WithTag("VolumeFilesystemType", options.FilesystemType);

    auto tagSet = TTagSet({{"type", "nbd"}});

    auto deviceFuture = CreateRWChunkNbdDevice(tag, std::move(options));

    return PrepareNbdVolume(
        Logger,
        tag,
        tagSet,
        std::move(deviceFuture),
        TCreateNbdVolumeOptions{
            .JobId = jobId,
            .DeviceId = deviceId,
            .FilesystemType = ToString(filesystemType),
            .IsReadOnly = false,
        },
        MakeVolumeFactory<TRWNbdVolume>());
}

TFuture<std::vector<std::string>> TNbdVolumeFactory::FindDataNodesWithMedium(
    const TSessionId& sessionId,
    const TPrepareRWNbdVolumeOptions& options)
{
    const auto& chunkOptions = GetOrCrash<TChunkNbdVolumeOptions>(options.BackendOptions);
    if (chunkOptions.Spec.DataNodeAddress) {
        return MakeFuture<std::vector<std::string>>({*chunkOptions.Spec.DataNodeAddress});
    }

    // Create AllocateWriteTargets request.
    auto cellTag = Bootstrap_->GetConnection()->GetRandomMasterCellTagWithRoleOrThrow(NCellMasterClient::EMasterCellRole::ChunkHost);
    auto channel = Bootstrap_->GetMasterChannel(std::move(cellTag));
    TChunkServiceProxy proxy(channel);
    auto req = proxy.AllocateWriteTargets();
    req->SetTimeout(chunkOptions.Spec.MasterRpcTimeout);
    auto* subrequest = req->add_subrequests();
    ToProto(subrequest->mutable_session_id(), sessionId);
    subrequest->set_min_target_count(chunkOptions.Spec.MinDataNodeCount);
    subrequest->set_desired_target_count(chunkOptions.Spec.MaxDataNodeCount);
    subrequest->set_is_nbd_chunk(true);

    // Invoke AllocateWriteTargets request and process response.
    return req->Invoke().Apply(BIND([this, this_ = MakeStrong(this), mediumIndex = chunkOptions.Spec.MediumIndex] (const TErrorOr<TChunkServiceProxy::TRspAllocateWriteTargetsPtr>& rspOrError) {
        if (!rspOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to find suitable data nodes")
                .With("medium_index", mediumIndex)
                .With("error", rspOrError);
        }

        const auto& rsp = rspOrError.Value();
        const auto& subresponse = rsp->subresponses(0);
        if (subresponse.has_error()) {
            THROW_ERROR_EXCEPTION("Failed to find suitable data nodes")
                .With("medium_index", mediumIndex)
                .With("error", FromProto<TError>(subresponse.error()));
        }

        Bootstrap_->GetConnection()->GetNodeDirectory()->MergeFrom(rsp->node_directory());

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
    const auto& chunkOptions = GetOrCrash<TChunkNbdVolumeOptions>(options.BackendOptions);

    YT_TLOG_DEBUG("Trying to open NBD session on any suitable data node")
        .With("SessionId", sessionId)
        .With("DataNodeAddresses", addresses)
        .With("MediumIndex", chunkOptions.Spec.MediumIndex)
        .With("Size", options.DeviceSize)
        .With("FilesystemType", options.FilesystemType)
        .With("DataNodeRpcTimeout", chunkOptions.Spec.DataNodeRpcTimeout);

    for (const auto& address : addresses) {
        auto channel = Bootstrap_->GetConnection()->GetChannelFactory()->CreateChannel(address);
        if (!channel) {
            YT_TLOG_DEBUG("Failed to create channel to data node")
                .With("Address", address);
            continue;
        }

        NChunkClient::TDataNodeNbdServiceProxy proxy(channel);
        auto req = proxy.OpenSession();
        req->SetTimeout(chunkOptions.Spec.DataNodeRpcTimeout);
        ToProto(req->mutable_session_id(), sessionId);
        req->set_size(options.DeviceSize);
        req->set_fs_type(ToProto(options.FilesystemType));

        auto rspOrError = WaitFor(req->Invoke());

        if (!rspOrError.IsOK()) {
            YT_TLOG_INFO("Failed to open NBD session, skip data node")
                .With("Address", address)
                .With(rspOrError);
            continue;
        }

        YT_TLOG_INFO("Opened NBD session")
            .With("SessionId", sessionId)
            .With("DataNodeAddress", address)
            .With("MediumIndex", chunkOptions.Spec.MediumIndex)
            .With("Size", options.DeviceSize)
            .With("FilesystemType", options.FilesystemType);

        return std::make_tuple(std::move(channel), sessionId);
    }

    return std::nullopt;
}

TFuture<std::optional<std::tuple<NRpc::IChannelPtr, NYT::NChunkClient::TSessionId>>> TNbdVolumeFactory::PrepareRWChunkNbdSession(
    const TPrepareRWNbdVolumeOptions& options)
{
    const auto& chunkOptions = GetOrCrash<TChunkNbdVolumeOptions>(options.BackendOptions);

    auto sessionId = GenerateSessionId(chunkOptions.Spec.MediumIndex);

    YT_TLOG_DEBUG("Prepare NBD session")
        .With("SessionId", sessionId)
        .With("MediumIndex", chunkOptions.Spec.MediumIndex)
        .With("Size", options.DeviceSize)
        .With("FilesystemType", options.FilesystemType)
        .With("DeviceId", options.DeviceId);

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
                    const auto& chunkOptions = GetOrCrash<TChunkNbdVolumeOptions>(options.BackendOptions);
                    THROW_ERROR_EXCEPTION("No data node address suitable for NBD disk has been found")
                        .With("medium_index", chunkOptions.Spec.MediumIndex)
                        .With("size", options.DeviceSize)
                        .With("filesystem_type", options.FilesystemType);
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

            YT_TLOG_DEBUG("Loading existing cached Porto layer")
                .With("LayerId", layerMeta.Id)
                .With("ArtifactPath", layerMeta.artifact_key().data_source().path());

            auto layer = New<TLayer>(layerMeta, key, location);
            auto cookie = BeginInsert(layer->GetKey());
            if (cookie.IsActive()) {
                cookie.EndInsert(layer);
            } else {
                YT_TLOG_DEBUG("Failed to insert cached Porto layer")
                    .With("LayerId", layerMeta.Id)
                    .With("ArtifactPath", layerMeta.artifact_key().data_source().path());
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

TLayerLocationPtr TLayerCache::PickVolumeLocation() const
{
    return PickLocation(LayerLocations_, [] (const TLayerLocationPtr& candidate, const TLayerLocationPtr& current) {
        return candidate->GetVolumeCount() < current->GetVolumeCount();
    });
}

TLayerLocationPtr TLayerCache::PickRandomLocation() const
{
    // Separate locations into non-importing and importing.
    std::vector<TLayerLocationPtr> nonImportingLocations;
    std::vector<TLayerLocationPtr> importingLocations;

    for (const auto& location : LayerLocations_) {
        if (!location->IsEnabled() || location->IsFull()) {
            continue;
        }

        if (location->IsLayerImportInProgress()) {
            importingLocations.push_back(location);
        } else {
            nonImportingLocations.push_back(location);
        }
    }

    // Prefer non-importing locations, pick randomly from them.
    if (!nonImportingLocations.empty()) {
        auto index = RandomNumber<size_t>(nonImportingLocations.size());
        return nonImportingLocations[index];
    }

    // If all are importing, pick randomly from importing locations.
    if (!importingLocations.empty()) {
        auto index = RandomNumber<size_t>(importingLocations.size());
        return importingLocations[index];
    }

    // For our purposes it is all right to return unavailable location.
    if (!LayerLocations_.empty()) {
        auto index = RandomNumber<size_t>(LayerLocations_.size());
        return LayerLocations_[index];
    }

    // No location available.
    THROW_ERROR_EXCEPTION(
        NExecNode::EErrorCode::NoLayerLocationAvailable,
        "Failed to get any layer location");
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

    YT_TLOG_WARNING("Layer cache is disabled")
        .With(reason);

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

void TLayerCache::ValidatePrepareLayerOptions(const TPrepareLayerOptions& options)
{
    const auto& artifactKey = options.ArtifactKey;
    YT_VERIFY(!artifactKey.has_access_method() || FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Local);
    YT_VERIFY(!artifactKey.has_filesystem() || FromProto<ELayerFilesystem>(artifactKey.filesystem()) == ELayerFilesystem::Archive);
}

TFuture<TLayerPtr> TLayerCache::GetOrCreateLayer(
    TGuid tag,
    TPrepareLayerOptions options)
{
    ValidatePrepareLayerOptions(options);

    const auto& artifactKey = options.ArtifactKey;
    const auto& downloadOptions = options.ArtifactDownloadOptions;

    auto Logger = ExecNodeLogger()
        .WithTag("Tag", tag)
        .WithTag("JobId", options.JobId)
        .WithTag("CypressPath", options.ArtifactKey.data_source().path());

    YT_TLOG_DEBUG("Getting layer");

    auto layer = FindLayerInTmpfs(artifactKey, tag);
    if (layer) {
        YT_TLOG_DEBUG("Found layer in tmpfs cache");
        return MakeFuture(layer);
    }

    auto cookie = BeginInsert(artifactKey);
    auto value = cookie.GetValue();
    if (cookie.IsActive()) {
        DownloadAndImportLayer(artifactKey, downloadOptions, tag, nullptr)
            .Subscribe(BIND([=, cookie = std::move(cookie)] (const TErrorOr<TLayerPtr>& layerOrError) mutable {
                if (layerOrError.IsOK()) {
                    YT_TLOG_DEBUG("Layer has been inserted into cache")
                        .With("LayerId", layerOrError.Value()->GetMeta().Id);
                    cookie.EndInsert(layerOrError.Value());
                } else {
                    YT_TLOG_DEBUG("Canceling insertion of layer into cache")
                        .With(layerOrError);
                    cookie.Cancel(layerOrError);
                }
            })
            .Via(GetCurrentInvoker()));
    } else {
        YT_TLOG_DEBUG("Layer is either already in the cache or is being inserted")
            .With("LayerId", value.IsSet() && value.GetOrCrash().IsOK() ? ToString(value.GetOrCrash().Value()->GetMeta().Id) : "<importing>");
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
    YT_TLOG_DEBUG("Layer added to cache")
        .With("LayerId", layer->GetMeta().Id)
        .With("ArtifactPath", layer->GetCypressPath())
        .With("Size", layer->GetSize());
}

void TLayerCache::OnRemoved(const TLayerPtr& layer)
{
    YT_TLOG_DEBUG("Layer removed from cache")
        .With("LayerId", layer->GetMeta().Id)
        .With("ArtifactPath", layer->GetCypressPath())
        .With("Size", layer->GetSize());
}

void TLayerCache::OnWeightUpdated(i64 weightDelta)
{
    YT_TLOG_DEBUG("Layer cache weight updated")
        .With("WeightDelta", weightDelta);
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
    auto findLayer = [&] (TTmpfsLayerCachePtr& tmpfsCache, const std::string& cacheName) -> TLayerPtr {
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
        .WithTag("Tag", tag)
        .WithTag("LayerId", layerId)
        .WithTag("ArtifactPath", artifactKey.data_source().path());

    YT_TLOG_DEBUG("Start loading layer into cache")
        .With("HasTargetLocation", static_cast<bool>(location));

    auto downloadCpuStart = GetCpuInstant();
    return ArtifactCache_->DownloadArtifact(artifactKey, downloadOptions)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const IVolumeArtifactPtr& artifactChunk) mutable {
            auto downloadCpuDuration = GetCpuInstant() - downloadCpuStart;
            YT_TLOG_DEBUG("Layer artifact loaded, starting import");

            // NB(psushin): we limit number of concurrently imported layers, since this is heavy operation
            // which may delay light operations performed in the same IO thread pool inside Porto daemon.
            // PORTO-518
            NConcurrency::TAsyncSemaphoreGuard guard;
            while (!(guard = NConcurrency::TAsyncSemaphoreGuard::TryAcquire(Semaphore_))) {
                WaitFor(Semaphore_->GetReadyEvent())
                    .ThrowOnError();
            }

            if (!location) {
                location = PickLayerLocation();
            }

            // Import layer in context of container, i.e. account memory allocations to container, e.g.
            // "self" container. If container is empty, memory allocations are accounted to Porto daemon.
            TString container;
            if (location->ResidesOnTmpfs()) {
                container = "self";
            }

            auto importCpuStart = GetCpuInstant();
            auto layerMeta = WaitFor(location->ImportLayer(artifactKey, TString(artifactChunk->GetFileName()), container, layerId, tag))
                .ValueOrThrow();
            auto importCpuDuration = GetCpuInstant() - importCpuStart;

            if (downloadOptions.OnLayerDownloaded) {
                downloadOptions.OnLayerDownloaded(downloadCpuDuration, importCpuDuration);
            }

            return New<TLayer>(layerMeta, artifactKey, location);
        })
        // We must pass this action through invoker to avoid synchronous execution.
        // WaitFor calls inside this action can ruin context-switch-free handlers inside TJob.
        .AsyncVia(GetCurrentInvoker()));
}

TLayerLocationPtr TLayerCache::PickLayerLocation() const
{
    return PickLocation(LayerLocations_, [] (const TLayerLocationPtr& candidate, const TLayerLocationPtr& current) {
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
