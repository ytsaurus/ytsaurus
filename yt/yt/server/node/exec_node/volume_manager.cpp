#include "volume_manager.h"

#include "artifact.h"
#include "artifact_cache.h"
#include "bootstrap.h"
#include "helpers.h"
#include "private.h"
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
using namespace NNbd;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NContainers;
using namespace NClusterNode;
using namespace NNode;
using namespace NLogging;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NScheduler;
using namespace NTools;
using namespace NYson;
using namespace NYTree;
using namespace NServer;
using namespace NRpc;

using NControllerAgent::ELayerAccessMethod;
using NControllerAgent::ELayerFilesystem;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;
static const auto ProfilingPeriod = TDuration::Seconds(1);

static const TString MountSuffix = "mount";

////////////////////////////////////////////////////////////////////////////////

namespace {

IImageReaderPtr CreateCypressFileImageReader(
    const TArtifactKey& artifactKey,
    TChunkReaderHostPtr readerHost,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    IInvokerPtr invoker,
    const TLogger& logger)
{
    YT_VERIFY(artifactKey.has_filesystem());

    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs(
        artifactKey.chunk_specs().begin(),
        artifactKey.chunk_specs().end());

    auto reader = CreateRandomAccessFileReader(
        std::move(chunkSpecs),
        artifactKey.data_source().path(),
        std::move(readerHost),
        std::move(inThrottler),
        std::move(outRpsThrottler),
        std::move(invoker),
        logger);

    return CreateCypressFileImageReader(
        std::move(reader),
        std::move(logger));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TVolumeArtifactAdapter
    : public IVolumeArtifact
{
public:
    TVolumeArtifactAdapter(TArtifactPtr artifact)
        : Artifact_(std::move(artifact))
    { }

    const std::string& GetFileName() const override
    {
        return Artifact_->GetFileName();
    }

private:
    TArtifactPtr Artifact_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
using TAsyncMapValueBase = TAsyncCacheValueBase<TKey, TValue>;

// NB(pogorelov): It is pretty dirty map.
// The cache shard capacity is calculated to be 1,
// so since we have the weight of each element equal to 2,
// we get that the cache does not work as a cache, but works as a ValueMap.
template <class TKey, class TValue>
class TAsyncMapBase
    : public TAsyncSlruCacheBase<TKey, TValue>
{
    using TBase = TAsyncSlruCacheBase<TKey, TValue>;
public:
    TAsyncMapBase(const TProfiler& profiler = {})
        : TBase(TSlruCacheConfig::CreateWithCapacity(0, /*shardCount*/ 1), profiler)
    { }

private:
    i64 GetWeight(const TBase::TValuePtr& /*value*/) const override
    {
        return 2;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVolumeArtifactCacheAdapter
    : public IVolumeArtifactCache
{
public:
    TVolumeArtifactCacheAdapter(TArtifactCachePtr artifactCache)
        : ArtifactCache_(artifactCache)
    { }

    TFuture<IVolumeArtifactPtr> DownloadArtifact(
        const TArtifactKey& key,
        const TArtifactDownloadOptions& artifactDownloadOptions) override
    {
        auto artifact = ArtifactCache_->DownloadArtifact(key, artifactDownloadOptions);
        return artifact.Apply(BIND([] (TArtifactPtr artifact) {
            return IVolumeArtifactPtr(New<TVolumeArtifactAdapter>(artifact));
        }));
    }

private:
    TArtifactCachePtr ArtifactCache_;
};

////////////////////////////////////////////////////////////////////////////////

IVolumeArtifactCachePtr CreateVolumeArtifactCacheAdapter(TArtifactCachePtr artifactCache)
{
    return New<TVolumeArtifactCacheAdapter>(std::move(artifactCache));
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TPortoVolumeManager)

////////////////////////////////////////////////////////////////////////////////

using TLayerId = TGuid;

//! Used for layer and for volume meta files.
struct TLayerMetaHeader
{
    ui64 Signature = ExpectedSignature;

    //! Version of layer meta format. Update every time layer meta version is updated.
    ui64 Version = ExpectedVersion;

    ui64 MetaChecksum;

    static constexpr ui64 ExpectedSignature = 0xbe17d73ce7ff9ea6ull; // YTLMH001
    static constexpr ui64 ExpectedVersion = 1;
};

struct TLayerMeta
    : public NProto::TLayerMeta
{
    std::string Path;
    TLayerId Id;
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeMeta
    : public NProto::TVolumeMeta
{
    TVolumeId Id;
    TString MountPath;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLayer)

class TOverlayData
{
public:
    TOverlayData() = default;

    explicit TOverlayData(TLayerPtr layer)
        : Variant_(std::move(layer))
    { }

    explicit TOverlayData(IVolumePtr volume)
        : Variant_(std::move(volume))
    { }

    const std::string& GetPath() const;

    bool IsLayer() const
    {
        return std::holds_alternative<TLayerPtr>(Variant_);
    }

    const TLayerPtr& GetLayer() const
    {
        return std::get<TLayerPtr>(Variant_);
    }

    bool IsVolume() const
    {
        return !IsLayer();
    }

    const IVolumePtr& GetVolume() const
    {
        return std::get<IVolumePtr>(Variant_);
    }

    TFuture<void> Remove();

private:
    std::variant<TLayerPtr, IVolumePtr> Variant_;
};

////////////////////////////////////////////////////////////////////////////////

static const TString VolumesName = "volumes";
static const TString LayersName = "porto_layers";
static const TString LayersMetaName = "layers_meta";
static const TString VolumesMetaName = "volumes_meta";

class TLayerLocation
    : public NNode::TDiskLocation
{
public:
    TLayerLocation(
        NDataNode::TLayerLocationConfigPtr locationConfig,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        TDiskHealthCheckerConfigPtr healthCheckerConfig,
        IPortoExecutorPtr volumeExecutor,
        IPortoExecutorPtr layerExecutor,
        const TString& id)
        : TDiskLocation(locationConfig, id, ExecNodeLogger())
        , Config_(locationConfig)
        , DynamicConfigManager_(dynamicConfigManager)
        , VolumeExecutor_(std::move(volumeExecutor))
        , LayerExecutor_(std::move(layerExecutor))
        , LocationQueue_(New<TActionQueue>(id))
        , VolumesPath_(NFS::CombinePaths(Config_->Path, VolumesName))
        , VolumesMetaPath_(NFS::CombinePaths(Config_->Path, VolumesMetaName))
        , LayersPath_(NFS::CombinePaths(Config_->Path, LayersName))
        , LayersMetaPath_(NFS::CombinePaths(Config_->Path, LayersMetaName))
        // If true, location is placed on a YT-specific drive, bound into container from dom0 host,
        // so it has absolute path relative to dom0 root.
        // Otherwise, location is placed inside a persistent volume, and should be treated differently.
        // More details here: PORTO-460.
        , PlacePath_((Config_->LocationIsAbsolute ? "" : "//") + Config_->Path)
    {
        auto profiler = NDataNode::LocationProfiler()
            .WithPrefix("/layer")
            .WithTag("location_id", ToString(Id_));

        PerformanceCounters_ = TLayerLocationPerformanceCounters{profiler};

        if (healthCheckerConfig) {
            HealthChecker_ = New<TDiskHealthChecker>(
                healthCheckerConfig,
                Config_->Path,
                LocationQueue_->GetInvoker(),
                Logger,
                profiler);
        }
    }

    TFuture<void> Initialize()
    {
        DynamicConfig_.Store(DynamicConfigManager_->GetConfig()->ExecNode->SlotManager->VolumeManager->LayerCache);

        return BIND(&TLayerLocation::DoInitialize, MakeStrong(this))
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<TLayerMeta> ImportLayer(const TArtifactKey& artifactKey, const TString& archivePath, const TString& container, TLayerId layerId, TGuid tag)
    {
        return BIND(&TLayerLocation::DoImportLayer, MakeStrong(this), artifactKey, archivePath, container, layerId, tag)
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<void> RemoveLayer(const TLayerId& layerId)
    {
        return BIND(&TLayerLocation::DoRemoveLayer, MakeStrong(this), layerId)
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TError GetAlert()
    {
        auto guard = Guard(SpinLock_);
        return Alert_;
    }

    TFuture<TVolumeMeta> CreateNbdVolume(
        TGuid tag,
        TTagSet tagSet,
        TNbdConfigPtr nbdConfig,
        TCreateNbdVolumeOptions options)
    {
        return BIND(
            &TLayerLocation::DoCreateNbdVolume,
            MakeStrong(this),
            tag,
            Passed(std::move(tagSet)),
            Passed(std::move(nbdConfig)),
            Passed(std::move(options)))
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<TVolumeMeta> CreateTmpfsVolume(
        TGuid tag,
        TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        TTmpfsVolumeParams tmpfsVolume)
    {
        return BIND(
            &TLayerLocation::DoCreateTmpfsVolume,
            MakeStrong(this),
            tag,
            Passed(std::move(tagSet)),
            Passed(std::move(volumeCreateTimeGuard)),
            Passed(std::move(tmpfsVolume)))
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    //! TODO(yuryalekseev): Remove me when slot rbind is removed.
    TFuture<IVolumePtr> RbindRootVolume(
        const IVolumePtr& volume,
        const TString& slotPath)
    {
        ValidateEnabled();

        THashMap<TString, TString> volumeProperties {
            {"backend", "rbind"},
            {"storage", slotPath},
        };

        return BIND([volume, slotPath, volumeProperties = std::move(volumeProperties), this, this_ = MakeStrong(this)]() {
            // TODO(dgolear): Switch to std::string.
            TString path = NFS::CombinePaths(volume->GetPath(), "slot");

            if (!NFS::Exists(path)) {
                YT_LOG_DEBUG("Creating rbind directory (Path: %v)",
                    path);

                NFS::MakeDirRecursive(path);
            }

            YT_LOG_DEBUG("Rbinding root volume (Path: %v, SlotPath: %v)",
                path,
                slotPath);

            // The rbind volume is destroyed when the passed in root volume is destroyed.
            return VolumeExecutor_->CreateVolume(path, volumeProperties);
        })
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run()
            .Apply(BIND([volume](const TString&) {
                // Just return the passed in volume.
                return volume;
            }));
    }

    TFuture<TVolumeMeta> CreateOverlayVolume(
        TGuid tag,
        TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TUserSandboxOptions& options,
        const std::vector<TOverlayData>& overlayDataArray)
    {
        return BIND(
            &TLayerLocation::DoCreateOverlayVolume,
            MakeStrong(this),
            tag,
            Passed(std::move(tagSet)),
            Passed(std::move(volumeCreateTimeGuard)),
            options,
            overlayDataArray)
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<TVolumeMeta> CreateSquashFSVolume(
        TGuid tag,
        TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TArtifactKey& artifactKey,
        const std::string& squashFSFilePath)
    {
        return BIND(
            &TLayerLocation::DoCreateSquashFSVolume,
            MakeStrong(this),
            tag,
            Passed(std::move(tagSet)),
            Passed(std::move(volumeCreateTimeGuard)),
            artifactKey,
            squashFSFilePath)
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<void> RemoveVolume(TTagSet tagSet, TVolumeId volumeId)
    {
        return BIND(&TLayerLocation::DoRemoveVolume, MakeStrong(this), std::move(tagSet), std::move(volumeId))
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run()
            .ToUncancelable();
    }

    TFuture<void> LinkVolume(
        TGuid tag,
        const TString& source,
        const TString& target)
    {
        return BIND(
            &TLayerLocation::DoLinkVolume,
            MakeStrong(this),
            tag,
            source,
            target)
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<void> UnlinkVolume(
        const TString& source,
        const TString& target)
    {
        return BIND(
            &TLayerLocation::DoUnlinkVolume,
            MakeStrong(this),
            source,
            target)
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    std::vector<TLayerMeta> GetAllLayers() const
    {
        std::vector<TLayerMeta> layers;

        auto guard = Guard(SpinLock_);
        for (const auto& [id, layer] : Layers_) {
            layers.push_back(layer);
        }
        return layers;
    }

    TFuture<void> GetVolumeReleaseEvent()
    {
        auto guard = Guard(SpinLock_);
        return VolumesReleaseEvent_
            .ToFuture()
            .ToUncancelable();
    }

    void Disable(const TError& error, bool persistentDisable = true)
    {
        // TODO(don-dron): Research and fix unconditional Disabled.
        if (State_.exchange(ELocationState::Disabled) != ELocationState::Enabled) {
            return;
        }

        YT_LOG_WARNING("Layer location disabled (Path: %v)", Config_->Path);

        if (HealthChecker_) {
            auto result = WaitFor(HealthChecker_->Stop());
            YT_LOG_WARNING_IF(!result.IsOK(), result, "Layer location health checker stopping failed");
        }

        auto guard = Guard(SpinLock_);

        Alert_ = TError(NExecNode::EErrorCode::LayerLocationDisabled, "Layer location disabled")
            << TErrorAttribute("path", Config_->Path)
            << error;

        if (persistentDisable) {
            // Save the reason in a file and exit.
            // Location will be disabled during the scan in the restarted process.
            auto lockFilePath = NFS::CombinePaths(Config_->Path, DisabledLockFileName);
            try {
                TFile file(lockFilePath, CreateAlways | WrOnly | Seq | CloseOnExec);
                TFileOutput fileOutput(file);
                fileOutput << ConvertToYsonString(error, NYson::EYsonFormat::Pretty).AsStringBuf();
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Error creating location lock file");
                // Exit anyway.
            }

            YT_LOG_ERROR(error, "Volume manager disabled; terminating");

            if (DynamicConfigManager_->GetConfig()->DataNode->AbortOnLocationDisabled) {
                YT_LOG_FATAL(error, "Volume manager disabled; terminating");
            }
        }

        AvailableSpace_ = 0;
        UsedSpace_ = 0;
        Volumes_.clear();
        Layers_.clear();

        VolumesReleaseEvent_.TrySet();

        PerformanceCounters_ = {};
    }

    TLayerLocationPerformanceCounters& GetPerformanceCounters()
    {
        return PerformanceCounters_;
    }

    int GetLayerCount() const
    {
        auto guard = Guard(SpinLock_);
        return Layers_.size();
    }

    int GetVolumeCount() const
    {
        auto guard = Guard(SpinLock_);
        return Volumes_.size();
    }

    bool IsFull()
    {
        return GetAvailableSpace() < Config_->LowWatermark;
    }

    bool IsLayerImportInProgress() const
    {
        return LayerImportsInProgress_.load() > 0;
    }

    i64 GetCapacity()
    {
        return std::max<i64>(0, UsedSpace_ + GetAvailableSpace() - Config_->LowWatermark);
    }

    i64 GetUsedSpace() const
    {
        return UsedSpace_;
    }

    i64 GetAvailableSpace()
    {
        if (!IsEnabled()) {
            return 0;
        }

        const auto& path = Config_->Path;

        try {
            auto statistics = NFS::GetDiskSpaceStatistics(path);
            AvailableSpace_ = statistics.AvailableSpace;
        } catch (const std::exception& ex) {
            auto error = TError("Failed to compute available space")
                << ex;
            Disable(error);
        }

        i64 remainingQuota = std::max(static_cast<i64>(0), GetQuota() - UsedSpace_);
        AvailableSpace_ = std::min(AvailableSpace_, remainingQuota);

        return AvailableSpace_;
    }

    bool ResidesOnTmpfs() const
    {
        return Config_->ResidesOnTmpfs;
    }

    void OnDynamicConfigChanged(
        const TLayerCacheDynamicConfigPtr& oldConfig,
        const TLayerCacheDynamicConfigPtr& newConfig)
    {
        if (*newConfig == *oldConfig) {
            return;
        }

        DynamicConfig_.Store(newConfig);

        VolumeExecutor_->OnDynamicConfigChanged(newConfig->VolumePortoExecutor);
        LayerExecutor_->OnDynamicConfigChanged(newConfig->LayerPortoExecutor);

        if (HealthChecker_) {
            HealthChecker_->Reconfigure(Config_->DiskHealthChecker->ApplyDynamic(*newConfig->DiskHealthChecker));
        }
    }

private:
    const NDataNode::TLayerLocationConfigPtr Config_;
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    TAtomicIntrusivePtr<TLayerCacheDynamicConfig> DynamicConfig_;
    const IPortoExecutorPtr VolumeExecutor_;
    const IPortoExecutorPtr LayerExecutor_;

    const TActionQueuePtr LocationQueue_ ;
    const TString VolumesPath_;
    const TString VolumesMetaPath_;
    const TString LayersPath_;
    const TString LayersMetaPath_;
    const TString PlacePath_;

    TDiskHealthCheckerPtr HealthChecker_;

    TLayerLocationPerformanceCounters PerformanceCounters_;

    std::atomic<int> LayerImportsInProgress_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    THashMap<TLayerId, TLayerMeta> Layers_;
    THashMap<TVolumeId, TVolumeMeta> Volumes_;

    TPromise<void> VolumesReleaseEvent_ = MakePromise<void>(TError());

    mutable i64 AvailableSpace_ = 0;
    i64 UsedSpace_ = 0;
    TError Alert_;

    std::string GetLayerPath(const TLayerId& id) const
    {
        return NFS::CombinePaths(LayersPath_, ToString(id));
    }

    std::string GetLayerMetaPath(const TLayerId& id) const
    {
        return NFS::CombinePaths(LayersMetaPath_, ToString(id)) + ".meta";
    }

    std::string GetVolumePath(const TVolumeId& id) const
    {
        return NFS::CombinePaths(VolumesPath_, ToString(id));
    }

    std::string GetVolumeMetaPath(const TVolumeId& id) const
    {
        return NFS::CombinePaths(VolumesMetaPath_, ToString(id)) + ".meta";
    }

    void ValidateEnabled() const
    {
        if (!IsEnabled()) {
            THROW_ERROR_EXCEPTION(
                //EErrorCode::SlotLocationDisabled,
                "Layer location at %v is disabled",
                Config_->Path);
        }
    }

    THashSet<TLayerId> LoadLayerIds()
    {
        auto fileNames = NFS::EnumerateFiles(LayersMetaPath_);
        THashSet<TGuid> fileIds;
        for (const auto& fileName : fileNames) {
            auto filePath = NFS::CombinePaths(LayersMetaPath_, fileName);
            if (fileName.ends_with(NFS::TempFileSuffix)) {
                YT_LOG_DEBUG(
                    "Remove temporary file (Path: %v)",
                    filePath);
                NFS::Remove(filePath);
                continue;
            }

            auto nameWithoutExtension = NFS::GetFileNameWithoutExtension(fileName);
            TGuid id;
            if (!TGuid::FromString(nameWithoutExtension, &id)) {
                YT_LOG_WARNING(
                    "Unrecognized file in layer location directory (Path: %v)",
                    filePath);
                continue;
            }

            fileIds.insert(id);
        }

        THashSet<TGuid> confirmedIds;
        auto layerNames = WaitFor(LayerExecutor_->ListLayers(PlacePath_))
            .ValueOrThrow();

        for (const auto& layerName : layerNames) {
            TGuid id;
            if (!TGuid::FromString(layerName, &id)) {
                YT_LOG_ERROR(
                    "Unrecognized layer name in layer location directory (LayerName: %v)",
                    layerName);
                continue;
            }

            if (!fileIds.contains(id)) {
                YT_LOG_DEBUG(
                    "Remove directory without a corresponding meta file (LayerName: %v)",
                    layerName);
                WaitFor(LayerExecutor_->RemoveLayer(layerName, PlacePath_, DynamicConfig_.Acquire()->EnableAsyncLayerRemoval))
                    .ThrowOnError();
                continue;
            }

            YT_VERIFY(confirmedIds.insert(id).second);
            YT_VERIFY(fileIds.erase(id) == 1);
        }

        for (const auto& id : fileIds) {
            auto path = GetLayerMetaPath(id);
            YT_LOG_DEBUG(
                "Remove layer meta file with no matching layer (Path: %v)",
                path);
            NFS::Remove(path);
        }

        return confirmedIds;
    }

    void LoadLayers()
    {
        auto ids = LoadLayerIds();

        for (const auto& id : ids) {
            auto metaFileName = GetLayerMetaPath(id);

            TFile metaFile(
                metaFileName,
                OpenExisting | RdOnly | Seq | CloseOnExec);

            if (metaFile.GetLength() < static_cast<ssize_t>(sizeof(TLayerMetaHeader))) {
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::IncorrectLayerFileSize,
                    "Layer meta file %v is too short: at least %v bytes expected",
                    metaFileName,
                    sizeof(TLayerMetaHeader));
            }

            auto metaFileBlob = TSharedMutableRef::Allocate(metaFile.GetLength());

            NFS::WrapIOErrors([&] {
                TFileInput metaFileInput(metaFile);
                metaFileInput.Read(metaFileBlob.Begin(), metaFile.GetLength());
            });

            const auto* metaHeader = reinterpret_cast<const TLayerMetaHeader*>(metaFileBlob.Begin());
            if (metaHeader->Signature != TLayerMetaHeader::ExpectedSignature) {
                THROW_ERROR_EXCEPTION(
                    "Incorrect layer header signature %x in layer meta file %v",
                    metaHeader->Signature,
                    metaFileName);
            }

            auto metaBlob = TRef(metaFileBlob.Begin() + sizeof(TLayerMetaHeader), metaFileBlob.End());
            if (metaHeader->MetaChecksum != GetChecksum(metaBlob)) {
                THROW_ERROR_EXCEPTION(
                    "Incorrect layer meta checksum in layer meta file %v",
                    metaFileName);
            }

            NProto::TLayerMeta protoMeta;
            if (!TryDeserializeProtoWithEnvelope(&protoMeta, metaBlob)) {
                THROW_ERROR_EXCEPTION(
                    "Failed to parse chunk meta file %v",
                    metaFileName);
            }

            TLayerMeta meta;
            meta.MergeFrom(protoMeta);
            meta.Id = id;
            meta.Path = GetLayerPath(id);

            {
                auto guard = Guard(SpinLock_);
                YT_VERIFY(Layers_.emplace(id, meta).second);

                UsedSpace_ += meta.size();
            }
        }
    }

    i64 GetQuota() const
    {
        return Config_->Quota.value_or(std::numeric_limits<i64>::max());
    }

    void DoInitialize()
    {
        {
            auto guard = Guard(SpinLock_);
            ChangeState(ELocationState::Enabled);
        }

        try {
            NFS::MakeDirRecursive(Config_->Path, 0755);

            if (HealthChecker_) {
                HealthChecker_->RunCheck();
            }
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            Disable(
                ex,
                /*persistentDisable*/ !error.FindMatching(NChunkClient::EErrorCode::LockFileIsFound).has_value());

            THROW_ERROR_EXCEPTION(
                "Failed to initialize layer location %v",
                Config_->Path)
                << ex;
        }

        try {
            RemoveVolumes();

            RunTool<TRemoveDirAsRootTool>(VolumesPath_);
            RunTool<TRemoveDirAsRootTool>(VolumesMetaPath_);

            NFS::MakeDirRecursive(VolumesPath_, 0755);
            NFS::MakeDirRecursive(LayersPath_, 0755);
            NFS::MakeDirRecursive(VolumesMetaPath_, 0755);
            NFS::MakeDirRecursive(LayersMetaPath_, 0755);
            // This is requires to use directory as place.
            NFS::MakeDirRecursive(NFS::CombinePaths(Config_->Path, "porto_volumes"), 0755);
            NFS::MakeDirRecursive(NFS::CombinePaths(Config_->Path, "porto_storage"), 0755);

            ValidateMinimumSpace();

            LoadLayers();

            if (HealthChecker_) {
                HealthChecker_->SubscribeFailed(BIND([=, this, weakThis = MakeWeak(this)] (const TError& result) {
                    if (auto this_ = weakThis.Lock()) {
                        Disable(
                            result,
                            /*persistentDisable*/ !result.FindMatching(NChunkClient::EErrorCode::LockFileIsFound).has_value());
                    }
                }).Via(LocationQueue_->GetInvoker()));
                HealthChecker_->Start();
            }
        } catch (const std::exception& ex) {
            Disable(ex);
            THROW_ERROR_EXCEPTION(
                "Failed to initialize layer location %v",
                Config_->Path)
                << ex;
        }
    }

    void DoFinalizeLayerImport(const TLayerMeta& layerMeta, TGuid tag)
    {
        auto metaBlob = SerializeProtoToRefWithEnvelope(layerMeta);

        TLayerMetaHeader header;
        header.MetaChecksum = GetChecksum(metaBlob);

        auto layerMetaFileName = GetLayerMetaPath(layerMeta.Id);
        auto temporaryLayerMetaFileName = layerMetaFileName + std::string(NFS::TempFileSuffix);

        TFile metaFile(
            temporaryLayerMetaFileName,
            CreateAlways | WrOnly | Seq | CloseOnExec);
        metaFile.Write(&header, sizeof(header));
        metaFile.Write(metaBlob.Begin(), metaBlob.Size());
        metaFile.FlushData();
        metaFile.Close();

        NFS::Rename(temporaryLayerMetaFileName, layerMetaFileName);

        i64 usedSpace;
        i64 availableSpace;

        {
            auto guard = Guard(SpinLock_);
            ValidateEnabled();
            Layers_[layerMeta.Id] = layerMeta;

            AvailableSpace_ -= layerMeta.size();
            UsedSpace_ += layerMeta.size();

            usedSpace = UsedSpace_;
            availableSpace = AvailableSpace_;
        }

        YT_LOG_INFO(
            "Finished layer import (LayerId: %v, LayerPath: %v, UsedSpace: %v, AvailableSpace: %v, Tag: %v)",
            layerMeta.Id,
            layerMeta.Path,
            usedSpace,
            availableSpace,
            tag);
    }

    TLayerMeta DoImportLayer(const TArtifactKey& artifactKey, const TString& archivePath, const TString& container, TLayerId layerId, TGuid tag)
    {
        ValidateEnabled();

        auto dynamicConfig = DynamicConfig_.Acquire();

        auto Logger = ExecNodeLogger()
            .WithTag("Tag: %v, LayerId: %v", tag, layerId);

        LayerImportsInProgress_.fetch_add(1);

        auto finally = Finally([&]{
            LayerImportsInProgress_.fetch_add(-1);
        });
        try {
            YT_LOG_DEBUG(
                "Ensure that cached layer archive is not in use (ArchivePath: %v)",
                archivePath);

            {
                // Take exclusive lock in blocking fashion to ensure that no
                // forked process is holding an open descriptor to the source file.
                TFile file(archivePath, RdOnly | CloseOnExec);
                file.Flock(LOCK_EX);
            }

            auto layerDirectory = GetLayerPath(layerId);
            i64 layerSize = 0;

            try {
                YT_LOG_DEBUG(
                    "Unpack layer (Path: %v)",
                    layerDirectory);

                TEventTimerGuard timer(PerformanceCounters_.ImportLayerTimer);
                WaitFor(LayerExecutor_->ImportLayer(archivePath, ToString(layerId), PlacePath_, container))
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(
                    ex,
                    "Layer unpacking failed (ArchivePath: %v)",
                    archivePath);
                THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::LayerUnpackingFailed, "Layer unpacking failed")
                    << ex;
            }

            auto config = New<TGetDirectorySizesAsRootConfig>();
            config->Paths = {layerDirectory};
            config->IgnoreUnavailableFiles = true;
            config->DeduplicateByINodes = true;

            layerSize = RunTool<TGetDirectorySizesAsRootTool>(config).front();
            YT_LOG_DEBUG(
                "Calculated layer size (Size: %v, Tag: %v)",
                layerSize,
                tag);

            TLayerMeta layerMeta;
            layerMeta.Path = layerDirectory;
            layerMeta.Id = layerId;
            layerMeta.mutable_artifact_key()->MergeFrom(artifactKey);
            layerMeta.set_size(layerSize);
            ToProto(layerMeta.mutable_id(), layerId);

            DoFinalizeLayerImport(layerMeta, tag);

            if (auto delay = dynamicConfig->DelayAfterLayerImported) {
                TDelayedExecutor::WaitForDuration(*delay);
            }

            return layerMeta;
        } catch (const std::exception& ex) {
            auto error = TError("Failed to import layer %v", layerId)
                << TErrorAttribute("layer_path", artifactKey.data_source().path())
                << ex;

            auto innerError = TError(ex);
            if (innerError.GetCode() == NExecNode::EErrorCode::LayerUnpackingFailed) {
                THROW_ERROR(error);
            }

            if (ResidesOnTmpfs()) {
                // Don't disable location if it resides on tmpfs.
                THROW_ERROR(error);
            }

            Disable(error);

            if (dynamicConfig->AbortOnOperationWithLayerFailed) {
                YT_LOG_FATAL(error);
            } else {
                THROW_ERROR(error);
            }
        }
    }

    void DoRemoveLayer(const TLayerId& layerId)
    {
        auto config = DynamicConfig_.Acquire();

        auto layerPath = GetLayerPath(layerId);
        auto layerMetaPath = GetLayerMetaPath(layerId);

        auto Logger = ExecNodeLogger()
            .WithTag("LayerId: %v, LayerPath: %v", layerId, layerPath);

        {
            auto guard = Guard(SpinLock_);
            ValidateEnabled();

            if (!Layers_.contains(layerId)) {
                YT_LOG_FATAL("Layer already removed");
            }
        }

        try {
            YT_LOG_INFO("Removing layer");

            YT_UNUSED_FUTURE(LayerExecutor_->RemoveLayer(ToString(layerId), PlacePath_, config->EnableAsyncLayerRemoval));

            NFS::Remove(layerMetaPath);

            {
                auto guard = Guard(SpinLock_);

                if (!IsEnabled()) {
                    return;
                }

                i64 layerSize = Layers_[layerId].size();

                YT_VERIFY(Layers_.erase(layerId));

                UsedSpace_ -= layerSize;
                AvailableSpace_ += layerSize;
            }
        } catch (const std::exception& ex) {
            auto error = TError(
                "Failed to remove layer %v",
                layerId)
                << ex;
            Disable(error);

            if (config->AbortOnOperationWithLayerFailed) {
                YT_LOG_FATAL(error);
            } else {
                THROW_ERROR(error);
            }
        }
    }

    TVolumeMeta DoCreateVolume(
        TGuid tag,
        TTagSet tagSet,
        std::optional<TEventTimerGuard> volumeCreateTimeGuard,
        TVolumeMeta volumeMeta,
        THashMap<TString, TString>&& volumeProperties)
    {
        ValidateEnabled();

        auto guard = std::move(volumeCreateTimeGuard);

        auto volumeId = TVolumeId::Create();
        auto volumePath = GetVolumePath(volumeId);
        auto volumeType = FromProto<EVolumeType>(volumeMeta.type());
        // TODO(dgolear): Switch to std::string.
        TString mountPath = NFS::CombinePaths(volumePath, MountSuffix);

        try {
            YT_LOG_DEBUG(
                "Creating volume (Tag: %v, Type: %v, VolumeId: %v)",
                tag,
                volumeType,
                volumeId);

            NFS::MakeDirRecursive(mountPath, 0755);

            auto path = WaitFor(VolumeExecutor_->CreateVolume(mountPath, volumeProperties))
                .ValueOrThrow();

            YT_VERIFY(path == mountPath);

            auto volumeGuard = Finally([&volumePath, &mountPath, this] {
                try {
                    WaitFor(VolumeExecutor_->UnlinkVolume(mountPath, "self")).ThrowOnError();
                } catch (const std::exception& ex) {
                    YT_LOG_ERROR(
                        ex,
                        "Failed to unlink volume (MountPath: %v)",
                        mountPath);
                }

                try {
                    NFS::RemoveRecursive(volumePath);
                } catch (const std::exception& ex) {
                    YT_LOG_ERROR(
                        ex,
                        "Failed to remove volume path (VolumePath: %v)",
                        volumePath);
                }
            });

            YT_LOG_INFO(
                "Created volume (Tag: %v, Type: %v, VolumeId: %v, VolumeMountPath: %v)",
                tag,
                volumeType,
                volumeId,
                mountPath);

            ToProto(volumeMeta.mutable_id(), volumeId);
            volumeMeta.MountPath = mountPath;
            volumeMeta.Id = volumeId;

            auto metaBlob = SerializeProtoToRefWithEnvelope(volumeMeta);

            TLayerMetaHeader header;
            header.MetaChecksum = GetChecksum(metaBlob);

            auto volumeMetaFileName = GetVolumeMetaPath(volumeId);
            auto tempVolumeMetaFileName = volumeMetaFileName + std::string(NFS::TempFileSuffix);

            {
                auto metaFile = std::make_unique<TFile>(
                    tempVolumeMetaFileName,
                    CreateAlways | WrOnly | Seq | CloseOnExec);
                metaFile->Write(&header, sizeof(header));
                metaFile->Write(metaBlob.Begin(), metaBlob.Size());
                metaFile->Close();
            }

            NFS::Rename(tempVolumeMetaFileName, volumeMetaFileName);

            auto volumeMetaGuard = Finally([&volumeMetaFileName, this] {
                try {
                    NFS::Remove(volumeMetaFileName);
                } catch (const std::exception& ex) {
                    YT_LOG_ERROR(
                        ex,
                        "Failed to remove volume meta (VolumeMetaPath: %v)",
                        volumeMetaFileName);
                }
            });

            YT_LOG_INFO(
                "Created volume meta (Tag: %v, Type: %v, VolumeId: %v, MetaFileName: %v)",
                tag,
                volumeType,
                volumeId,
                volumeMetaFileName);

            {
                auto guard = Guard(SpinLock_);
                ValidateEnabled();
                YT_VERIFY(Volumes_.emplace(volumeId, volumeMeta).second);

                if (VolumesReleaseEvent_.IsSet()) {
                    VolumesReleaseEvent_ = NewPromise<void>();
                }
            }

            TVolumeProfilerCounters::Get()->GetGauge(tagSet, "/count")
                .Update(VolumeCounters().Increment(tagSet));
            TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/created").Increment(1);

            volumeGuard.Release();
            volumeMetaGuard.Release();

            return volumeMeta;
        } catch (const std::exception& ex) {
            TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/create_errors").Increment(1);

            YT_LOG_ERROR(
                ex,
                "Failed to create volume (Tag: %v, Type: %v, VolumeId: %v)",
                tag,
                volumeType,
                volumeId);

            auto error = TError(
                "Failed to create %Qlv volume %v",
                volumeType,
                volumeId)
                << ex;

            // Don't disable location in case of InvalidImage or NBD errors.
            switch (static_cast<EPortoErrorCode>(TError(ex).GetCode())) {
                case EPortoErrorCode::InvalidFilesystem:
                    THROW_ERROR_EXCEPTION(
                        NExecNode::EErrorCode::InvalidImage,
                        "Invalid filesystem of %Qlv volume %v",
                        volumeType,
                        volumeId)
                        << ex;

                case EPortoErrorCode::NbdProtoError:
                case EPortoErrorCode::NbdSocketError:
                case EPortoErrorCode::NbdSocketTimeout:
                case EPortoErrorCode::NbdSocketUnavailable:
                case EPortoErrorCode::NbdUnknownExport:
                    break;

                default:
                    Disable(error);
                    break;
            }

            if (DynamicConfig_.Acquire()->AbortOnOperationWithVolumeFailed) {
                YT_LOG_FATAL(error);
            } else {
                THROW_ERROR(error);
            }
        }
    }

    TVolumeMeta DoCreateNbdVolume(
        TGuid tag,
        TTagSet tagSet,
        TNbdConfigPtr nbdConfig,
        TCreateNbdVolumeOptions options)
    {
        ValidateEnabled();

        YT_VERIFY(nbdConfig);

        THashMap<TString, TString> volumeProperties = {
            {"backend", "nbd"},
            {"place", PlacePath_}
        };

        if (options.IsReadOnly) {
            volumeProperties["read_only"] = "true";
        }

        TStringBuilder builder;
        if (nbdConfig->Server->UnixDomainSocket) {
            builder.AppendFormat("unix+tcp:%v?", nbdConfig->Server->UnixDomainSocket->Path);
        } else {
            YT_VERIFY(nbdConfig->Server->InternetDomainSocket);
            builder.AppendFormat("tcp://%v:%v/?", NNet::GetLocalHostName(), nbdConfig->Server->InternetDomainSocket->Port);
        }
        builder.AppendFormat("timeout=%v", ToString(nbdConfig->Client->IOTimeout.Seconds()));
        builder.AppendFormat("&reconn-timeout=%v", ToString(nbdConfig->Client->ReconnectTimeout.Seconds()));

        auto connectionCount = 1;
        if (options.IsReadOnly) {
            connectionCount = nbdConfig->Client->ConnectionCount;
        }

        builder.AppendFormat("&num-connections=%v", connectionCount);
        builder.AppendFormat("&export=%v", options.DeviceId);
        builder.AppendFormat("&fs-type=%v", options.Filesystem);
        volumeProperties["storage"] = builder.Flush();

        TVolumeMeta volumeMeta;
        volumeMeta.set_type(ToProto(EVolumeType::Nbd));

        return DoCreateVolume(
            tag,
            std::move(tagSet),
            /*volumeCreateTimeGuard*/std::nullopt,
            std::move(volumeMeta),
            std::move(volumeProperties));
    }

    TVolumeMeta DoCreateOverlayVolume(
        TGuid tag,
        TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TUserSandboxOptions& options,
        const std::vector<TOverlayData>& overlayDataArray)
    {
        ValidateEnabled();

        // Place overlayfs (upper and work directories) in root volume, if it is present.
        std::optional<TString> placePath;
        for (const auto& overlayData : overlayDataArray) {
            if (overlayData.IsVolume() && overlayData.GetVolume()->IsRootVolume()) {
                if (placePath) {
                    THROW_ERROR_EXCEPTION("Can not have multiple root volumes in overlay volume")
                        << TErrorAttribute("first_root_volume", placePath)
                        << TErrorAttribute("second_root_volume", overlayData.GetPath());
                }
                // See PORTO-460 for "//" prefix.
                placePath = "//" + overlayData.GetPath();
            }
        }

        if (!placePath) {
            if (options.SlotPath && options.EnableRootVolumeDiskQuota) {
                // Place overlayfs (upper and work directories) in user slot.
                placePath = "//" + NFS::CombinePaths(ToString(options.SlotPath.value()), "overlay");
            } else {
                placePath = PlacePath_;
            }
        }

        THashMap<TString, TString> volumeProperties = {
            {"backend", "overlay"},
            {"user", ToString(options.UserId)},
            {"permissions", "0777"},
            {"place", placePath.value()},
        };

        // NB: Root volume quota is independent from sandbox quota but enforces the same limits.
        if (options.EnableDiskQuota && options.EnableRootVolumeDiskQuota) {
            if (options.DiskSpaceLimit) {
                volumeProperties["space_limit"] = ToString(*options.DiskSpaceLimit);
            }

            if (options.InodeLimit) {
                volumeProperties["inode_limit"] = ToString(*options.InodeLimit);
            }
        }

        TStringBuilder builder;
        JoinToString(
            &builder,
            overlayDataArray.begin(),
            overlayDataArray.end(),
            [] (TStringBuilderBase* builder, const TOverlayData& volumeOrLayer) {
                // Do not add root volume to overlayfs layers, it will be used as a "place".
                if (!volumeOrLayer.IsVolume() || !volumeOrLayer.GetVolume()->IsRootVolume()) {
                    builder->AppendString(volumeOrLayer.GetPath());
                }
            },
            ";");

        volumeProperties["layers"] = builder.Flush();

        TVolumeMeta volumeMeta;
        volumeMeta.set_type(ToProto(EVolumeType::Local));

        for (const auto& volumeOrLayer : overlayDataArray) {
            YT_ASSERT(!volumeOrLayer.GetPath().empty());
            volumeMeta.add_layer_paths(volumeOrLayer.GetPath());
        }

        return DoCreateVolume(
            tag,
            std::move(tagSet),
            std::move(volumeCreateTimeGuard),
            std::move(volumeMeta),
            std::move(volumeProperties));
    }

    TVolumeMeta DoCreateSquashFSVolume(
        TGuid tag,
        TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TArtifactKey& artifactKey,
        const std::string& squashFSFilePath)
    {
        ValidateEnabled();

        THashMap<TString, TString> volumeProperties {
            {"backend", "squash"},
            {"read_only", "true"},
            {"layers", TString(squashFSFilePath)}
        };

        TVolumeMeta volumeMeta;
        volumeMeta.set_type(ToProto(EVolumeType::Local));
        volumeMeta.add_layer_artifact_keys()->MergeFrom(artifactKey);
        volumeMeta.add_layer_paths(squashFSFilePath);

        return DoCreateVolume(
            tag,
            std::move(tagSet),
            std::move(volumeCreateTimeGuard),
            std::move(volumeMeta),
            std::move(volumeProperties));
    }

    TVolumeMeta DoCreateTmpfsVolume(
        TGuid tag,
        TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        TTmpfsVolumeParams volumeParams)
    {
        ValidateEnabled();

        THashMap<TString, TString> volumeProperties {
            {"backend", "tmpfs"},
            {"user", ToString(volumeParams.UserId)},
            {"permissions", "0777"},
            {"space_limit", ToString(volumeParams.Size)},
        };

        TVolumeMeta volumeMeta;
        volumeMeta.set_type(ToProto(EVolumeType::Tmpfs));

        return DoCreateVolume(
            tag,
            std::move(tagSet),
            std::move(volumeCreateTimeGuard),
            std::move(volumeMeta),
            std::move(volumeProperties));
    }

    void DoRemoveVolume(TTagSet tagSet, TVolumeId volumeId)
    {
        auto volumePath = GetVolumePath(volumeId);
        // TODO(dgolear): Switch to std::string.
        TString mountPath = NFS::CombinePaths(volumePath, MountSuffix);
        auto volumeMetaPath = GetVolumeMetaPath(volumeId);

        {
            auto guard = Guard(SpinLock_);

            // When location is disabled, volumes is empty.
            if (IsEnabled() && !Volumes_.contains(volumeId)) {
                YT_LOG_FATAL(
                    "Volume already removed (VolumeId: %v, VolumePath: %v, VolumeMetaPath: %v)",
                    volumeId,
                    volumePath,
                    volumeMetaPath);
            }
        }

        try {
            // The location could be disabled while we were getting here.
            // Any how try to unlink volume and remove associated data.

            YT_LOG_DEBUG(
                "Removing volume (VolumeId: %v)",
                volumeId);

            WaitFor(VolumeExecutor_->UnlinkVolume(mountPath, "self"))
                .ThrowOnError();

            YT_LOG_DEBUG(
                "Volume unlinked (VolumeId: %v)",
                volumeId);

            NFS::RemoveRecursive(volumePath);
            NFS::Remove(volumeMetaPath);

            TVolumeProfilerCounters::Get()->GetGauge(tagSet, "/count")
                .Update(VolumeCounters().Decrement(tagSet));
            TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/removed").Increment(1);

            YT_LOG_INFO(
                "Volume directory and meta removed (VolumeId: %v, VolumePath: %v, VolumeMetaPath: %v)",
                volumeId,
                volumePath,
                volumeMetaPath);

            {
                auto guard = Guard(SpinLock_);

                // The location could be disabled while we were getting here.
                // So check that location is enabled prior to erasing volume.

                if (!IsEnabled()) {
                    return;
                }

                YT_VERIFY(Volumes_.erase(volumeId));

                if (Volumes_.empty()) {
                    VolumesReleaseEvent_.TrySet();
                }
            }
        } catch (const std::exception& ex) {
            TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/remove_errors").Increment(1);

            YT_LOG_ERROR(
                ex,
                "Failed to remove volume (VolumeId: %v)",
                volumeId);

            auto error = TError("Failed to remove volume")
                << ex
                << TErrorAttribute("volume_id", volumeId);

            // Don't disable location in case of VolumeNotFound, VolumeNotLinked or NBD errors.
            switch (static_cast<EPortoErrorCode>(TError(ex).GetCode())) {
                case EPortoErrorCode::VolumeNotFound:
                case EPortoErrorCode::VolumeNotLinked:
                case EPortoErrorCode::NbdProtoError:
                case EPortoErrorCode::NbdSocketError:
                case EPortoErrorCode::NbdSocketTimeout:
                case EPortoErrorCode::NbdSocketUnavailable:
                case EPortoErrorCode::NbdUnknownExport:
                    THROW_ERROR(error);
                default:
                    break;
            }

            Disable(error);

            if (DynamicConfig_.Acquire()->AbortOnOperationWithVolumeFailed) {
                YT_LOG_FATAL(error);
            } else {
                THROW_ERROR(error);
            }
        }
    }

    void DoLinkVolume(
        TGuid tag,
        const TString& source,
        const TString& target)
    {
        YT_LOG_DEBUG("Linking volume (Tag: %v, Source: %v, Target: %v)",
            tag,
            source,
            target);

        // If target does not exist, it is created by porto.
        WaitFor(VolumeExecutor_->LinkVolume(source, "self", target))
            .ThrowOnError();
    }

    void DoUnlinkVolume(
        const TString& source,
        const TString& target)
    {
        YT_VERIFY(!source.empty());
        YT_VERIFY(!target.empty());

        YT_LOG_DEBUG("Unlinking volume (Source: %v, Target: %v)",
            source,
            target);

        WaitFor(VolumeExecutor_->UnlinkVolume(source, "self", target))
            .ThrowOnError();
    }

    //! Remove porto volumes that belong to this location.
    //! Volumes are not expected to be used since all jobs must be dead by now.
    void RemoveVolumes(TDuration timeout = TDuration::Minutes(30))
    {
        auto startTime = TInstant::Now();
        auto deadLine = startTime + timeout;

        YT_LOG_DEBUG("Removing volumes (DeadLine: %v)",
            deadLine);

        auto checkDeadLine = [&] {
            auto now = TInstant::Now();
            if (now > deadLine) {
                THROW_ERROR_EXCEPTION("Failed to wait for volumes already being unlinked")
                    << TErrorAttribute("timeout", timeout);
            }
        };

        int volumesRemoved = 0;

        while (true) {
            checkDeadLine();

            auto volumes = WaitFor(VolumeExecutor_->GetVolumes())
                .ValueOrThrow();

            auto waitForVolumesToBecomeReady = false;
            std::vector<TFuture<void>> unlinkFutures;
            for (const auto& volume : volumes) {
                if (!volume.Path.StartsWith(VolumesPath_)) {
                    // This volume is not from my location.
                    continue;
                }

                static const TString ReadyState = "ready";
                if (volume.State != ReadyState) {
                    waitForVolumesToBecomeReady = true;
                    YT_LOG_DEBUG("Volume is not ready (Path: %v, State: %v)",
                        volume.Path,
                        volume.State);
                    continue;
                }

                YT_LOG_DEBUG("Trying to unlink volume (Path: %v, State: %v)",
                    volume.Path,
                    volume.State);

                // Unlink volume even if it was linked to a different container.
                unlinkFutures.push_back(VolumeExecutor_->UnlinkVolume(volume.Path, AnyContainer));
            }

            if (!waitForVolumesToBecomeReady && unlinkFutures.empty()) {
                // All volumes have been unlinked.
                break;
            }

            auto unlinkResults = WaitFor(AllSet(unlinkFutures))
                .ValueOrThrow();

            for (const auto& unlinkError : unlinkResults) {
                if (unlinkError.IsOK()) {
                    ++volumesRemoved;
                } else if (unlinkError.GetCode() != EPortoErrorCode::VolumeNotLinked && unlinkError.GetCode() != EPortoErrorCode::VolumeNotFound) {
                    THROW_ERROR(unlinkError);
                }
            }

            if (waitForVolumesToBecomeReady) {
                checkDeadLine();

                static const TDuration Duration = TDuration::Seconds(30);

                YT_LOG_DEBUG("Waiting for volumes to become ready (Duration: %v)",
                    Duration);

                TDelayedExecutor::WaitForDuration(Duration);
            }
        }

        YT_LOG_DEBUG("Removed volumes (Count: %v, Duration: %v)",
            volumesRemoved,
            (TInstant::Now() - startTime));
    }
};

DEFINE_REFCOUNTED_TYPE(TLayerLocation)
DECLARE_REFCOUNTED_CLASS(TLayerLocation)

////////////////////////////////////////////////////////////////////////////////

class TPortoVolumeBase
    : public IVolume
{
public:
    TPortoVolumeBase(
        TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr layerLocation)
        : TagSet_(std::move(tagSet))
        , VolumeMeta_(std::move(volumeMeta))
        , LayerLocation_(std::move(layerLocation))
    { }

    const TVolumeId& GetId() const override final
    {
        return VolumeMeta_.Id;
    }

    const std::string& GetPath() const override final
    {
        return VolumeMeta_.MountPath;
    }

    TFuture<void> Link(
        TGuid tag,
        const TString& target) override final
    {
        return TAsyncLockWriterGuard::Acquire(&Lock_)
            .AsUnique().Apply(BIND([tag, target, this, this_ = MakeStrong(this)] (
                TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>&& guard)
            {
                // Targets_ is protected with guard.
                Y_UNUSED(guard);

                Targets_.push_back(target);

                // TODO(dgolear): Switch to std::string.
                auto source = TString(GetPath());
                return LayerLocation_->LinkVolume(tag, source, target);
            }));
    }

    TFuture<void> Remove() override final
    {
        if (!RemovalRequested_.exchange(true)) {
            TAsyncLockWriterGuard::Acquire(&Lock_).AsUnique().Subscribe(BIND(
                [
                    removalCallback = RemoveCallback_,
                    removePromise = RemovePromise_,
                    targets = Targets_,
                    volumePath = VolumeMeta_.MountPath
                ] (TErrorOr<TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>>&& guardOrError) mutable {
                    YT_LOG_FATAL_UNLESS(guardOrError.IsOK(), guardOrError, "Failed to acquire lock (VolumePath: %v)", volumePath);

                    auto guard = std::move(guardOrError.Value());
                    auto removeFuture = removalCallback(targets).Apply(BIND(
                        [guard = std::move(guard), volumePath = std::move(volumePath)] (const TError& error) {
                            if (!error.IsOK()) {
                                YT_LOG_ERROR(
                                    error,
                                    "Failed to remove volume (VolumePath: %v)",
                                    volumePath);
                            }
                            return error;
                        }));
                    removePromise.SetFrom(removeFuture);
                }));
        }

        return RemovePromise_;
    }

    bool IsCached() const override
    {
        return false;
    }

protected:
    const TTagSet TagSet_;
    const TVolumeMeta VolumeMeta_;
    const TLayerLocationPtr LayerLocation_;

    TPromise<void> RemovePromise_ = NewPromise<void>();
    std::atomic<bool> RemovalRequested_{false};

    static TFuture<void> DoRemoveVolumeCommon(
        const TString& volumeType,
        TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta,
        TCallback<TFuture<void>(const TLogger&)> postRemovalCleanup = {})
    {
        TEventTimerGuard volumeRemoveTimeGuard(
            TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/remove_time"));

        const auto& volumeId = volumeMeta.Id;
        const auto& volumePath = volumeMeta.MountPath;

        auto Logger = ExecNodeLogger()
            .WithTag("VolumeType: %v, VolumeId: %v, VolumePath: %v",
                volumeType,
                volumeId,
                volumePath);

        YT_LOG_DEBUG("Removing volume");

        return location->RemoveVolume(tagSet, volumeId)
            .Apply(BIND(
                [
                    Logger,
                    cleanup = std::move(postRemovalCleanup),
                    volumeRemoveTimeGuard = std::move(volumeRemoveTimeGuard)
                ] (const TError& error) {
                    if (!error.IsOK()) {
                        YT_LOG_WARNING(error, "Failed to remove volume");
                    } else {
                        YT_LOG_DEBUG("Removed volume");
                    }

                    // Perform post-removal cleanup if provided (e.g., device finalization, overlay cleanup)
                    if (cleanup) {
                        return cleanup(Logger);
                    }
                    return VoidFuture;
                }))
            .ToUncancelable();
    }

    void SetRemoveCallback(TCallback<TFuture<void>()> callback)
    {
        // Unlink targets prior to removing volume.
        RemoveCallback_ = BIND(
            [
                location = LayerLocation_,
                volumePath = VolumeMeta_.MountPath,
                callback = std::move(callback)
            ] (const std::vector<TString>& targets) {
                return UnlinkTargets(location, volumePath, targets)
                    .AsUnique().Apply(BIND([volumePath, callback = std::move(callback)] (TError&& error) {
                        if (!error.IsOK()) {
                            YT_LOG_WARNING(error, "Failed to unlink targets (VolumePath: %v)",
                                volumePath);
                        } else {
                            YT_LOG_DEBUG("Unlinked targets (VolumePath: %v)",
                                volumePath);
                        }
                        // Now remove the actual volume.
                        return callback();
                    }));
            });
    }

private:
    TAsyncReaderWriterLock Lock_;
    std::vector<TString> Targets_;

    TCallback<TFuture<void>(const std::vector<TString>&)> RemoveCallback_;

    static TFuture<void> UnlinkTargets(TLayerLocationPtr location, TString source, std::vector<TString> targets)
    {
        YT_LOG_DEBUG("Unlinking targets (VolumePath: %v, Targets: %v)",
            source,
            targets);

        if (targets.empty()) {
            return OKFuture;
        }

        std::vector<TFuture<void>> futures;
        futures.reserve(targets.size());
        for (const auto& target : targets) {
            futures.emplace_back(location->UnlinkVolume(source, target));
        }

        return AllSucceeded(std::move(futures))
            .ToUncancelable();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TKey>
class TCachedVolume
    : public TPortoVolumeBase
    , public TAsyncMapValueBase<TKey, TCachedVolume<TKey>>
{
public:
    TCachedVolume(
        TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr layerLocation,
        const TKey& key)
        : TPortoVolumeBase(
            std::move(tagSet),
            std::move(volumeMeta),
            std::move(layerLocation))
        , TAsyncMapValueBase<TKey, TCachedVolume<TKey>>(key)
    { }

    bool IsCached() const override final
    {
        return true;
    }

    bool IsRootVolume() const override final
    {
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSquashFSVolume
    : public TCachedVolume<TArtifactKey>
{
public:
    TSquashFSVolume(
        TTagSet tagSet,
        TVolumeMeta volumeMeta,
        IVolumeArtifactPtr artifact,
        TLayerLocationPtr location,
        const TArtifactKey& artifactKey)
        : TCachedVolume(
            std::move(tagSet),
            std::move(volumeMeta),
            std::move(location),
            artifactKey)
        , Artifact_(std::move(artifact))
    {
        SetRemoveCallback(BIND(
            &TSquashFSVolume::DoRemove,
            TagSet_,
            LayerLocation_,
            VolumeMeta_));
    }

    ~TSquashFSVolume() override
    {
        YT_UNUSED_FUTURE(Remove());
    }

private:
    // We store chunk cache artifact here to make sure that SquashFS file outlives SquashFS volume.
    const IVolumeArtifactPtr Artifact_;

    static TFuture<void> DoRemove(
        TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta)
    {
        return DoRemoveVolumeCommon(
            "SquashFS",
            std::move(tagSet),
            std::move(location),
            std::move(volumeMeta));
    }
};

DECLARE_REFCOUNTED_CLASS(TSquashFSVolume)
DEFINE_REFCOUNTED_TYPE(TSquashFSVolume)

////////////////////////////////////////////////////////////////////////////////

class TRWNbdVolume
    : public TPortoVolumeBase
{
public:
    TRWNbdVolume(
        TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr layerLocation,
        TString nbdDeviceId,
        INbdServerPtr nbdServer)
        : TPortoVolumeBase(
            std::move(tagSet),
            std::move(volumeMeta),
            std::move(layerLocation))
        , NbdDeviceId_(std::move(nbdDeviceId))
        , NbdServer_(std::move(nbdServer))
    {
        SetRemoveCallback(BIND(
            &TRWNbdVolume::DoRemove,
            TagSet_,
            LayerLocation_,
            VolumeMeta_,
            NbdDeviceId_,
            NbdServer_));
    }

    ~TRWNbdVolume() override
    {
        YT_UNUSED_FUTURE(Remove());
    }

    bool IsRootVolume() const override final
    {
        return true;
    }

private:
    const TString NbdDeviceId_;
    const INbdServerPtr NbdServer_;

    static TFuture<void> DoRemove(
        TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta,
        TString nbdDeviceId,
        INbdServerPtr nbdServer)
    {
        // First, unregister device. At this point device is removed from the
        // server but it remains in existing device connections.
        auto device = nbdServer->TryUnregisterDevice(nbdDeviceId);

        // Second, remove volume. At this point all device connections are going
        // be terminated.
        auto postRemovalCleanup = BIND_NO_PROPAGATE(
            [device = std::move(device)] (const TLogger& Logger) -> TFuture<void> {
                if (device) {
                    YT_LOG_DEBUG("Finalizing RW NBD device");
                    return device->Finalize();
                } else {
                    YT_LOG_WARNING("Failed to finalize device; unknown device");
                    return VoidFuture;
                }
            })
            .AsyncVia(nbdServer->GetInvoker());

        return DoRemoveVolumeCommon(
            "RW NBD",
            std::move(tagSet),
            std::move(location),
            std::move(volumeMeta),
            std::move(postRemovalCleanup));
    }
};

DECLARE_REFCOUNTED_CLASS(TRWNbdVolume)
DEFINE_REFCOUNTED_TYPE(TRWNbdVolume)

////////////////////////////////////////////////////////////////////////////////

class TRONbdVolume
    : public TCachedVolume<TString>
{
public:
    TRONbdVolume(
        TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr layerLocation,
        TString nbdDeviceId,
        INbdServerPtr nbdServer)
        : TCachedVolume(
            std::move(tagSet),
            std::move(volumeMeta),
            std::move(layerLocation),
            nbdDeviceId)
        , NbdDeviceId_(std::move(nbdDeviceId))
        , NbdServer_(std::move(nbdServer))
    {
        SetRemoveCallback(BIND(
            &TRONbdVolume::DoRemove,
            TagSet_,
            LayerLocation_,
            VolumeMeta_,
            NbdDeviceId_,
            NbdServer_));
    }

    ~TRONbdVolume() override
    {
        YT_UNUSED_FUTURE(Remove());
    }

private:
    const TString NbdDeviceId_;
    const INbdServerPtr NbdServer_;

    static TFuture<void> DoRemove(
        TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta,
        TString nbdDeviceId,
        INbdServerPtr nbdServer)
    {
        // First, unregister device. At this point device is removed from the
        // server but it remains in existing device connections.
        auto device = nbdServer->TryUnregisterDevice(nbdDeviceId);

        // Second, remove volume. At this point all device connections are going
        // be terminated.
        auto postRemovalCleanup = BIND_NO_PROPAGATE(
            [device = std::move(device)] (const TLogger& Logger) -> TFuture<void> {
                if (device) {
                    YT_LOG_DEBUG("Finalizing RO NBD device");
                    return device->Finalize();
                } else {
                    YT_LOG_WARNING("Failed to finalize device; unknown device");
                    return VoidFuture;
                }
            })
            .AsyncVia(nbdServer->GetInvoker());

        return DoRemoveVolumeCommon(
            "RO NBD",
            std::move(tagSet),
            std::move(location),
            std::move(volumeMeta),
            std::move(postRemovalCleanup));
    }
};

DECLARE_REFCOUNTED_CLASS(TRONbdVolume)
DEFINE_REFCOUNTED_TYPE(TRONbdVolume)

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

class TLayer
    : public TAsyncCacheValueBase<TArtifactKey, TLayer>
{
public:
    TLayer(const TLayerMeta& layerMeta, const TArtifactKey& artifactKey, const TLayerLocationPtr& layerLocation)
        : TAsyncCacheValueBase<TArtifactKey, TLayer>(artifactKey)
        , LayerMeta_(layerMeta)
        , Location_(layerLocation)
    { }

    ~TLayer()
    {
        auto removalNeeded = IsLayerRemovalNeeded_;
        YT_LOG_INFO(
            "Layer is destroyed (LayerId: %v, LayerPath: %v, RemovalNeeded: %v)",
            LayerMeta_.Id,
            LayerMeta_.Path,
            removalNeeded);

        if (removalNeeded) {
            Location_->RemoveLayer(LayerMeta_.Id)
                .Subscribe(BIND([layerId = LayerMeta_.Id] (const TError& result) {
                    YT_LOG_ERROR_IF(!result.IsOK(), result, "Failed to remove layer (LayerId: %v)", layerId);
                }));
        }
    }

    const TString& GetCypressPath() const
    {
        return GetKey().data_source().path();
    }

    const std::string& GetPath() const
    {
        return LayerMeta_.Path;
    }

    i64 GetSize() const
    {
        return LayerMeta_.size();
    }

    const TLayerMeta& GetMeta() const
    {
        return LayerMeta_;
    }

    void IncreaseHitCount()
    {
        HitCount_.fetch_add(1);
    }

    int GetHitCount() const
    {
        return HitCount_.load();
    }

    void SetLayerRemovalNotNeeded()
    {
        IsLayerRemovalNeeded_ = false;
    }

private:
    const TLayerMeta LayerMeta_;
    const TLayerLocationPtr Location_;
    std::atomic<int> HitCount_;

    // If the slot manager is disabled, the layers that are currently in the layer cache
    // do not need to be removed from the porto. If you delete them, firstly, in this
    // case they will need to be re-imported into the porto, and secondly, then there
    // may be a problem when inserting the same layers when starting a new volume manager.
    // Namely, a layer object that is already in the new cache may be deleted from the old cache,
    // in which case the layer object in the new cache will be corrupted.
    bool IsLayerRemovalNeeded_ = true;
};

DEFINE_REFCOUNTED_TYPE(TLayer)

////////////////////////////////////////////////////////////////////////////////

using TAbsorbLayerCallback = TCallback<TFuture<TLayerPtr>(
    const TArtifactKey& artifactKey,
    const TArtifactDownloadOptions& downloadOptions,
    TGuid tag,
    TLayerLocationPtr targetLocation)>;

class TTmpfsLayerCache
    : public TRefCounted
{
public:
    TTmpfsLayerCache(
        IBootstrap* const bootstrap,
        NDataNode::TTmpfsLayerCacheConfigPtr config,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        IInvokerPtr controlInvoker,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        const TString& cacheName,
        IPortoExecutorPtr portoExecutor,
        TAbsorbLayerCallback absorbLayer)
        : Config_(std::move(config))
        , DynamicConfigManager_(std::move(dynamicConfigManager))
        , ControlInvoker_(std::move(controlInvoker))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
        , CacheName_(cacheName)
        , Bootstrap_(bootstrap)
        , PortoExecutor_(std::move(portoExecutor))
        , AbsorbLayer_(std::move(absorbLayer))
        , Profiler_(ExecNodeProfiler()
            .WithPrefix("/layer_cache")
            .WithTag("cache_name", CacheName_))
        , HitCounter_(Profiler_.Counter("/tmpfs_cache_hits"))
        , UpdateFailedCounter_(Profiler_.Gauge("/update_failed"))
        , TmpfsLayerCacheCounters_(Profiler_.WithGlobal())
    {
        if (Bootstrap_) {
            Bootstrap_->SubscribePopulateAlerts(BIND(
                &TTmpfsLayerCache::PopulateTmpfsAlert,
                MakeWeak(this)));
        }
    }

    TLayerPtr FindLayer(const TArtifactKey& artifactKey)
    {
        auto guard = Guard(DataSpinLock_);
        auto it = CachedLayers_.find(artifactKey);
        if (it != CachedLayers_.end()) {
            auto layer = it->second;
            guard.Release();

            // The following counter can help find layers that do not benefit from residing in tmpfs layer cache.
            auto cacheHitsCypressPathCounter = TmpfsLayerCacheCounters_.GetCounter(
                TTagSet({{"cypress_path", artifactKey.data_source().path()}}),
                "/tmpfs_layer_hits");

            cacheHitsCypressPathCounter.Increment();
            HitCounter_.Increment();
            return layer;
        } else {
            // The following counter can help find layers that could benefit from residing in tmpfs layer cache.
            auto cacheMissesCypressPathCounter = TmpfsLayerCacheCounters_.GetCounter(
                TTagSet({{"cypress_path", artifactKey.data_source().path()}}),
                "/tmpfs_layer_misses");

            cacheMissesCypressPathCounter.Increment();
        }
        return nullptr;
    }

    TFuture<void> Initialize()
    {
        if (!Config_->LayersDirectoryPath) {
            return OKFuture;
        }

        auto path = NFS::CombinePaths(NFs::CurrentWorkingDirectory(), Format("%v_tmpfs_layers", CacheName_));

        {
            YT_LOG_DEBUG("Cleanup tmpfs layer cache volume (Path: %v)", path);
            auto error = WaitFor(PortoExecutor_->UnlinkVolume(TString(path), "self"));
            if (!error.IsOK()) {
                YT_LOG_DEBUG(error, "Failed to unlink volume (Path: %v)", path);
            }
        }

        TFuture<void> result;
        try {
            YT_LOG_DEBUG("Create tmpfs layer cache volume (Path: %v)", path);

            NFS::MakeDirRecursive(path, 0777);

            THashMap<TString, TString> volumeProperties;
            volumeProperties["backend"] = "tmpfs";
            volumeProperties["permissions"] = "0777";
            volumeProperties["space_limit"] = ToString(Config_->Capacity);

            WaitFor(PortoExecutor_->CreateVolume(TString(path), volumeProperties))
                .ThrowOnError();

            MemoryUsageTracker_->Acquire(Config_->Capacity);

            auto locationConfig = New<NDataNode::TLayerLocationConfig>();
            locationConfig->Quota = Config_->Capacity;
            locationConfig->LowWatermark = 0;
            locationConfig->MinDiskSpace = 0;
            locationConfig->Path = path;
            locationConfig->LocationIsAbsolute = false;
            locationConfig->ResidesOnTmpfs = true;

            TmpfsLocation_ = New<TLayerLocation>(
                std::move(locationConfig),
                DynamicConfigManager_,
                nullptr,
                PortoExecutor_,
                PortoExecutor_,
                Format("%v_tmpfs_layer", CacheName_));

            WaitFor(TmpfsLocation_->Initialize())
                .ThrowOnError();

            if (Bootstrap_) {
                LayerUpdateExecutor_ = New<TPeriodicExecutor>(
                    ControlInvoker_,
                    BIND(&TTmpfsLayerCache::UpdateLayers, MakeWeak(this)),
                    Config_->LayersUpdatePeriod);

                LayerUpdateExecutor_->Start();
            }

            result = Initialized_.ToFuture();
        } catch (const std::exception& ex) {
            auto error = TError("Failed to create %v tmpfs layer volume cache", CacheName_) << ex;
            SetAlert(error);
            // That's a fatal error; tmpfs layer cache is broken and we shouldn't start jobs on this node.
            result = MakeFuture(error);
        }
        return result;
    }

    TFuture<void> Disable(const TError& error, bool persistentDisable = false)
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        YT_LOG_WARNING("Disable tmfps layer cache (Path: %v)", CacheName_);

        if (TmpfsLocation_) {
            MemoryUsageTracker_->Release(Config_->Capacity);

            if (LayerUpdateExecutor_) {
                return LayerUpdateExecutor_->Stop()
                    .Apply(BIND(&TLayerLocation::Disable, TmpfsLocation_, error, persistentDisable));
            } else {
                TmpfsLocation_->Disable(error, persistentDisable);
                return OKFuture;
            }
        } else {
            return OKFuture;
        }
    }

    void BuildOrchid(TFluentMap fluent) const
    {
        auto guard1 = Guard(DataSpinLock_);
        auto guard2 = Guard(AlertSpinLock_);
        fluent
            .Item("layer_count").Value(CachedLayers_.size())
            .Item("alert").Value(Alert_)
            .Item("layers").BeginMap()
                .DoFor(CachedLayers_, [] (TFluentMap fluent, const auto& item) {
                    fluent
                        .Item(item.second->GetCypressPath())
                            .BeginMap()
                                .Item("size").Value(item.second->GetSize())
                                .Item("hit_count").Value(item.second->GetHitCount())
                            .EndMap();
                })
            .EndMap();
    }

    const TLayerLocationPtr& GetLocation() const
    {
        return TmpfsLocation_;
    }

private:
    const NDataNode::TTmpfsLayerCacheConfigPtr Config_;
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    const IInvokerPtr ControlInvoker_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;
    const TString CacheName_;
    IBootstrap* const Bootstrap_;
    IPortoExecutorPtr PortoExecutor_;
    TAbsorbLayerCallback AbsorbLayer_;

    TLayerLocationPtr TmpfsLocation_;

    THashMap<TYPath, TFetchedArtifactKey> CachedLayerDescriptors_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, DataSpinLock_);
    THashMap<TArtifactKey, TLayerPtr> CachedLayers_;
    TPeriodicExecutorPtr LayerUpdateExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, AlertSpinLock_);
    TError Alert_;

    TPromise<void> Initialized_ = NewPromise<void>();

    const TProfiler Profiler_;

    TCounter HitCounter_;
    TGauge UpdateFailedCounter_;

    TTmpfsLayerCacheCounters TmpfsLayerCacheCounters_;

    void PopulateTmpfsAlert(std::vector<TError>* errors)
    {
        auto guard = Guard(AlertSpinLock_);
        if (!Alert_.IsOK()) {
            errors->push_back(Alert_);
        }
    }

    void SetAlert(const TError& error)
    {
        auto guard = Guard(AlertSpinLock_);
        if (error.IsOK() && !Alert_.IsOK()) {
            YT_LOG_INFO("Tmpfs layer cache alert reset (CacheName: %v)", CacheName_);
            UpdateFailedCounter_.Update(0);
        } else if (!error.IsOK()) {
            YT_LOG_WARNING(error, "Tmpfs layer cache alert set (CacheName: %v)", CacheName_);
            UpdateFailedCounter_.Update(1);
        }

        Alert_ = error;
    }

    void UpdateLayers()
    {
        const auto& client = Bootstrap_->GetClient();

        auto tag = TGuid::Create();
        auto Logger = ExecNodeLogger()
            .WithTag("Tag: %v", tag);

        YT_LOG_INFO("Started updating tmpfs layers");

        TListNodeOptions listNodeOptions;
        listNodeOptions.ReadFrom = EMasterChannelKind::Cache;
        listNodeOptions.Attributes = {"id"};
        auto listNodeRspOrError = WaitFor(client->ListNode(
            *Config_->LayersDirectoryPath,
            listNodeOptions));

        if (!listNodeRspOrError.IsOK()) {
            SetAlert(TError(
                NExecNode::EErrorCode::TmpfsLayerImportFailed,
                "Failed to list %v tmpfs layers directory %v",
                CacheName_,
                Config_->LayersDirectoryPath)
                << listNodeRspOrError);
            return;
        }
        const auto& listNodeRsp = listNodeRspOrError.Value();

        THashSet<TYPath> paths;
        try {
            auto listNode = ConvertToNode(listNodeRsp)->AsList();
            for (const auto& node : listNode->GetChildren()) {
                auto idString = node->Attributes().Get<TString>("id");
                auto id = TObjectId::FromString(idString);
                paths.insert(FromObjectId(id));
            }
        } catch (const std::exception& ex) {
            SetAlert(TError(
                NExecNode::EErrorCode::TmpfsLayerImportFailed,
                "Tmpfs layers directory %v has invalid structure",
                Config_->LayersDirectoryPath)
                << ex);
            return;
        }

        YT_LOG_INFO(
            "Listed tmpfs layers (CacheName: %v, Count: %v)",
            CacheName_,
            paths.size());

        {
            THashMap<TYPath, TFetchedArtifactKey> cachedLayerDescriptors;
            for (const auto& path : paths) {
                auto it = CachedLayerDescriptors_.find(path);
                if (it != CachedLayerDescriptors_.end()) {
                    cachedLayerDescriptors.insert(*it);
                } else {
                    cachedLayerDescriptors.emplace(path, TFetchedArtifactKey{});
                }
            }

            CachedLayerDescriptors_.swap(cachedLayerDescriptors);
        }

        std::vector<TFuture<TFetchedArtifactKey>> futures;
        for (const auto& pair : CachedLayerDescriptors_) {
            auto future = BIND(
                [=, this, this_ = MakeStrong(this)] {
                    const auto& path = pair.first;
                    const auto& fetchedKey = pair.second;
                    auto revision = fetchedKey.ArtifactKey
                        ? fetchedKey.ContentRevision
                        : NHydra::NullRevision;
                    return FetchLayerArtifactKeyIfRevisionChanged(path, revision, Bootstrap_, Logger);
                })
                .AsyncVia(GetCurrentInvoker())
                .Run();

            futures.push_back(std::move(future));
        }

        auto fetchResultsOrError = WaitFor(AllSucceeded(futures));
        if (!fetchResultsOrError.IsOK()) {
            SetAlert(TError(NExecNode::EErrorCode::TmpfsLayerImportFailed, "Failed to fetch tmpfs layer descriptions")
                << fetchResultsOrError);
            return;
        }

        int index = 0;
        THashSet<TArtifactKey> newArtifacts;
        for (auto& [_, fetchedKey] : CachedLayerDescriptors_) {
            const auto& fetchResult = fetchResultsOrError.Value()[index];
            if (fetchResult.ArtifactKey) {
                fetchedKey = fetchResult;
            }
            ++index;
            YT_VERIFY(fetchedKey.ArtifactKey);
            newArtifacts.insert(*fetchedKey.ArtifactKey);
        }

        YT_LOG_DEBUG("Listed unique tmpfs layers (Count: %v)", newArtifacts.size());

        {
            std::vector<TArtifactKey> artifactsToRemove;
            artifactsToRemove.reserve(CachedLayers_.size());

            auto guard = Guard(DataSpinLock_);
            for (const auto& [key, layer] : CachedLayers_) {
                if (!newArtifacts.contains(key)) {
                    artifactsToRemove.push_back(key);
                } else {
                    newArtifacts.erase(key);
                }
            }

            for (const auto& artifactKey : artifactsToRemove) {
                CachedLayers_.erase(artifactKey);
            }

            guard.Release();

            YT_LOG_INFO_IF(
                !artifactsToRemove.empty(),
                "Released cached tmpfs layers (Count: %v)",
                artifactsToRemove.size());
        }

        std::vector<TFuture<TLayerPtr>> newLayerFutures;
        newLayerFutures.reserve(newArtifacts.size());

        TArtifactDownloadOptions downloadOptions{
            .WorkloadDescriptorAnnotations = {"Type: TmpfsLayersUpdate"},
        };
        for (const auto& artifactKey : newArtifacts) {
            newLayerFutures.push_back(AbsorbLayer_(
                artifactKey,
                downloadOptions,
                tag,
                TmpfsLocation_));
        }

        auto newLayersOrError = WaitFor(AllSet(newLayerFutures));
        if (!newLayersOrError.IsOK()) {
            SetAlert(TError(NExecNode::EErrorCode::TmpfsLayerImportFailed, "Failed to import new tmpfs layers")
                << newLayersOrError);
            return;
        }

        bool hasFailedLayer = false;
        bool hasImportedLayer = false;
        for (const auto& newLayerOrError : newLayersOrError.Value()) {
            if (!newLayerOrError.IsOK()) {
                hasFailedLayer = true;
                SetAlert(TError(NExecNode::EErrorCode::TmpfsLayerImportFailed, "Failed to import new %v tmpfs layer", CacheName_)
                    << newLayerOrError);
                continue;
            }

            const auto& layer = newLayerOrError.Value();
            YT_LOG_INFO(
                "Successfully imported new tmpfs layer (LayerId: %v, ArtifactPath: %v, CacheName: %v)",
                layer->GetMeta().Id,
                layer->GetMeta().artifact_key().data_source().path(),
                CacheName_);
            hasImportedLayer = true;

            TArtifactKey key;
            key.CopyFrom(layer->GetMeta().artifact_key());

            auto guard = Guard(DataSpinLock_);
            CachedLayers_[key] = layer;
        }

        if (!hasFailedLayer) {
            // No alert, everything is fine.
            SetAlert(TError());
        }

        if (hasImportedLayer || !hasFailedLayer) {
            // If at least one tmpfs layer was successfully imported,
            // we consider tmpfs layer cache initialization completed.
            Initialized_.TrySet();
        }

        YT_LOG_INFO("Finished updating tmpfs layers");
    }
};

DEFINE_REFCOUNTED_TYPE(TTmpfsLayerCache)
DECLARE_REFCOUNTED_CLASS(TTmpfsLayerCache)

////////////////////////////////////////////////////////////////////////////////

//! This class caches volumes generated from cypress files (layers).
template <typename TKey>
class TVolumeCacheBase
    : public TAsyncMapBase<TKey, TCachedVolume<TKey>>
{
public:
    TVolumeCacheBase(
        const TProfiler& profiler,
        IBootstrap* const bootstrap,
        std::vector<TLayerLocationPtr> layerLocations)
        : TAsyncMapBase<TKey, TCachedVolume<TKey>>(profiler)
        , Bootstrap_(bootstrap)
        , LayerLocations_(std::move(layerLocations))
    { }

    bool IsEnabled() const
    {
        for (const auto& location : LayerLocations_) {
            if (location->IsEnabled()) {
                return true;
            }
        }

        return false;
    }

protected:
    const IBootstrap* const Bootstrap_;
    const std::vector<TLayerLocationPtr> LayerLocations_;

    TLayerLocationPtr PickLocation()
    {
        return DoPickLocation(LayerLocations_, [] (const TLayerLocationPtr& candidate, const TLayerLocationPtr& current) {
            return candidate->GetVolumeCount() < current->GetVolumeCount();
        });
    }

    void OnAdded(const TIntrusivePtr<TCachedVolume<TKey>>& volume) override
    {
        YT_LOG_DEBUG("Volume added to cache (VolumeId: %v)",
            volume->GetId());
    }

    void OnRemoved(const TIntrusivePtr<TCachedVolume<TKey>>& volume) override
    {
        YT_LOG_DEBUG("Volume removed from cache (VolumeId: %v)",
            volume->GetId());
    }

    void OnWeightUpdated(i64 weightDelta) override
    {
        YT_LOG_DEBUG("Volume cache total weight updated (WeightDelta: %v)",
            weightDelta);
    }
};

DEFINE_REFCOUNTED_TYPE(TVolumeCacheBase<TArtifactKey>)

////////////////////////////////////////////////////////////////////////////////

//! This class caches volumes generated from cypress files (layers).
class TSquashFSVolumeCache
    : public TVolumeCacheBase<TArtifactKey>
{
public:
    TSquashFSVolumeCache(
        IBootstrap* const bootstrap,
        std::vector<TLayerLocationPtr> layerLocations,
        IVolumeArtifactCachePtr artifactCache)
        : TVolumeCacheBase(
            ExecNodeProfiler().WithPrefix("/squashfs_volume_cache"),
            bootstrap,
            std::move(layerLocations))
        , ArtifactCache_(std::move(artifactCache))
    { }

    TFuture<IVolumePtr> GetOrCreateVolume(
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

private:
    const IVolumeArtifactCachePtr ArtifactCache_;

    TFuture<TSquashFSVolumePtr> DownloadAndPrepareVolume(
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

    TSquashFSVolumePtr CreateSquashFSVolume(
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
};

DECLARE_REFCOUNTED_CLASS(TSquashFSVolumeCache)
DEFINE_REFCOUNTED_TYPE(TSquashFSVolumeCache)

////////////////////////////////////////////////////////////////////////////////

//! This class caches volumes generated from cypress files (layers).
class TRONbdVolumeCache
    : public TVolumeCacheBase<TString>
{
public:
    using TVolumePtr = TIntrusivePtr<TCachedVolume<TString>>;

    TRONbdVolumeCache(
        IBootstrap* const bootstrap,
        TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        std::vector<TLayerLocationPtr> layerLocations)
        : TVolumeCacheBase(
            ExecNodeProfiler().WithPrefix("/ronbd_volume_cache"),
            bootstrap,
            std::move(layerLocations))
        , DynamicConfigManager_(std::move(dynamicConfigManager))
    { }

    TFuture<IVolumePtr> GetOrCreateVolume(
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

private:
    const TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, InsertLock_);

    static void ValidatePrepareNbdVolumeOptions(const TPrepareRONbdVolumeOptions& options)
    {
        const auto& artifactKey = options.ArtifactKey;
        YT_VERIFY(artifactKey.has_access_method());
        YT_VERIFY(FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Nbd);
        YT_VERIFY(artifactKey.has_filesystem());
        YT_VERIFY(artifactKey.has_nbd_device_id());
        const auto& deviceId = artifactKey.nbd_device_id();
        YT_VERIFY(!deviceId.empty());
    }

    TInsertCookie GetInsertCookie(const TString& deviceId, const INbdServerPtr& nbdServer)
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

    //! Make callback that subscribes job for NBD device errors.
    TExtendedCallback<TVolumePtr(const TErrorOr<TVolumePtr>&)> MakeJobSubscriberForDeviceErrors(
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

    IImageReaderPtr CreateArtifactReader(
        const TLogger& Logger,
        const TArtifactKey& artifactKey)
    {
        YT_LOG_DEBUG("Creating NBD artifact reader");

        return CreateCypressFileImageReader(
            artifactKey,
            Bootstrap_->GetLayerReaderHost(),
            Bootstrap_->GetDefaultInThrottler(),
            Bootstrap_->GetReadRpsOutThrottler(),
            Bootstrap_->GetNbdServer()->GetInvoker(),
            Bootstrap_->GetNbdServer()->GetLogger());
    }

    TFuture<IBlockDevicePtr> CreateRONbdDevice(
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

    TFuture<TRONbdVolumePtr> CreateRONbdVolume(
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

    //! Create RO NBD volume. The order of creation is as follows:
    //! 1. Create RO NBD device.
    //! 2. Register RO NBD device with NBD server.
    //! 3. Create RO NBD porto volume connected to RO NBD device.
    TFuture<TRONbdVolumePtr> PrepareRONbdVolume(
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
};

DECLARE_REFCOUNTED_CLASS(TRONbdVolumeCache)
DEFINE_REFCOUNTED_TYPE(TRONbdVolumeCache)

////////////////////////////////////////////////////////////////////////////////

class TLayerCache
    : public TAsyncSlruCacheBase<TArtifactKey, TLayer>
{
public:
    TLayerCache(
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
        , ProfilingExecutor_(New<TPeriodicExecutor>(
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

    TFuture<void> Initialize()
    {
        Semaphore_ = New<TAsyncSemaphore>(
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

    bool IsEnabled() const
    {
        for (const auto& location : LayerLocations_) {
            if (location->IsEnabled()) {
                return true;
            }
        }

        return false;
    }

    TLayerLocationPtr PickLocation()
    {
        return DoPickLocation(LayerLocations_, [] (const TLayerLocationPtr& candidate, const TLayerLocationPtr& current) {
            return candidate->GetVolumeCount() < current->GetVolumeCount();
        });
    }

    void PopulateAlerts(std::vector<TError>* alerts)
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

    TFuture<void> Disable(const TError& reason)
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

    TFuture<TLayerPtr> PrepareLayer(
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

    TFuture<void> GetVolumeReleaseEvent()
    {
        std::vector<TFuture<void>> futures;
        for (const auto& location : LayerLocations_) {
            futures.push_back(location->GetVolumeReleaseEvent());
        }

        return AllSet(std::move(futures))
            .AsVoid()
            .ToUncancelable();
    }

    bool IsLayerCached(const TArtifactKey& artifactKey)
    {
        auto layer = FindLayerInTmpfs(artifactKey);
        if (layer) {
            return true;
        }

        return Find(artifactKey) != nullptr;
    }

    void Touch(const TLayerPtr& layer)
    {
        layer->IncreaseHitCount();
        Find(layer->GetKey());
    }

    void BuildOrchid(TFluentAny fluent) const
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

    void OnDynamicConfigChanged(
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

private:
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    const IVolumeArtifactCachePtr ArtifactCache_;
    const IInvokerPtr ControlInvoker_;
    const std::vector<TLayerLocationPtr> LayerLocations_;
    const IPortoExecutorPtr TmpfsExecutor_;

    TAsyncSemaphorePtr Semaphore_;

    TTmpfsLayerCachePtr RegularTmpfsLayerCache_;
    TTmpfsLayerCachePtr NirvanaTmpfsLayerCache_;

    TPeriodicExecutorPtr ProfilingExecutor_;

    static TSlruCacheConfigPtr CreateCacheConfig(
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

    i64 GetWeight(const TLayerPtr& layer) const override
    {
        return layer->GetSize();
    }

    void OnAdded(const TLayerPtr& layer) override
    {
        YT_LOG_DEBUG(
            "Layer added to cache (LayerId: %v, ArtifactPath: %v, Size: %v)",
            layer->GetMeta().Id,
            layer->GetCypressPath(),
            layer->GetSize());
    }

    void OnRemoved(const TLayerPtr& layer) override
    {
        YT_LOG_DEBUG(
            "Layer removed from cache (LayerId: %v, ArtifactPath: %v, Size: %v)",
            layer->GetMeta().Id,
            layer->GetCypressPath(),
            layer->GetSize());
    }

    void OnWeightUpdated(i64 weightDelta) override
    {
        YT_LOG_DEBUG("Layer cache weight updated (WeightDelta: %v)", weightDelta);
    }

    void ProfileLocation(const TLayerLocationPtr& location) {
        auto& performanceCounters = location->GetPerformanceCounters();

        performanceCounters.AvailableSpace.Update(location->GetAvailableSpace());
        performanceCounters.UsedSpace.Update(location->GetUsedSpace());
        performanceCounters.TotalSpace.Update(location->GetCapacity());
        performanceCounters.Full.Update(location->IsFull() ? 1 : 0);
        performanceCounters.LayerCount.Update(location->GetLayerCount());
        performanceCounters.VolumeCount.Update(location->GetVolumeCount());
    }

    TLayerPtr FindLayerInTmpfs(const TArtifactKey& artifactKey, const TGuid& tag = TGuid()) {
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

    TFuture<TLayerPtr> DownloadAndImportLayer(
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
                TAsyncSemaphoreGuard guard;
                while (!(guard = TAsyncSemaphoreGuard::TryAcquire(Semaphore_))) {
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

    TLayerLocationPtr PickLocation() const
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

    void OnProfiling()
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
};

DECLARE_REFCOUNTED_CLASS(TLayerCache)
DEFINE_REFCOUNTED_TYPE(TLayerCache)

////////////////////////////////////////////////////////////////////////////////

class TOverlayVolume
    : public TPortoVolumeBase
{
public:
    TOverlayVolume(
        TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr location,
        std::vector<TOverlayData> overlayDataArray)
        : TPortoVolumeBase(
            std::move(tagSet),
            std::move(volumeMeta),
            std::move(location))
        , OverlayDataArray_(std::move(overlayDataArray))
    {
        SetRemoveCallback(BIND(
            &TOverlayVolume::DoRemove,
            TagSet_,
            LayerLocation_,
            VolumeMeta_,
            OverlayDataArray_));
    }

    ~TOverlayVolume() override
    {
        YT_LOG_DEBUG("Overlay volume object is destroyed (VolumeId: %v)", GetId());
        YT_UNUSED_FUTURE(Remove());
    }

    bool IsRootVolume() const override final
    {
        return false;
    }

private:
    // Holds volumes and layers (so that they are not destroyed) while they are needed.
    const std::vector<TOverlayData> OverlayDataArray_;

    static TFuture<void> DoRemove(
        TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta,
        std::vector<TOverlayData> overlayDataArray)
    {
        // At first remove overlay volume, then remove constituent volumes and layers.
        auto postRemovalCleanup = BIND_NO_PROPAGATE([overlayDataArray = std::move(overlayDataArray)] (const TLogger&) mutable -> TFuture<void> {
            std::vector<TFuture<void>> futures;
            futures.reserve(overlayDataArray.size());
            for (auto& overlayData : overlayDataArray) {
                futures.push_back(overlayData.Remove());
            }
            return AllSucceeded(std::move(futures));
        });

        return DoRemoveVolumeCommon(
            "Overlay",
            std::move(tagSet),
            std::move(location),
            std::move(volumeMeta),
            std::move(postRemovalCleanup));
    }
};

DECLARE_REFCOUNTED_CLASS(TOverlayVolume)
DEFINE_REFCOUNTED_TYPE(TOverlayVolume)

////////////////////////////////////////////////////////////////////////////////

class TTmpfsVolume
    : public TPortoVolumeBase
{
public:
    TTmpfsVolume(
        TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr location)
        : TPortoVolumeBase(
            std::move(tagSet),
            std::move(volumeMeta),
            std::move(location))
    {
        SetRemoveCallback(BIND(
            &TTmpfsVolume::DoRemove,
            TagSet_,
            LayerLocation_,
            VolumeMeta_));
    }

    ~TTmpfsVolume() override
    {
        YT_UNUSED_FUTURE(Remove());
    }

    bool IsRootVolume() const override final
    {
        return false;
    }

private:
    static TFuture<void> DoRemove(
        TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta)
    {
        return DoRemoveVolumeCommon(
            "Tmpfs",
            std::move(tagSet),
            std::move(location),
            std::move(volumeMeta));
    }
};

DEFINE_REFCOUNTED_TYPE(TTmpfsVolume)

////////////////////////////////////////////////////////////////////////////////

class TSimpleTmpfsVolume
    : public IVolume
{
public:
    TSimpleTmpfsVolume(
        TTagSet tagSet,
        const std::string& path,
        IInvokerPtr invoker,
        bool detachUnmount)
        : TagSet_(std::move(tagSet))
        , Path_(path)
        , VolumeId_(
            [&path] {
                auto [low, high] = CityHash128(path.c_str(), path.size());
                return TGuid(low, high);
            }())
        , Invoker_(std::move(invoker))
        , DetachUnmount_(detachUnmount)
    { }

    ~TSimpleTmpfsVolume() override
    {
        YT_UNUSED_FUTURE(Remove());
    }

    bool IsCached() const final
    {
        return false;
    }

    TFuture<void> Link(
        TGuid,
        const TString&) override final
    {
        // Simple volume is created inside sandbox, so we don't need to link it.
        YT_UNIMPLEMENTED("Link is not implemented for SimpleTmpfsVolume");
    }

    TFuture<void> Remove() override final
    {
        if (RemoveFuture_) {
            return RemoveFuture_;
        }

        TEventTimerGuard volumeRemoveTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(TagSet_, "/remove_time"));

        const auto volumeType = EVolumeType::Tmpfs;
        const auto& volumeId = VolumeId_;
        const auto& volumePath = Path_;

        auto Logger = ExecNodeLogger()
            .WithTag("VolumeType: %v, VolumeId: %v, VolumePath: %v",
                volumeType,
                volumeId,
                volumePath);

        RemoveFuture_ = BIND(
            [
                tagSet = TagSet_,
                Logger,
                this,
                this_ = MakeStrong(this)
            ] {
                try {
                    RunTool<TRemoveDirContentAsRootTool>(Path_);

                    auto config = New<TUmountConfig>();
                    config->Path = Path_;
                    config->Detach = DetachUnmount_;
                    RunTool<TUmountAsRootTool>(config);

                    TVolumeProfilerCounters::Get()->GetGauge(tagSet, "/count")
                        .Update(VolumeCounters().Decrement(tagSet));
                    TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/removed").Increment(1);
                } catch (const std::exception& ex) {
                    TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/remove_errors").Increment(1);

                    YT_LOG_ERROR(
                        ex,
                        "Failed to remove volume");

                    THROW_ERROR_EXCEPTION("Failed to remove volume")
                        << ex;
                }
            })
            .AsyncVia(Invoker_)
            .Run()
            .ToUncancelable();

        return RemoveFuture_;
    }

    const TVolumeId& GetId() const override final
    {
        return VolumeId_;
    }

    const std::string& GetPath() const override final
    {
        return Path_;
    }

    bool IsRootVolume() const override final
    {
        return false;
    }

private:
    const TTagSet TagSet_;
    const std::string Path_;
    const TVolumeId VolumeId_;
    const IInvokerPtr Invoker_;
    const bool DetachUnmount_;
    TFuture<void> RemoveFuture_;
};

DEFINE_REFCOUNTED_TYPE(TSimpleTmpfsVolume)

////////////////////////////////////////////////////////////////////////////////

const std::string& TOverlayData::GetPath() const
{
    if (std::holds_alternative<TLayerPtr>(Variant_)) {
        return std::get<TLayerPtr>(Variant_)->GetPath();
    }

    return std::get<IVolumePtr>(Variant_)->GetPath();
}

TFuture<void> TOverlayData::Remove()
{
    if (IsLayer()) {
        return OKFuture;
    }

    const auto& self = GetVolume();
    if (self->IsCached()) {
        return OKFuture;
    }

    return self->Remove();
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

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

} // namespace NYT::NExecNode
