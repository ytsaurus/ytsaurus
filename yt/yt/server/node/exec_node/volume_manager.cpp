#include "volume_manager.h"

#include "bootstrap.h"
#include "chunk_cache.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/server/node/data_node/private.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/artifact.h>
#include <yt/yt/server/node/data_node/chunk.h>
#include <yt/yt/server/node/data_node/disk_location.h>

#include <yt/yt/server/node/exec_node/volume.pb.h>
#include <yt/yt/server/node/exec_node/bootstrap.h>

#include <yt/yt/server/lib/nbd/cypress_file_block_device.h>

#include <yt/yt/library/containers/instance.h>
#include <yt/yt/library/containers/porto_executor.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/misc/disk_health_checker.h>
#include <yt/yt/server/lib/misc/private.h>

#include <yt/yt/server/tools/tools.h>
#include <yt/yt/server/tools/proc.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/library/profiling/tagged_counters.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/net/connection.h>

#include <yt/yt/library/process/process.h>

#include <library/cpp/resource/resource.h>

#include <library/cpp/yt/string/string.h>

#include <util/system/fs.h>

#include <yt/yt/core/net/local_address.h>

namespace NYT::NExecNode {

using namespace NApi;
using namespace NNbd;
using namespace NConcurrency;
using namespace NContainers;
using namespace NClusterNode;
using namespace NDataNode;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NTools;
using namespace NYson;
using namespace NYTree;

using NControllerAgent::ELayerAccessMethod;
using NControllerAgent::ELayerFilesystem;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;
static const auto ProfilingPeriod = TDuration::Seconds(1);

static const TString StorageSuffix = "storage";
static const TString MountSuffix = "mount";

////////////////////////////////////////////////////////////////////////////////

namespace {

IBlockDevicePtr CreateCypressFileBlockDevice(
    TNbdConfigPtr nbdConfig,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    const TArtifactKey& artifactKey,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    const NLogging::TLogger& Logger)
{
    YT_VERIFY(artifactKey.has_filesystem());
    YT_VERIFY(artifactKey.has_nbd_export_id());

    YT_LOG_INFO("Creating NBD Cypress file block device (Path: %v, FileSystem: %v, ExportId: %v, ChunkSpecs: %v)",
        artifactKey.data_source().path(),
        FromProto<ELayerFilesystem>(artifactKey.filesystem()),
        artifactKey.nbd_export_id(),
        artifactKey.chunk_specs_size());

    auto config = New<TCypressFileBlockDeviceConfig>();
    config->Path = artifactKey.data_source().path();
    config->TestSleepBeforeRead = nbdConfig->Server->TestBlockDeviceSleepBeforeRead;
    if (config->Path.empty()) {
        THROW_ERROR_EXCEPTION("Empty file path for filesystem layer")
            << TErrorAttribute("type_name", artifactKey.GetTypeName())
            << TErrorAttribute("filesystem", FromProto<ELayerFilesystem>(artifactKey.filesystem()))
            << TErrorAttribute("access_method", artifactKey.access_method())
            << TErrorAttribute("nbd_export_id", artifactKey.nbd_export_id());
    }

    auto device = CreateCypressFileBlockDevice(
        artifactKey.nbd_export_id(),
        artifactKey.chunk_specs(),
        std::move(config),
        std::move(inThrottler),
        std::move(outRpsThrottler),
        std::move(client),
        std::move(invoker),
        Logger);

    YT_LOG_INFO("Created NBD Cypress file block device (Path: %v, FileSystem: %v, ExportId: %v, ChunkSpecs: %v)",
        artifactKey.data_source().path(),
        FromProto<ELayerFilesystem>(artifactKey.filesystem()),
        artifactKey.nbd_export_id(),
        artifactKey.chunk_specs_size());

    return device;
}

////////////////////////////////////////////////////////////////////////////////

class TVolumeProfilerCounters
{
public:
    TVolumeProfilerCounters()
        : VolumeProfiler_("/volumes")
    { }

    NProfiling::TCounter GetCounter(const NProfiling::TTagSet& tagSet, const TString& name)
    {
        auto key = CreateKey(tagSet, name);

        auto guard = Guard(Lock_);
        auto [it, inserted] = Counters_.emplace(key, NProfiling::TCounter());
        if (inserted) {
            it->second = VolumeProfiler_.WithTags(tagSet).Counter(name);
        }

        return it->second;
    }

    NProfiling::TGauge GetGauge(const NProfiling::TTagSet& tagSet, const TString& name)
    {
        auto key = CreateKey(tagSet, name);

        auto guard = Guard(Lock_);
        auto [it, inserted] = Gauges_.emplace(key, NProfiling::TGauge());
        if (inserted) {
            it->second = VolumeProfiler_.WithTags(tagSet).Gauge(name);
        }

        return it->second;
    }

    NProfiling::TEventTimer GetTimeHistogram(const NProfiling::TTagSet& tagSet, const TString& name)
    {
        auto key = CreateKey(tagSet, name);

        auto guard = Guard(Lock_);
        auto [it, inserted] = EventTimers_.emplace(key, NProfiling::TEventTimer());
        if (inserted) {
            std::vector<TDuration> bounds{
                TDuration::Zero(),
                TDuration::MilliSeconds(100),
                TDuration::MilliSeconds(500),
                TDuration::Seconds(1),
                TDuration::Seconds(5),
                TDuration::Seconds(10),
            };
            it->second = VolumeProfiler_.WithTags(tagSet).TimeHistogram(name, std::move(bounds));
        }

        return it->second;
    }

    NProfiling::TEventTimer GetTimer(const NProfiling::TTagSet& tagSet, const TString& name)
    {
        auto key = CreateKey(tagSet, name);

        auto guard = Guard(Lock_);
        auto [it, inserted] = EventTimers_.emplace(key, NProfiling::TEventTimer());
        if (inserted) {
            it->second = VolumeProfiler_.WithTags(tagSet).Timer(name);
        }

        return it->second;
    }

    static NProfiling::TTagSet MakeTagSet(const TString& volumeType, const TString& volumeFilePath)
    {
        return NProfiling::TTagSet({{"type", volumeType}, {"file_path", volumeFilePath}});
    }

    static TVolumeProfilerCounters* Get()
    {
        return Singleton<TVolumeProfilerCounters>();
    }

private:
    using TKey = NProfiling::TTagList;

    static TKey CreateKey(const NProfiling::TTagSet& tagSet, const TString& name)
    {
        auto key = tagSet.Tags();
        key.push_back({"name", name});
        return key;
    }

private:
    const NProfiling::TProfiler VolumeProfiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TKey, NProfiling::TCounter> Counters_;
    THashMap<TKey, NProfiling::TGauge> Gauges_;
    THashMap<TKey, NProfiling::TEventTimer> EventTimers_;
};

NProfiling::TTaggedCounters<int>& VolumeCounters()
{
    static NProfiling::TTaggedCounters<int> result;
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TVolumeArtifactAdapter
    : public IVolumeArtifact
{
public:
    TVolumeArtifactAdapter(IChunkPtr chunk)
        : Chunk_(chunk)
    { }

    TString GetFileName() const override
    {
        return Chunk_->GetFileName();
    }

private:
    IChunkPtr Chunk_;
};

////////////////////////////////////////////////////////////////////////////////

class TVolumeChunkCacheAdapter
    : public IVolumeChunkCache
{
public:
    TVolumeChunkCacheAdapter(TChunkCachePtr chunkCache)
        : ChunkCache_(chunkCache)
    { }

    TFuture<IVolumeArtifactPtr> DownloadArtifact(
        const TArtifactKey& key,
        const TArtifactDownloadOptions& artifactDownloadOptions) override
    {
        auto artifact = ChunkCache_->DownloadArtifact(key, artifactDownloadOptions);
        return artifact.Apply(BIND([] (IChunkPtr artifact) {
            return IVolumeArtifactPtr(New<TVolumeArtifactAdapter>(artifact));
        }));
    }

private:
    TChunkCachePtr ChunkCache_;
};

////////////////////////////////////////////////////////////////////////////////

IVolumeChunkCachePtr CreateVolumeChunkCacheAdapter(TChunkCachePtr chunkCache)
{
    return New<TVolumeChunkCacheAdapter>(chunkCache);
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
    : public NDataNode::NProto::TLayerMeta
{
    TString Path;
    TLayerId Id;
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeMeta
    : public NDataNode::NProto::TVolumeMeta
{
    TVolumeId Id;
    TString MountPath;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLayer)
DECLARE_REFCOUNTED_CLASS(TNbdVolume)

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

    const TString& GetPath() const;
    const TArtifactKey& GetArtifactKey() const;

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

struct TLayerLocationPerformanceCounters
{
    TLayerLocationPerformanceCounters() = default;

    explicit TLayerLocationPerformanceCounters(const TProfiler& profiler)
    {
        LayerCount = profiler.Gauge("/layer_count");
        VolumeCount = profiler.Gauge("/volume_count");

        AvailableSpace = profiler.Gauge("/available_space");
        UsedSpace = profiler.Gauge("/used_space");
        AvailableSpace = profiler.Gauge("/available_space");
        TotalSpace = profiler.Gauge("/total_space");
        Full = profiler.Gauge("/full");

        ImportLayerTimer = profiler.Timer("/import_layer_time");
    }

    NProfiling::TGauge LayerCount;
    NProfiling::TGauge VolumeCount;

    NProfiling::TGauge TotalSpace;
    NProfiling::TGauge UsedSpace;
    NProfiling::TGauge AvailableSpace;
    NProfiling::TGauge Full;

    TEventTimer ImportLayerTimer;
};

////////////////////////////////////////////////////////////////////////////////

static const TString VolumesName = "volumes";
static const TString LayersName = "porto_layers";
static const TString LayersMetaName = "layers_meta";
static const TString VolumesMetaName = "volumes_meta";

class TLayerLocation
    : public TDiskLocation
{
public:
    TLayerLocation(
        TLayerLocationConfigPtr locationConfig,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        TDiskHealthCheckerConfigPtr healthCheckerConfig,
        IPortoExecutorPtr volumeExecutor,
        IPortoExecutorPtr layerExecutor,
        const TString& id)
        : TDiskLocation(locationConfig, id, ExecNodeLogger)
        , Config_(locationConfig)
        , DynamicConfigManager_(dynamicConfigManager)
        , VolumeExecutor_(std::move(volumeExecutor))
        , LayerExecutor_(std::move(layerExecutor))
        , LocationQueue_(New<TActionQueue>(id))
        , VolumesPath_(NFS::CombinePaths(Config_->Path, VolumesName))
        , VolumesMetaPath_(NFS::CombinePaths(Config_->Path, VolumesMetaName))
        , LayersPath_(NFS::CombinePaths(Config_->Path, LayersName))
        , LayersMetaPath_(NFS::CombinePaths(Config_->Path, LayersMetaName))
        // If true, location is placed on a YT-specific drive, binded into container from dom0 host,
        // so it has absolute path relative to dom0 root.
        // Otherwise, location is placed inside a persistent volume, and should be treated differently.
        // More details here: PORTO-460.
        , PlacePath_((Config_->LocationIsAbsolute ? "" : "//") + Config_->Path)
    {
        auto profiler = LocationProfiler
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
        return BIND(&TLayerLocation::DoInitialize, MakeStrong(this))
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<TLayerMeta> ImportLayer(const TArtifactKey& artifactKey, const TString& archivePath, TGuid tag)
    {
        return BIND(&TLayerLocation::DoImportLayer, MakeStrong(this), artifactKey, archivePath, tag)
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<TLayerMeta> InternalizeLayer(const TLayerMeta& layerMeta, TGuid tag)
    {
        return BIND(&TLayerLocation::DoInternalizeLayer, MakeStrong(this), layerMeta, tag)
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<void> RemoveLayer(const TLayerId& layerId)
    {
        return BIND(&TLayerLocation::DoRemoveLayer, MakeStrong(this), layerId)
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<TVolumeMeta> CreateNbdVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TArtifactKey& artifactKey,
        TNbdConfigPtr nbdConfig)
    {
        return BIND(&TLayerLocation::DoCreateNbdVolume, MakeStrong(this), tag, std::move(tagSet), Passed(std::move(volumeCreateTimeGuard)), artifactKey, std::move(nbdConfig))
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<TVolumeMeta> CreateOverlayVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TUserSandboxOptions& options,
        const std::vector<TOverlayData>& overlayDataArray)
    {
        return BIND(&TLayerLocation::DoCreateOverlayVolume, MakeStrong(this), tag, std::move(tagSet), Passed(std::move(volumeCreateTimeGuard)), options, overlayDataArray)
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<TVolumeMeta> CreateSquashFSVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TArtifactKey& artifactKey,
        const TString& squashFSFilePath)
    {
        return BIND(&TLayerLocation::DoCreateSquashFSVolume, MakeStrong(this), tag, std::move(tagSet), Passed(std::move(volumeCreateTimeGuard)), artifactKey, squashFSFilePath)
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<void> RemoveVolume(NProfiling::TTagSet tagSet, const TVolumeId& volumeId)
    {
        return BIND(&TLayerLocation::DoRemoveVolume, MakeStrong(this), std::move(tagSet), volumeId)
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

    void StopHealthChecker()
    {
        if (HealthChecker_) {
            HealthChecker_->Stop();
        }
    }

    void Disable(const TError& error, bool persistentDisable = true)
    {
        // TODO(don-dron): Research and fix unconditional Disabled.
        auto guard = Guard(SpinLock_);
        if (State_.exchange(ELocationState::Disabled) != ELocationState::Enabled) {
            return;
        }

        YT_LOG_WARNING("Layer location disabled (Path: %v)", Config_->Path);

        StopHealthChecker();

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

private:
    const TLayerLocationConfigPtr Config_;
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
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

    TString GetLayerPath(const TLayerId& id) const
    {
        return NFS::CombinePaths(LayersPath_, ToString(id));
    }

    TString GetLayerMetaPath(const TLayerId& id) const
    {
        return NFS::CombinePaths(LayersMetaPath_, ToString(id)) + ".meta";
    }

    TString GetVolumePath(const TVolumeId& id) const
    {
        return NFS::CombinePaths(VolumesPath_, ToString(id));
    }

    TString GetVolumeMetaPath(const TVolumeId& id) const
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
            if (fileName.EndsWith(NFS::TempFileSuffix)) {
                YT_LOG_DEBUG("Remove temporary file (Path: %v)",
                    fileName);
                NFS::Remove(fileName);
                continue;
            }

            auto nameWithoutExtension = NFS::GetFileNameWithoutExtension(fileName);
            TGuid id;
            if (!TGuid::FromString(nameWithoutExtension, &id)) {
                YT_LOG_ERROR("Unrecognized file in layer location directory (Path: %v)",
                    fileName);
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
                YT_LOG_ERROR("Unrecognized layer name in layer location directory (LayerName: %v)",
                    layerName);
                continue;
            }

            if (!fileIds.contains(id)) {
                YT_LOG_DEBUG("Remove directory without a corresponding meta file (LayerName: %v)",
                    layerName);
                auto async = DynamicConfigManager_
                    ->GetConfig()
                    ->ExecNode
                    ->VolumeManager
                    ->EnableAsyncLayerRemoval;
                WaitFor(LayerExecutor_->RemoveLayer(layerName, PlacePath_, async))
                    .ThrowOnError();
                continue;
            }

            YT_VERIFY(confirmedIds.insert(id).second);
            YT_VERIFY(fileIds.erase(id) == 1);
        }

        for (const auto& id : fileIds) {
            auto path = GetLayerMetaPath(id);
            YT_LOG_DEBUG("Remove layer meta file with no matching layer (Path: %v)",
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
                    sizeof (TLayerMetaHeader));
            }

            auto metaFileBlob = TSharedMutableRef::Allocate(metaFile.GetLength());

            NFS::WrapIOErrors([&] () {
                TFileInput metaFileInput(metaFile);
                metaFileInput.Read(metaFileBlob.Begin(), metaFile.GetLength());
            });

            const auto* metaHeader = reinterpret_cast<const TLayerMetaHeader*>(metaFileBlob.Begin());
            if (metaHeader->Signature != TLayerMetaHeader::ExpectedSignature) {
                THROW_ERROR_EXCEPTION("Incorrect layer header signature %x in layer meta file %v",
                    metaHeader->Signature,
                    metaFileName);
            }

            auto metaBlob = TRef(metaFileBlob.Begin() + sizeof(TLayerMetaHeader), metaFileBlob.End());
            if (metaHeader->MetaChecksum != GetChecksum(metaBlob)) {
                THROW_ERROR_EXCEPTION("Incorrect layer meta checksum in layer meta file %v",
                    metaFileName);
            }

            NDataNode::NProto::TLayerMeta protoMeta;
            if (!TryDeserializeProtoWithEnvelope(&protoMeta, metaBlob)) {
                THROW_ERROR_EXCEPTION("Failed to parse chunk meta file %v",
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
        try {
            NFS::MakeDirRecursive(Config_->Path, 0755);
            if (HealthChecker_) {
                WaitFor(HealthChecker_->RunCheck())
                    .ThrowOnError();
            }

            // Volumes are not expected to be used since all jobs must be dead by now.
            auto volumePaths = WaitFor(VolumeExecutor_->ListVolumePaths())
                .ValueOrThrow();

            std::vector<TFuture<void>> unlinkFutures;
            for (const auto& volumePath : volumePaths) {
                if (volumePath.StartsWith(VolumesPath_)) {
                    unlinkFutures.push_back(VolumeExecutor_->UnlinkVolume(volumePath, "self"));
                }
            }

            auto unlinkResults = WaitFor(AllSet(unlinkFutures))
                .ValueOrThrow();

            for (const auto& unlinkError : unlinkResults) {
                if (!unlinkError.IsOK() && unlinkError.GetCode() != EPortoErrorCode::VolumeNotLinked && unlinkError.GetCode() != EPortoErrorCode::VolumeNotFound) {
                    THROW_ERROR unlinkError;
                }
            }

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
                HealthChecker_->SubscribeFailed(BIND([=, this, this_ = MakeWeak(this)] (const TError& result) {
                    Disable(result, true);
                })
                    .Via(LocationQueue_->GetInvoker()));
                HealthChecker_->Start();
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to initialize layer location %v",
                Config_->Path)
                << ex;
        }

        {
            auto guard = Guard(SpinLock_);
            ChangeState(ELocationState::Enabled);
        }
    }

    TLayerMeta DoInternalizeLayer(const TLayerMeta& layerMeta, TGuid tag)
    {
        ValidateEnabled();
        auto layerDirectory = GetLayerPath(layerMeta.Id);

        try {
            YT_LOG_DEBUG("Copy layer (Destination: %v, Source: %v, Tag: %v)",
                layerDirectory,
                layerMeta.Path,
                tag);
            auto config = New<TCopyDirectoryContentConfig>();
            config->Source = layerMeta.Path;
            config->Destination = LayersPath_;
            RunTool<TCopyDirectoryContentTool>(config);

            TLayerMeta newMeta = layerMeta;
            newMeta.Path = layerDirectory;

            DoFinalizeLayerImport(newMeta, tag);
            return newMeta;
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Layer internalization failed (LayerId: %v, SourcePath: %v, Tag: %v)",
                layerMeta.Id,
                layerMeta.Path,
                tag);

            try {
                RunTool<TRemoveDirAsRootTool>(layerDirectory);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to cleanup directory after failed layer internalization");
            }

            THROW_ERROR_EXCEPTION(EErrorCode::LayerUnpackingFailed, "Layer internalization failed")
                << TErrorAttribute("layer_path", layerMeta.artifact_key().data_source().path())
                << ex;
        }
    }

    void DoFinalizeLayerImport(const TLayerMeta& layerMeta, TGuid tag)
    {
        auto metaBlob = SerializeProtoToRefWithEnvelope(layerMeta);

        TLayerMetaHeader header;
        header.MetaChecksum = GetChecksum(metaBlob);

        auto layerMetaFileName = GetLayerMetaPath(layerMeta.Id);
        auto temporaryLayerMetaFileName = layerMetaFileName + NFS::TempFileSuffix;

        TFile metaFile(
            temporaryLayerMetaFileName,
            CreateAlways | WrOnly | Seq | CloseOnExec);
        metaFile.Write(&header, sizeof(header));
        metaFile.Write(metaBlob.Begin(), metaBlob.Size());
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

        YT_LOG_INFO("Finished layer import (LayerId: %v, LayerPath: %v, UsedSpace: %v, AvailableSpace: %v, Tag: %v)",
            layerMeta.Id,
            layerMeta.Path,
            usedSpace,
            availableSpace,
            tag);
    }

    TLayerMeta DoImportLayer(const TArtifactKey& artifactKey, const TString& archivePath, TGuid tag)
    {
        ValidateEnabled();

        auto id = TLayerId::Create();
        LayerImportsInProgress_.fetch_add(1);

        auto finally = Finally([&]{
            LayerImportsInProgress_.fetch_add(-1);
        });
        try {
            YT_LOG_DEBUG("Ensure that cached layer archive is not in use (LayerId: %v, ArchivePath: %v, Tag: %v)",
                id,
                archivePath,
                tag);

            {
                // Take exclusive lock in blocking fashion to ensure that no
                // forked process is holding an open descriptor to the source file.
                TFile file(archivePath, RdOnly | CloseOnExec);
                file.Flock(LOCK_EX);
            }

            auto layerDirectory = GetLayerPath(id);
            i64 layerSize = 0;

            try {
                YT_LOG_DEBUG("Unpack layer (Path: %v, Tag: %v)",
                    layerDirectory,
                    tag);

                TEventTimerGuard timer(PerformanceCounters_.ImportLayerTimer);
                WaitFor(LayerExecutor_->ImportLayer(archivePath, ToString(id), PlacePath_))
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Layer unpacking failed (LayerId: %v, ArchivePath: %v, Tag: %v)",
                    id,
                    archivePath,
                    tag);
                THROW_ERROR_EXCEPTION(EErrorCode::LayerUnpackingFailed, "Layer unpacking failed")
                    << ex;
            }

            auto config = New<TGetDirectorySizesAsRootConfig>();
            config->Paths = {layerDirectory};
            config->IgnoreUnavailableFiles = true;
            config->DeduplicateByINodes = false;

            layerSize = RunTool<TGetDirectorySizesAsRootTool>(config).front();
            YT_LOG_DEBUG("Calculated layer size (LayerId: %v, Size: %v, Tag: %v)",
                id,
                layerSize,
                tag);

            TLayerMeta layerMeta;
            layerMeta.Path = layerDirectory;
            layerMeta.Id = id;
            layerMeta.mutable_artifact_key()->MergeFrom(artifactKey);
            layerMeta.set_size(layerSize);
            ToProto(layerMeta.mutable_id(), id);

            DoFinalizeLayerImport(layerMeta, tag);

            if (auto delay = DynamicConfigManager_->GetConfig()->ExecNode->VolumeManager->DelayAfterLayerImported) {
                TDelayedExecutor::WaitForDuration(*delay);
            }

            return layerMeta;
        } catch (const std::exception& ex) {
            auto error = TError("Failed to import layer %v", id)
                << TErrorAttribute("layer_path", artifactKey.data_source().path())
                << ex;

            auto innerError = TError(ex);
            if (innerError.GetCode() == EErrorCode::LayerUnpackingFailed) {
                THROW_ERROR error;
            }

            Disable(error);

            if (DynamicConfigManager_->GetConfig()->ExecNode->VolumeManager->AbortOnOperationWithLayerFailed) {
                YT_LOG_FATAL(error);
            } else {
                THROW_ERROR(error);
            }
        }
    }

    void DoRemoveLayer(const TLayerId& layerId)
    {
        auto layerPath = GetLayerPath(layerId);
        auto layerMetaPath = GetLayerMetaPath(layerId);

        {
            auto guard = Guard(SpinLock_);
            ValidateEnabled();

            if (!Layers_.contains(layerId)) {
                YT_LOG_FATAL("Layer already removed (LayerId: %v, LayerPath: %v)",
                    layerId,
                    layerPath);
            }
        }

        try {
            YT_LOG_INFO("Removing layer (LayerId: %v, LayerPath: %v)",
                layerId,
                layerPath);

            auto async = DynamicConfigManager_
                ->GetConfig()
                ->ExecNode
                ->VolumeManager
                ->EnableAsyncLayerRemoval;
            YT_UNUSED_FUTURE(LayerExecutor_->RemoveLayer(ToString(layerId), PlacePath_, async));

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
            auto error = TError("Failed to remove layer %v",
                layerId)
                << ex;
            Disable(error);

            if (DynamicConfigManager_->GetConfig()->ExecNode->VolumeManager->AbortOnOperationWithLayerFailed) {
                YT_LOG_FATAL(error);
            } else {
                THROW_ERROR(error);
            }
        }
    }

    TVolumeMeta DoCreateVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        TVolumeMeta&& volumeMeta,
        THashMap<TString, TString>&& volumeProperties)
    {
        ValidateEnabled();

        auto guard = std::move(volumeCreateTimeGuard);

        auto volumeId = TVolumeId::Create();
        auto volumePath = GetVolumePath(volumeId);

        auto mountPath = NFS::CombinePaths(volumePath, MountSuffix);

        try {
            YT_LOG_DEBUG("Creating volume (Tag: %v, Type: %v, VolumeId: %v)",
                tag,
                FromProto<EVolumeType>(volumeMeta.type()),
                volumeId);

            NFS::MakeDirRecursive(mountPath, 0755);

            auto volumePath = WaitFor(VolumeExecutor_->CreateVolume(mountPath, volumeProperties))
                .ValueOrThrow();

            YT_VERIFY(volumePath == mountPath);

            YT_LOG_INFO("Created volume (Tag: %v, Type: %v, VolumeId: %v, VolumeMountPath: %v)",
                tag,
                FromProto<EVolumeType>(volumeMeta.type()),
                volumeId,
                mountPath);

            ToProto(volumeMeta.mutable_id(), volumeId);
            volumeMeta.MountPath = mountPath;
            volumeMeta.Id = volumeId;

            auto metaBlob = SerializeProtoToRefWithEnvelope(volumeMeta);

            TLayerMetaHeader header;
            header.MetaChecksum = GetChecksum(metaBlob);

            auto volumeMetaFileName = GetVolumeMetaPath(volumeId);
            auto tempVolumeMetaFileName = volumeMetaFileName + NFS::TempFileSuffix;

            {
                auto metaFile = std::make_unique<TFile>(
                    tempVolumeMetaFileName ,
                    CreateAlways | WrOnly | Seq | CloseOnExec);
                metaFile->Write(&header, sizeof(header));
                metaFile->Write(metaBlob.Begin(), metaBlob.Size());
                metaFile->Close();
            }

            NFS::Rename(tempVolumeMetaFileName, volumeMetaFileName);

            YT_LOG_INFO("Created volume meta (Tag: %v, Type: %v, VolumeId: %v, MetaFileName: %v)",
                tag,
                FromProto<EVolumeType>(volumeMeta.type()),
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

            return volumeMeta;
        } catch (const std::exception& ex) {
            TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/create_errors").Increment(1);

            YT_LOG_ERROR(ex, "Failed to create volume (Tag: %v, Type: %v, VolumeId: %v)",
                tag,
                FromProto<EVolumeType>(volumeMeta.type()),
                volumeId);

            auto error = TError("Failed to create %v volume %v",
                FromProto<EVolumeType>(volumeMeta.type()),
                volumeId) << ex;

            // Don't disable location in case of InvalidImage or NBD errors.
            switch (static_cast<EPortoErrorCode>(TError(ex).GetCode())) {
                case EPortoErrorCode::InvalidFilesystem:
                    THROW_ERROR_EXCEPTION(
                        EErrorCode::InvalidImage,
                        "Failed to create %Qlv volume", FromProto<EVolumeType>(volumeMeta.type()))
                        << ex;
                case EPortoErrorCode::NbdProtoError:
                case EPortoErrorCode::NbdSocketError:
                case EPortoErrorCode::NbdSocketTimeout:
                case EPortoErrorCode::NbdSocketUnavaliable:
                case EPortoErrorCode::NbdUnkownExport:
                    break;
                default:
                    Disable(error);
                    break;
            }

            if (DynamicConfigManager_->GetConfig()->ExecNode->VolumeManager->AbortOnOperationWithVolumeFailed) {
                YT_LOG_FATAL(error);
            } else {
                THROW_ERROR(error);
            }
        }
    }

    TVolumeMeta DoCreateNbdVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TArtifactKey& artifactKey,
        TNbdConfigPtr nbdConfig)
    {
        ValidateEnabled();

        YT_VERIFY(nbdConfig);
        YT_VERIFY(artifactKey.has_filesystem());
        YT_VERIFY(artifactKey.has_nbd_export_id());

        THashMap<TString, TString> volumeProperties = {
            {"backend", "nbd"},
            {"read_only", "true"},
            {"place", PlacePath_}
        };

        TStringBuilder builder;
        if (nbdConfig->Server->UnixDomainSocket) {
            builder.AppendFormat("unix+tcp:%v?", nbdConfig->Server->UnixDomainSocket->Path);
        } else {
            YT_VERIFY(nbdConfig->Server->InternetDomainSocket);
            builder.AppendFormat("tcp://%v:%v/?", NNet::GetLocalHostName(), nbdConfig->Server->InternetDomainSocket->Port);
        }
        builder.AppendFormat("timeout=%v", ToString(nbdConfig->Client->IOTimeout.Seconds()));
        builder.AppendFormat("&reconn-timeout=%v", ToString(nbdConfig->Client->ReconnectTimeout.Seconds()));
        builder.AppendFormat("&num-connections=%v", nbdConfig->Client->ConnectionCount);
        builder.AppendFormat("&export=%v", artifactKey.nbd_export_id());
        builder.AppendFormat("&fs-type=%v", ToString(FromProto<ELayerFilesystem>(artifactKey.filesystem())));
        volumeProperties["storage"] = builder.Flush();

        TVolumeMeta volumeMeta;
        volumeMeta.set_type(ToProto<int>(EVolumeType::Nbd));
        volumeMeta.add_layer_artifact_keys()->MergeFrom(artifactKey);
        volumeMeta.add_layer_paths("nbd:" + artifactKey.data_source().path());

        return DoCreateVolume(
            tag,
            std::move(tagSet),
            std::move(volumeCreateTimeGuard),
            std::move(volumeMeta),
            std::move(volumeProperties)
        );
    }

    TVolumeMeta DoCreateOverlayVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TUserSandboxOptions& options,
        const std::vector<TOverlayData>& overlayDataArray)
    {
        ValidateEnabled();

        THashMap<TString, TString> volumeProperties = {
            {"backend", "overlay"},
            {"place", PlacePath_}
        };

        if (options.EnableDiskQuota && options.HasRootFSQuota) {
            volumeProperties["user"] = ToString(options.UserId);
            volumeProperties["permissions"] = "0777";

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
            [](TStringBuilderBase* builder, const TOverlayData& volumeOrLayer) {
                builder->AppendString(volumeOrLayer.GetPath());
            },
            ";");

        volumeProperties["layers"] = builder.Flush();

        TVolumeMeta volumeMeta;
        volumeMeta.set_type(ToProto<int>(EVolumeType::Overlay));

        for (const auto& volumeOrLayer : overlayDataArray) {
            YT_ASSERT(!volumeOrLayer.GetPath().empty());
            volumeMeta.add_layer_artifact_keys()->MergeFrom(volumeOrLayer.GetArtifactKey());
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
        NProfiling::TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TArtifactKey& artifactKey,
        const TString& squashFSFilePath)
    {
        ValidateEnabled();

        THashMap<TString, TString> volumeProperties {
            {"backend", "squash"},
            {"read_only", "true"},
            {"layers", squashFSFilePath}
        };

        TVolumeMeta volumeMeta;
        volumeMeta.set_type(ToProto<int>(EVolumeType::SquashFS));
        volumeMeta.add_layer_artifact_keys()->MergeFrom(artifactKey);
        volumeMeta.add_layer_paths(squashFSFilePath);

        return DoCreateVolume(
            tag,
            std::move(tagSet),
            std::move(volumeCreateTimeGuard),
            std::move(volumeMeta),
            std::move(volumeProperties)
        );
    }

    void DoRemoveVolume(NProfiling::TTagSet tagSet, const TVolumeId& volumeId)
    {
        auto volumePath = GetVolumePath(volumeId);
        auto mountPath = NFS::CombinePaths(volumePath, MountSuffix);
        auto volumeMetaPath = GetVolumeMetaPath(volumeId);

        {
            auto guard = Guard(SpinLock_);
            ValidateEnabled();

            if (!Volumes_.contains(volumeId)) {
                YT_LOG_FATAL("Volume already removed (VolumeId: %v, VolumePath: %v, VolumeMetaPath: %v)",
                    volumeId,
                    volumePath,
                    volumeMetaPath);
            }
        }

        try {
            YT_LOG_DEBUG("Removing volume (VolumeId: %v)",
                volumeId);

            WaitFor(VolumeExecutor_->UnlinkVolume(mountPath, "self"))
                .ThrowOnError();

            YT_LOG_DEBUG("Volume unlinked (VolumeId: %v)",
                volumeId);

            NFS::RemoveRecursive(volumePath);
            NFS::Remove(volumeMetaPath);

            YT_LOG_INFO("Volume directory and meta removed (VolumeId: %v, VolumePath: %v, VolumeMetaPath: %v)",
                volumeId,
                volumePath,
                volumeMetaPath);

            {
                auto guard = Guard(SpinLock_);

                if (!IsEnabled()) {
                    return;
                }

                YT_VERIFY(Volumes_.erase(volumeId));

                if (Volumes_.empty()) {
                    VolumesReleaseEvent_.Set();
                }
            }
        } catch (const std::exception& ex) {
            TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/remove_errors").Increment(1);

            auto error = TError("Failed to remove volume %v", volumeId)
                << ex;
            Disable(error);

            if (DynamicConfigManager_->GetConfig()->ExecNode->VolumeManager->AbortOnOperationWithVolumeFailed) {
                YT_LOG_FATAL(error);
            } else {
                THROW_ERROR(error);
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TLayerLocation)
DECLARE_REFCOUNTED_CLASS(TLayerLocation)

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
        THROW_ERROR_EXCEPTION("Failed to get layer location; all locations are disabled");
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
        YT_LOG_INFO("Layer is destroyed (LayerId: %v, LayerPath: %v)",
            LayerMeta_.Id,
            LayerMeta_.Path);

        Location_->RemoveLayer(LayerMeta_.Id)
            .Subscribe(BIND([] (const TError& result) {
                YT_LOG_ERROR_IF(!result.IsOK(), result, "Layer remove failed");
            }));
    }

    const TString& GetCypressPath() const
    {
        return GetKey().data_source().path();
    }

    const TString& GetPath() const
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

private:
    const TLayerMeta LayerMeta_;
    const TLayerLocationPtr Location_;
    std::atomic<int> HitCount_;
};

DEFINE_REFCOUNTED_TYPE(TLayer)

/////////////////////////////////////////////////////////////////////////////

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
        TTmpfsLayerCacheConfigPtr config,
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
        , HitCounter_(ExecNodeProfiler
            .WithTag("cache_name", CacheName_)
            .Counter("/layer_cache/tmpfs_cache_hits"))
        , UpdateFailedCounter_(ExecNodeProfiler
            .WithTag("cache_name", CacheName_)
            .Gauge("/layer_cache/update_failed"))
    {  }

    TLayerPtr FindLayer(const TArtifactKey& artifactKey)
    {
        auto guard = Guard(DataSpinLock_);
        auto it = CachedLayers_.find(artifactKey);
        if (it != CachedLayers_.end()) {
            auto layer = it->second;
            guard.Release();

            HitCounter_.Increment();
            return layer;
        }
        return nullptr;
    }

    TFuture<void> Initialize()
    {
        if (!Config_->LayersDirectoryPath) {
            return VoidFuture;
        }

        auto path = NFS::CombinePaths(NFs::CurrentWorkingDirectory(), Format("%v_tmpfs_layers", CacheName_));

        if (Bootstrap_) {
            Bootstrap_->SubscribePopulateAlerts(BIND(
                &TTmpfsLayerCache::PopulateTmpfsAlert,
                MakeWeak(this)));
        }

        {
            YT_LOG_DEBUG("Cleanup tmpfs layer cache volume (Path: %v)", path);
            auto error = WaitFor(PortoExecutor_->UnlinkVolume(path, "self"));
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

            WaitFor(PortoExecutor_->CreateVolume(path, volumeProperties))
                .ThrowOnError();

            MemoryUsageTracker_->Acquire(Config_->Capacity);

            auto locationConfig = New<TLayerLocationConfig>();
            locationConfig->Quota = Config_->Capacity;
            locationConfig->LowWatermark = 0;
            locationConfig->MinDiskSpace = 0;
            locationConfig->Path = path;
            locationConfig->LocationIsAbsolute = false;

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

    TFuture<void> Disable(const TError& error, bool persistentDisable = true)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        YT_LOG_WARNING("Disable tmfps layer cache (Path: %v)", CacheName_);

        if (TmpfsLocation_) {
            MemoryUsageTracker_->Release(Config_->Capacity);

            if (LayerUpdateExecutor_) {
                return LayerUpdateExecutor_->Stop()
                    .Apply(BIND(&TLayerLocation::Disable, TmpfsLocation_, error, persistentDisable));
            } else {
                TmpfsLocation_->Disable(error, persistentDisable);
                return VoidFuture;
            }
        } else {
            return VoidFuture;
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
    const TTmpfsLayerCacheConfigPtr Config_;
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

    TCounter HitCounter_;
    TGauge UpdateFailedCounter_;

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
        auto Logger = ExecNodeLogger.WithTag("Tag: %v", tag);

        YT_LOG_INFO("Started updating tmpfs layers");

        TListNodeOptions listNodeOptions;
        listNodeOptions.ReadFrom = EMasterChannelKind::Cache;
        listNodeOptions.Attributes = std::vector<TString>{"id"};
        auto listNodeRspOrError = WaitFor(client->ListNode(
            *Config_->LayersDirectoryPath,
            listNodeOptions));

        if (!listNodeRspOrError.IsOK()) {
            SetAlert(TError(EErrorCode::TmpfsLayerImportFailed, "Failed to list %v tmpfs layers directory %v",
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
            SetAlert(TError(EErrorCode::TmpfsLayerImportFailed, "Tmpfs layers directory %v has invalid structure",
                Config_->LayersDirectoryPath)
                << ex);
            return;
        }

        YT_LOG_INFO("Listed tmpfs layers (CacheName: %v, Count: %v)",
            CacheName_,
            paths.size());

        {
            THashMap<TYPath, TFetchedArtifactKey> cachedLayerDescriptors;
            for (const auto& path : paths) {
                auto it = CachedLayerDescriptors_.find(path);
                if (it != CachedLayerDescriptors_.end()) {
                    cachedLayerDescriptors.insert(*it);
                } else {
                    cachedLayerDescriptors.emplace(
                        path,
                        TFetchedArtifactKey{.ContentRevision = 0});
                }
            }

            CachedLayerDescriptors_.swap(cachedLayerDescriptors);
        }

        std::vector<TFuture<TFetchedArtifactKey>> futures;
        for (const auto& pair : CachedLayerDescriptors_) {
            auto future = BIND(
                [=, this, this_ = MakeStrong(this)] () {
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
            SetAlert(TError(EErrorCode::TmpfsLayerImportFailed, "Failed to fetch tmpfs layer descriptions")
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

            YT_LOG_INFO_IF(!artifactsToRemove.empty(), "Released cached tmpfs layers (Count: %v)",
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
            SetAlert(TError(EErrorCode::TmpfsLayerImportFailed, "Failed to import new tmpfs layers")
                << newLayersOrError);
            return;
        }

        bool hasFailedLayer = false;
        bool hasImportedLayer = false;
        for (const auto& newLayerOrError : newLayersOrError.Value()) {
            if (!newLayerOrError.IsOK()) {
                hasFailedLayer = true;
                SetAlert(TError(EErrorCode::TmpfsLayerImportFailed, "Failed to import new %v tmpfs layer", CacheName_)
                    << newLayerOrError);
                continue;
            }

            const auto& layer = newLayerOrError.Value();
            YT_LOG_INFO("Successfully imported new tmpfs layer (LayerId: %v, ArtifactPath: %v, CacheName: %v)",
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

/////////////////////////////////////////////////////////////////////////////

class TLayerCache
    : public TAsyncSlruCacheBase<TArtifactKey, TLayer>
{
public:
    TLayerCache(
        const TVolumeManagerConfigPtr& config,
        const NClusterNode::TClusterNodeDynamicConfigManagerPtr& dynamicConfigManager,
        std::vector<TLayerLocationPtr> layerLocations,
        IPortoExecutorPtr tmpfsExecutor,
        IVolumeChunkCachePtr chunkCache,
        IInvokerPtr controlInvoker,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        IBootstrap* bootstrap)
        : TAsyncSlruCacheBase(
            TSlruCacheConfig::CreateWithCapacity(
                config->EnableLayersCache
                ? static_cast<i64>(GetCacheCapacity(layerLocations) * config->CacheCapacityFraction)
                : 0),
            ExecNodeProfiler.WithPrefix("/layer_cache"))
        , Config_(config)
        , DynamicConfigManager_(dynamicConfigManager)
        , ChunkCache_(chunkCache)
        , ControlInvoker_(controlInvoker)
        , LayerLocations_(std::move(layerLocations))
        , Semaphore_(New<TAsyncSemaphore>(config->LayerImportConcurrency))
        , ProfilingExecutor_(New<TPeriodicExecutor>(
            ControlInvoker_,
            BIND(&TLayerCache::OnProfiling, MakeWeak(this)),
            ProfilingPeriod))
    {
        auto absorbLayer = BIND(
            &TLayerCache::DownloadAndImportLayer,
            MakeStrong(this));

        RegularTmpfsLayerCache_ = New<TTmpfsLayerCache>(
            bootstrap,
            Config_->RegularTmpfsLayerCache,
            DynamicConfigManager_,
            ControlInvoker_,
            memoryUsageTracker,
            "regular",
            tmpfsExecutor,
            absorbLayer);

        NirvanaTmpfsLayerCache_ = New<TTmpfsLayerCache>(
            bootstrap,
            Config_->NirvanaTmpfsLayerCache,
            DynamicConfigManager_,
            ControlInvoker_,
            memoryUsageTracker,
            "nirvana",
            tmpfsExecutor,
            absorbLayer);
    }

    TFuture<void> Initialize()
    {
        for (const auto& location : LayerLocations_) {
            for (const auto& layerMeta : location->GetAllLayers()) {
                TArtifactKey key;
                key.MergeFrom(layerMeta.artifact_key());

                YT_LOG_DEBUG("Loading existing cached Porto layer (LayerId: %v)", layerMeta.Id);

                auto layer = New<TLayer>(layerMeta, key, location);
                auto cookie = BeginInsert(layer->GetKey());
                if (cookie.IsActive()) {
                    cookie.EndInsert(layer);
                }
            }
        }

        ProfilingExecutor_->Start();

        return AllSucceeded(std::vector<TFuture<void>>{
            RegularTmpfsLayerCache_->Initialize(),
            NirvanaTmpfsLayerCache_->Initialize()
        });
    }

    TFuture<void> Disable(const TError& reason)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        YT_LOG_WARNING(reason, "Layer cache disabled");

        for (const auto& location : LayerLocations_) {
            location->Disable(reason, false);
        }

        return AllSucceeded(std::vector<TFuture<void>>{
            ProfilingExecutor_->Stop(),
            RegularTmpfsLayerCache_->Disable(reason, false),
            NirvanaTmpfsLayerCache_->Disable(reason, false)
        }).Apply(BIND([=, this, this_ = MakeStrong(this)] () {
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
                        cookie.EndInsert(layerOrError.Value());
                    } else {
                        cookie.Cancel(layerOrError);
                    }
                })
                .Via(GetCurrentInvoker()));
        } else {
            YT_LOG_DEBUG("Layer is already being loaded into cache (Tag: %v, ArtifactPath: %v)",
                tag,
                artifactKey.data_source().path());
        }

        return value;
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

private:
    const TVolumeManagerConfigPtr Config_;
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    const IVolumeChunkCachePtr ChunkCache_;
    const IInvokerPtr ControlInvoker_;
    const std::vector<TLayerLocationPtr> LayerLocations_;

    TAsyncSemaphorePtr Semaphore_;

    TTmpfsLayerCachePtr RegularTmpfsLayerCache_;
    TTmpfsLayerCachePtr NirvanaTmpfsLayerCache_;

    TPeriodicExecutorPtr ProfilingExecutor_;

    bool IsResurrectionSupported() const override
    {
        return false;
    }

    i64 GetWeight(const TLayerPtr& layer) const override
    {
        return layer->GetSize();
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
                YT_LOG_DEBUG_IF(tag, "Found layer in %v tmpfs cache (LayerId: %v, ArtifactPath: %v, Tag: %v)",
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
        TLayerLocationPtr targetLocation)
    {
        YT_LOG_DEBUG("Start loading layer into cache (Tag: %v, ArtifactPath: %v, HasTargetLocation: %v)",
            tag,
            artifactKey.data_source().path(),
            static_cast<bool>(targetLocation));

        return ChunkCache_->DownloadArtifact(artifactKey, downloadOptions)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const IVolumeArtifactPtr& artifactChunk) {
                YT_LOG_DEBUG("Layer artifact loaded, starting import (Tag: %v, ArtifactPath: %v)",
                    tag,
                    artifactKey.data_source().path());

                // NB(psushin): we limit number of concurrently imported layers, since this is heavy operation
                // which may delay light operations performed in the same IO thread pool inside Porto daemon.
                // PORTO-518
                TAsyncSemaphoreGuard guard;
                while (!(guard = TAsyncSemaphoreGuard::TryAcquire(Semaphore_))) {
                    WaitFor(Semaphore_->GetReadyEvent())
                        .ThrowOnError();
                }

                auto location = this_->PickLocation();
                auto layerMeta = WaitFor(location->ImportLayer(artifactKey, artifactChunk->GetFileName(), tag))
                    .ValueOrThrow();

                if (targetLocation) {
                    // For tmpfs layers we cannot import them directly to tmpfs location,
                    // since tar/gzip are run in special /portod-helpers cgroup, which suffers from
                    // OOM when importing into tmpfs. To workaround this, we first import to disk locations
                    // and then copy into in-memory.

                    auto finally = Finally(BIND([=] () {
                        location->RemoveLayer(layerMeta.Id)
                            .Subscribe(BIND([] (const TError& result) {
                                YT_LOG_ERROR_IF(!result.IsOK(), result, "Layer remove failed");
                            }));
                    }));

                    auto tmpfsLayerMeta = WaitFor(targetLocation->InternalizeLayer(layerMeta, tag))
                        .ValueOrThrow();
                    return New<TLayer>(tmpfsLayerMeta, artifactKey, targetLocation);
                } else {
                    return New<TLayer>(layerMeta, artifactKey, location);
                }
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

class TNbdVolume
    : public IVolume
{
public:
    TNbdVolume(
        NProfiling::TTagSet tagSet,
        TVolumeMeta&& meta,
        const TArtifactKey& artifactKey,
        TLayerLocationPtr location,
        INbdServerPtr nbdServer)
        : TagSet_(std::move(tagSet))
        , VolumeMeta_(std::move(meta))
        , ArtifactKey_(artifactKey)
        , Location_(std::move(location))
        , NbdServer_(std::move(nbdServer))
    { }

    ~TNbdVolume()
    {
        YT_UNUSED_FUTURE(Remove());
    }

    TFuture<void> Remove() override
    {
        if (RemoveFuture_) {
            return RemoveFuture_;
        }

        TVolumeProfilerCounters::Get()->GetGauge(TagSet_, "/count")
            .Update(VolumeCounters().Decrement(TagSet_));

        TVolumeProfilerCounters::Get()->GetCounter(TagSet_, "/removed").Increment(1);
        TEventTimerGuard volumeRemoveTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(TagSet_, "/remove_time"));

        const auto& volumeId = GetId();
        YT_LOG_DEBUG("Removing NBD volume (VolumeId: %v, ExportId: %v, Path: %v)",
            volumeId,
            ArtifactKey_.nbd_export_id(),
            ArtifactKey_.data_source().path());

        // At first remove volume, then unregister export.
        auto future = Location_->RemoveVolume(TagSet_, volumeId);
        RemoveFuture_ = future.Apply(BIND([volumeId = volumeId, layer = ArtifactKey_, nbdServer = NbdServer_, volumeRemoveTimeGuard = std::move(volumeRemoveTimeGuard)]() {
            YT_LOG_DEBUG("Removed NBD volume (VolumeId: %v, ExportId: %v, Path: %v)",
                volumeId,
                layer.nbd_export_id(),
                layer.data_source().path());

            nbdServer->TryUnregisterDevice(layer.nbd_export_id());
        }));

        return RemoveFuture_;
    }

    const TVolumeId& GetId() const override
    {
        return VolumeMeta_.Id;
    }

    const TString& GetPath() const override
    {
        return VolumeMeta_.MountPath;
    }

    const TArtifactKey& GetArtifactKey() const override
    {
        return ArtifactKey_;
    }

private:
    const NProfiling::TTagSet TagSet_;
    const TVolumeMeta VolumeMeta_;
    const TArtifactKey ArtifactKey_;
    const TLayerLocationPtr Location_;
    const INbdServerPtr NbdServer_;

    TFuture<void> RemoveFuture_;
};

DEFINE_REFCOUNTED_TYPE(TNbdVolume)

////////////////////////////////////////////////////////////////////////////////

class TOverlayVolume
    : public IVolume
{
public:
    TOverlayVolume(
        NProfiling::TTagSet tagSet,
        TVolumeMeta&& meta,
        TLayerLocationPtr location,
        std::vector<TOverlayData> overlayDataArray)
        : TagSet_(std::move(tagSet))
        , VolumeMeta_(std::move(meta))
        , Location_(std::move(location))
        , OverlayDataArray_(std::move(overlayDataArray))
    { }

    ~TOverlayVolume() override
    {
        YT_UNUSED_FUTURE(Remove());
    }

    TFuture<void> Remove() override
    {
        if (RemoveFuture_) {
            return RemoveFuture_;
        }

        TVolumeProfilerCounters::Get()->GetGauge(TagSet_, "/count")
            .Update(VolumeCounters().Decrement(TagSet_));

        TVolumeProfilerCounters::Get()->GetCounter(TagSet_, "/removed").Increment(1);
        TEventTimerGuard volumeRemoveTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(TagSet_, "/remove_time"));

        const auto& volumeId = GetId();
        YT_LOG_DEBUG("Removing Overlay volume (VolumeId: %v)", volumeId);

        // At first remove overlay volume, then remove constituent volumes and layers.
        auto future = Location_->RemoveVolume(TagSet_, volumeId);
        RemoveFuture_ = future.Apply(BIND([volumeId = volumeId, overlayDataArray = OverlayDataArray_, volumeRemoveTimeGuard = std::move(volumeRemoveTimeGuard)]() mutable {
            YT_LOG_DEBUG("Removed Overlay volume (VolumeId: %v)", volumeId);

            std::vector<TFuture<void>> futures;
            futures.reserve(overlayDataArray.size());
            for (auto& overlayData : overlayDataArray) {
                futures.push_back(overlayData.Remove());
            }
            return AllSucceeded(std::move(futures));
        }));

        return RemoveFuture_;
    }

    const TVolumeId& GetId() const override
    {
        return VolumeMeta_.Id;
    }

    const TString& GetPath() const override
    {
        return VolumeMeta_.MountPath;
    }

    const TArtifactKey& GetArtifactKey() const override
    {
        static const TArtifactKey empty;
        return empty;
    }

    const std::vector<TOverlayData>& GetoverlayDataArray() const
    {
        return OverlayDataArray_;
    }

private:
    const NProfiling::TTagSet TagSet_;
    const TVolumeMeta VolumeMeta_;
    const TLayerLocationPtr Location_;
    // Holds volumes and layers (so that they are not destroyed) while they are needed.
    const std::vector<TOverlayData> OverlayDataArray_;

    TFuture<void> RemoveFuture_;
};

DECLARE_REFCOUNTED_CLASS(TOverlayVolume)
DEFINE_REFCOUNTED_TYPE(TOverlayVolume)

////////////////////////////////////////////////////////////////////////////////

class TSquashFSVolume
    : public IVolume
{
public:
    TSquashFSVolume(
        NProfiling::TTagSet tagSet,
        TVolumeMeta&& meta,
        const TArtifactKey& artifactKey,
        IVolumeArtifactPtr chunkCacheArtifact,
        TLayerLocationPtr location)
        : TagSet_(std::move(tagSet))
        , VolumeMeta_(std::move(meta))
        , ArtifactKey_(artifactKey)
        , ChunkCacheArtifact_(std::move(chunkCacheArtifact))
        , Location_(std::move(location))
    { }

    ~TSquashFSVolume()
    {
        YT_UNUSED_FUTURE(Remove());
    }

    TFuture<void> Remove() override
    {
        if (RemoveFuture_) {
            return RemoveFuture_;
        }

        TVolumeProfilerCounters::Get()->GetGauge(TagSet_, "/count")
            .Update(VolumeCounters().Decrement(TagSet_));

        TVolumeProfilerCounters::Get()->GetCounter(TagSet_, "/removed").Increment(1);
        TEventTimerGuard volumeRemoveTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(TagSet_, "/remove_time"));

        const auto& volumeId = GetId();
        const auto& volumePath = GetPath();
        YT_LOG_DEBUG("Removing squashfs volume (VolumeId: %v, VolumePath: %v)",
            volumeId,
            volumePath);

        RemoveFuture_ = Location_->RemoveVolume(TagSet_, volumeId).Apply(BIND([volumeId = volumeId, volumePath = volumePath, volumeRemoveTimeGuard = std::move(volumeRemoveTimeGuard)]() {
            YT_LOG_DEBUG("Removed squashfs volume (VolumeId: %v, VolumePath: %v)",
                volumeId,
                volumePath);
        }));

        return RemoveFuture_;
    }

    const TVolumeId& GetId() const override
    {
        return VolumeMeta_.Id;
    }

    const TString& GetPath() const override
    {
        return VolumeMeta_.MountPath;
    }

    const TArtifactKey& GetArtifactKey() const override
    {
        return ArtifactKey_;
    }

private:
    const NProfiling::TTagSet TagSet_;
    const TVolumeMeta VolumeMeta_;
    const TArtifactKey ArtifactKey_;
    // We store chunk cache artifact here to make sure that SquashFS file outlives SquashFS volume.
    const IVolumeArtifactPtr ChunkCacheArtifact_;
    const TLayerLocationPtr Location_;

    TFuture<void> RemoveFuture_;
};

DECLARE_REFCOUNTED_CLASS(TSquashFSVolume)
DEFINE_REFCOUNTED_TYPE(TSquashFSVolume)

////////////////////////////////////////////////////////////////////////////////

const TString& TOverlayData::GetPath() const
{
    if (std::holds_alternative<TLayerPtr>(Variant_)) {
        return std::get<TLayerPtr>(Variant_)->GetPath();
    }

    return std::get<IVolumePtr>(Variant_)->GetPath();
}

const TArtifactKey& TOverlayData::GetArtifactKey() const
{
    if (std::holds_alternative<TLayerPtr>(Variant_)) {
        return std::get<TLayerPtr>(Variant_)->GetKey();
    }

    return std::get<IVolumePtr>(Variant_)->GetArtifactKey();
}

TFuture<void> TOverlayData::Remove()
{
    if (IsLayer()) {
        return VoidFuture;
    }

    return GetVolume()->Remove();
}

////////////////////////////////////////////////////////////////////////////////

class TPortoVolumeManager
    : public IVolumeManager
{
public:
    TPortoVolumeManager(
        NDataNode::TDataNodeConfigPtr config,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        IVolumeChunkCachePtr chunkCache,
        IInvokerPtr controlInvoker,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        IBootstrap* const bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , DynamicConfigManager_(std::move(dynamicConfigManager))
        , ChunkCache_(std::move(chunkCache))
        , ControlInvoker_(std::move(controlInvoker))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
    { }

    TFuture<void> Initialize()
    {
        if (Bootstrap_) {
            Bootstrap_->SubscribePopulateAlerts(BIND(&TPortoVolumeManager::PopulateAlerts, MakeWeak(this)));
        }
        // Create locations.

        std::vector<TFuture<void>> initLocationResults;
        std::vector<TLayerLocationPtr> locations;
        for (int index = 0; index < std::ssize(Config_->VolumeManager->LayerLocations); ++index) {
            const auto& locationConfig = Config_->VolumeManager->LayerLocations[index];
            auto id = Format("layer%v", index);

            try {
                auto location = New<TLayerLocation>(
                    locationConfig,
                    DynamicConfigManager_,
                    Config_->DiskHealthChecker,
                    CreatePortoExecutor(
                        Config_->VolumeManager->PortoExecutor,
                        Format("volume%v", index),
                        ExecNodeProfiler.WithPrefix("/location_volumes/porto").WithTag("location_id", id)),
                    CreatePortoExecutor(
                        Config_->VolumeManager->PortoExecutor,
                        Format("layer%v", index),
                        ExecNodeProfiler.WithPrefix("/location_layers/porto").WithTag("location_id", id)),
                    id);
                locations.push_back(location);
                initLocationResults.push_back(location->Initialize());
            } catch (const std::exception& ex) {
                auto error = TError("Layer location initialization failed (Path: %v)", locationConfig->Path)
                    << ex;
                YT_LOG_WARNING(error);
                Alerts_.push_back(error);
            }
        }

        auto errorOrResults = WaitFor(AllSet(initLocationResults));

        if (!errorOrResults.IsOK()) {
            auto wrappedError = TError("Failed to initialize layer locations") << errorOrResults;
            YT_LOG_WARNING(wrappedError);
            Alerts_.push_back(wrappedError);
            Locations_.clear();
        }

        for (int index = 0; index < std::ssize(errorOrResults.Value()); ++index) {
            const auto& error = errorOrResults.Value()[index];
            if (error.IsOK()) {
                Locations_.push_back(locations[index]);
            } else {
                const auto& locationConfig = Config_->VolumeManager->LayerLocations[index];
                auto wrappedError = TError("Layer location async initialization failed (Path: %v)", locationConfig->Path)
                    << error;
                YT_LOG_WARNING(wrappedError);
                Alerts_.push_back(wrappedError);
            }
        }

        auto tmpfsExecutor = CreatePortoExecutor(
            Config_->VolumeManager->PortoExecutor,
            "tmpfs_layer",
            ExecNodeProfiler.WithPrefix("/tmpfs_layers/porto"));
        LayerCache_ = New<TLayerCache>(
            Config_->VolumeManager,
            DynamicConfigManager_,
            Locations_,
            tmpfsExecutor,
            ChunkCache_,
            ControlInvoker_,
            MemoryUsageTracker_,
            Bootstrap_);
        return LayerCache_->Initialize();
    }

    TFuture<void> GetVolumeReleaseEvent() override
    {
        std::vector<TFuture<void>> futures;
        for (const auto& location : Locations_) {
            futures.push_back(location->GetVolumeReleaseEvent());
        }

        return AllSet(std::move(futures))
            .AsVoid()
            .ToUncancelable();
    }

    TFuture<void> DisableLayerCache(const TError& reason) override
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        return LayerCache_->Disable(reason)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] () {
                Locations_.clear();
            })
            .AsyncVia(ControlInvoker_));
    }

    TFuture<IVolumePtr> PrepareVolume(
        const std::vector<TArtifactKey>& artifactKeys,
        const TArtifactDownloadOptions& downloadOptions,
        const TUserSandboxOptions& options) override
    {
        YT_VERIFY(!artifactKeys.empty());

        auto tag = TGuid::Create();

        YT_LOG_DEBUG("Prepare volume (Tag: %v, Artifacts: %v)",
            tag,
            artifactKeys.size());

        std::vector<int> nbdArtifactPositions;
        std::vector<TArtifactKey> nbdArtifactKeys;
        std::vector<int> squashFSArtifactPositions;
        std::vector<TArtifactKey> squashFSArtifactKeys;
        std::vector<int> archiveArtifactPositions;
        std::vector<TArtifactKey> archiveArtifactKeys;
        for (auto i = 0; i < ssize(artifactKeys); ++i) {
            const auto& artifactKey = artifactKeys[i];

            if (FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Nbd) {
                nbdArtifactPositions.push_back(i);
                nbdArtifactKeys.push_back(artifactKey);
            } else if (FromProto<ELayerFilesystem>(artifactKey.filesystem()) == ELayerFilesystem::SquashFS) {
                squashFSArtifactPositions.push_back(i);
                squashFSArtifactKeys.push_back(artifactKey);
            } else {
                archiveArtifactPositions.push_back(i);
                archiveArtifactKeys.push_back(artifactKey);
            }
        }

        std::vector<TFuture<TOverlayData>> overlayDataFutures;
        overlayDataFutures.resize(artifactKeys.size());

        if (!archiveArtifactKeys.empty()) {
            auto futures = PrepareLayers(tag, archiveArtifactKeys, downloadOptions);
            YT_VERIFY(archiveArtifactKeys.size() == futures.size());
            for (auto i = 0; i < ssize(futures); ++i) {
                auto position = archiveArtifactPositions[i];
                overlayDataFutures[position] = std::move(futures[i]);
            }
        }

        if (!nbdArtifactKeys.empty()) {
            auto futures = PrepareNbdVolumes(tag, nbdArtifactKeys);
            YT_VERIFY(nbdArtifactKeys.size() == futures.size());
            for (auto i = 0; i < ssize(futures); ++i) {
                auto position = nbdArtifactPositions[i];
                overlayDataFutures[position] = std::move(futures[i]);
            }
        }

        if (!squashFSArtifactKeys.empty()) {
            auto futures = PrepareSquashFSVolumes(tag, squashFSArtifactKeys, downloadOptions);
            YT_VERIFY(squashFSArtifactKeys.size() == futures.size());
            for (auto i = 0; i < ssize(futures); ++i) {
                auto position = squashFSArtifactPositions[i];
                overlayDataFutures[position] = std::move(futures[i]);
            }
        }

        // ToDo(psushin): choose proper invoker.
        // Avoid sync calls to WaitFor, to respect job preparation context switch guards.
        return AllSucceeded(std::move(overlayDataFutures))
            .Apply(BIND([=, this_ = MakeStrong(this)] (const std::vector<TOverlayData>& overlayDataArray) {
                auto tagSet = TVolumeProfilerCounters::MakeTagSet(/*volumeType*/ "overlay", /*volumeFilePath*/ "n/a");
                TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/created").Increment(1);
                TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));
                return this_->CreateOverlayVolume(tag, std::move(tagSet), std::move(volumeCreateTimeGuard), options, overlayDataArray);
            }).AsyncVia(GetCurrentInvoker()))
            .ToImmediatelyCancelable()
            .As<IVolumePtr>();
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

private:
    IBootstrap* const Bootstrap_;
    const TDataNodeConfigPtr Config_;
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    const IVolumeChunkCachePtr ChunkCache_;
    const IInvokerPtr ControlInvoker_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    std::vector<TLayerLocationPtr> Locations_;

    TLayerCachePtr LayerCache_;
    std::vector<TError> Alerts_;

    TLayerLocationPtr PickLocation()
    {
        return DoPickLocation(Locations_, [] (const TLayerLocationPtr& candidate, const TLayerLocationPtr& current) {
            return candidate->GetVolumeCount() < current->GetVolumeCount();
        });
    }

    void BuildOrchid(NYTree::TFluentAny fluent) const override
    {
        LayerCache_->BuildOrchid(fluent);
    }

    //! Register NBD layers (images) with NBD server. It must be done prior to creating NBD volumes.
    TFuture<void> PrepareNbdExport(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        const TArtifactKey& layer)
    {
        auto future = VoidFuture;
        try {
            THROW_ERROR_EXCEPTION_IF(!layer.has_filesystem(),
                "NBD layer %v does not have filesystem",
                layer.data_source().path());

            THROW_ERROR_EXCEPTION_IF(!layer.has_nbd_export_id(),
                "NBD layer %v does not have export id",
                layer.data_source().path());

            YT_LOG_DEBUG("Preparing NBD export (Tag: %v, ExportId: %v, Path: %v, Filesystem: %v)",
                tag,
                layer.nbd_export_id(),
                layer.data_source().path(),
                FromProto<ELayerFilesystem>(layer.filesystem()));

            auto nbdServer = Bootstrap_->GetNbdServer();
            if (!nbdServer) {
                auto error = TError("NBD server is not present")
                    << TErrorAttribute("export_id", layer.nbd_export_id())
                    << TErrorAttribute("path", layer.data_source().path())
                    << TErrorAttribute("filesystem", FromProto<ELayerFilesystem>(layer.filesystem()));

                THROW_ERROR(error);
            }

            // TODO(yuryalekseev): user
            auto clientOptions =  NYT::NApi::TClientOptions::FromUser(NSecurityClient::RootUserName);
            auto client = nbdServer->GetConnection()->CreateNativeClient(clientOptions);

            auto device = CreateCypressFileBlockDevice(
                Bootstrap_->GetDynamicConfig()->ExecNode->Nbd,
                Bootstrap_->GetDefaultInThrottler(),
                Bootstrap_->GetReadRpsOutThrottler(),
                layer,
                std::move(client),
                nbdServer->GetInvoker(),
                nbdServer->GetLogger());

            auto initializeFuture = device->Initialize();
            nbdServer->RegisterDevice(layer.nbd_export_id(), std::move(device));

            future = initializeFuture.Apply(BIND([tag = tag, layer = layer, Logger = Logger] () {
                YT_LOG_DEBUG("Prepared NBD export (Tag: %v, ExportId: %v, Path: %v, Filesystem: %v)",
                    tag,
                    layer.nbd_export_id(),
                    layer.data_source().path(),
                    FromProto<ELayerFilesystem>(layer.filesystem()));
            }));
        } catch (const std::exception& ex) {
            TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/create_errors").Increment(1);

            auto error = TError(ex);
            YT_LOG_ERROR(error, "Failed to prepare NBD export");
            future = MakeFuture<void>(error);
        }

        return future;
    }

    //! Download and extract tar archives (tar layers).
    std::vector<TFuture<TOverlayData>> PrepareLayers(
        TGuid tag,
        const std::vector<TArtifactKey>& artifactKeys,
        const TArtifactDownloadOptions& downloadOptions)
    {
        YT_VERIFY(!artifactKeys.empty());

        YT_LOG_DEBUG("Prepare layers (Tag: %v, Artifacts: %v)",
            tag,
            artifactKeys.size());

        std::vector<TFuture<TOverlayData>> futures;
        futures.reserve(artifactKeys.size());

        for (const auto& artifactKey : artifactKeys) {
            YT_VERIFY(!artifactKey.has_access_method() || FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Local);
            YT_VERIFY(!artifactKey.has_filesystem() || FromProto<ELayerFilesystem>(artifactKey.filesystem()) == ELayerFilesystem::Archive);
            YT_VERIFY(!artifactKey.has_nbd_export_id());
            futures.push_back(LayerCache_->PrepareLayer(artifactKey, downloadOptions, tag).As<TOverlayData>());
        }

        return futures;
    }

    //! Create NBD volumes.
    std::vector<TFuture<TOverlayData>> PrepareNbdVolumes(
        TGuid tag,
        const std::vector<TArtifactKey>& artifactKeys)
    {
        YT_VERIFY(!artifactKeys.empty());

        YT_LOG_DEBUG("Prepare NBD volumes (Tag: %v, Artifacts: %v)",
            tag,
            artifactKeys.size());

        std::vector<TFuture<TOverlayData>> futures;
        futures.reserve(artifactKeys.size());

        for (const auto& artifactKey : artifactKeys) {
            YT_VERIFY(FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Nbd);
            YT_VERIFY(artifactKey.has_filesystem());
            YT_VERIFY(artifactKey.has_nbd_export_id());

            auto tagSet = TVolumeProfilerCounters::MakeTagSet(/*volumeType*/ "nbd", /*volumeFilePath*/ artifactKey.data_source().path());
            TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/created").Increment(1);
            TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

            // There could be a CreateNbdVolume() failure after a successful PrepareNbdExport().
            // In such cases NBD exports are unregistered by TJob::Cleanup().
            auto exportFuture = PrepareNbdExport(tag, tagSet, artifactKey);
            auto volumeFuture = exportFuture.Apply(BIND(
                &TPortoVolumeManager::CreateNbdVolume,
                MakeStrong(this),
                tag,
                std::move(tagSet),
                Passed(std::move(volumeCreateTimeGuard)),
                artifactKey)
            .AsyncVia(GetCurrentInvoker()))
            // This uncancelable future ensures that TOverlayData object owning the volume will be created
            // and protects from porto volume leak.
            .ToUncancelable()
            .As<TOverlayData>();

            futures.push_back(std::move(volumeFuture));
        }

        return futures;
    }

    //! Download SquashFS files and create volumes from them.
    std::vector<TFuture<TOverlayData>> PrepareSquashFSVolumes(
        TGuid tag,
        const std::vector<TArtifactKey>& artifactKeys,
        const TArtifactDownloadOptions& downloadOptions)
    {
        YT_VERIFY(!artifactKeys.empty());

        YT_LOG_DEBUG("Prepare squashfs volumes (Tag: %v, Artifacts: %v)",
            tag,
            artifactKeys.size());

        std::vector<TFuture<TOverlayData>> futures;
        futures.reserve(artifactKeys.size());

        for (const auto& artifactKey : artifactKeys) {
            YT_VERIFY(!artifactKey.has_access_method() || FromProto<ELayerAccessMethod>(artifactKey.access_method()) == ELayerAccessMethod::Local);
            YT_VERIFY(FromProto<ELayerFilesystem>(artifactKey.filesystem()) == ELayerFilesystem::SquashFS);
            YT_VERIFY(!artifactKey.has_nbd_export_id());

            auto downloadFuture = ChunkCache_->DownloadArtifact(artifactKey, downloadOptions);
            auto volumeFuture = downloadFuture.Apply(
                BIND([=, this_ = MakeStrong(this)] (const IVolumeArtifactPtr& chunkCacheArtifact) {
                    auto tagSet = TVolumeProfilerCounters::MakeTagSet(/*volumeType*/ "squashfs", /*volumeFilePath*/ artifactKey.data_source().path());
                    TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/created").Increment(1);
                    TEventTimerGuard volumeCreateTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/create_time"));

                    // We pass chunkCacheArtifact here to later save it in SquashFS volume so that SquashFS file outlives SquashFS volume.
                    return this_->CreateSquashFSVolume(
                        tag,
                        std::move(tagSet),
                        std::move(volumeCreateTimeGuard),
                        artifactKey,
                        chunkCacheArtifact);
                }).AsyncVia(GetCurrentInvoker())).As<TOverlayData>();

            futures.push_back(std::move(volumeFuture));
        }

        return futures;
    }

    TNbdVolumePtr CreateNbdVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TArtifactKey& artifactKey)
    {
        YT_LOG_DEBUG("Creating NBD volume (Tag: %v, ExportId: %v, Path: %v, Filesytem: %v)",
            tag,
            artifactKey.nbd_export_id(),
            artifactKey.data_source().path(),
            FromProto<ELayerFilesystem>(artifactKey.filesystem()));

        auto nbdServer = Bootstrap_->GetNbdServer();

        try {
            auto nbdConfig = DynamicConfigManager_->GetConfig()->ExecNode->Nbd;

            if (!nbdConfig || !nbdConfig->Enabled || !nbdServer) {
                TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/create_errors").Increment(1);

                auto error = TError("NBD is not configured")
                    << TErrorAttribute("export_id", artifactKey.nbd_export_id())
                    << TErrorAttribute("path", artifactKey.data_source().path())
                    << TErrorAttribute("filesystem", FromProto<ELayerFilesystem>(artifactKey.filesystem()));

                THROW_ERROR(error);
            }

            auto location = PickLocation();
            auto volumeMetaFuture = location->CreateNbdVolume(tag, tagSet, std::move(volumeCreateTimeGuard), artifactKey, nbdConfig);
            auto volumeFuture = volumeMetaFuture.ApplyUnique(BIND(
                [
                    tagSet = std::move(tagSet),
                    artifactKey = artifactKey,
                    location = std::move(location),
                    nbdServer = nbdServer
                ] (TVolumeMeta&& volumeMeta) {
                return New<TNbdVolume>(
                    std::move(tagSet),
                    std::move(volumeMeta),
                    artifactKey,
                    std::move(location),
                    std::move(nbdServer));
            })).ToUncancelable();
            // This uncancelable future ensures that TNbdVolume object owning the volume will be created
            // and protects from porto volume leak.

            auto volume = WaitFor(volumeFuture)
                .ValueOrThrow();

            YT_LOG_DEBUG("Created NBD volume (Tag: %v, VolumeId: %v, ExportId: %v, Path: %v, Filesytem: %v)",
                tag,
                volume->GetId(),
                artifactKey.nbd_export_id(),
                artifactKey.data_source().path(),
                FromProto<ELayerFilesystem>(artifactKey.filesystem()));

            return volume;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to create NBD volume (Tag: %v, ExportId: %v, Path: %v, Filesytem: %v)",
                tag,
                artifactKey.nbd_export_id(),
                artifactKey.data_source().path(),
                FromProto<ELayerFilesystem>(artifactKey.filesystem()));

                nbdServer->TryUnregisterDevice(artifactKey.nbd_export_id());

            throw;
        }
    }

    TOverlayVolumePtr CreateOverlayVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TUserSandboxOptions& options,
        const std::vector<TOverlayData>& overlayDataArray)
    {
        YT_LOG_INFO("All layers and volumes have been prepared (Tag: %v, OverlayDataArraySize: %v)",
            tag,
            overlayDataArray.size());

        YT_LOG_DEBUG("Creating overlay volume (Tag: %v, OverlayDataArraySize: %v)",
            tag,
            overlayDataArray.size());

        for (const auto& volumeOrLayer : overlayDataArray) {
            if (volumeOrLayer.IsLayer()) {
                LayerCache_->Touch(volumeOrLayer.GetLayer());

                YT_LOG_DEBUG("Using layer to create new overlay volume (Tag: %v, LayerId: %v)",
                    tag,
                    volumeOrLayer.GetLayer()->GetMeta().Id);
            } else {
                YT_LOG_DEBUG("Using volume to create new overlay volume (Tag: %v, VolumeId: %v)",
                    tag,
                    volumeOrLayer.GetVolume()->GetId());
            }
        }

        auto location = PickLocation();
        auto volumeMetaFuture = location->CreateOverlayVolume(tag, tagSet, std::move(volumeCreateTimeGuard), options, overlayDataArray);
        // This future is intentionally uncancellable: we don't want to interrupt invoked volume creation,
        // until it is completed and the OverlayVolume object is fully created.
        auto volumeFuture = volumeMetaFuture.ApplyUnique(BIND(
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

        YT_LOG_DEBUG("Created overlay volume (Tag: %v, VolumeId: %v)",
            tag,
            volume->GetId());

        return volume;
    }

    TSquashFSVolumePtr CreateSquashFSVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        TEventTimerGuard volumeCreateTimeGuard,
        const TArtifactKey& artifactKey,
        IVolumeArtifactPtr chunkCacheArtifact)
    {
        auto squashFSFilePath = chunkCacheArtifact->GetFileName();

        YT_LOG_DEBUG("Creating squashfs volume (Tag: %v, SquashFSFilePath: %v)",
            tag,
            squashFSFilePath);

        auto location = PickLocation();
        auto volumeMetaFuture = location->CreateSquashFSVolume(tag, tagSet, std::move(volumeCreateTimeGuard), artifactKey, squashFSFilePath);
        auto volumeFuture = volumeMetaFuture.ApplyUnique(BIND(
            [
                tagSet = std::move(tagSet),
                artifactKey,
                chunkCacheArtifact = std::move(chunkCacheArtifact),
                location = std::move(location)
            ] (TVolumeMeta&& volumeMeta) {
            return New<TSquashFSVolume>(
                std::move(tagSet),
                std::move(volumeMeta),
                artifactKey,
                std::move(chunkCacheArtifact),
                std::move(location));
        })).ToUncancelable();
        // This uncancelable future ensures that TSquashFSVolume object owning the volume will be created
        // and protects from porto volume leak.

        auto volume = WaitFor(volumeFuture)
            .ValueOrThrow();

        YT_LOG_INFO("Created squashfs volume (Tag: %v, VolumeId: %v, SquashFSFilePath: %v)",
            tag,
            volume->GetId(),
            squashFSFilePath);

        return volume;
    }

    void PopulateAlerts(std::vector<TError>* alerts)
    {
        std::copy(Alerts_.begin(), Alerts_.end(), std::back_inserter(*alerts));
    }
};

DEFINE_REFCOUNTED_TYPE(TPortoVolumeManager)

////////////////////////////////////////////////////////////////////////////////

TFuture<IVolumeManagerPtr> CreatePortoVolumeManager(
    TDataNodeConfigPtr config,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    IVolumeChunkCachePtr chunkCache,
    IInvokerPtr controlInvoker,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    IBootstrap* bootstrap)
{
    auto volumeManager = New<TPortoVolumeManager>(
        std::move(config),
        std::move(dynamicConfigManager),
        std::move(chunkCache),
        std::move(controlInvoker),
        std::move(memoryUsageTracker),
        bootstrap);

    return volumeManager->Initialize().Apply(BIND([=] () {
        return static_cast<IVolumeManagerPtr>(volumeManager);
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
