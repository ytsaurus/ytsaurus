#ifdef __linux__

#include "volume_manager.h"

#include "artifact.h"
#include "chunk.h"
#include "chunk_cache.h"
#include "master_connector.h"
#include "private.h"

#include <yt/server/data_node/volume.pb.h>

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/containers/porto_executor.h>

#include <yt/server/misc/disk_location.h>
#include <yt/server/misc/disk_health_checker.h>
#include <yt/server/misc/private.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/async_cache.h>
#include <yt/core/misc/checksum.h>
#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>

#include <yt/core/tools/tools.h>

namespace NYT {
namespace NDataNode {

using namespace NConcurrency;
using namespace NContainers;
using namespace NCellNode;
using namespace NTools;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = DataNodeLogger;

static const TString StorageSuffix = "storage";
static const TString MountSuffix = "mount";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TPortoVolumeManager);

////////////////////////////////////////////////////////////////////////////////

using TLayerId = TGuid;
using TVolumeId = TGuid;

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
    TString Path;
    TLayerId Id;
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeKey
{
    const std::vector<TArtifactKey> LayerKeys;

    explicit TVolumeKey(const std::vector<TArtifactKey>& layerKeys)
        : LayerKeys(layerKeys)
    { }

    // Hasher.
    operator size_t() const
    {
        size_t result = 0;
        for (const auto& artifactKey : LayerKeys) {
            HashCombine(result, size_t(artifactKey));
        }
        return result;
    }

    bool operator == (const TVolumeKey& other) const
    {
        return LayerKeys == other.LayerKeys;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeMeta
    : public NProto::TVolumeMeta
{
    TVolumeId Id;
    TString StoragePath;
    TString MountPath;
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
        const TLayerLocationConfigPtr& locationConfig,
        const TDiskHealthCheckerConfigPtr healthCheckerConfig,
        const IPortoExecutorPtr& executor,
        const TString& id)
        : TDiskLocation(locationConfig, id, DataNodeLogger)
        , Config_(locationConfig)
        , Executor_(executor)
        , LocationQueue_(New<TActionQueue>(id))
        , VolumesPath_(NFS::CombinePaths(Config_->Path, VolumesName))
        , VolumesMetaPath_(NFS::CombinePaths(Config_->Path, VolumesMetaName))
        , LayersPath_(NFS::CombinePaths(Config_->Path, LayersName))
        , LayersMetaPath_(NFS::CombinePaths(Config_->Path, LayersMetaName))
    {
        HealthChecker_ = New<TDiskHealthChecker>(
            healthCheckerConfig,
            locationConfig->Path,
            LocationQueue_->GetInvoker(),
            Logger);

        try {
            WaitFor(HealthChecker_->RunCheck())
                .ThrowOnError();

            // Volumes are not expected to be used since all jobs must be dead by now.
            auto volumes = WaitFor(Executor_->ListVolumes())
                .ValueOrThrow();

            std::vector<TFuture<void>> unlinkFutures;
            for (const auto& volume : volumes) {
                if (volume.Path.StartsWith(VolumesPath_)) {
                    unlinkFutures.push_back(Executor_->UnlinkVolume(volume.Path, "self"));
                }
            }
            WaitFor(Combine(unlinkFutures))
                .ThrowOnError();

            RunTool<TRemoveDirAsRootTool>(VolumesPath_);
            RunTool<TRemoveDirAsRootTool>(VolumesMetaPath_);

            NFS::MakeDirRecursive(VolumesPath_, 0755);
            NFS::MakeDirRecursive(LayersPath_, 0755);
            NFS::MakeDirRecursive(VolumesMetaPath_, 0755);
            NFS::MakeDirRecursive(LayersMetaPath_, 0755);
            NFS::MakeDirRecursive(LayersMetaPath_, 0755);
            // This is requires to use directory as place.
            NFS::MakeDirRecursive(NFS::CombinePaths(Config_->Path, "porto_volumes"), 0755);
            NFS::MakeDirRecursive(NFS::CombinePaths(Config_->Path, "porto_storage"), 0755);

            ValidateMinimumSpace();

            LoadLayers();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to initialize layer location %v", Config_->Path)
                << ex;
        }

        HealthChecker_->SubscribeFailed(BIND(&TLayerLocation::Disable, MakeWeak(this))
            .Via(LocationQueue_->GetInvoker()));
        HealthChecker_->Start();
        Enabled_ = true;
    }

    TFuture<TLayerMeta> ImportLayer(const TArtifactKey& artifactKey, const TString& archivePath, const TGuid& tag)
    {
        return BIND(&TLayerLocation::DoImportLayer, MakeStrong(this), artifactKey, archivePath, tag)
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    void RemoveLayer(const TLayerId& layerId)
    {
        BIND(&TLayerLocation::DoRemoveLayer, MakeStrong(this), layerId)
            .Via(LocationQueue_->GetInvoker())
            .Run();
    }

    TFuture<TVolumeMeta> CreateVolume(const std::vector<TLayerMeta>& layers)
    {
        return BIND(&TLayerLocation::DoCreateVolume, MakeStrong(this), layers)
            .AsyncVia(LocationQueue_->GetInvoker())
            .Run();
    }

    void RemoveVolume(const TVolumeId& volumeId)
    {
        BIND(&TLayerLocation::DoRemoveVolume, MakeStrong(this), volumeId)
            .Via(LocationQueue_->GetInvoker())
            .Run();
    }

    std::vector<TLayerMeta> GetAllLayers() const
    {
        std::vector<TLayerMeta> layers;

        auto guard = Guard(SpinLock);
        for (const auto& pair : Layers_) {
            layers.push_back(pair.second);
        }
        return layers;
    }

    void Disable(const TError& error)
    {
        if (!Enabled_.exchange(false)) {
            Sleep(TDuration::Max());
        }

        // Save the reason in a file and exit.
        // Location will be disabled during the scan in the restart process.
        auto lockFilePath = NFS::CombinePaths(Config_->Path, DisabledLockFileName);
        try {
            auto errorData = ConvertToYsonString(error, NYson::EYsonFormat::Pretty).GetData();
            TFile file(lockFilePath, CreateAlways | WrOnly | Seq | CloseOnExec);
            TFileOutput fileOutput(file);
            fileOutput << errorData;
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error creating location lock file");
            // Exit anyway.
        }

        LOG_ERROR("Volume manager disabled; terminating");
        NLogging::TLogManager::Get()->Shutdown();
        _exit(1);
    }

    int GetLayerCount() const
    {
        auto guard = Guard(SpinLock);
        return Layers_.size();
    }

    int GetVolumeCount() const
    {
        auto guard = Guard(SpinLock);
        return Volumes_.size();
    }

    bool IsFull()
    {
        return GetAvailableSpace() < Config_->LowWatermark;
    }

    i64 GetCapacity()
    {
        return std::max<i64>(0, UsedSpace_ + GetAvailableSpace() - Config_->LowWatermark);
    }

private:
    const TLayerLocationConfigPtr Config_;
    const NCellNode::TBootstrap* Bootstrap_;

    const IPortoExecutorPtr Executor_;

    const TActionQueuePtr LocationQueue_ ;
    TDiskHealthCheckerPtr HealthChecker_;

    TSpinLock SpinLock;
    const TString VolumesPath_;
    const TString VolumesMetaPath_;
    const TString LayersPath_;
    const TString LayersMetaPath_;

    THashMap<TLayerId, TLayerMeta> Layers_;
    THashMap<TVolumeId, TVolumeMeta> Volumes_;

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
                LOG_DEBUG("Remove temporary file (Path: %v)", fileName);
                NFS::Remove(fileName);
                continue;
            }

            auto nameWithoutExtension = NFS::GetFileNameWithoutExtension(fileName);
            TGuid id;
            if (!TGuid::FromString(nameWithoutExtension, &id)) {
                LOG_ERROR("Unrecognized file in layer location directory (Path: %v)", fileName);
                continue;
            }

            fileIds.insert(id);
        }

        THashSet<TGuid> confirmedIds;
        auto layerNames = WaitFor(Executor_->ListLayers(Config_->Path))
            .ValueOrThrow();

        for (const auto& layerName : layerNames) {
            TGuid id;
            if (!TGuid::FromString(layerName, &id)) {
                LOG_ERROR("Unrecognized layer name in layer location directory (LayerName: %v)", layerName);
                continue;
            }

            if (!fileIds.has(id)) {
                LOG_DEBUG("Remove directory without a corresponding meta file (LayerName: %v)", layerName);
                WaitFor(Executor_->RemoveLayer(layerName, Config_->Path))
                    .ThrowOnError();
                continue;
            }

            YCHECK(confirmedIds.insert(id).second);
            YCHECK(fileIds.erase(id) == 1);
        }

        for (const auto& id : fileIds) {
            auto path = GetLayerMetaPath(id);
            LOG_DEBUG("Remove layer meta file with no matching layer (Path: %v)", path);
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

            if (metaFile.GetLength() < sizeof (TLayerMetaHeader)) {
                THROW_ERROR_EXCEPTION("Layer meta file %v is too short: at least %v bytes expected",
                    metaFileName,
                    sizeof (TLayerMetaHeader));
            }

            auto metaFileBlob = TSharedMutableRef::Allocate(metaFile.GetLength());

            NFS::ExpectIOErrors([&] () {
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

            NProto::TLayerMeta protoMeta;
            if (!TryDeserializeProtoWithEnvelope(&protoMeta, metaBlob)) {
                THROW_ERROR_EXCEPTION("Failed to parse chunk meta file %v",
                    metaFileName);
            }

            TLayerMeta meta;
            meta.MergeFrom(protoMeta);
            meta.Id = id;
            meta.Path = GetLayerPath(id);

            UsedSpace_ += meta.size();

            auto guard = Guard(SpinLock);
            YCHECK(Layers_.insert(std::make_pair(id, meta)).second);
        }
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
            Y_UNREACHABLE(); // Disable() exits the process.
        }

        i64 remainingQuota = std::max(static_cast<i64>(0), GetQuota() - UsedSpace_);
        AvailableSpace_ = std::min(AvailableSpace_, remainingQuota);

        return AvailableSpace_;
    }

    i64 GetQuota() const
    {
        return Config_->Quota.Get(std::numeric_limits<i64>::max());
    }

    TLayerMeta DoImportLayer(const TArtifactKey& artifactKey, const TString& archivePath, const TGuid& tag)
    {
        ValidateEnabled();

        auto id = TLayerId::Create();
        try {   
            LOG_DEBUG("Ensure that cached layer archive is not in use (LayerId: %v, ArchivePath: %v, Tag: %v)", id, archivePath, tag);

            {
                // Take exclusive lock in blocking fashion to ensure that no
                // forked process is holding an open descriptor to the source file.
                TFile file(archivePath, RdOnly | CloseOnExec);
                file.Flock(LOCK_EX);
            }

            LOG_DEBUG("Create new directory for layer (LayerId: %v, Tag: %v)", id, tag);
            auto layerDirectory = GetLayerPath(id);

            try {
                LOG_DEBUG("Unpack layer (Path: %v, Tag: %v)", layerDirectory, tag);
                WaitFor(Executor_->ImportLayer(archivePath, ToString(id), Config_->Path))
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Layer unpacking failed (LayerId: %v, ArchivePath: %v, Tag: %v)", id, archivePath, tag);
                THROW_ERROR_EXCEPTION(EErrorCode::LayerUnpackingFailed, "Layer unpacking failed")
                    << ex;
            }

            auto layerSize = RunTool<TGetDirectorySizeAsRootTool>(layerDirectory);

            LOG_DEBUG("Calculated layer size (LayerId: %v, Size: %v, Tag: %v)", id, layerSize, tag);

            TLayerMeta layerMeta;
            layerMeta.Path = layerDirectory;
            layerMeta.Id = id;
            layerMeta.mutable_artifact_key()->MergeFrom(artifactKey);
            layerMeta.set_size(layerSize);
            ToProto(layerMeta.mutable_id(), id);

            auto metaBlob = SerializeProtoToRefWithEnvelope(layerMeta);

            TLayerMetaHeader header;
            header.MetaChecksum = GetChecksum(metaBlob);

            auto layerMetaFileName = GetLayerMetaPath(id);
            auto temporaryLayerMetaFileName = layerMetaFileName + NFS::TempFileSuffix;

            TFile metaFile(
                temporaryLayerMetaFileName,
                CreateAlways | WrOnly | Seq | CloseOnExec);
            metaFile.Write(&header, sizeof(header));
            metaFile.Write(metaBlob.Begin(), metaBlob.Size());
            metaFile.Close();

            NFS::Rename(temporaryLayerMetaFileName, layerMetaFileName);

            AvailableSpace_ -= layerSize;
            UsedSpace_ += layerSize;

            {
                auto guard = Guard(SpinLock);
                Layers_[id] = layerMeta;
            }

            LOG_INFO("Finished importing layer (LayerId: %v, LayerPath: %v, UsedSpace: %v, AvailableSpace: %v, Tag: %v)",
                id,
                layerDirectory,
                UsedSpace_,
                AvailableSpace_,
                tag);

            return layerMeta;
        } catch (const std::exception& ex) {
            auto error = TError("Failed to import layer %v", id)
                << ex;

            auto innerError = TError(ex);
            if (innerError.GetCode() == EErrorCode::LayerUnpackingFailed) {
                THROW_ERROR error;
            }

            Disable(error);
            Y_UNREACHABLE();
        }
    }

    void DoRemoveLayer(const TLayerId& layerId)
    {
        ValidateEnabled();

        auto layerPath = GetLayerPath(layerId);
        auto layerMetaPath = GetLayerMetaPath(layerId);

        try {
            LOG_INFO("Removing layer (LayerId: %v, LayerPath: %v)", layerId, layerPath);
            Executor_->RemoveLayer(ToString(layerId), Config_->Path);
            NFS::Remove(layerMetaPath);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to remove layer %v", layerId)
                << ex;
            Disable(error);
            Y_UNREACHABLE();
        }

        i64 layerSize = -1;

        {
            auto guard = Guard(SpinLock);
            layerSize = Layers_[layerId].size();
            Layers_.erase(layerId);
        }

        UsedSpace_ -= layerSize;
        AvailableSpace_ += layerSize;
    }

    TVolumeMeta DoCreateVolume(const std::vector<TLayerMeta>& layers)
    {
        ValidateEnabled();

        auto id = TVolumeId::Create();
        auto volumePath = GetVolumePath(id);

        auto storagePath = NFS::CombinePaths(volumePath, StorageSuffix);
        auto mountPath = NFS::CombinePaths(volumePath, MountSuffix);

        try {
            LOG_DEBUG("Creating volume (VolumeId: %v)", id);

            NFS::MakeDirRecursive(storagePath, 0755);
            NFS::MakeDirRecursive(mountPath, 0755);

            std::map<TString, TString> parameters;
            parameters["backend"] = "overlay";
            parameters["storage"] = storagePath;

            TStringBuilder builder;
            for (const auto& layer : layers) {
                if (builder.GetLength() > 0) {
                    builder.AppendChar(';');
                }
                builder.AppendString(layer.Path);
            }

            parameters["layers"] = builder.Flush();

            auto volumeId = WaitFor(Executor_->CreateVolume(mountPath, parameters))
                .ValueOrThrow();

            YCHECK(volumeId.Path == mountPath);

            LOG_INFO("Volume created (VolumeId: %v, VolumeMountPath: %v)", id, mountPath);

            TVolumeMeta volumeMeta;
            for (const auto& layer : layers) {
                volumeMeta.add_layer_artifact_keys()->MergeFrom(layer.artifact_key());
                volumeMeta.add_layer_paths(layer.Path);
            }
            ToProto(volumeMeta.mutable_id(), id);
            volumeMeta.StoragePath = storagePath;
            volumeMeta.MountPath = mountPath;
            volumeMeta.Id = id;

            auto metaBlob = SerializeProtoToRefWithEnvelope(volumeMeta);

            TLayerMetaHeader header;
            header.MetaChecksum = GetChecksum(metaBlob);

            auto volumeMetaFileName = GetVolumeMetaPath(id);
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

            LOG_INFO("Volume meta created (VolumeId: %v, MetaFileName: %v)", id, volumeMetaFileName);

            auto guard = Guard(SpinLock);
            YCHECK(Volumes_.insert(std::make_pair(id, volumeMeta)).second);

            return volumeMeta;
        } catch (const std::exception& ex) {
            auto error = TError("Failed to create volume %v", id)
                << ex;
            Disable(error);
            Y_UNREACHABLE();
        }
    }

    void DoRemoveVolume(const TVolumeId& volumeId)
    {
        ValidateEnabled();

        {
            auto guard = Guard(SpinLock);
            YCHECK(Volumes_.has(volumeId));
        }

        auto volumePath = GetVolumePath(volumeId);
        auto mountPath = NFS::CombinePaths(volumePath, MountSuffix);
        auto volumeMetaPath = GetVolumeMetaPath(volumeId);

        try {
            LOG_DEBUG("Removing volume (VolumeId: %v)", volumeId);

            WaitFor(Executor_->UnlinkVolume(mountPath, "self"))
                .ThrowOnError();

            LOG_DEBUG("Volume unlinked (VolumeId: %v)", volumeId);

            RunTool<TRemoveDirAsRootTool>(volumePath);
            NFS::Remove(volumeMetaPath);

            LOG_INFO("Volume directory and meta removed (VolumeId: %v, VolumePath: %v, VolumeMetaPath: %v)",
                volumeId,
                volumePath,
                volumeMetaPath);

            auto guard = Guard(SpinLock);
            YCHECK(Volumes_.erase(volumeId) == 1);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to remove volume %v", volumeId)
                << ex;
            Disable(error);
            Y_UNREACHABLE();
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
        LOG_INFO("Layer is destroyed (LayerId: %v, LayerPath: %v)", LayerMeta_.Id, LayerMeta_.Path);
        Location_->RemoveLayer(LayerMeta_.Id);
    }

    const TString& GetPath() const
    {
        return LayerMeta_.Path;
    }

    void SubscribeEvicted(TCallback<void()> callback)
    {
        Evicted_.ToFuture()
           .Subscribe(BIND([=] (const TError& error) {
                YCHECK(error.IsOK());
                callback.Run();
            }));
    }

    i64 GetSize() const
    {
        return LayerMeta_.size();
    }

    void OnEvicted()
    {
        LOG_DEBUG("Layer is evicted (LayerId: %v)", LayerMeta_.Id);
        Evicted_.Set();
    }

    const TLayerMeta& GetMeta() const
    {
        return LayerMeta_;
    }

private:
    const TLayerMeta LayerMeta_;

    const TLayerLocationPtr Location_;

    TPromise<void> Evicted_ = NewPromise<void>();
};

DEFINE_REFCOUNTED_TYPE(TLayer)
DECLARE_REFCOUNTED_CLASS(TLayer)

////////////////////////////////////////////////////////////////////////////////

class TLayerCache
    : public TAsyncSlruCacheBase<TArtifactKey, TLayer>
{
public:
    TLayerCache(
        const TVolumeManagerConfigPtr& config,
        const std::vector<TLayerLocationPtr>& layerLocations,
        const TBootstrap* bootstrap)
        : TAsyncSlruCacheBase(
            New<TSlruCacheConfig>(GetCacheCapacity(layerLocations) * config->CacheCapacityFraction),
            NProfiling::TProfiler(DataNodeProfiler.GetPathPrefix() + "/layer_cache"))
        , Bootstrap_(bootstrap)
        , LayerLocations_(layerLocations)
    {
        for (const auto& location : LayerLocations_) {
            for (const auto& layerMeta : location->GetAllLayers()) {
                TArtifactKey key;
                key.MergeFrom(layerMeta.artifact_key());
                auto layer = New<TLayer>(layerMeta, key, location);
                auto cookie = BeginInsert(layer->GetKey());
                if (cookie.IsActive()) {
                    cookie.EndInsert(layer);
                }
            }
        }
    }

    TFuture<TLayerPtr> PrepareLayer(const TArtifactKey& artifactKey, const TGuid& tag)
    {
        auto cookie = BeginInsert(artifactKey);
        auto value = cookie.GetValue();
        if (cookie.IsActive()) {
            auto& chunkCache = Bootstrap_->GetChunkCache();

            LOG_DEBUG("Start loading layer into cache (Tag: %v, ArtifactKey: %v)", tag, artifactKey);

            chunkCache->PrepareArtifact(artifactKey, Bootstrap_->GetNodeDirectory())
                .Subscribe(BIND([=, this_ = MakeStrong(this), cookie_ = std::move(cookie)] (const TErrorOr<IChunkPtr>& artifactChunkOrError) mutable {
                    try {
                        LOG_DEBUG("Layer artifact loaded, starting import (Tag: %v, Error: %v, ArtifactKey: %v)", tag, artifactChunkOrError, artifactKey);

                        // NB: ensure that artifact stays alive until the end of layer import.
                        const auto& artifactChunk = artifactChunkOrError.ValueOrThrow();
                        auto location = this_->PickLocation();

                        auto layerMeta = WaitFor(location->ImportLayer(artifactKey, artifactChunk->GetFileName(), tag))
                            .ValueOrThrow();

                        auto layer = New<TLayer>(layerMeta, artifactKey, location);
                        cookie_.EndInsert(layer);
                    } catch (const std::exception& ex) {
                        cookie_.Cancel(ex);
                    }
                })
                // We must pass this action through invoker to avoid synchronous execution.
                // WaitFor calls inside this action can ruin context-switch-free handlers inside TJob.
                .Via(GetCurrentInvoker()));
        } else {
            LOG_DEBUG("Layer is already being loaded into cache (Tag: %v, ArtifactKey: %v)", tag, artifactKey);
        }

        return value;
    }

    void Touch(const TLayerPtr& layer)
    {
        Find(layer->GetKey());
    }

private:
    const TBootstrap* Bootstrap_;

    std::vector<TLayerLocationPtr> LayerLocations_;

    virtual i64 GetWeight(const TLayerPtr& layer) const override
    {
        return layer->GetSize();
    }

    virtual void OnRemoved(const TLayerPtr& layer) override
    {
        layer->OnEvicted();
    }

    TLayerLocationPtr PickLocation() const
    {
        return DoPickLocation(LayerLocations_, [] (const TLayerLocationPtr& candidate, const TLayerLocationPtr& current) {
            return candidate->GetLayerCount() < current->GetLayerCount();
        });
    }
};

DECLARE_REFCOUNTED_CLASS(TLayerCache)
DEFINE_REFCOUNTED_TYPE(TLayerCache)

////////////////////////////////////////////////////////////////////////////////

class TVolumeState
    : public TRefCounted
{
public:
    TVolumeState(
        const TVolumeMeta& meta,
        const TPortoVolumeManagerPtr& owner,
        const TLayerLocationPtr& location,
        const std::vector<TLayerPtr>& layers)
        : VolumeMeta_(meta)
        , Owner_(owner)
        , Location_(location)
        , Layers_(layers)
    {
        auto callback = BIND(&TVolumeState::OnLayerEvicted, MakeWeak(this));
        for (auto& layer : layers) {
            layer->SubscribeEvicted(callback);
        }
    }

    ~TVolumeState()
    {
        LOG_INFO("Destroying volume (VolumeId: %v)",
            VolumeMeta_.Id);

        Location_->RemoveVolume(VolumeMeta_.Id);
    }

    bool TryAcquireLock()
    {
        auto guard = Guard(SpinLock_);
        if (Evicted_) {
            return false;
        }

        ActiveCount_ += 1;
        return true;
    }

    void ReleaseLock()
    {
        auto guard = Guard(SpinLock_);
        ActiveCount_ -= 1;

        if (Evicted_ && ActiveCount_ == 0) {
            ReleaseLayers(std::move(guard));
        }
    }

    const TString& Path() const
    {
        return VolumeMeta_.MountPath;
    }

    const std::vector<TLayerPtr>& Layers() const
    {
        return Layers_;
    }

private:
    const TVolumeMeta VolumeMeta_;
    const TPortoVolumeManagerPtr Owner_;
    const TLayerLocationPtr Location_;

    TSpinLock SpinLock_;
    std::vector<TLayerPtr> Layers_;

    int ActiveCount_= 1;
    bool Evicted_ = false;

    void OnLayerEvicted();

    void ReleaseLayers(TGuard<TSpinLock>&& guard)
    {
        std::vector<TLayerPtr> layers;
        std::swap(layers, Layers_);
        guard.Release();
    }
};

DECLARE_REFCOUNTED_CLASS(TVolumeState)
DEFINE_REFCOUNTED_TYPE(TVolumeState)

////////////////////////////////////////////////////////////////////////////////

class TLayeredVolume
    : public IVolume
{
public:
    TLayeredVolume(TVolumeStatePtr volumeState, bool isLocked)
        : VolumeState_(std::move(volumeState))
    {
        if (!isLocked && !VolumeState_->TryAcquireLock()) {
            THROW_ERROR_EXCEPTION("Failed to lock volume state, volume is waiting to be destroyed");
        }
    }

    ~TLayeredVolume()
    {
        VolumeState_->ReleaseLock();
    }

    virtual const TString& GetPath() const override
    {
        return VolumeState_->Path();
    }

private:
    TVolumeStatePtr VolumeState_;
};

////////////////////////////////////////////////////////////////////////////////

class TPortoVolumeManager
    : public IVolumeManager
{
public:
    TPortoVolumeManager(const TVolumeManagerConfigPtr& config, const TBootstrap* bootstrap)
        : Executor_(CreatePortoExecutor(config->PortoRetryTimeout, config->PortoPollPeriod))
    {
        // Create locations.
        for (int index = 0; index < config->LayerLocations.size(); ++index) {
            const auto& locationConfig = config->LayerLocations[index];
            auto id = Format("layers_%v", index);

            try {
                auto location = New<TLayerLocation>(
                    locationConfig,
                    bootstrap->GetConfig()->DataNode->DiskHealthChecker,
                    Executor_,
                    id);
                Locations_.push_back(location);
            } catch (const std::exception& ex) {
                auto error = TError("Layer location at %v is disabled", locationConfig->Path)
                    << ex;
                LOG_WARNING(error);
                auto masterConnector = bootstrap->GetMasterConnector();
                masterConnector->RegisterAlert(error);
            }
        }

        LayerCache_ = New<TLayerCache>(config, Locations_, bootstrap);
    }

    virtual TFuture<IVolumePtr> PrepareVolume(const std::vector<TArtifactKey>& layers) override
    {
        YCHECK(!layers.empty());

        auto volumeKey = TVolumeKey(layers);
        auto tag = TGuid::Create();

        auto createVolume = [=] (bool isLocked, const TVolumeStatePtr& volumeState) {
            for (const auto& layer : volumeState->Layers()) {
                LayerCache_->Touch(layer);
            }

            LOG_DEBUG("Creating new layered volume (Tag: %v, Path: %v)", tag, volumeState->Path());

            return New<TLayeredVolume>(volumeState, isLocked);
        };

        TPromise<TVolumeStatePtr> promise;
        {
            auto guard = Guard(SpinLock_);
            auto it = Volumes_.find(volumeKey);

            if (it != Volumes_.end()) {
                // Better release guard before calling Apply.
                guard.Release();

                LOG_DEBUG("Extracting volume from cache (Tag: %v, OriginalVolumeTag: %v)", tag, it->second.Tag);

                return it->second.VolumeFuture
                    .Apply(BIND(createVolume, false))
                    .As<IVolumePtr>();
            } else {
                LOG_DEBUG("Volume is not cached, will be created (Tag: %v)", tag);

                promise = NewPromise<TVolumeStatePtr>();
                YCHECK(Volumes_.insert(std::make_pair(volumeKey, TAsyncVolume{promise.ToFuture(), tag})).second);
            }
        }

        YCHECK(promise);

        // We have to create a new volume.
        std::vector<TFuture<TLayerPtr>> layerFutures;
        layerFutures.reserve(layers.size());
        for (const auto& layerKey : layers) {
            layerFutures.push_back(LayerCache_->PrepareLayer(layerKey, tag));
        }

        // ToDo(psushin): choose proper invoker.
        // Avoid sync calls to WaitFor, to please job preparation context switch guards.
        Combine(layerFutures)
            .Subscribe(BIND(
                &TPortoVolumeManager::OnLayersPrepared,
                MakeStrong(this),
                promise,
                volumeKey,
                tag)
            .Via(GetCurrentInvoker()));

        // This promise is intentionally uncancelable. If we decide to abort job cancel job preparation
        // this volume will hopefully be reused by another job.
        return promise.ToFuture()
            .Apply(BIND(createVolume, true))
            .As<IVolumePtr>();
    }

    bool RemoveVolume(const TVolumeKey& key)
    {
        auto guard = Guard(SpinLock_);
        return Volumes_.erase(key) == 1;
    }

private:
    struct TAsyncVolume
    {
        TFuture<TVolumeStatePtr> VolumeFuture;

        //! This tag allows to trace the history of a volume being cached.
        TGuid Tag;
    };

    IPortoExecutorPtr Executor_;

    std::vector<TLayerLocationPtr> Locations_;

    TLayerCachePtr LayerCache_;

    TSpinLock SpinLock_;
    THashMap<TVolumeKey, TAsyncVolume> Volumes_;

    std::atomic<bool> Enabled_ = { true };

    TLayerLocationPtr PickLocation()
    {
        return DoPickLocation(Locations_, [] (const TLayerLocationPtr& candidate, const TLayerLocationPtr& current) {
            return candidate->GetVolumeCount() < current->GetVolumeCount();
        });
    }

    void OnLayersPrepared(
        TPromise<TVolumeStatePtr> volumeStatePromise,
        const TVolumeKey& key,
        const TGuid& tag,
        const TErrorOr<std::vector<TLayerPtr>>& errorOrLayers)
    {
        try {
            LOG_DEBUG("All layers prepared (Tag: %v, Error: %v)", tag, errorOrLayers);

            const auto& layers = errorOrLayers
                .ValueOrThrow();

            std::vector<TLayerMeta> layerMetas;
            layerMetas.reserve(layers.size());
            for (const auto& layer : layers) {
                layerMetas.push_back(layer->GetMeta());
            }

            auto location = PickLocation();
            auto volumeMeta = WaitFor(location->CreateVolume(layerMetas))
                .ValueOrThrow();

            auto volumeState = New<TVolumeState>(
                volumeMeta,
                this,
                location,
                layers);

            LOG_DEBUG("Created volume state (Tag: %v, VolumeId: %v)", tag, volumeMeta.Id);

            volumeStatePromise.Set(volumeState);
        } catch (const std::exception& ex) {
            volumeStatePromise.Set(TError(ex));
            YCHECK(RemoveVolume(key));
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TPortoVolumeManager)

////////////////////////////////////////////////////////////////////////////////

// This method is defined after TPortoVolumeManager since it calls RemoveVolume.
void TVolumeState::OnLayerEvicted()
{
    std::vector<TArtifactKey> layerKeys;
    for (const auto& layerKey : VolumeMeta_.layer_artifact_keys()) {
        TArtifactKey key;
        key.MergeFrom(layerKey);
        layerKeys.push_back(key);
    }

    auto volumeKey = TVolumeKey(std::move(layerKeys));
    Owner_->RemoveVolume(volumeKey);

    auto guard = Guard(SpinLock_);
    Evicted_ = true;
    if (ActiveCount_ == 0) {
        ReleaseLayers(std::move(guard));
    }
}

////////////////////////////////////////////////////////////////////////////////

IVolumeManagerPtr CreatePortoVolumeManager(
    const TVolumeManagerConfigPtr& config,
    const TBootstrap* bootstrap)
{
    return New<TPortoVolumeManager>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

#endif
