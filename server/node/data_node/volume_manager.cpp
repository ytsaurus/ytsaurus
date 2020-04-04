#include "volume_manager.h"

#ifndef __linux__

// (psushin) Just a stub for Mac-lovers.

namespace NYT::NDataNode {

IVolumeManagerPtr CreatePortoVolumeManager(
    TVolumeManagerConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    THROW_ERROR_EXCEPTION("Volume manager is not supported");
}

} // namespace NYT::NDataNode

#else

#include "disk_location.h"

#include "artifact.h"
#include "chunk.h"
#include "chunk_cache.h"
#include "helpers.h"
#include "master_connector.h"
#include "private.h"

#include <yt/server/node/data_node/volume.pb.h>

#include <yt/server/node/cell_node/bootstrap.h>
#include <yt/server/node/cell_node/config.h>

#include <yt/server/lib/containers/instance.h>
#include <yt/server/lib/containers/porto_executor.h>

#include <yt/server/lib/misc/disk_health_checker.h>
#include <yt/server/lib/misc/private.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/tools/tools.h>
#include <yt/ytlib/tools/proc.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/client/api/client.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/async_semaphore.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/async_cache.h>
#include <yt/core/misc/checksum.h>
#include <yt/core/misc/fs.h>
#include <yt/core/misc/finally.h>
#include <yt/core/misc/proc.h>

#include <yt/core/profiling/profile_manager.h>

#include <util/system/fs.h>

namespace NYT::NDataNode {

using namespace NApi;
using namespace NConcurrency;
using namespace NContainers;
using namespace NCellNode;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NTools;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;
static const auto ProfilingPeriod = TDuration::Seconds(1);

static const TString StorageSuffix = "storage";
static const TString MountSuffix = "mount";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TPortoVolumeManager)

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

struct TVolumeMeta
    : public NProto::TVolumeMeta
{
    TVolumeId Id;
    TString StoragePath;
    TString MountPath;
};

////////////////////////////////////////////////////////////////////////////////

struct TLayerLocationPerformanceCounters
{
    NProfiling::TSimpleGauge LayerCount;
    NProfiling::TSimpleGauge VolumeCount;

    NProfiling::TAggregateGauge TotalSpace;
    NProfiling::TAggregateGauge UsedSpace;
    NProfiling::TAggregateGauge AvailableSpace;
    NProfiling::TSimpleGauge Full;
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
        IPortoExecutorPtr volumeExecutor,
        IPortoExecutorPtr layerExecutor,
        const TString& id)
        : TDiskLocation(locationConfig, id, DataNodeLogger)
          , Config_(locationConfig)
          , VolumeExecutor_(std::move(volumeExecutor))
          , LayerExecutor_(std::move(layerExecutor))
          , LocationQueue_(New<TActionQueue>(id))
          , VolumesPath_(NFS::CombinePaths(Config_->Path, VolumesName))
          , VolumesMetaPath_(NFS::CombinePaths(Config_->Path, VolumesMetaName))
          , LayersPath_(NFS::CombinePaths(Config_->Path, LayersName))
          , LayersMetaPath_(NFS::CombinePaths(Config_->Path, LayersMetaName))
    {
        // If true, location is placed on a YT-specific drive, binded into container from dom0 host,
        // so it has absolute path relative to dom0 root.
        // Otherwise, location is placed inside a persistent volume, and should be treated differently.
        // More details here: PORTO-460.
        PlacePath_ = (Config_->LocationIsAbsolute ? "" : "//") + Config_->Path;

        auto* profileManager = NProfiling::TProfileManager::Get();
        Profiler_ = DataNodeProfiler
            .AppendPath("/layer_location")
            .AddTags({
                 profileManager->RegisterTag("location_id", Id_),
             });

        PerformanceCounters_.LayerCount = {"/layer_count", {}};
        PerformanceCounters_.VolumeCount = {"/volume_count", {}};

        PerformanceCounters_.AvailableSpace = {"/available_space", {}, NProfiling::EAggregateMode::All};
        PerformanceCounters_.UsedSpace = {"/used_space", {}, NProfiling::EAggregateMode::All};
        PerformanceCounters_.AvailableSpace = {"/available_space", {}, NProfiling::EAggregateMode::All};
        PerformanceCounters_.TotalSpace = {"/total_space", {}, NProfiling::EAggregateMode::All};
        PerformanceCounters_.Full = {"/full"};

        try {
            NFS::MakeDirRecursive(Config_->Path, 0755);

            if (healthCheckerConfig) {
                HealthChecker_ = New<TDiskHealthChecker>(
                    healthCheckerConfig,
                    locationConfig->Path,
                    LocationQueue_->GetInvoker(),
                    Logger);
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

            if (HealthChecker_) {
                HealthChecker_->SubscribeFailed(BIND(&TLayerLocation::Disable, MakeWeak(this))
                    .Via(LocationQueue_->GetInvoker()));
                HealthChecker_->Start();
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to initialize layer location %v",
                Config_->Path)
                << ex;
        }

        Enabled_ = true;
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
            YT_LOG_ERROR(ex, "Error creating location lock file");
            // Exit anyway.
        }

        YT_LOG_ERROR("Volume manager disabled; terminating");
        NLogging::TLogManager::Get()->Shutdown();
        _exit(1);
    }

    const NProfiling::TProfiler& GetProfiler() const
    {
        return Profiler_;
    }

    TLayerLocationPerformanceCounters& GetPerformanceCounters()
    {
        return PerformanceCounters_;
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
            YT_ABORT(); // Disable() exits the process.
        }

        i64 remainingQuota = std::max(static_cast<i64>(0), GetQuota() - UsedSpace_);
        AvailableSpace_ = std::min(AvailableSpace_, remainingQuota);

        return AvailableSpace_;
    }

private:
    const TLayerLocationConfigPtr Config_;
    const IPortoExecutorPtr VolumeExecutor_;
    const IPortoExecutorPtr LayerExecutor_;

    const TActionQueuePtr LocationQueue_ ;
    TDiskHealthCheckerPtr HealthChecker_;

    NProfiling::TProfiler Profiler_;
    TLayerLocationPerformanceCounters PerformanceCounters_;

    TString PlacePath_;

    TSpinLock SpinLock;
    const TString VolumesPath_;
    const TString VolumesMetaPath_;
    const TString LayersPath_;
    const TString LayersMetaPath_;

    std::atomic<int> LayerImportsInProgress_ = { 0 };

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
                WaitFor(LayerExecutor_->RemoveLayer(layerName, PlacePath_))
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

            if (metaFile.GetLength() < sizeof (TLayerMetaHeader)) {
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::IncorrectLayerFileSize,
                    "Layer meta file %v is too short: at least %v bytes expected",
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
            YT_VERIFY(Layers_.insert(std::make_pair(id, meta)).second);
        }
    }

    i64 GetQuota() const
    {
        return Config_->Quota.value_or(std::numeric_limits<i64>::max());
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

        AvailableSpace_ -= layerMeta.size();
        UsedSpace_ += layerMeta.size();

        {
            auto guard = Guard(SpinLock);
            Layers_[layerMeta.Id] = layerMeta;
        }

        YT_LOG_INFO("Finished layer import (LayerId: %v, LayerPath: %v, UsedSpace: %v, AvailableSpace: %v, Tag: %v)",
            layerMeta.Id,
            layerMeta.Path,
            UsedSpace_,
            AvailableSpace_,
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

            YT_LOG_DEBUG("Create new directory for layer (LayerId: %v, Tag: %v)",
                id,
                tag);

            auto layerDirectory = GetLayerPath(id);

            try {
                YT_LOG_DEBUG("Unpack layer (Path: %v, Tag: %v)",
                    layerDirectory,
                    tag);
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

            auto layerSize = RunTool<TGetDirectorySizeAsRootTool>(layerDirectory);

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

            return layerMeta;
        } catch (const std::exception& ex) {
            auto error = TError("Failed to import layer %v", id)
                << ex;

            auto innerError = TError(ex);
            if (innerError.GetCode() == EErrorCode::LayerUnpackingFailed) {
                THROW_ERROR error;
            }

            Disable(error);
            YT_ABORT();
        }
    }

    void DoRemoveLayer(const TLayerId& layerId)
    {
        ValidateEnabled();

        auto layerPath = GetLayerPath(layerId);
        auto layerMetaPath = GetLayerMetaPath(layerId);

        try {
            YT_LOG_INFO("Removing layer (LayerId: %v, LayerPath: %v)",
                layerId,
                layerPath);
            LayerExecutor_->RemoveLayer(ToString(layerId), PlacePath_);
            NFS::Remove(layerMetaPath);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to remove layer %v",
                layerId)
                << ex;
            Disable(error);
            YT_ABORT();
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
            YT_LOG_DEBUG("Creating volume (VolumeId: %v)",
                id);

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

            auto volumePath = WaitFor(VolumeExecutor_->CreateVolume(mountPath, parameters))
                .ValueOrThrow();

            YT_VERIFY(volumePath == mountPath);

            YT_LOG_INFO("Volume created (VolumeId: %v, VolumeMountPath: %v)",
                id,
                mountPath);

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

            YT_LOG_INFO("Volume meta created (VolumeId: %v, MetaFileName: %v)",
                id,
                volumeMetaFileName);

            auto guard = Guard(SpinLock);
            YT_VERIFY(Volumes_.insert(std::make_pair(id, volumeMeta)).second);

            return volumeMeta;
        } catch (const std::exception& ex) {
            auto error = TError("Failed to create volume %v", id)
                << ex;
            Disable(error);
            YT_ABORT();
        }
    }

    void DoRemoveVolume(const TVolumeId& volumeId)
    {
        ValidateEnabled();

        {
            auto guard = Guard(SpinLock);
            YT_VERIFY(Volumes_.contains(volumeId));
        }

        auto volumePath = GetVolumePath(volumeId);
        auto mountPath = NFS::CombinePaths(volumePath, MountSuffix);
        auto volumeMetaPath = GetVolumeMetaPath(volumeId);

        try {
            YT_LOG_DEBUG("Removing volume (VolumeId: %v)",
                volumeId);

            WaitFor(VolumeExecutor_->UnlinkVolume(mountPath, "self"))
                .ThrowOnError();

            YT_LOG_DEBUG("Volume unlinked (VolumeId: %v)",
                volumeId);

            RunTool<TRemoveDirAsRootTool>(volumePath);
            NFS::Remove(volumeMetaPath);

            YT_LOG_INFO("Volume directory and meta removed (VolumeId: %v, VolumePath: %v, VolumeMetaPath: %v)",
                volumeId,
                volumePath,
                volumeMetaPath);

            auto guard = Guard(SpinLock);
            YT_VERIFY(Volumes_.erase(volumeId) == 1);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to remove volume %v", volumeId)
                << ex;
            Disable(error);
            YT_ABORT();
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
                YT_VERIFY(error.IsOK());
                callback.Run();
            }));
    }

    i64 GetSize() const
    {
        return LayerMeta_.size();
    }

    void OnEvicted()
    {
        YT_LOG_DEBUG("Layer is evicted (LayerId: %v)",
            LayerMeta_.Id);
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

/////////////////////////////////////////////////////////////////////////////

class TLayerCache
    : public TAsyncSlruCacheBase<TArtifactKey, TLayer>
{
public:
    TLayerCache(
        const TVolumeManagerConfigPtr& config,
        std::vector<TLayerLocationPtr> layerLocations,
        IPortoExecutorPtr tmpfsExecutor,
        TBootstrap* bootstrap)
        : TAsyncSlruCacheBase(
            New<TSlruCacheConfig>(GetCacheCapacity(layerLocations) * config->CacheCapacityFraction),
            DataNodeProfiler.AppendPath("/layer_cache"))
        , Config_(config)
        , Bootstrap_(bootstrap)
        , LayerLocations_(std::move(layerLocations))
        , Semaphore_(New<TAsyncSemaphore>(config->LayerImportConcurrency))
        , TmpfsPortoExecutor_(tmpfsExecutor)
        , ProfilingExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TLayerCache::OnProfiling, MakeWeak(this)),
            ProfilingPeriod))
        , TmpfsCacheHitCounter_("/layer_cache/tmpfs_cache_hits")
    {
        InitTmpfsLayerCache();

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

        ProfilingExecutor_->Start();
    }

    TFuture<TLayerPtr> PrepareLayer(const TArtifactKey& artifactKey, TGuid tag)
    {
        {
            auto guard = Guard(TmpfsCacheDataSpinLock_);
            auto it = CachedTmpfsLayers_.find(artifactKey);
            if (it != CachedTmpfsLayers_.end()) {
                auto layer = it->second;
                guard.Release();

                DataNodeProfiler.Increment(TmpfsCacheHitCounter_);
                YT_LOG_DEBUG("Found layer in tmpfs cache (LayerId: %v, ArtifactPath: %v, Tag: %v)",
                    layer->GetMeta().Id,
                    artifactKey.data_source().path(),
                    tag);
                return MakeFuture(layer);
            }
        }

        auto cookie = BeginInsert(artifactKey);
        auto value = cookie.GetValue();
        if (cookie.IsActive()) {
            DownloadAndImportLayer(artifactKey, tag, false)
                .Subscribe(BIND([=, this_ = MakeStrong(this), cookie_ = std::move(cookie)] (const TErrorOr<TLayerPtr>& layerOrError) mutable {
                    if (!layerOrError.IsOK()) {
                        cookie_.Cancel(layerOrError);
                    } else {
                        cookie_.EndInsert(layerOrError.Value());
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

    void Touch(const TLayerPtr& layer)
    {
        Find(layer->GetKey());
    }

    void BuildOrchidYson(TFluentMap fluent) const
    {
        fluent
            .Item("cached_layer_count").Value(GetSize())
            .Item("tmpfs_cache").DoMap([&] (auto fluentMap) {
                auto guard1 = Guard(TmpfsCacheDataSpinLock_);
                auto guard2 = Guard(TmpfsCacheAlertSpinLock_);
                fluentMap
                    .Item("layer_count").Value(CachedTmpfsLayers_.size())
                    .Item("alert").Value(TmpfsCacheAlert_);
            });
    }

private:
    const TVolumeManagerConfigPtr Config_;
    TBootstrap* const Bootstrap_;
    const std::vector<TLayerLocationPtr> LayerLocations_;

    TAsyncSemaphorePtr Semaphore_;

    THashMap<TYPath, TFetchedArtifactKey> CachedLayerDescriptors_;

    TSpinLock TmpfsCacheDataSpinLock_;
    THashMap<TArtifactKey, TLayerPtr> CachedTmpfsLayers_;
    TPeriodicExecutorPtr UpdateTmpfsLayersExecutor_;

    TSpinLock TmpfsCacheAlertSpinLock_;
    TError TmpfsCacheAlert_;

    IPortoExecutorPtr TmpfsPortoExecutor_;
    TLayerLocationPtr TmpfsLocation_;

    TPeriodicExecutorPtr ProfilingExecutor_;

    TMonotonicCounter TmpfsCacheHitCounter_;

    virtual bool IsResurrectionSupported() const override
    {
        return false;
    }

    virtual i64 GetWeight(const TLayerPtr& layer) const override
    {
        return layer->GetSize();
    }

    virtual void OnRemoved(const TLayerPtr& layer) override
    {
        layer->OnEvicted();
    }

    void InitTmpfsLayerCache()
    {
        if (!Config_->TmpfsLayerCache->LayersDirectoryPath) {
            return;
        }

        auto path = NFS::CombinePaths(NFs::CurrentWorkingDirectory(), "tmpfs_layers");
        Bootstrap_->GetMasterConnector()->SubscribePopulateAlerts(BIND(
            &TLayerCache::PopulateTmpfsAlert,
            MakeWeak(this)));

        {
            YT_LOG_DEBUG("Cleanup tmpfs layer cache volume (Path: %v)", path);
            auto error = WaitFor(TmpfsPortoExecutor_->UnlinkVolume(path, "self"));
            if (!error.IsOK()) {
                YT_LOG_DEBUG(error, "Failed to unlink volume (Path: %v)", path);
            }
        }

        try {
            YT_LOG_DEBUG("Create tmpfs layer cache volume (Path: %v)", path);

            NFS::MakeDirRecursive(path, 0777);

            std::map<TString, TString> volumeProperties;
            volumeProperties["backend"] = "tmpfs";
            volumeProperties["permissions"] = "0777";
            volumeProperties["space_limit"] = ToString(Config_->TmpfsLayerCache->Capacity);

            WaitFor(TmpfsPortoExecutor_->CreateVolume(path, volumeProperties))
                .ThrowOnError();

            Bootstrap_->GetMemoryUsageTracker()->Acquire(
                NNodeTrackerClient::EMemoryCategory::TmpfsLayers,
                Config_->TmpfsLayerCache->Capacity);

            auto locationConfig = New<TLayerLocationConfig>();
            locationConfig->Quota = Config_->TmpfsLayerCache->Capacity;
            locationConfig->LowWatermark = 0;
            locationConfig->MinDiskSpace = 0;
            locationConfig->Path = path;
            locationConfig->LocationIsAbsolute = false;

            TmpfsLocation_ = New<TLayerLocation>(
                std::move(locationConfig),
                nullptr,
                TmpfsPortoExecutor_,
                TmpfsPortoExecutor_,
                "tmpfs_layers");

            UpdateTmpfsLayersExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetControlInvoker(),
                BIND(&TLayerCache::OnTmpfsLayersUpdate, MakeWeak(this)),
                Config_->TmpfsLayerCache->LayersUpdatePeriod);
            UpdateTmpfsLayersExecutor_->Start();
        } catch (const std::exception& ex) {
            SetTmpfsAlert(TError("Failed to create tmpfs layer volume cache") << ex);
        }
    }

    TFuture<TLayerPtr> DownloadAndImportLayer(const TArtifactKey& artifactKey, TGuid tag, bool useTmpfsLocation)
    {
        auto& chunkCache = Bootstrap_->GetChunkCache();

        YT_LOG_DEBUG("Start loading layer into cache (Tag: %v, ArtifactPath: %v, UseTmpfsLocation: %v)",
            tag,
            artifactKey.data_source().path(),
            useTmpfsLocation);

        TArtifactDownloadOptions downloadOptions;
        downloadOptions.NodeDirectory = Bootstrap_->GetNodeDirectory();
        return chunkCache->DownloadArtifact(artifactKey, downloadOptions)
            .Apply(BIND([=, this_ = MakeStrong(this)] (const IChunkPtr& artifactChunk) {
                 YT_LOG_DEBUG("Layer artifact loaded, starting import (Tag: %v, ArtifactPath: %v)",
                     tag,
                     artifactKey.data_source().path());

                  // NB(psushin): we limit number of concurrently imported layers, since this is heavy operation
                  // which may delay light operations performed in the same IO thread pool inside porto daemon.
                  // PORTO-518
                  TAsyncSemaphoreGuard guard;
                  while (!(guard = TAsyncSemaphoreGuard::TryAcquire(Semaphore_))) {
                      WaitFor(Semaphore_->GetReadyEvent())
                          .ThrowOnError();
                  }

                  auto location = this_->PickLocation();
                  auto layerMeta = WaitFor(location->ImportLayer(artifactKey, artifactChunk->GetFileName(), tag))
                      .ValueOrThrow();

                  if (useTmpfsLocation) {
                      // For tmpfs layers we cannot import them directly to tmpfs location,
                      // since tar/gzip are run in special /portod-helpers cgroup, which suffers from
                      // OOM when importing into tmpfs. To workaround this, we first import to disk locations
                      // and then copy into in-memory.

                      auto finally = Finally(BIND([=] () {
                          location->RemoveLayer(layerMeta.Id);
                      }));

                      auto tmpfsLayerMeta = WaitFor(TmpfsLocation_->InternalizeLayer(layerMeta, tag))
                          .ValueOrThrow();
                      return New<TLayer>(tmpfsLayerMeta, artifactKey, TmpfsLocation_);
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

    void OnTmpfsLayersUpdate()
    {
        const auto& client = Bootstrap_->GetMasterClient();

        auto tag = TGuid::Create();
        auto Logger = NLogging::TLogger(DataNodeLogger)
            .AddTag("Tag: %v", tag);

        YT_LOG_INFO("Started updating tmpfs layers");

        TListNodeOptions listNodeOptions;
        listNodeOptions.ReadFrom = EMasterChannelKind::Cache;
        listNodeOptions.Attributes = std::vector<TString>{"id"};
        auto listNodeRspOrError = WaitFor(client->ListNode(
            *Config_->TmpfsLayerCache->LayersDirectoryPath,
            listNodeOptions));

        if (!listNodeRspOrError.IsOK()) {
            SetTmpfsAlert(TError("Failed to list tmpfs layers directory %v",
                Config_->TmpfsLayerCache->LayersDirectoryPath)
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
            SetTmpfsAlert(TError("Tmpfs layers directory %v has invalid structure",
                Config_->TmpfsLayerCache->LayersDirectoryPath)
                << ex);
            return;
        }

        YT_LOG_INFO("Listed tmpfs layers (Count: %v)", paths.size());

        {
            THashMap<TYPath, TFetchedArtifactKey> cachedLayerDescriptors;
            for (const auto& path : paths) {
                auto it = CachedLayerDescriptors_.find(path);
                if (it != CachedLayerDescriptors_.end()) {
                    cachedLayerDescriptors.insert(*it);
                } else {
                    cachedLayerDescriptors.insert(std::make_pair(
                        path,
                    TFetchedArtifactKey{.ContentRevision = 0}));
                }
            }

            CachedLayerDescriptors_.swap(cachedLayerDescriptors);
        }

        std::vector<TFuture<TFetchedArtifactKey>> futures;
        for (const auto& pair : CachedLayerDescriptors_) {
            auto future = BIND([=, this_ = MakeStrong(this)] () {
                const auto& path = pair.first;
                const auto& fetchedKey = pair.second;
                auto revision = fetchedKey.ArtifactKey
                    ? fetchedKey.ContentRevision
                    : NHydra::NullRevision;
                return FetchLayerArtifactKeyIfRevisionChanged(path, revision, Bootstrap_, EMasterChannelKind::Cache, Logger);
            })
            .AsyncVia(GetCurrentInvoker())
            .Run();

            futures.push_back(std::move(future));
        }

        auto fetchResultsOrError = WaitFor(Combine(futures));
        if (!fetchResultsOrError.IsOK()) {
            SetTmpfsAlert(TError("Failed to fetch tmpfs layer descriptions")
                << fetchResultsOrError);
            return;
        }

        int index = 0;
        THashSet<TArtifactKey> newArtifacts;
        for (auto& pair : CachedLayerDescriptors_) {
            const auto& fetchResult = fetchResultsOrError.Value()[index];
            if (fetchResult.ArtifactKey) {
                pair.second = fetchResult;
            }
            ++index;
            YT_VERIFY(pair.second.ArtifactKey);
            newArtifacts.insert(*pair.second.ArtifactKey);
        }

        YT_LOG_DEBUG("Listed unique tmpfs layers (Count: %v)", newArtifacts.size());

        {
            std::vector<TArtifactKey> artifactsToRemove;
            artifactsToRemove.reserve(CachedTmpfsLayers_.size());

            auto guard = Guard(TmpfsCacheDataSpinLock_);
            for (const auto& [key, layer] : CachedTmpfsLayers_) {
                if (!newArtifacts.contains(key)) {
                    artifactsToRemove.push_back(key);
                    layer->OnEvicted();
                } else {
                    newArtifacts.erase(key);
                }
            }

            for (const auto& artifactKey : artifactsToRemove) {
                CachedTmpfsLayers_.erase(artifactKey);
            }

            guard.Release();

            YT_LOG_INFO_IF(!artifactsToRemove.empty(), "Released cached tmpfs layers (Count: %v)",
                artifactsToRemove.size());
        }

        std::vector<TFuture<TLayerPtr>> newLayerFutures;
        newLayerFutures.reserve(newArtifacts.size());
        for (const auto& artifactKey : newArtifacts) {
            newLayerFutures.push_back(DownloadAndImportLayer(artifactKey, tag, true));
        }

        auto newLayersOrError = WaitFor(CombineAll(newLayerFutures));
        if (!newLayersOrError.IsOK()) {
            SetTmpfsAlert(TError("Failed to import new tmpfs layers")
                << newLayersOrError);
            return;
        }

        bool hasFailedLayer = false;
        for (const auto& newLayerOrError : newLayersOrError.Value()) {
            if (!newLayerOrError.IsOK()) {
                hasFailedLayer = true;
                SetTmpfsAlert(TError("Failed to import new tmpfs layer")
                    << newLayerOrError);
                continue;
            }

            const auto& layer = newLayerOrError.Value();
            YT_LOG_INFO("Successfully imported new tmpfs layer (LayerId: %v, ArtifactPath: %v)",
                layer->GetMeta().Id,
                layer->GetMeta().artifact_key().data_source().path());

            TArtifactKey key;
            key.CopyFrom(layer->GetMeta().artifact_key());

            auto guard = Guard(TmpfsCacheDataSpinLock_);
            CachedTmpfsLayers_[key] = layer;
        }

        if (!hasFailedLayer) {
            // No alert, everything is fine.
            SetTmpfsAlert(TError());
        }

        YT_LOG_INFO("Finished updating tmpfs layers");
    }

    void SetTmpfsAlert(const TError& error)
    {
        auto guard = Guard(TmpfsCacheAlertSpinLock_);
        if (error.IsOK() && !TmpfsCacheAlert_.IsOK()) {
            YT_LOG_INFO("Tmpfs layer cache alert reset");
        } else if (!error.IsOK()) {
            YT_LOG_WARNING(error, "Tmpfs layer cache alert set");
        }

        TmpfsCacheAlert_ = error;
    }

    void PopulateTmpfsAlert(std::vector<TError>* errors)
    {
        auto guard = Guard(TmpfsCacheAlertSpinLock_);
        if (!TmpfsCacheAlert_.IsOK()) {
            errors->push_back(TmpfsCacheAlert_);
        }
    }

    void OnProfiling()
    {
        auto profileLocation = [] (const TLayerLocationPtr& location) {
            const auto& profiler = location->GetProfiler();
            auto& performanceCounters = location->GetPerformanceCounters();
            profiler.Update(performanceCounters.AvailableSpace, location->GetAvailableSpace());
            profiler.Update(performanceCounters.UsedSpace, location->GetUsedSpace());
            profiler.Update(performanceCounters.TotalSpace, location->GetCapacity());
            profiler.Update(performanceCounters.Full, location->IsFull() ? 1 : 0);
            profiler.Update(performanceCounters.LayerCount, location->GetLayerCount());
            profiler.Update(performanceCounters.VolumeCount, location->GetVolumeCount());
        };

        if (TmpfsLocation_) {
            profileLocation(TmpfsLocation_);
        }

        for (const auto& location : LayerLocations_) {
            profileLocation(location);
        }
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
        TPortoVolumeManagerPtr owner,
        TLayerLocationPtr location,
        const std::vector<TLayerPtr>& layers)
        : VolumeMeta_(meta)
        , Owner_(std::move(owner))
        , Location_(std::move(location))
        , Layers_(layers)
    {
        auto callback = BIND(&TVolumeState::OnLayerEvicted, MakeWeak(this));
        // NB: We need a copy of layers vector here since OnLayerEvicted may be invoked in-place and cause Layers_ change.
        for (const auto& layer : layers) {
            layer->SubscribeEvicted(callback);
        }
    }

    ~TVolumeState()
    {
        YT_LOG_INFO("Destroying volume (VolumeId: %v)",
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

    const TString& GetPath() const
    {
        return VolumeMeta_.MountPath;
    }

    const std::vector<TLayerPtr>& GetLayers() const
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

    void OnLayerEvicted()
    {
        auto guard = Guard(SpinLock_);
        Evicted_ = true;
        if (ActiveCount_ == 0) {
            ReleaseLayers(std::move(guard));
        }
    }

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
        return VolumeState_->GetPath();
    }

private:
    const TVolumeStatePtr VolumeState_;
};

////////////////////////////////////////////////////////////////////////////////

class TPortoVolumeManager
    : public IVolumeManager
{
public:
    TPortoVolumeManager(const TVolumeManagerConfigPtr& config, TBootstrap* bootstrap)
    {
        // Create locations.
        for (int index = 0; index < config->LayerLocations.size(); ++index) {
            const auto& locationConfig = config->LayerLocations[index];
            auto id = Format("layer%v", index);

            try {
                auto location = New<TLayerLocation>(
                    locationConfig,
                    bootstrap->GetConfig()->DataNode->DiskHealthChecker,
                    CreatePortoExecutor(
                        config->PortoExecutor,
                        Format("volume%v", index),
                        DataNodeProfiler.AppendPath("/location_volumes/porto")),
                    CreatePortoExecutor(
                        config->PortoExecutor,
                        Format("layer%v", index),
                        DataNodeProfiler.AppendPath("/location_layers/porto")),
                    id);
                Locations_.push_back(location);
            } catch (const std::exception& ex) {
                auto error = TError("Layer location at %v is disabled", locationConfig->Path)
                    << ex;
                YT_LOG_WARNING(error);
                auto masterConnector = bootstrap->GetMasterConnector();
                masterConnector->RegisterAlert(error);
            }
        }

        auto tmpfsExecutor = CreatePortoExecutor(
            config->PortoExecutor,
            "tmpfs_layer",
            DataNodeProfiler.AppendPath("/tmpfs_layers/porto"));
        LayerCache_ = New<TLayerCache>(config, Locations_, tmpfsExecutor, bootstrap);
    }

    virtual TFuture<IVolumePtr> PrepareVolume(const std::vector<TArtifactKey>& layers) override
    {
        YT_VERIFY(!layers.empty());

        auto tag = TGuid::Create();

        auto createVolume = [=, this_ = MakeStrong(this)] (bool isLocked, const TVolumeStatePtr& volumeState) {
            for (const auto& layer : volumeState->GetLayers()) {
                LayerCache_->Touch(layer);
            }

            YT_LOG_DEBUG("Creating new layered volume (Tag: %v, Path: %v)",
                tag,
                volumeState->GetPath());

            return New<TLayeredVolume>(volumeState, isLocked);
        };

        auto promise = NewPromise<TVolumeStatePtr>();
        promise.OnCanceled(BIND([=] (const TError& error) {
            promise.TrySet(TError(NYT::EErrorCode::Canceled, "Root volume preparation canceled")
                << TErrorAttribute("preparation_tag", tag)
                << error);
        }));

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
                tag)
            .Via(GetCurrentInvoker()));

        return promise.ToFuture()
            .Apply(BIND(createVolume, true))
            .As<IVolumePtr>();
    }

private:
    std::vector<TLayerLocationPtr> Locations_;

    TLayerCachePtr LayerCache_;

    TLayerLocationPtr PickLocation()
    {
        return DoPickLocation(Locations_, [] (const TLayerLocationPtr& candidate, const TLayerLocationPtr& current) {
            return candidate->GetVolumeCount() < current->GetVolumeCount();
        });
    }

    virtual void BuildOrchidYson(NYTree::TFluentMap fluent) const override
    {
        LayerCache_->BuildOrchidYson(fluent);
    }

    void OnLayersPrepared(
        TPromise<TVolumeStatePtr> volumeStatePromise,
        TGuid tag,
        const TErrorOr<std::vector<TLayerPtr>>& errorOrLayers)
    {
        try {
            YT_LOG_DEBUG(errorOrLayers, "All layers prepared (Tag: %v)",
                tag);

            const auto& layers = errorOrLayers
                .ValueOrThrow();

            std::vector<TLayerMeta> layerMetas;
            layerMetas.reserve(layers.size());
            for (const auto& layer : layers) {
                layerMetas.push_back(layer->GetMeta());
                YT_LOG_DEBUG("Using layer to create new volume (LayerId: %v, Tag: %v)",
                    layer->GetMeta().Id,
                    tag);
            }

            auto location = PickLocation();
            auto volumeMeta = WaitFor(location->CreateVolume(layerMetas))
                .ValueOrThrow();

            auto volumeState = New<TVolumeState>(
                volumeMeta,
                this,
                location,
                layers);

            YT_LOG_DEBUG("Created volume state (Tag: %v, VolumeId: %v)",
                tag,
                volumeMeta.Id);

            volumeStatePromise.TrySet(volumeState);
        } catch (const std::exception& ex) {
            volumeStatePromise.TrySet(TError(ex));
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TPortoVolumeManager)

////////////////////////////////////////////////////////////////////////////////

IVolumeManagerPtr CreatePortoVolumeManager(
    TVolumeManagerConfigPtr config,
    TBootstrap* bootstrap)
{
    return New<TPortoVolumeManager>(std::move(config), bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

#endif
