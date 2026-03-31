#include "layer_location.h"

#include "private.h"
#include "volume.h"
#include "volume_counters.h"
#include "volume_options.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/server/lib/misc/disk_health_checker.h>
#include <yt/yt/server/lib/misc/private.h>

#include <yt/yt/library/containers/porto_executor.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/net/local_address.h>

#include <library/cpp/yt/string/string.h>

#include <util/system/fs.h>

namespace NYT::NExecNode {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NContainers;
using namespace NNode;
using namespace NTools;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TLayerLocation::TLayerLocation(
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
    auto profiler = NProfiling::TProfiler()
        .WithPrefix("/layer")
        .WithTag("location_id", ToString(Id_));

    PerformanceCounters_ = TLayerLocationPerformanceCounters{profiler};

    if (healthCheckerConfig) {
        HealthChecker_ = New<NServer::TDiskHealthChecker>(
            healthCheckerConfig,
            Config_->Path,
            LocationQueue_->GetInvoker(),
            Logger,
            profiler);
    }
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TLayerLocation::Initialize()
{
    DynamicConfig_.Store(DynamicConfigManager_->GetConfig()->ExecNode->SlotManager->VolumeManager->LayerCache);

    return BIND(&TLayerLocation::DoInitialize, MakeStrong(this))
        .AsyncVia(LocationQueue_->GetInvoker())
        .Run();
}

TFuture<TLayerMeta> TLayerLocation::ImportLayer(
    const TArtifactKey& artifactKey,
    const TString& archivePath,
    const TString& container,
    TLayerId layerId,
    TGuid tag)
{
    return BIND(&TLayerLocation::DoImportLayer, MakeStrong(this), artifactKey, archivePath, container, layerId, tag)
        .AsyncVia(LocationQueue_->GetInvoker())
        .Run();
}

TFuture<void> TLayerLocation::RemoveLayer(const TLayerId& layerId)
{
    return BIND(&TLayerLocation::DoRemoveLayer, MakeStrong(this), layerId)
        .AsyncVia(LocationQueue_->GetInvoker())
        .Run();
}

TError TLayerLocation::GetAlert()
{
    auto guard = Guard(SpinLock_);
    return Alert_;
}

TFuture<TVolumeMeta> TLayerLocation::CreateNbdVolume(
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

TFuture<TVolumeMeta> TLayerLocation::CreateTmpfsVolume(
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
TFuture<IVolumePtr> TLayerLocation::RbindRootVolume(
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

TFuture<TVolumeMeta> TLayerLocation::CreateOverlayVolume(
    TGuid tag,
    TTagSet tagSet,
    TEventTimerGuard volumeCreateTimeGuard,
    int userId,
    const std::optional<TString>& placePath,
    std::optional<int> diskSpaceLimit,
    std::optional<int> inodeLimit,
    const std::vector<TOverlayData>& overlayDataArray,
    bool placeInUserSlot)
{
    return BIND(
        &TLayerLocation::DoCreateOverlayVolume,
        MakeStrong(this),
        tag,
        Passed(std::move(tagSet)),
        Passed(std::move(volumeCreateTimeGuard)),
        userId,
        placePath,
        diskSpaceLimit,
        inodeLimit,
        overlayDataArray,
        placeInUserSlot)
        .AsyncVia(LocationQueue_->GetInvoker())
        .Run();
}

TFuture<TVolumeMeta> TLayerLocation::CreateSquashFSVolume(
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

TFuture<void> TLayerLocation::RemoveVolume(
    TTagSet tagSet,
    TVolumeId volumeId,
    std::optional<std::string> portoPlacePath)
{
    return BIND(&TLayerLocation::DoRemoveVolume, MakeStrong(this), std::move(tagSet), std::move(volumeId), std::move(portoPlacePath))
        .AsyncVia(LocationQueue_->GetInvoker())
        .Run()
        .ToUncancelable();
}

TFuture<void> TLayerLocation::LinkVolume(
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

TFuture<void> TLayerLocation::UnlinkVolume(
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

std::vector<TLayerMeta> TLayerLocation::GetAllLayers() const
{
    std::vector<TLayerMeta> layers;
    {
        auto guard = Guard(SpinLock_);
        layers.reserve(LayerIdToMeta_.size());
        for (const auto& [id, meta] : LayerIdToMeta_) {
            layers.push_back(meta);
        }
    }
    return layers;
}

TFuture<void> TLayerLocation::GetVolumeReleaseEvent()
{
    return VolumesReleasePromise_.ToFuture();
}

void TLayerLocation::Disable(const TError& error, bool persistentDisable)
{
    // TODO(don-dron): Research and fix unconditional Disabled.
    if (State_.exchange(ELocationState::Disabled) != ELocationState::Enabled) {
        return;
    }

    YT_LOG_WARNING("Layer location disabled (Path: %v)", Config_->Path);

    if (HealthChecker_) {
        //! It should not be a problem to stop health checker asynchronously.
        HealthChecker_->Stop()
            .Subscribe(BIND(
                [
                    this,
                    weakThis = MakeWeak(this)
                ] (const TError& error) {
                    if (auto this_ = weakThis.Lock()) {
                        YT_LOG_WARNING_IF(!error.IsOK(), error, "Layer location health checker stopping failed");
                    }
                }));
    }

    {
        auto guard = Guard(SpinLock_);

        Alert_ = TError(NExecNode::EErrorCode::LayerLocationDisabled, "Layer location disabled")
            << TErrorAttribute("path", Config_->Path)
            << error;

        if (persistentDisable) {
            // Save the reason in a file and exit.
            // Location will be disabled during the scan in the restarted process.
            auto lockFilePath = NFS::CombinePaths(Config_->Path, NServer::DisabledLockFileName);
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
        VolumeIdToMeta_.clear();
        LayerIdToMeta_.clear();
        PerformanceCounters_ = {};
    }

    VolumesReleasePromise_.TrySet();
}

TLayerLocationPerformanceCounters& TLayerLocation::GetPerformanceCounters()
{
    return PerformanceCounters_;
}

int TLayerLocation::GetLayerCount() const
{
    auto guard = Guard(SpinLock_);
    return LayerIdToMeta_.size();
}

int TLayerLocation::GetVolumeCount() const
{
    auto guard = Guard(SpinLock_);
    return VolumeIdToMeta_.size();
}

bool TLayerLocation::IsFull()
{
    return GetAvailableSpace() < Config_->LowWatermark;
}

bool TLayerLocation::IsLayerImportInProgress() const
{
    return LayerImportsInProgress_.load() > 0;
}

i64 TLayerLocation::GetCapacity()
{
    return std::max<i64>(0, GetUsedSpace() + GetAvailableSpace() - Config_->LowWatermark);
}

i64 TLayerLocation::GetUsedSpace() const
{
    auto guard = Guard(SpinLock_);
    return UsedSpace_;
}

i64 TLayerLocation::GetAvailableSpace()
{
    if (!IsEnabled()) {
        return 0;
    }

    const auto& path = Config_->Path;

    try {
        auto statistics = NFS::GetDiskSpaceStatistics(path);
        {
            auto guard = Guard(SpinLock_);
            AvailableSpace_ = statistics.AvailableSpace;
        }
    } catch (const std::exception& ex) {
        auto error = TError("Failed to compute available space")
            << ex;
        Disable(error);
    }

    i64 availableSpace;
    {
        auto guard = Guard(SpinLock_);
        i64 remainingQuota = std::max(static_cast<i64>(0), GetQuota() - UsedSpace_);
        availableSpace = std::min(AvailableSpace_, remainingQuota);
        AvailableSpace_ = availableSpace;
    }

    return availableSpace;
}

bool TLayerLocation::ResidesOnTmpfs() const
{
    return Config_->ResidesOnTmpfs;
}

void TLayerLocation::OnDynamicConfigChanged(
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

////////////////////////////////////////////////////////////////////////////////

std::string TLayerLocation::GetLayerPath(const TLayerId& id) const
{
    return NFS::CombinePaths(LayersPath_, ToString(id));
}

std::string TLayerLocation::GetLayerMetaPath(const TLayerId& id) const
{
    return NFS::CombinePaths(LayersMetaPath_, ToString(id)) + ".meta";
}

std::string TLayerLocation::GetVolumePath(
    const TVolumeId& id,
    const std::optional<std::string>& portoPlacePath) const
{
    if (portoPlacePath) {
        return NFS::CombinePaths(NFS::CombinePaths(portoPlacePath.value(), VolumesName), ToString(id));
    }
    return NFS::CombinePaths(VolumesPath_, ToString(id));
}

std::string TLayerLocation::GetVolumeMetaPath(
    const TVolumeId& id,
    const std::optional<std::string>& portoPlacePath) const
{
    if (portoPlacePath) {
        return NFS::CombinePaths(NFS::CombinePaths(portoPlacePath.value(), VolumesMetaName), ToString(id)) + ".meta";
    }
    return NFS::CombinePaths(VolumesMetaPath_, ToString(id)) + ".meta";
}

void TLayerLocation::ValidateEnabled() const
{
    if (!IsEnabled()) {
        THROW_ERROR_EXCEPTION(
            //EErrorCode::SlotLocationDisabled,
            "Layer location at %v is disabled",
            Config_->Path);
    }
}

THashSet<TLayerId> TLayerLocation::LoadLayerIds()
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

void TLayerLocation::LoadLayers()
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
            YT_VERIFY(LayerIdToMeta_.emplace(id, meta).second);

            UsedSpace_ += meta.size();
        }
    }
}

i64 TLayerLocation::GetQuota() const
{
    return Config_->Quota.value_or(std::numeric_limits<i64>::max());
}

void TLayerLocation::DoInitialize()
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
        RemoveVolumes(TDuration::Minutes(20));

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

void TLayerLocation::DoFinalizeLayerImport(const TLayerMeta& layerMeta, TGuid tag)
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
        LayerIdToMeta_[layerMeta.Id] = layerMeta;

        i64 layerMetaSize = layerMeta.size();
        AvailableSpace_ -= layerMetaSize;
        UsedSpace_ += layerMetaSize;

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

TLayerMeta TLayerLocation::DoImportLayer(const TArtifactKey& artifactKey, const TString& archivePath, const TString& container, TLayerId layerId, TGuid tag)
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

void TLayerLocation::DoRemoveLayer(const TLayerId& layerId)
{
    auto config = DynamicConfig_.Acquire();

    auto layerPath = GetLayerPath(layerId);
    auto layerMetaPath = GetLayerMetaPath(layerId);

    auto Logger = ExecNodeLogger()
        .WithTag("LayerId: %v, LayerPath: %v", layerId, layerPath);

    {
        auto guard = Guard(SpinLock_);
        ValidateEnabled();

        if (!LayerIdToMeta_.contains(layerId)) {
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

            i64 layerMetaSize = LayerIdToMeta_[layerId].size();

            YT_VERIFY(LayerIdToMeta_.erase(layerId));

            UsedSpace_ -= layerMetaSize;
            AvailableSpace_ += layerMetaSize;
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

TVolumeMeta TLayerLocation::DoCreateVolume(
    TGuid tag,
    TTagSet tagSet,
    std::optional<TEventTimerGuard> volumeCreateTimeGuard,
    TVolumeMeta volumeMeta,
    THashMap<TString, TString>&& volumeProperties,
    std::optional<std::string> portoPlacePath)
{
    ValidateEnabled();

    auto guard = std::move(volumeCreateTimeGuard);

    auto volumeId = TVolumeId::Create();
    auto volumePath = GetVolumePath(volumeId, portoPlacePath);
    auto volumeType = FromProto<EVolumeType>(volumeMeta.type());
    // TODO(dgolear): Switch to std::string.
    TString mountPath = NFS::CombinePaths(volumePath, MountSuffix);

    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v, VolumeType: %v, VolumeId: %v",
            tag,
            volumeType,
            volumeId);

    try {
        YT_LOG_DEBUG("Creating volume");

        NFS::MakeDirRecursive(mountPath, 0755);

        auto path = WaitFor(VolumeExecutor_->CreateVolume(mountPath, volumeProperties))
            .ValueOrThrow();

        YT_VERIFY(path == mountPath);

        auto volumeGuard = Finally([&Logger, &volumePath, &mountPath, this] {
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

        YT_LOG_DEBUG(
            "Created volume (MountPath: %v)",
            mountPath);

        ToProto(volumeMeta.mutable_id(), volumeId);
        volumeMeta.Id = volumeId;
        volumeMeta.MountPath = mountPath;
        volumeMeta.PortoPlacePath = portoPlacePath;

        auto metaBlob = SerializeProtoToRefWithEnvelope(volumeMeta);

        TLayerMetaHeader header;
        header.MetaChecksum = GetChecksum(metaBlob);

        auto volumeMetaFileName = GetVolumeMetaPath(volumeId, portoPlacePath);
        auto tempVolumeMetaFileName = volumeMetaFileName + std::string(NFS::TempFileSuffix);

        YT_LOG_DEBUG(
            "Creating volume meta (MetaFileName: %v)",
            volumeMetaFileName);

        {
            auto metaFile = std::make_unique<TFile>(
                tempVolumeMetaFileName,
                CreateAlways | WrOnly | Seq | CloseOnExec);
            metaFile->Write(&header, sizeof(header));
            metaFile->Write(metaBlob.Begin(), metaBlob.Size());
            metaFile->FlushData();
            metaFile->Close();
        }

        NFS::Rename(tempVolumeMetaFileName, volumeMetaFileName);

        auto volumeMetaGuard = Finally([&Logger, &volumeMetaFileName] {
            try {
                NFS::Remove(volumeMetaFileName);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(
                    ex,
                    "Failed to remove volume meta (VolumeMetaPath: %v)",
                    volumeMetaFileName);
            }
        });

        YT_LOG_DEBUG(
            "Created volume meta (MetaFileName: %v)",
            volumeMetaFileName);

        {
            auto guard = Guard(SpinLock_);
            ValidateEnabled();
            YT_VERIFY(VolumeIdToMeta_.emplace(volumeId, volumeMeta).second);

            if (VolumesReleasePromise_.IsSet()) {
                VolumesReleasePromise_ = NewPromise<void>();
            }
        }

        volumeGuard.Release();
        volumeMetaGuard.Release();

        TVolumeProfilerCounters::Get()->GetGauge(tagSet, "/count")
            .Update(VolumeCounters().Increment(tagSet));
        TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/created").Increment(1);

        return volumeMeta;
    } catch (const std::exception& ex) {
        TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/create_errors").Increment(1);

        YT_LOG_ERROR(
            ex,
            "Failed to create volume");

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

TVolumeMeta TLayerLocation::DoCreateNbdVolume(
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

TVolumeMeta TLayerLocation::DoCreateOverlayVolume(
    TGuid tag,
    TTagSet tagSet,
    TEventTimerGuard volumeCreateTimeGuard,
    int userId,
    const std::optional<TString>& placePath,
    std::optional<int> diskSpaceLimit,
    std::optional<int> inodeLimit,
    const std::vector<TOverlayData>& overlayDataArray,
    bool placeInUserSlot)
{
    ValidateEnabled();

    TString portoPlacePath;

    if (!placePath) {
        portoPlacePath = PlacePath_;
        YT_LOG_DEBUG("Place overlay volume in layer location (PortoPlace: %v)",
            placePath);
    } else {
        // See PORTO-460 for "//" prefix.
        portoPlacePath = (Config_->LocationIsAbsolute ? "" : "//") + placePath.value();
    }

    THashMap<TString, TString> volumeProperties = {
        {"backend", "overlay"},
        {"user", ToString(userId)},
        {"permissions", "0777"},
        {"place", portoPlacePath},
    };

    if (diskSpaceLimit) {
        volumeProperties["space_limit"] = ToString(*diskSpaceLimit);
    }

    if (inodeLimit) {
        volumeProperties["inode_limit"] = ToString(*inodeLimit);
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
        std::move(volumeProperties),
        // TODO(krasovav): refactor it
        placeInUserSlot ? std::move(placePath) : std::nullopt);
}

TVolumeMeta TLayerLocation::DoCreateSquashFSVolume(
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

TVolumeMeta TLayerLocation::DoCreateTmpfsVolume(
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

void TLayerLocation::DoRemoveVolume(
    TTagSet tagSet,
    TVolumeId volumeId,
    std::optional<std::string> portoPlacePath)
{
    auto volumePath = GetVolumePath(volumeId, portoPlacePath);
    // TODO(dgolear): Switch to std::string.
    TString mountPath = NFS::CombinePaths(volumePath, MountSuffix);
    auto volumeMetaPath = GetVolumeMetaPath(volumeId, portoPlacePath);

    auto Logger = ExecNodeLogger()
        .WithTag("VolumeId: %v, VolumePath: %v, VolumeMetaPath: %v, PortoPlacePath: %v",
            volumeId,
            volumePath,
            volumeMetaPath,
            portoPlacePath);

    YT_LOG_DEBUG("Removing volume");

    try {
        auto removeGuard = Finally([&Logger, &volumePath, &volumeMetaPath, &volumeId, this] {
            try {
                NFS::RemoveRecursive(volumePath);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(
                    ex,
                    "Failed to remove volume directory");
            }

            try {
                NFS::Remove(volumeMetaPath);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(
                    ex,
                    "Failed to remove volume meta");
            }

            YT_LOG_DEBUG("Volume directory and meta removed");

            bool setVolumesReleasePromise = false;
            {
                auto guard = Guard(SpinLock_);

                // NB. The location could be disabled while we were getting here.
                if (VolumeIdToMeta_.erase(volumeId) == 0 && IsEnabled()) {
                    YT_LOG_FATAL("Volume already removed");
                }

                // It is all right to set promise even if location is disabled.
                if (VolumeIdToMeta_.empty()) {
                    setVolumesReleasePromise = true;
                }
            }

            if (setVolumesReleasePromise) {
                VolumesReleasePromise_.TrySet();
            }

            YT_LOG_DEBUG("Volume removed");
        });

        auto timeout = TDuration::Minutes(10);
        auto deadLine = TInstant::Now() + timeout;
        auto checkDeadLine = [&] {
            auto now = TInstant::Now();
            if (now > deadLine) {
                THROW_ERROR_EXCEPTION("Failed to wait for volume to be removed")
                    << TErrorAttribute("timeout", timeout)
                    << TErrorAttribute("volume_path", mountPath);
            }
        };

        while (true) {
            checkDeadLine();

            auto unlinkError = WaitFor(VolumeExecutor_->UnlinkVolume(mountPath, "self"));
            if (unlinkError.IsOK()) {
                break;
            }

            if (unlinkError.GetCode() == EPortoErrorCode::VolumeNotReady) {
                YT_LOG_DEBUG("Waiting for volume to become ready");
                TDelayedExecutor::WaitForDuration(TDuration::Seconds(5));
                continue;
            }

            if (unlinkError.GetCode() == EPortoErrorCode::VolumeNotFound ||
                    unlinkError.GetCode() == EPortoErrorCode::VolumeNotLinked) {
                if (portoPlacePath) {
                    // Ignore VolumeNotFound and VolumeNotLinked errors for custom porto places.
                    YT_LOG_INFO(
                        unlinkError,
                        "Ignoring volume unlink error for custom porto place");
                    break;
                }
                // For volumes in default locations, these errors should be thrown.
            }

            unlinkError.ThrowOnError();
        }

        YT_LOG_DEBUG("Volume unlinked");

        TVolumeProfilerCounters::Get()->GetGauge(tagSet, "/count")
            .Update(VolumeCounters().Decrement(tagSet));
        TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/removed").Increment(1);

    } catch (const std::exception& ex) {
        TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/remove_errors").Increment(1);

        YT_LOG_ERROR(
            ex,
            "Failed to remove volume");

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

void TLayerLocation::DoLinkVolume(
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

void TLayerLocation::DoUnlinkVolume(
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

//! Remove porto volumes planted in VolumesPath_.
//! Volumes are not expected to be used since all jobs must be dead by now.
void TLayerLocation::RemoveVolumes(TDuration timeout)
{
    RemoveVolumes(VolumesPath_, timeout);
}

//! Remove layers planted at a given place.
void TLayerLocation::RemoveLayers(
    const TString& place,
    TDuration timeout)
{
    auto startTime = TInstant::Now();

    auto Logger = ExecNodeLogger()
        .WithTag("Place: %v", place);

    YT_LOG_DEBUG(
        "Removing layers from porto place (Timeout: %v)",
        timeout);

    std::vector<TString> removedLayers;

    auto layerIds = WaitFor(LayerExecutor_->ListLayers(place).WithTimeout(timeout))
        .ValueOrThrow();

    std::vector<TFuture<void>> removeFutures;
    for (const auto& layerId : layerIds) {
        YT_LOG_DEBUG(
            "Trying to remove layer (LayerId: %v)",
            layerId);

        removedLayers.push_back(layerId);
        removeFutures.push_back(LayerExecutor_->RemoveLayer(
            layerId,
            place,
            false /*async*/));
    }

    auto removeResults = WaitFor(AllSetWithTimeout(std::move(removeFutures), timeout))
        .ValueOrThrow();

    for (const auto& removeError : removeResults) {
        if (!removeError.IsOK()) {
            YT_LOG_WARNING(
                removeError,
                "Failed to remove layer");
        }
    }

    YT_LOG_DEBUG(
        "Removed layers (LayerNames: %v, Duration: %v)",
        MakeShrunkFormattableView(removedLayers, TDefaultFormatter(), 10),
        (TInstant::Now() - startTime));
}

//! Remove volumes planted at a given directory.
void TLayerLocation::RemoveVolumes(
    const TString& path,
    TDuration timeout)
{
    auto startTime = TInstant::Now();
    auto deadLine = startTime + timeout;

    auto Logger = ExecNodeLogger()
        .WithTag("Path: %v", path);

    YT_LOG_DEBUG(
        "Removing volumes from path (DeadLine: %v)",
        deadLine);

    auto checkDeadLine = [&] {
        auto now = TInstant::Now();
        if (now > deadLine) {
            THROW_ERROR_EXCEPTION("Failed to wait for volumes to be removed")
                << TErrorAttribute("timeout", timeout)
                << TErrorAttribute("path", path);
        }
    };

    std::vector<TString> removedVolumes;

    while (true) {
        checkDeadLine();

        auto volumes = WaitFor(VolumeExecutor_->GetVolumes())
            .ValueOrThrow();

        auto waitForVolumesToBecomeReady = false;
        std::vector<TFuture<void>> unlinkFutures;

        for (const auto& volume : volumes) {
            if (!volume.Path.StartsWith(path)) {
                // This volume is not from the given directory.
                continue;
            }

            if (volume.State == "destroyed" || volume.State == "unlinked") {
                // Skipping destroyed and unlinked volumes.
                YT_LOG_DEBUG(
                    "Skipping volume (VolumePath: %v, State: %v)",
                    volume.Path,
                    volume.State);
                continue;
            }

            if (volume.State != "ready") {
                waitForVolumesToBecomeReady = true;
                YT_LOG_DEBUG(
                    "Volume is not ready (VolumePath: %v, State: %v)",
                    volume.Path,
                    volume.State);
                continue;
            }

            YT_LOG_DEBUG(
                "Trying to unlink volume (VolumePath: %v, State: %v)",
                volume.Path,
                volume.State);

            // Unlink volume even if it was linked to a different container.
            removedVolumes.push_back(volume.Path);
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
                continue;
            }

            if (unlinkError.GetCode() != EPortoErrorCode::VolumeNotLinked &&
                    unlinkError.GetCode() != EPortoErrorCode::VolumeNotFound &&
                    unlinkError.GetCode() != EPortoErrorCode::VolumeNotReady) {
                THROW_ERROR(unlinkError);
            }
        }

        if (waitForVolumesToBecomeReady) {
            checkDeadLine();

            static const TDuration Duration = TDuration::Seconds(30);

            YT_LOG_DEBUG(
                "Waiting for volumes to become ready (Duration: %v)",
                Duration);

            TDelayedExecutor::WaitForDuration(Duration);
        }
    }

    YT_LOG_DEBUG(
        "Removed volumes (VolumePaths: %v, Duration: %v)",
        MakeShrunkFormattableView(removedVolumes, TDefaultFormatter(), 10),
        (TInstant::Now() - startTime));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TLayerLocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
