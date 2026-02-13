
#include "layer_location.h"

#include "private.h"
#include "volume.h"
#include "volume_counters.h"
#include "volume_options.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/tools/tools.h>
#include <yt/yt/server/tools/proc.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/misc/disk_health_checker.h>
#include <yt/yt/server/lib/misc/private.h>

#include <yt/yt/server/tools/tools.h>

#include <yt/yt/library/containers/porto_executor.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/ytree/fluent.h>

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

static const TString MountSuffix = "mount";
static const TString VolumesName = "volumes";
static const TString LayersName = "porto_layers";
static const TString LayersMetaName = "layers_meta";
static const TString VolumesMetaName = "volumes_meta";

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

TFuture<void> TLayerLocation::RemoveVolume(TTagSet tagSet, TVolumeId volumeId)
{
    return BIND(&TLayerLocation::DoRemoveVolume, MakeStrong(this), std::move(tagSet), std::move(volumeId))
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
        layers.reserve(Layers_.size());
        for (const auto& [id, meta] : Layers_) {
            layers.push_back(meta);
        }
    }
    return layers;
}

TFuture<void> TLayerLocation::GetVolumeReleaseEvent()
{
    return VolumesReleaseEvent_.ToFuture();
}

void TLayerLocation::Disable(const TError& error, bool persistentDisable)
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
    Volumes_.clear();
    Layers_.clear();

    VolumesReleaseEvent_.TrySet();

    PerformanceCounters_ = {};
}

TLayerLocationPerformanceCounters& TLayerLocation::GetPerformanceCounters()
{
    return PerformanceCounters_;
}

int TLayerLocation::GetLayerCount() const
{
    auto guard = Guard(SpinLock_);
    return Layers_.size();
}

int TLayerLocation::GetVolumeCount() const
{
    auto guard = Guard(SpinLock_);
    return Volumes_.size();
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
    return std::max<i64>(0, UsedSpace_ + GetAvailableSpace() - Config_->LowWatermark);
}

i64 TLayerLocation::GetUsedSpace() const
{
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

std::string TLayerLocation::GetVolumePath(const TVolumeId& id) const
{
    return NFS::CombinePaths(VolumesPath_, ToString(id));
}

std::string TLayerLocation::GetVolumeMetaPath(const TVolumeId& id) const
{
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
            YT_VERIFY(Layers_.emplace(id, meta).second);

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

TVolumeMeta TLayerLocation::DoCreateVolume(
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

void TLayerLocation::DoRemoveVolume(TTagSet tagSet, TVolumeId volumeId)
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

//! Remove porto volumes that belong to this location.
//! Volumes are not expected to be used since all jobs must be dead by now.
void TLayerLocation::RemoveVolumes(TDuration timeout)
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

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TLayerLocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
