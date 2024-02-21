#include "location_manager.h"

#include "bootstrap.h"
#include "private.h"

#include <yt/yt/server/node/data_node/chunk_store.h>

#include <yt/yt/server/lib/misc/restart_manager.h>

#include <yt/yt/library/containers/disk_manager/disk_info_provider.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/fs.h>

namespace NYT::NDataNode {

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NContainers;
using namespace NFS;
using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TLocationManager::TLocationManager(
    IBootstrap* bootstrap,
    TChunkStorePtr chunkStore,
    IInvokerPtr controlInvoker,
    TDiskInfoProviderPtr diskInfoProvider)
    : DiskInfoProvider_(std::move(diskInfoProvider))
    , ChunkStore_(std::move(chunkStore))
    , ControlInvoker_(std::move(controlInvoker))
{
    bootstrap->SubscribePopulateAlerts(
        BIND(&TLocationManager::PopulateAlerts, MakeWeak(this)));
}

TFuture<void> TLocationManager::FailDiskByName(
    const TString& diskName,
    const TError& error)
{
    return DiskInfoProvider_->GetYTDiskInfos()
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const std::vector<TDiskInfo>& diskInfos) {
            for (const auto& diskInfo : diskInfos) {
                if (diskInfo.DeviceName == diskName &&
                    diskInfo.State == NContainers::EDiskState::OK)
                {
                    // Try to fail not accessible disk.
                    return DiskInfoProvider_->FailDisk(
                        diskInfo.DiskId,
                        error.GetMessage())
                        .Apply(BIND([=] (const TError& result) {
                            if (!result.IsOK()) {
                                YT_LOG_ERROR(result,
                                    "Error marking the disk as failed (DiskName: %v)",
                                    diskInfo.DeviceName);
                            }
                        }));
                }
            }

            return VoidFuture;
        }));
}

void TLocationManager::PopulateAlerts(std::vector<TError>* alerts)
{
    for (auto alert : DiskFailedAlerts_.Load()) {
        if (!alert.IsOK()) {
            alerts->push_back(std::move(alert));
        }
    }

    for (auto alert : DiskWaitingReplacementAlerts_.Load()) {
        if (!alert.IsOK()) {
            alerts->push_back(std::move(alert));
        }
    }
}

void TLocationManager::SetFailedDiskAlerts(std::vector<TError> alerts)
{
    DiskFailedAlerts_.Store(alerts);
}

void TLocationManager::SetWaitingReplacementDiskAlerts(std::vector<TError> alerts)
{
    DiskWaitingReplacementAlerts_.Store(alerts);
}

void TLocationManager::SetFailedUnlinkedDiskIds(std::vector<TString> diskIds)
{
    FailedUnlinkedDiskIds_.Store(diskIds);
}

std::vector<TLocationLivenessInfo> TLocationManager::MapLocationToLivenessInfo(
    const std::vector<TDiskInfo>& disks)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    THashMap<TString, TDiskInfo> diskNameToDisk;
    THashSet<TString> failedDisks;

    for (const auto& disk : disks) {
        diskNameToDisk.emplace(disk.DeviceName, disk);
        if (disk.State == NContainers::EDiskState::Failed) {
            failedDisks.insert(disk.DeviceName);
        }
    }

    std::vector<TLocationLivenessInfo> locationLivenessInfos;
    const auto& locations = ChunkStore_->Locations();
    locationLivenessInfos.reserve(locations.size());

    for (const auto& location : locations) {
        auto it = diskNameToDisk.find(location->GetStaticConfig()->DeviceName);

        if (it == diskNameToDisk.end()) {
            YT_LOG_WARNING("Unknown location disk (DeviceName: %v)",
                location->GetStaticConfig()->DeviceName);
        } else {
            locationLivenessInfos.push_back(TLocationLivenessInfo{
                .Location = location,
                .DiskId = it->second.DiskId,
                .LocationState = location->GetState(),
                .IsDiskAlive = !failedDisks.contains(location->GetStaticConfig()->DeviceName),
                .DiskState = it->second.State
            });
        }
    }

    return locationLivenessInfos;
}

TFuture<bool> TLocationManager::GetHotSwapEnabledFuture()
{
    return DiskInfoProvider_->GetHotSwapEnabledFuture();
}

TFuture<std::vector<TDiskInfo>> TLocationManager::GetDiskInfos()
{
    return DiskInfoProvider_->GetYTDiskInfos();
}

TFuture<void> TLocationManager::UpdateDiskCache()
{
    return DiskInfoProvider_->UpdateDiskCache();
}

std::vector<TGuid> TLocationManager::DoDisableLocations(const THashSet<TGuid>& locationUuids)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<TGuid> locationsForDisable;

    for (const auto& location : ChunkStore_->Locations()) {
        if (locationUuids.contains(location->GetUuid())) {
            // Manual location disable.
            auto result = location->ScheduleDisable(TError("Manual location disabling")
                << TErrorAttribute("location_uuid", location->GetUuid())
                << TErrorAttribute("location_path", location->GetPath())
                << TErrorAttribute("location_disk", location->GetStaticConfig()->DeviceName));

            if (result) {
                locationsForDisable.push_back(location->GetUuid());
            }
        }
    }

    return locationsForDisable;
}

std::vector<TGuid> TLocationManager::DoDestroyLocations(bool recoverUnlinkedDisk, const THashSet<TGuid>& locationUuids)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<TGuid> locationsForDestroy;

    for (const auto& location : ChunkStore_->Locations()) {
        if (locationUuids.contains(location->GetUuid())) {
            // Manual location destroy.
            if (location->StartDestroy()) {
                locationsForDestroy.push_back(location->GetUuid());
            }
        }
    }

    if (recoverUnlinkedDisk) {
        auto unlinkedDiskIds = FailedUnlinkedDiskIds_.Exchange(std::vector<TString>());
        for (const auto& diskId : unlinkedDiskIds) {
            YT_UNUSED_FUTURE(RecoverDisk(diskId)
                .Apply(BIND([] (const TError& result) {
                    YT_LOG_ERROR_IF(!result.IsOK(), result);
                })));
        }
    }

    return locationsForDestroy;
}

std::vector<TGuid> TLocationManager::DoResurrectLocations(const THashSet<TGuid>& locationUuids)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<TGuid> locationsForResurrect;

    for (const auto& location : ChunkStore_->Locations()) {
        if (locationUuids.contains(location->GetUuid())) {
            // Manual location resurrect.

            if (location->Resurrect()) {
                locationsForResurrect.push_back(location->GetUuid());
            }
        }
    }

    return locationsForResurrect;
}

TFuture<std::vector<TGuid>> TLocationManager::DestroyChunkLocations(
    bool recoverUnlinkedDisks,
    const THashSet<TGuid>& locationUuids)
{
    return BIND(&TLocationManager::DoDestroyLocations, MakeStrong(this))
        .AsyncVia(ControlInvoker_)
        .Run(recoverUnlinkedDisks, locationUuids);
}

TFuture<std::vector<TGuid>> TLocationManager::DisableChunkLocations(const THashSet<TGuid>& locationUuids)
{
    return BIND(&TLocationManager::DoDisableLocations, MakeStrong(this))
        .AsyncVia(ControlInvoker_)
        .Run(locationUuids);
}

TFuture<std::vector<TGuid>> TLocationManager::ResurrectChunkLocations(const THashSet<TGuid>& locationUuids)
{
    return BIND(&TLocationManager::DoResurrectLocations, MakeStrong(this))
        .AsyncVia(ControlInvoker_)
        .Run(locationUuids);
}

TFuture<void> TLocationManager::RecoverDisk(const TString& diskId)
{
    return DiskInfoProvider_->RecoverDisk(diskId);
}

////////////////////////////////////////////////////////////////////////////////

TLocationHealthChecker::TLocationHealthChecker(
    TChunkStorePtr chunkStore,
    TLocationManagerPtr locationManager,
    IInvokerPtr invoker,
    TRestartManagerPtr restartManager,
    const TProfiler& profiler)
    : DynamicConfig_(New<TLocationHealthCheckerDynamicConfig>())
    , ChunkStore_(std::move(chunkStore))
    , LocationManager_(std::move(locationManager))
    , Invoker_(std::move(invoker))
    , RestartManager_(std::move(restartManager))
    , HealthCheckerExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TLocationHealthChecker::OnLocationsHealthCheck, MakeWeak(this)),
        DynamicConfig_.Acquire()->HealthCheckPeriod))
    , Profiler_(profiler)
{
    for (auto diskState : TEnumTraits<EDiskState>::GetDomainValues()) {
        for (auto storageClass : TEnumTraits<EStorageClass>::GetDomainValues()) {
            auto diskStateName = FormatEnum(diskState);
            auto diskFamilyName = FormatEnum(storageClass);

            diskStateName.to_upper();
            diskFamilyName.to_upper();

            Gauges_[diskState][storageClass] = Profiler_
                .WithTags(TTagSet(TTagList{
                    {"diskman_state", diskStateName},
                    {"disk_family", diskFamilyName}}))
                .Gauge("/diskman_state");
        }
    }
}

void TLocationHealthChecker::Initialize()
{
    VERIFY_THREAD_AFFINITY_ANY();

    for (const auto& location : ChunkStore_->Locations()) {
        location->SubscribeDiskCheckFailed(
            BIND(&TLocationHealthChecker::OnDiskHealthCheckFailed, MakeStrong(this), location));
    }
}

void TLocationHealthChecker::Start()
{
    HealthCheckerExecutor_->Start();
}

void TLocationHealthChecker::OnDynamicConfigChanged(const TLocationHealthCheckerDynamicConfigPtr& newConfig)
{
    auto config = DynamicConfig_.Acquire();
    auto newHealthCheckPeriod = newConfig->HealthCheckPeriod;
    HealthCheckerExecutor_->SetPeriod(newHealthCheckPeriod);

    DynamicConfig_.Store(newConfig);
}

void TLocationHealthChecker::OnDiskHealthCheckFailed(
    const TStoreLocationPtr& location,
    const TError& error)
{
    auto config = DynamicConfig_.Acquire();

    if (config->Enabled && config->EnableManualDiskFailures) {
        YT_UNUSED_FUTURE(LocationManager_->FailDiskByName(location->GetStaticConfig()->DeviceName, error));
    }
}

void TLocationHealthChecker::OnLocationsHealthCheck()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    auto hotSwapEnabled = WaitFor(LocationManager_->GetHotSwapEnabledFuture());

    // Fast path.
    if (!hotSwapEnabled.IsOK()) {
        YT_LOG_DEBUG(hotSwapEnabled);
        return;
    }

    if (!hotSwapEnabled.Value()) {
        YT_LOG_DEBUG(hotSwapEnabled, "Hot swap disabled");
        return;
    }

    auto result = WaitFor(LocationManager_->UpdateDiskCache());

    if (!result.IsOK()) {
        YT_LOG_WARNING(result, "Failed to update disk cache");
    }

    auto diskInfosOrError = WaitFor(LocationManager_->GetDiskInfos());

    // Fast path.
    if (!diskInfosOrError.IsOK()) {
        YT_LOG_ERROR(diskInfosOrError, "Failed to list disk infos");
        return;
    }

    auto diskInfos = diskInfosOrError.Value();

    auto config = DynamicConfig_.Acquire();

    if (config->Enabled) {
        HandleHotSwap(diskInfos);
    }

    PushCounters(diskInfos);
}

void TLocationHealthChecker::PushCounters(std::vector<TDiskInfo> diskInfos)
{
    TEnumIndexedArray<NContainers::EDiskState, TEnumIndexedArray<NContainers::EStorageClass, i64>> counters;

    for (auto diskState : TEnumTraits<EDiskState>::GetDomainValues()) {
        for (auto storageClass : TEnumTraits<EStorageClass>::GetDomainValues()) {
            counters[diskState][storageClass] = 0;
        }
    }

    for (const auto& diskInfo : diskInfos) {
        counters[diskInfo.State][diskInfo.StorageClass]++;
    }

    for (auto diskState : TEnumTraits<EDiskState>::GetDomainValues()) {
        for (auto storageClass : TEnumTraits<EStorageClass>::GetDomainValues()) {
            Gauges_[diskState][storageClass].Update(counters[diskState][storageClass]);
        }
    }
}

void TLocationHealthChecker::HandleHotSwap(std::vector<TDiskInfo> diskInfos)
{
    auto livenessInfos = LocationManager_->MapLocationToLivenessInfo(diskInfos);

    THashMap<TString, TError> diskFailedAlertsMap;
    THashMap<TString, TError> diskWaitingReplacementAlertsMap;
    std::vector<TString> unlinkedDiskIds;

    for (const auto& diskInfo : diskInfos) {
        if (diskInfo.State == NContainers::EDiskState::Failed) {
            diskFailedAlertsMap[diskInfo.DiskId] = TError(
                NChunkClient::EErrorCode::DiskFailed,
                "Disk failed, need hot swap")
                << TErrorAttribute("disk_id", diskInfo.DiskId)
                << TErrorAttribute("disk_model", diskInfo.DiskModel)
                << TErrorAttribute("disk_state", diskInfo.State)
                << TErrorAttribute("disk_path", diskInfo.DevicePath)
                << TErrorAttribute("disk_name", diskInfo.DeviceName);
        } else if (diskInfo.State == NContainers::EDiskState::RecoverWait) {
            diskWaitingReplacementAlertsMap[diskInfo.DiskId] = TError(
                NChunkClient::EErrorCode::DiskWaitingReplacement,
                "Disk is waiting replacement")
                << TErrorAttribute("disk_id", diskInfo.DiskId)
                << TErrorAttribute("disk_model", diskInfo.DiskModel)
                << TErrorAttribute("disk_state", diskInfo.State)
                << TErrorAttribute("disk_path", diskInfo.DevicePath)
                << TErrorAttribute("disk_name", diskInfo.DeviceName);
        }
    }

    for (const auto& [diskAlertId, _] : diskFailedAlertsMap) {
        bool diskLinkedWithLocation = false;

        for (const auto& livenessInfo : livenessInfos) {
            if (livenessInfo.DiskId == diskAlertId) {
                diskLinkedWithLocation = true;
                break;
            }
        }

        if (!diskLinkedWithLocation) {
            unlinkedDiskIds.push_back(diskAlertId);
        }
    }

    LocationManager_->SetFailedDiskAlerts(GetValues(diskFailedAlertsMap));
    LocationManager_->SetWaitingReplacementDiskAlerts(GetValues(diskWaitingReplacementAlertsMap));
    LocationManager_->SetFailedUnlinkedDiskIds(unlinkedDiskIds);

    THashSet<TString> diskWithLivenessLocations;
    THashSet<TString> diskWithNotDestroyingLocations;
    THashSet<TString> diskWithDestroyingLocations;

    for (const auto& livenessInfo : livenessInfos) {
        const auto& location = livenessInfo.Location;

        if (livenessInfo.IsDiskAlive) {
            diskWithLivenessLocations.insert(livenessInfo.DiskId);
        }

        if (livenessInfo.DiskState == NContainers::EDiskState::Failed) {
            location->MarkLocationDiskFailed();
        } else if (livenessInfo.DiskState == NContainers::EDiskState::RecoverWait) {
            location->MarkLocationDiskWaitingReplacement();
        }

        if (livenessInfo.LocationState == ELocationState::Destroying) {
            diskWithDestroyingLocations.insert(livenessInfo.DiskId);
        } else {
            diskWithNotDestroyingLocations.insert(livenessInfo.DiskId);
        }
    }

    for (const auto& diskId : diskWithDestroyingLocations) {
        TError error;

        if (diskWithLivenessLocations.contains(diskId)) {
            error = TError("Disk cannot be repaired, because it contains alive locations");
        } else if (diskWithNotDestroyingLocations.contains(diskId)) {
            error = TError("Disk contains not destroying locations");
        } else {
            error = WaitFor(LocationManager_->RecoverDisk(diskId));
        }

        for (const auto& livenessInfo : livenessInfos) {
            // If disk recover request is successful than mark locations as destroyed.
            if (livenessInfo.DiskId == diskId &&
                livenessInfo.LocationState == ELocationState::Destroying)
            {
                livenessInfo.Location->FinishDestroy(error.IsOK(), error);
            }
        }
    }

    for (const auto& livenessInfo : livenessInfos) {
        if (livenessInfo.IsDiskAlive &&
            livenessInfo.DiskState == NContainers::EDiskState::OK &&
            livenessInfo.LocationState == ELocationState::Destroyed)
        {
            livenessInfo.Location->OnDiskRepaired();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TLocationHealthCheckerPtr CreateLocationHealthChecker(
    TChunkStorePtr chunkStore,
    TLocationManagerPtr locationManager,
    IInvokerPtr invoker,
    TRestartManagerPtr restartManager)
{
    return New<TLocationHealthChecker>(
        chunkStore,
        locationManager,
        invoker,
        restartManager,
        NProfiling::TProfiler("/location"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
