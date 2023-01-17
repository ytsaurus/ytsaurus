#include "location_manager.h"

#include "private.h"

#include <yt/yt/server/node/data_node/chunk_store.h>

#include <yt/yt/library/containers/disk_manager/disk_info_provider.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NDataNode {

using namespace NConcurrency;
using namespace NContainers;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TLocationManager::TLocationManager(
    TChunkStorePtr chunkStore,
    IInvokerPtr controlInvoker,
    TDiskInfoProviderPtr diskInfoProvider)
    : ChunkStore_(std::move(chunkStore))
    , ControlInvoker_(std::move(controlInvoker))
    , DiskInfoProvider_(std::move(diskInfoProvider))
{ }

std::vector<TLocationLivenessInfo> TLocationManager::MapLocationToLivenessInfo(
    const std::vector<TDiskInfo>& failedDisks)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    THashSet<TString> diskNames;

    for (const auto& failedDisk : failedDisks) {
        diskNames.insert(failedDisk.DeviceName);
    }

    std::vector<TLocationLivenessInfo> locationLivenessInfos;
    const auto& locations = ChunkStore_->Locations();
    locationLivenessInfos.reserve(locations.size());

    for (const auto& location : locations) {
        locationLivenessInfos.push_back({
            .Location = location,
            .LocationState = location->GetState(),
            .IsDiskAlive = !diskNames.contains(location->GetStaticConfig()->DeviceName)});
    }

    return locationLivenessInfos;
}

TFuture<std::vector<TLocationLivenessInfo>> TLocationManager::GetLocationsLiveness()
{
    return DiskInfoProvider_->GetYtDiskInfos(NContainers::EDiskState::Failed)
        .Apply(BIND(&TLocationManager::MapLocationToLivenessInfo, MakeStrong(this))
        .AsyncVia(ControlInvoker_));
}

std::vector<TGuid> TLocationManager::DoDisableLocations(const THashSet<TGuid>& locationUuids)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<TGuid> locationsForDisable;

    for (const auto& location : ChunkStore_->Locations()) {
        if (locationUuids.contains(location->GetUuid())) {
            // Manual location disable.
            auto result = location->Disable(TError("Manual location disabling")
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

std::vector<TGuid> TLocationManager::DoDestroyLocations(const THashSet<TGuid>& locationUuids)
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

TFuture<std::vector<TGuid>> TLocationManager::DestroyChunkLocations(const THashSet<TGuid>& locationUuids)
{
    return BIND(&TLocationManager::DoDestroyLocations, MakeStrong(this))
        .AsyncVia(ControlInvoker_)
        .Run(locationUuids);
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
    TLocationManagerPtr locationManager,
    IInvokerPtr invoker,
    TLocationHealthCheckerConfigPtr config)
    : Config_(std::move(config))
    , Enabled_(Config_->Enabled)
    , Invoker_(std::move(invoker))
    , LocationManager_(std::move(locationManager))
    , HealthCheckerExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TLocationHealthChecker::OnHealthCheck, MakeWeak(this)),
        Config_->HealthCheckPeriod))
{ }

void TLocationHealthChecker::Start()
{
    if (Enabled_) {
        YT_LOG_DEBUG("Starting location health checker");
        HealthCheckerExecutor_->Start();
    }
}

void TLocationHealthChecker::OnDynamicConfigChanged(const TLocationHealthCheckerDynamicConfigPtr& newConfig)
{
    auto oldEnabled = Enabled_;
    auto newEnabled = newConfig->Enabled.value_or(Config_->Enabled);
    auto newHealthCheckPeriod = newConfig->HealthCheckPeriod.value_or(Config_->HealthCheckPeriod);

    HealthCheckerExecutor_->SetPeriod(newHealthCheckPeriod);

    if (oldEnabled && !newEnabled) {
        YT_LOG_DEBUG("Stopping location health checker");
        HealthCheckerExecutor_->Stop();
    } else if (!oldEnabled && newEnabled) {
        YT_LOG_DEBUG("Starting location health checker");
        HealthCheckerExecutor_->Start();
    }

    Enabled_ = newEnabled;
}

void TLocationHealthChecker::OnHealthCheck()
{
    auto livenessInfosOrError = WaitFor(LocationManager_->GetLocationsLiveness());

    // Fast path.
    if (!livenessInfosOrError.IsOK()) {
        YT_LOG_ERROR(livenessInfosOrError, "Failed to list location livenesses");
        return;
    }

    const auto& livenessInfos = livenessInfosOrError.Value();

    THashSet<TString> diskWithLivenessLocations;
    THashSet<TString> diskWithNotDestroyingLocations;
    THashSet<TString> diskWithDestroyingLocations;

    for (const auto& livenessInfo : livenessInfos) {
        const auto& location = livenessInfo.Location;

        if (livenessInfo.IsDiskAlive) {
            diskWithLivenessLocations.insert(livenessInfo.DiskId);
            location->MarkLocationDiskHealthy();
        } else {
            location->MarkLocationDiskFailed();
        }

        if (livenessInfo.LocationState == ELocationState::Destroying) {
            diskWithDestroyingLocations.insert(livenessInfo.DiskId);
        } else {
            diskWithNotDestroyingLocations.insert(livenessInfo.DiskId);
        }
    }

    for (const auto& diskId : diskWithDestroyingLocations) {
        TErrorOr<void> resultOrError;

        if (diskWithLivenessLocations.contains(diskId)) {
            resultOrError = TError("Disk cannot be repaired, because it contains alive locations");
        } else if (diskWithNotDestroyingLocations.contains(diskId)) {
            resultOrError = TError("Disk contains not destroing locations");
        } else {
            resultOrError = WaitFor(LocationManager_->RecoverDisk(diskId));
        }

        for (const auto& livenessInfo : livenessInfos) {
            // If disk recover request is successfull than mark locations as destroyed.
            if (livenessInfo.DiskId == diskId &&
                livenessInfo.LocationState == ELocationState::Destroying)
            {
                livenessInfo.Location->FinishDestroy(resultOrError.IsOK(), resultOrError);
            }
        }
    }

    for (const auto& livenessInfo : livenessInfos) {
        if (livenessInfo.IsDiskAlive &&
            livenessInfo.LocationState == ELocationState::Destroyed)
        {
            livenessInfo.Location->OnDiskRepaired();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
