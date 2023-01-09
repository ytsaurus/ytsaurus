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

std::vector<TLocationLivenessInfo> TLocationManager::MapLocationToLivelinessInfo(
    const std::vector<TDiskInfo>& failedDisks)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    THashSet<TString> diskNames;

    for (const auto& failedDisk : failedDisks) {
        diskNames.insert(failedDisk.DeviceName);
    }

    std::vector<TLocationLivenessInfo> locationLivelinessInfos;
    const auto& locations = ChunkStore_->Locations();
    locationLivelinessInfos.reserve(locations.size());

    for (const auto& location : locations) {
        locationLivelinessInfos.push_back({
            .Location = location,
            .IsDisabled = !location->IsEnabled(),
            .IsDiskAlive = !diskNames.contains(location->GetStaticConfig()->DeviceName)});
    }

    return locationLivelinessInfos;
}

TFuture<std::vector<TLocationLivenessInfo>> TLocationManager::GetLocationsLiveliness()
{
    return DiskInfoProvider_->GetYtDiskInfos(NContainers::EDiskState::Failed)
        .Apply(BIND(&TLocationManager::MapLocationToLivelinessInfo, MakeStrong(this))
            .AsyncVia(ControlInvoker_));
}

std::vector<TGuid> TLocationManager::DisableChunkLocations(
    const std::vector<TDiskInfo>& failedDisks,
    const THashSet<TGuid>& locationUuids)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Fast path.
    if (failedDisks.empty()) {
        return {};
    }

    THashSet<TString> failedDiskNames;

    for (const auto& failedDisk : failedDisks) {
        failedDiskNames.insert(failedDisk.DeviceName);
    }

    std::vector<TGuid> locationsForDecommission;

    for (const auto& location : ChunkStore_->Locations()) {
        if (failedDiskNames.contains(location->GetStaticConfig()->DeviceName) &&
            locationUuids.contains(location->GetUuid()) &&
            location->IsEnabled())
        {
            // Manual location disable if location placed on failed disk.
            location->Disable(TError("Disk of chunk location is pending decommission")
                << TErrorAttribute("location_uuid", location->GetUuid())
                << TErrorAttribute("location_path", location->GetPath())
                << TErrorAttribute("location_disk", location->GetStaticConfig()->DeviceName));
            locationsForDecommission.push_back(location->GetUuid());
        }
    }

    return locationsForDecommission;
}

TFuture<std::vector<TGuid>> TLocationManager::DisableChunkLocations(const THashSet<TGuid>& locationUuids)
{
    return DiskInfoProvider_->GetYtDiskInfos(NContainers::EDiskState::Failed)
        .Apply(BIND([=] (const std::vector<TDiskInfo>& failedDisks) {
            return DisableChunkLocations(failedDisks, locationUuids);
        })
        .AsyncVia(ControlInvoker_));
}

TFuture<std::vector<TErrorOr<void>>> TLocationManager::RecoverDisks(const THashSet<TString>& diskIds)
{
    return DiskInfoProvider_->RecoverDisks(diskIds);
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
    auto livelinessInfosOrError = WaitFor(LocationManager_->GetLocationsLiveliness());

    // fast path
    if (!livelinessInfosOrError.IsOK()) {
        YT_LOG_ERROR(livelinessInfosOrError, "Failed to list location livelinesses");
        return;
    }

    const auto& livelinessInfos = livelinessInfosOrError.Value();

    THashSet<TString> allDisks;
    THashSet<TString> diskWithLivelinessLocations;
    THashSet<TString> diskWithDisabledLocations;

    for (const auto& livelinessInfo : livelinessInfos) {
        const auto& location = livelinessInfo.Location;
        allDisks.insert(livelinessInfo.DiskId);

        if (livelinessInfo.IsDiskAlive) {
            diskWithLivelinessLocations.insert(livelinessInfo.DiskId);
            location->MarkLocationDiskHealthy();
        } else {
            location->MarkLocationDiskFailed();

            if (livelinessInfo.IsDisabled) {
                diskWithDisabledLocations.insert(livelinessInfo.DiskId);
            }
        }
    }

    THashSet<TString> disksForDecommission;

    for (const auto& disk : allDisks) {
        // all locations on disk must be decommissed
        if (!diskWithLivelinessLocations.contains(disk) &&
            diskWithDisabledLocations.contains(disk))
        {
            disksForDecommission.insert(disk);
        }
    }

    // fast path
    if (disksForDecommission.empty()) {
        return;
    }

    auto resultOrErrors = WaitFor(LocationManager_->RecoverDisks(disksForDecommission));

    if (resultOrErrors.IsOK()) {
        for (const auto& resultOrError : resultOrErrors.Value()) {
            if (!resultOrError.IsOK()) {
                YT_LOG_ERROR(resultOrError, "Failed to send request to recover disk");
            }
        }
    } else {
        YT_LOG_ERROR(resultOrErrors, "Failed to send requests to recover disks");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
