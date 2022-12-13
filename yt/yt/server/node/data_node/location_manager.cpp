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
    const std::vector<TDiskInfo>& disks)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Fast path.
    if (disks.empty()) {
        return {};
    }

    THashSet<TString> diskNames;

    for (const auto& failedDisk : disks) {
        diskNames.insert(failedDisk.DeviceName);
    }

    std::vector<TLocationLivenessInfo> locationLivelinessInfos;
    const auto& locations = ChunkStore_->Locations();
    locationLivelinessInfos.reserve(locations.size());

    for (const auto& location : locations) {
        locationLivelinessInfos.push_back({
            .Location = location,
            .IsDiskAlive = !diskNames.contains(location->GetStaticConfig()->DeviceName)});
    }

    return locationLivelinessInfos;
}

TFuture<std::vector<TLocationLivenessInfo>> TLocationManager::GetLocationsLiveliness()
{
    return DiskInfoProvider_->GetFailedYtDisks()
        .Apply(BIND(&TLocationManager::MapLocationToLivelinessInfo, MakeStrong(this))
            .AsyncVia(ControlInvoker_));
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

    if (livelinessInfosOrError.IsOK()) {
        const auto& livelinessInfos = livelinessInfosOrError.Value();

        for (const auto& livelinessInfo : livelinessInfos) {
            const auto& location = livelinessInfo.Location;

            if (livelinessInfo.IsDiskAlive && !location->IsLocationDiskOK()) {
                YT_LOG_WARNING("Disk with store location repaired (LocationId: %v, DiskName: %v)",
                    location->GetUuid(),
                    location->GetStaticConfig()->DeviceName);
            } else if (!livelinessInfo.IsDiskAlive && location->IsLocationDiskOK()) {
                YT_LOG_WARNING("Disk with store location failed (LocationId: %v, DiskName: %v)",
                    location->GetUuid(),
                    location->GetStaticConfig()->DeviceName);
            }

            if (livelinessInfo.IsDiskAlive) {
                location->MarkLocationDiskAsOK();
            } else {
                location->MarkLocationDiskAsFailed();
            }
        }
    } else {
        YT_LOG_ERROR(livelinessInfosOrError, "Failed to list location livelinesses");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
