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

std::vector<TStoreLocationPtr> TLocationManager::GetDiskLocations(
    const std::vector<TDiskInfo>& disks)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Fast path.
    if (disks.empty()) {
        return std::vector<TStoreLocationPtr>();
    }

    THashSet<TString> diskNames;

    for (const auto& failedDisk : disks) {
        diskNames.insert(failedDisk.DeviceName);
    }

    std::vector<TStoreLocationPtr> locations;

    for (const auto& location : ChunkStore_->Locations()) {
        if (diskNames.contains(location->GetStaticConfig()->DeviceName)) {
            locations.push_back(location);
        }
    }

    return locations;
}

TFuture<std::vector<TStoreLocationPtr>> TLocationManager::GetFailedLocations()
{
    return DiskInfoProvider_->GetFailedYtDisks()
        .Apply(BIND(&TLocationManager::GetDiskLocations, MakeStrong(this))
            .AsyncVia(ControlInvoker_));
}

////////////////////////////////////////////////////////////////////////////////

TLocationHealthChecker::TLocationHealthChecker(
    TLocationManagerPtr locationManager,
    IInvokerPtr invoker,
    TLocationHealthCheckerConfigPtr config)
    : Config_(std::move(config))
    , Invoker_(std::move(invoker))
    , LocationManager_(std::move(locationManager))
    , HealthCheckerExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TLocationHealthChecker::OnHealthCheck, MakeWeak(this)),
        Config_->HealthCheckPeriod))
{ }

void TLocationHealthChecker::Start()
{
    if (Enabled_.load()) {
        YT_LOG_DEBUG("Starting location health checker");
        HealthCheckerExecutor_->Start();
    }
}

void TLocationHealthChecker::OnDynamicConfigChanged(const TLocationHealthCheckerDynamicConfigPtr& newConfig)
{
    auto oldEnabled = Enabled_.load();
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

    Enabled_.store(newEnabled);
}

void TLocationHealthChecker::OnHealthCheck()
{
    auto locationsOrError = WaitFor(LocationManager_->GetFailedLocations());

    if (locationsOrError.IsOK()) {
        const auto& failedLocations = locationsOrError.Value();

        // TODO(don-dron): Add alerting for node (push failed locations to alerts).
        for (const auto& failedLocation : failedLocations) {
            YT_LOG_WARNING("Disk with store location failed (LocationId: %v, DiskName: %v)",
                failedLocation->GetId(),
                failedLocation->GetStaticConfig()->DeviceName);
        }
    } else {
        YT_LOG_ERROR(locationsOrError, "Failed to list failed locations");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
