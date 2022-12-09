#include "location_manager.h"

#include "private.h"

#include <yt/yt/server/node/data_node/chunk_store.h>

#include <yt/yt/library/containers/disk_manager/disk_info_provider.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

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
    TLocationManagerPtr locationManager)
    : LocationManager_(std::move(locationManager))
{ }

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
