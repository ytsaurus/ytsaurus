#include "snapshot_quota_helpers.h"

#include <yt/core/misc/assert.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

int GetSnapshotThresholdId(
    std::vector<TSnapshotInfo> snapshots,
    std::optional<int> maxSnapshotCountToKeep,
    std::optional<i64> maxSnapshotSizeToKeep)
{
    if (snapshots.size() <= 1) {
        return -1;
    }

    std::sort(snapshots.begin(), snapshots.end(), [] (const TSnapshotInfo& lhs, const TSnapshotInfo& rhs) {
        return lhs.Id < rhs.Id;
    });

    int thresholdByCountId = -1;
    if (maxSnapshotCountToKeep && snapshots.size() > *maxSnapshotCountToKeep) {
        auto index = snapshots.size() - std::max(1, *maxSnapshotCountToKeep) - 1;
        thresholdByCountId = snapshots[index].Id;
    }

    i64 totalSize = 0;
    for (const auto& snapshot : snapshots) {
        totalSize += snapshot.Size;
    }

    int thresholdBySizeId = -1;
    if (maxSnapshotSizeToKeep && totalSize > *maxSnapshotSizeToKeep) {
        for (auto it = snapshots.begin(); it != snapshots.end() - 1; ++it) {
            const auto& snapshot = *it;
            totalSize -= snapshot.Size;
            thresholdBySizeId = snapshot.Id;
            if (totalSize <= *maxSnapshotSizeToKeep) {
                break;
            }
        }
    }

    int thresholdId = std::max(thresholdByCountId, thresholdBySizeId);

    // Make sure we never delete the latest snapshot.
    YCHECK(snapshots.back().Id > thresholdId);
    return thresholdId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
