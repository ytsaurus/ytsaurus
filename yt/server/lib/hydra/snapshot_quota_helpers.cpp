#include "snapshot_quota_helpers.h"

#include <yt/core/misc/assert.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

int GetSnapshotThresholdId(
    std::vector<TSnapshotInfo> snapshots,
    std::optional<int> maxSnapshotCountToKeep,
    std::optional<i64> maxSnapshotSizeToKeep)
{
    std::sort(snapshots.begin(), snapshots.end(), [] (const TSnapshotInfo& lhs, const TSnapshotInfo& rhs) {
        return lhs.Id < rhs.Id;
    });

    int thresholdByCountId = 0;
    if (maxSnapshotCountToKeep && snapshots.size() > *maxSnapshotCountToKeep) {
        auto index = snapshots.size() - std::max(1, *maxSnapshotCountToKeep);
        thresholdByCountId = snapshots[index].Id;
    }

    int thresholdBySizeId = 0;
    if (maxSnapshotSizeToKeep) {
        i64 totalSize = 0;
        for (auto it = snapshots.rbegin(); it != snapshots.rend(); ++it) {
            const auto& snapshot = *it;
            if (totalSize == 0 || totalSize + snapshot.Size <= *maxSnapshotSizeToKeep) {
                totalSize += snapshot.Size;
                thresholdBySizeId = snapshot.Id;
            } else {
                break;
            }
        }
    }

    int thresholdId = std::max(thresholdByCountId, thresholdBySizeId);

    // Make sure we never delete the latest snapshot.
    YT_VERIFY(snapshots.empty() ? thresholdId == 0 : snapshots.back().Id >= thresholdId);
    return thresholdId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
