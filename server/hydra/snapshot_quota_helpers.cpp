#include "snapshot_quota_helpers.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TNullable<int> ChooseThreshold(TNullable<int> firstThreshold, TNullable<int> secondThreshold)
{
    if (!secondThreshold) {
        return firstThreshold;
    }
    if (!firstThreshold) {
        return secondThreshold;
    }
    return TNullable<int>(std::max(*firstThreshold, *secondThreshold));
}

TNullable<int> GetSnapshotThresholdId(
    const std::vector<TSnapshotInfo>& snapshots,
    TNullable<int> maxSnapshotCountToKeep,
    TNullable<i64> maxSnapshotSizeToKeep)
{
    i64 totalSize = 0;
    for (const auto& snapshot : snapshots) {
        totalSize += snapshot.Size;
    }

    TNullable<int> thresholdByCountId;
    if (maxSnapshotCountToKeep && snapshots.size() > *maxSnapshotCountToKeep) {
        auto index = snapshots.size() - *maxSnapshotCountToKeep - 1;
        thresholdByCountId = snapshots[index].Id;
    }
    TNullable<int> thresholdBySizeId;
    if (maxSnapshotSizeToKeep && totalSize > *maxSnapshotSizeToKeep) {
        for (const auto &snapshot : snapshots) {
            totalSize -= snapshot.Size;
            thresholdBySizeId = snapshot.Id;
            if (totalSize <= *maxSnapshotSizeToKeep) {
                break;
            }
        }
    }

    return ChooseThreshold(thresholdByCountId, thresholdBySizeId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
