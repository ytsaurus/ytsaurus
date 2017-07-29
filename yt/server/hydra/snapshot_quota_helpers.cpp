#include "snapshot_quota_helpers.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

namespace {

TNullable<int> ChooseMaxThreshold(TNullable<int> firstThreshold, TNullable<int> secondThreshold)
{
    if (!secondThreshold) {
        return firstThreshold;
    }
    if (!firstThreshold) {
        return secondThreshold;
    }
    return MakeNullable(std::max(*firstThreshold, *secondThreshold));
}

} // namespace

TNullable<int> GetSnapshotThresholdId(
    std::vector<TSnapshotInfo> snapshots,
    TNullable<int> maxSnapshotCountToKeep,
    TNullable<i64> maxSnapshotSizeToKeep)
{
    if (snapshots.size() <= 1) {
        return Null;
    }

    std::sort(snapshots.begin(), snapshots.end(), [] (const TSnapshotInfo& lhs, const TSnapshotInfo& rhs) {
        return lhs.Id < rhs.Id;
    });

    TNullable<int> thresholdByCountId;
    if (maxSnapshotCountToKeep && snapshots.size() > *maxSnapshotCountToKeep) {
        auto index = snapshots.size() - std::max(1, *maxSnapshotCountToKeep) - 1;
        thresholdByCountId = snapshots[index].Id;
    }

    i64 totalSize = 0;
    for (const auto& snapshot : snapshots) {
        totalSize += snapshot.Size;
    }

    TNullable<int> thresholdBySizeId;
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

    return ChooseMaxThreshold(thresholdByCountId, thresholdBySizeId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
