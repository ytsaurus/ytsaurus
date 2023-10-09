#include "hydra_janitor_helpers.h"
#include "config.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

int ComputeJanitorThresholdId(
    std::vector<THydraFileInfo> files,
    std::optional<int> maxCountToKeep,
    std::optional<i64> maxSizeToKeep)
{
    if (files.empty()) {
        return 0;
    }

    std::sort(files.begin(), files.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.Id < rhs.Id;
    });

    int thresholdIdByCount = 0;
    if (maxCountToKeep && std::ssize(files) > *maxCountToKeep) {
        auto index = files.size() - std::max(1, *maxCountToKeep);
        thresholdIdByCount = files[index].Id;
    }

    int thresholdIdBySize = 0;
    if (maxSizeToKeep) {
        i64 totalSize = 0;
        auto it = files.rbegin();
        for (; it != files.rend(); ++it) {
            const auto& file = *it;
            if (totalSize == 0 || totalSize + file.Size <= *maxSizeToKeep) {
                totalSize += file.Size;
                thresholdIdBySize = file.Id;
            } else {
                break;
            }
        }
        if (it == files.rend()) {
            thresholdIdBySize = 0;
        }
    }

    return std::max(thresholdIdByCount, thresholdIdBySize);
}

int ComputeJanitorThresholdId(
    const std::vector<THydraFileInfo>& snapshots,
    const std::vector<THydraFileInfo>& changelogs,
    const THydraJanitorConfigPtr& config)
{
    int snapshotThresholdId = snapshots.empty()
        ? 0
        : ComputeJanitorThresholdId(
            snapshots,
            config->MaxSnapshotCountToKeep,
            config->MaxSnapshotSizeToKeep);

    int maxSnapshotId = 0;
    int minSnapshotId = snapshots.empty() ? 0 : std::numeric_limits<int>::max();
    for (const auto& snapshot : snapshots) {
        minSnapshotId = std::min(minSnapshotId, snapshot.Id);
        maxSnapshotId = std::max(maxSnapshotId, snapshot.Id);
    }
    YT_VERIFY(minSnapshotId <= maxSnapshotId);

    int changelogThresholdId = std::min(
        ComputeJanitorThresholdId(
            changelogs,
            config->MaxChangelogCountToKeep,
            config->MaxChangelogSizeToKeep),
        maxSnapshotId);

    // Zeroth changelog is vital for recovery.
    // Do not delete it unless config dictates otherwise.
    auto it = std::find_if(
        changelogs.begin(),
        changelogs.end(),
        [] (const THydraFileInfo& file) {
            return file.Id == 0;
        });

    if (it != changelogs.end() && changelogThresholdId == 0) {
        minSnapshotId = 0;
    }

    int thresholdId = std::max(
        std::max(snapshotThresholdId, changelogThresholdId),
        minSnapshotId);

    // Sanity checks, please take them very seriously.
    // Deleting wrong changelogs and snapshots may cause unrecoverable data loss.
    if (snapshots.empty()) {
        // No snapshots known: one must delete nothing.
        YT_VERIFY(thresholdId == 0);
    } else {
        // Some snapshot exists: one must not delete anything past the latest snapshot.
        YT_VERIFY(thresholdId <= maxSnapshotId);
    }

    return thresholdId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
