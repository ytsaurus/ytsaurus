#pragma once

#include <vector>

#include <yt/core/misc/optional.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotInfo
{
    int Id;
    i64 Size;
};

////////////////////////////////////////////////////////////////////////////////

//! All snapshots and changelogs with id less than or equal to this threshold
//! could be safely deleted.
int GetSnapshotThresholdId(
    std::vector<TSnapshotInfo> snapshots,
    std::optional<int> maxSnapshotCountToKeep,
    std::optional<i64> maxSnapshotSizeToKeep);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
