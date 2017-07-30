#pragma once

#include <vector>

#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NHydra {

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
    TNullable<int> maxSnapshotCountToKeep,
    TNullable<i64> maxSnapshotSizeToKeep);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
