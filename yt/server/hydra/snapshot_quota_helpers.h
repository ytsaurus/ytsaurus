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

TNullable<int> GetSnapshotThresholdId(
    std::vector<TSnapshotInfo> snapshots,
    TNullable<int> maxSnapshotCountToKeep,
    TNullable<i64> maxSnapshotSizeToKeep);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
