#pragma once

#include "public.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Describes either a snapshots or a changelog residing in persistent store.
struct THydraFileInfo
{
    int Id;
    i64 Size;
};

//! All files with id strictly less than this threshold could be safely deleted.
int ComputeJanitorThresholdId(
    std::vector<THydraFileInfo> files,
    std::optional<int> maxCountToKeep,
    std::optional<i64> maxSizeToKeep);

//! All snapshots and changelogs with id strictly less than this threshold
//! could be safely deleted.
int ComputeJanitorThresholdId(
    const std::vector<THydraFileInfo>& snapshots,
    const std::vector<THydraFileInfo>& changelogs,
    const THydraJanitorConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
