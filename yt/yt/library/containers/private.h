#pragma once

#include <yt/yt/core/logging/log.h>

#include <util/generic/strbuf.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ContainersLogger, "Containers");

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

// Extracts the current process's cgroup path within the pids hierarchy from
// Porto's "cgroups" property. Returns an empty view if no matching entry is present.
TStringBuf ParsePortoPidsCGroup(TStringBuf portoCGroups, bool isV2);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
