#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NReplicatedTableTracker {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger ReplicatedTableTrackerLogger("StandaloneRtt");
inline const NProfiling::TProfiler ReplicatedTableTrackerProfiler("/standalone_rtt");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
