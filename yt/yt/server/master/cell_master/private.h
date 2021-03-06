#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger CellMasterLogger;
extern const NProfiling::TProfiler CellMasterProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
