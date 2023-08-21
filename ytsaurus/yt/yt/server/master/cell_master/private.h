#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger CellMasterLogger("Master");
inline const NProfiling::TProfiler CellMasterProfiler("/master");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
