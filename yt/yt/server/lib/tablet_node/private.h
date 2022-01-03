#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger TabletNodeLogger("TabletNode");
inline const NProfiling::TProfiler TabletNodeProfiler("/tablet_node");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
