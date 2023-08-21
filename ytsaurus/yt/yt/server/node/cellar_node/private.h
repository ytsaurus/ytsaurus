#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger CellarNodeLogger("CellarNode");
inline const NProfiling::TProfiler CellarNodeProfiler("/cellar_node");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
