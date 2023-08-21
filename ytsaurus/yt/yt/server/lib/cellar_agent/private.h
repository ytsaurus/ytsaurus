#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger CellarAgentLogger("CellarAgent");
inline const NProfiling::TProfiler CellarAgentProfiler("/cellar_agent");

inline const TString TabletCellCypressPrefix("//sys/tablet_cells");
inline const TString ChaosCellCypressPrefix("//sys/chaos_cells");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
