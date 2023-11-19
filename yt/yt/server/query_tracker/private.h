#pragma once

#include "public.h"

#include <yt/yt/ytlib/query_tracker_client/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/common.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger AlertManagerLogger("AlertManager");
inline const NLogging::TLogger QueryTrackerLogger("QueryTracker");
inline const NProfiling::TProfiler QueryTrackerProfiler = NProfiling::TProfiler("/query_tracker").WithGlobal();

////////////////////////////////////////////////////////////////////////////////

namespace NAlerts {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((QueryTrackerInvalidState)            (40000))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAlerts

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
