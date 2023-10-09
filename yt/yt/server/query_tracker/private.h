#pragma once

#include <yt/yt/ytlib/query_tracker_client/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/common.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

using namespace NQueryTrackerClient;
using NQueryTrackerClient::TQueryId;

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger QueryTrackerLogger("QueryTracker");
inline const NProfiling::TProfiler QueryTrackerProfiler = NProfiling::TProfiler("/query_tracker").WithGlobal();

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueryTracker)
DECLARE_REFCOUNTED_CLASS(TQueryTrackerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TQueryTrackerServerConfig)
DECLARE_REFCOUNTED_CLASS(TQueryTrackerServerDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

DECLARE_REFCOUNTED_STRUCT(IQueryHandler)
DECLARE_REFCOUNTED_STRUCT(IQueryEngine)
DECLARE_REFCOUNTED_STRUCT(IQueryTracker)

DECLARE_REFCOUNTED_CLASS(TYqlEngineConfig)
DECLARE_REFCOUNTED_CLASS(TChytEngineConfig)
DECLARE_REFCOUNTED_CLASS(TSpytEngineConfig)
DECLARE_REFCOUNTED_CLASS(TQLEngineConfig)
DECLARE_REFCOUNTED_CLASS(TEngineConfigBase)

////////////////////////////////////////////////////////////////////////////////

using TTrackerId = TString;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
