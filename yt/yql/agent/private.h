#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/misc/global.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, YqlAgentLogger, "YqlAgent");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, YqlAgentProfiler, NProfiling::TProfiler("/yql_agent").WithGlobal());

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IYqlAgent)
DECLARE_REFCOUNTED_CLASS(TYqlAgentConfig)
DECLARE_REFCOUNTED_CLASS(TYqlAgentDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TYqlAgentServerConfig)
DECLARE_REFCOUNTED_CLASS(TYqlAgentServerDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

DECLARE_REFCOUNTED_CLASS(TVanillaJobFile)
DECLARE_REFCOUNTED_CLASS(TDQYTBackend)
DECLARE_REFCOUNTED_CLASS(TDQYTCoordinator)
DECLARE_REFCOUNTED_CLASS(TDQManagerConfig)

////////////////////////////////////////////////////////////////////////////////

using TAgentId = TString;

using TQueryId = TGuid;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
