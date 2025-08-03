#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

class TJobReport;

DECLARE_REFCOUNTED_STRUCT(TServerBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TNativeServerBootstrapConfig)

DECLARE_REFCOUNTED_STRUCT(TServerProgramConfig)

DECLARE_REFCOUNTED_STRUCT(TJobReporterConfig)
DECLARE_REFCOUNTED_CLASS(TJobReporter)

DECLARE_REFCOUNTED_CLASS(TDiskHealthChecker)
DECLARE_REFCOUNTED_STRUCT(TDiskHealthCheckerConfig)
DECLARE_REFCOUNTED_STRUCT(TDiskHealthCheckerDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(TDiskLocationConfig)
DECLARE_REFCOUNTED_STRUCT(TDiskLocationDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(TFormatConfigBase)
DECLARE_REFCOUNTED_STRUCT(TFormatConfig)
DECLARE_REFCOUNTED_CLASS(TFormatManager)

DECLARE_REFCOUNTED_STRUCT(THeapProfilerTestingOptions)
DECLARE_REFCOUNTED_CLASS(THeapUsageProfiler)

DECLARE_REFCOUNTED_STRUCT(IDaemonBootstrap)

class TServiceProfilerGuard;

DECLARE_REFCOUNTED_CLASS(TForkCounters)

////////////////////////////////////////////////////////////////////////////////

extern const TString ExecProgramName;
extern const TString JobProxyProgramName;

////////////////////////////////////////////////////////////////////////////////

extern const TString BanMessageAttributeName;
extern const TString ConfigAttributeName;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TArchiveReporterConfig)
DECLARE_REFCOUNTED_STRUCT(TArchiveHandlerConfig)
DECLARE_REFCOUNTED_CLASS(TArchiveVersionHolder)
DECLARE_REFCOUNTED_CLASS(TRestartManager)

DECLARE_REFCOUNTED_STRUCT(IArchiveReporter)

struct IArchiveRowlet;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TClusterThrottlersConfig)
DECLARE_REFCOUNTED_STRUCT(TClusterLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
