#pragma once

#include <yt/core/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TServerConfig)

DECLARE_REFCOUNTED_CLASS(TDiskHealthChecker)
DECLARE_REFCOUNTED_CLASS(TDiskHealthCheckerConfig)

DECLARE_REFCOUNTED_CLASS(TDiskLocationConfig)

////////////////////////////////////////////////////////////////////////////////

extern const char* ClusterMasterProgramName;
extern const char* ClusterNodeProgramName;
extern const char* ClusterSchedulerProgramName;
extern const char* ExecProgramName;
extern const char* JobProxyProgramName;

void InitMasterInternedAttributes();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
