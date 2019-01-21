#pragma once

#include <yt/core/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TServerConfig)

DECLARE_REFCOUNTED_CLASS(TDiskHealthChecker)
DECLARE_REFCOUNTED_CLASS(TDiskHealthCheckerConfig)

DECLARE_REFCOUNTED_CLASS(TDiskLocationConfig)

////////////////////////////////////////////////////////////////////////////////

extern const TString ClusterMasterProgramName;
extern const TString ClusterNodeProgramName;
extern const TString ClusterSchedulerProgramName;
extern const TString ExecProgramName;
extern const TString JobProxyProgramName;

////////////////////////////////////////////////////////////////////////////////

extern const TString BanMessageAttributeName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
