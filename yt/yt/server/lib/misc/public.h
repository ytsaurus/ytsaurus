#pragma once

#include <yt/core/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TServerConfig)

DECLARE_REFCOUNTED_CLASS(TDiskHealthChecker)
DECLARE_REFCOUNTED_CLASS(TDiskHealthCheckerConfig)

DECLARE_REFCOUNTED_CLASS(TDiskLocationConfig)

DECLARE_REFCOUNTED_CLASS(TFormatConfigBase)
DECLARE_REFCOUNTED_CLASS(TFormatConfig)
DECLARE_REFCOUNTED_CLASS(TFormatManager)

class TServiceProfilerGuard;

////////////////////////////////////////////////////////////////////////////////

extern const TString ExecProgramName;
extern const TString JobProxyProgramName;

////////////////////////////////////////////////////////////////////////////////

extern const TString BanMessageAttributeName;
extern const TString ConfigAttributeName;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TArchiveReporterConfig)
DECLARE_REFCOUNTED_CLASS(TArchiveVersionHolder)
DECLARE_REFCOUNTED_CLASS(TArchiveReporter)

class IArchiveRowlet;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
