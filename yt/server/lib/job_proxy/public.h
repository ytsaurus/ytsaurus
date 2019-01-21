#pragma once

#include <yt/ytlib/scheduler/public.h>

#include <yt/core/misc/enum.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJobProxyConfig)
DECLARE_REFCOUNTED_CLASS(TJobThrottlerConfig)
DECLARE_REFCOUNTED_CLASS(TJobSatelliteConnectionConfig)

DEFINE_ENUM(EJobProxyExitCode,
    ((HeartbeatFailed)        (20))
    ((ResultReportFailed)     (21))
    ((ResourcesUpdateFailed)  (22))
    ((SetRLimitFailed)        (23))
    ((ExecFailed)             (24))
    ((UncaughtException)      (25))
    ((GetJobSpecFailed)       (26))
    ((JobProxyPrepareFailed)  (27))
    ((InvalidSpecVersion)     (28))
    ((ResourceOverdraft)      (29))
    ((PortoManagmentFailed)   (30))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
