#pragma once

#include <yt/yt/ytlib/controller_agent/public.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobEnvironmentType,
    (Base)
    (Simple)
    (Porto)
    (Testing)
    (Cri)
);

DEFINE_ENUM(EJobProxyExitCode,
    ((SupervisorCommunicationFailed) (20))
    ((ResultReportFailed)            (21))
    ((ResourcesUpdateFailed)         (22))
    ((SetRLimitFailed)               (23))
    ((ExecFailed)                    (24))
    ((UncaughtException)             (25))
    ((GetJobSpecFailed)              (26))
    ((JobProxyPrepareFailed)         (27))
    ((InvalidSpecVersion)            (28))
    ((ResourceOverdraft)             (29))
    ((PortoManagementFailed)         (30))
);

DEFINE_ENUM(EJobThrottlerType,
    (InBandwidth)
    (OutBandwidth)
    (OutRps)
    // This throttler limits total rate of user job container creation on the node.
    // It eliminates system overload due to extensive use of MTN-enabled user job containers.
    (ContainerCreation)
);

DECLARE_REFCOUNTED_STRUCT(TCoreWatcherConfig)
DECLARE_REFCOUNTED_STRUCT(TTmpfsManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TMemoryTrackerConfig)
DECLARE_REFCOUNTED_STRUCT(TUserJobNetworkAddress)
DECLARE_REFCOUNTED_STRUCT(TJobProxyInternalConfig)
DECLARE_REFCOUNTED_STRUCT(TJobProxyTestingConfig)
DECLARE_REFCOUNTED_STRUCT(TJobProxyDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TJobThrottlerConfig)
DECLARE_REFCOUNTED_STRUCT(TBindConfig)
DECLARE_REFCOUNTED_STRUCT(TJobTraceEventProcessorConfig)
DECLARE_REFCOUNTED_STRUCT(TJobThrashingDetectorConfig)
DECLARE_REFCOUNTED_STRUCT(TJobEnvironmentConfigBase)
DECLARE_REFCOUNTED_STRUCT(TSimpleJobEnvironmentConfig)
DECLARE_REFCOUNTED_STRUCT(TTestingJobEnvironmentConfig)
DECLARE_REFCOUNTED_STRUCT(TPortoJobEnvironmentConfig)
DECLARE_REFCOUNTED_STRUCT(TCriJobEnvironmentConfig)

DECLARE_REFCOUNTED_STRUCT(IJobProbe)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
