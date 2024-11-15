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

DECLARE_REFCOUNTED_CLASS(TCoreWatcherConfig)
DECLARE_REFCOUNTED_CLASS(TTmpfsManagerConfig)
DECLARE_REFCOUNTED_CLASS(TMemoryTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TUserJobNetworkAddress)
DECLARE_REFCOUNTED_CLASS(TJobProxyInternalConfig)
DECLARE_REFCOUNTED_CLASS(TJobProxyTestingConfig)
DECLARE_REFCOUNTED_CLASS(TJobProxyDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TJobThrottlerConfig)
DECLARE_REFCOUNTED_CLASS(TBindConfig)
DECLARE_REFCOUNTED_CLASS(TJobTraceEventProcessorConfig)
DECLARE_REFCOUNTED_CLASS(TJobThrashingDetectorConfig)
DECLARE_REFCOUNTED_CLASS(TJobEnvironmentConfigBase)
DECLARE_REFCOUNTED_CLASS(TSimpleJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TTestingJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TPortoJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TCriJobEnvironmentConfig)

DECLARE_REFCOUNTED_STRUCT(IJobProbe)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
