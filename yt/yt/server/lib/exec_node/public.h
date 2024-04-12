#pragma once

#include <yt/yt/ytlib/exec_node/public.h>

#include <yt/yt/core/misc/error_code.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::TOperationId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESandboxKind,
    (User)
    (Udf)
    (Home)
    (Pipes)
    (Tmp)
    (Cores)
    (Logs)
);

DEFINE_ENUM(EJobEnvironmentType,
    (Simple)
    (Porto)
    (Testing)
    (Cri)
);

DEFINE_ENUM(EUserJobSensorSource,
    (Gpu)
    (Statistics)
);

////////////////////////////////////////////////////////////////////////////////

extern const TEnumIndexedVector<ESandboxKind, TString> SandboxDirectoryNames;
extern const TString EmptyCpuSet;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSlotLocationConfig)
DECLARE_REFCOUNTED_CLASS(TNumaNodeConfig)
DECLARE_REFCOUNTED_CLASS(TJobThrashingDetectorConfig)
DECLARE_REFCOUNTED_CLASS(TJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TSimpleJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TTestingJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TPortoJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TCriJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TSlotManagerConfig)
DECLARE_REFCOUNTED_CLASS(TSlotManagerTestingConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerConnectorConfig)
DECLARE_REFCOUNTED_CLASS(TControllerAgentConnectorConfig)
DECLARE_REFCOUNTED_CLASS(TMasterConnectorConfig)
DECLARE_REFCOUNTED_CLASS(TExecNodeConfig)
DECLARE_REFCOUNTED_CLASS(TNbdConfig)
DECLARE_REFCOUNTED_CLASS(TNbdClientConfig)
DECLARE_REFCOUNTED_CLASS(TUserJobMonitoringConfig)
DECLARE_REFCOUNTED_CLASS(TChunkCacheDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TUserJobMonitoringDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TMasterConnectorDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerConnectorDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TControllerAgentConnectorDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TSlotManagerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TVolumeManagerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TExecNodeDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TUserJobSensor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
