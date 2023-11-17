#pragma once

#include <yt/yt/ytlib/job_tracker_client/public.h>

#include <yt/yt/core/misc/error_code.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::TOperationId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;
using NJobTrackerClient::EJobPhase;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((ConfigCreationFailed)                  (1100))
    ((AbortByScheduler)                      (1101))
    ((ResourceOverdraft)                     (1102))
    ((WaitingJobTimeout)                     (1103))
    ((SlotNotFound)                          (1104))
    ((JobEnvironmentDisabled)                (1105))
    ((JobProxyConnectionFailed)              (1106))
    ((ArtifactCopyingFailed)                 (1107))
    ((NodeDirectoryPreparationFailed)        (1108))
    ((SlotLocationDisabled)                  (1109))
    ((QuotaSettingFailed)                    (1110))
    ((RootVolumePreparationFailed)           (1111))
    ((NotEnoughDiskSpace)                    (1112))
    ((ArtifactDownloadFailed)                (1113))
    ((JobProxyPreparationTimeout)            (1114))
    ((JobPreparationTimeout)                 (1115))
    ((FatalJobPreparationTimeout)            (1116))
    ((JobProxyFailed)                        (1120))
    ((SetupCommandFailed)                    (1121))
    ((GpuLayerNotFetched)                    (1122))
    ((GpuJobWithoutLayers)                   (1123))
    ((TmpfsOverflow)                         (1124))
    ((GpuCheckCommandFailed)                 (1125))
    ((GpuCheckCommandIncorrect)              (1126))
    ((JobProxyUnavailable)                   (1127))
    ((NodeResourceOvercommit)                (1128))
    ((LayerUnpackingFailed)                  (1129))
    ((TmpfsLayerImportFailed)                (1130))
    ((SchedulerJobsDisabled)                 (1131))
    ((DockerImagePullingFailed)              (1132))
);

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

DECLARE_REFCOUNTED_CLASS(TJobThrashingDetectorConfig)
DECLARE_REFCOUNTED_CLASS(TJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TSimpleJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TTestingJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TPortoJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TCriJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TSlotLocationConfig)
DECLARE_REFCOUNTED_CLASS(TNumaNodeConfig)
DECLARE_REFCOUNTED_CLASS(TSlotManagerTestingConfig)
DECLARE_REFCOUNTED_CLASS(TSlotManagerConfig)
DECLARE_REFCOUNTED_CLASS(TSlotManagerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TVolumeManagerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TUserJobSensor)
DECLARE_REFCOUNTED_CLASS(TUserJobMonitoringConfig)
DECLARE_REFCOUNTED_CLASS(TUserJobMonitoringDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TControllerAgentConnectorDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TMasterConnectorDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerConnectorDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TGpuInfoSourceConfig)
DECLARE_REFCOUNTED_CLASS(TGpuManagerTestingConfig)
DECLARE_REFCOUNTED_CLASS(TGpuManagerConfig)
DECLARE_REFCOUNTED_CLASS(TGpuManagerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TShellCommandConfig)
DECLARE_REFCOUNTED_CLASS(TJobControllerConfig)
DECLARE_REFCOUNTED_CLASS(TJobControllerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TNbdConfig)
DECLARE_REFCOUNTED_CLASS(TNbdClientConfig)
DECLARE_REFCOUNTED_CLASS(TExecNodeConfig)
DECLARE_REFCOUNTED_CLASS(TExecNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
