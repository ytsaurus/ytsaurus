#pragma once

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::TOperationId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;
using NJobTrackerClient::EJobPhase;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((ConfigCreationFailed)          (1100))
    ((AbortByScheduler)              (1101))
    ((ResourceOverdraft)             (1102))
    ((WaitingJobTimeout)             (1103))
    ((SlotNotFound)                  (1104))
    ((JobEnvironmentDisabled)        (1105))
    ((JobProxyConnectionFailed)      (1106))
    ((ArtifactCopyingFailed)         (1107))
    ((NodeDirectoryPreparationFailed)(1108))
    ((SlotLocationDisabled)          (1109))
    ((QuotaSettingFailed)            (1110))
    ((RootVolumePreparationFailed)   (1111))
    ((NotEnoughDiskSpace)            (1112))
    ((ArtifactDownloadFailed)        (1113))
    ((JobProxyPreparationTimeout)    (1114))
    ((JobPreparationTimeout)         (1115))
    ((JobProxyFailed)                (1120))
    ((SetupCommandFailed)            (1121))
    ((GpuLayerNotFetched)            (1122))
    ((GpuJobWithoutLayers)           (1123))
);

DEFINE_ENUM(ESandboxKind,
    (User)
    (Udf)
    (Home)
    (Pipes)
    (Tmp)
    (Cores)
);

DEFINE_ENUM(EJobEnvironmentType,
    (Simple)
    (Cgroups)
    (Porto)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSlotLocationConfig)
DECLARE_REFCOUNTED_CLASS(TJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TSimpleJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TCGroupJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TPortoJobEnvironmentConfig)
DECLARE_REFCOUNTED_CLASS(TSlotManagerConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerConnectorConfig)
DECLARE_REFCOUNTED_CLASS(TExecAgentConfig)
DECLARE_REFCOUNTED_CLASS(TBindConfig)

////////////////////////////////////////////////////////////////////////////////

extern const TEnumIndexedVector<ESandboxKind, TString> SandboxDirectoryNames;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
