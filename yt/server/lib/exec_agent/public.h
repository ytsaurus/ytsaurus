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

DEFINE_ENUM(ESandboxKind,
    (User)
    (Udf)
    (Home)
    (Pipes)
    (Tmp)
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

extern const TEnumIndexedVector<TString, ESandboxKind> SandboxDirectoryNames;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
