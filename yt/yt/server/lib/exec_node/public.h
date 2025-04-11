#pragma once

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/ytlib/exec_node/public.h>

#include <yt/yt/core/misc/error_code.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

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

DEFINE_ENUM(EJobProxyLoggingMode,
    (Simple)
    (PerJobDirectory)
);

DEFINE_ENUM(EExecNodeThrottlerKind,
    //! Controls incoming bandwidth used by Artifact Cache downloads.
    (ArtifactCacheIn)
    //! Controls incoming bandwidth consumed by local jobs.
    (JobIn)
    //! Controls outcoming bandwidth consumed by local jobs.
    (JobOut)
);

DEFINE_ENUM(EThrottlerTrafficType,
    (Bandwidth)
    (Rps)
);

////////////////////////////////////////////////////////////////////////////////

extern const TEnumIndexedArray<ESandboxKind, TString> SandboxDirectoryNames;
extern const TString EmptyCpuSet;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSlotLocationConfig)
DECLARE_REFCOUNTED_STRUCT(TNumaNodeConfig)
DECLARE_REFCOUNTED_STRUCT(TSlotManagerTestingConfig)
DECLARE_REFCOUNTED_STRUCT(TSlotManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TSlotManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TChunkCacheDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TVolumeManagerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TUserJobSensor)
DECLARE_REFCOUNTED_CLASS(TUserJobStatisticSensor)
DECLARE_REFCOUNTED_STRUCT(TUserJobMonitoringDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TControllerAgentConnectorDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TMasterConnectorDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TSchedulerConnectorDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TGpuManagerTestingConfig)
DECLARE_REFCOUNTED_STRUCT(TGpuManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TGpuManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TShellCommandConfig)
DECLARE_REFCOUNTED_STRUCT(TTestingConfig)
DECLARE_REFCOUNTED_STRUCT(TJobProbeConfig)
DECLARE_REFCOUNTED_STRUCT(TJobCommonConfig)
DECLARE_REFCOUNTED_STRUCT(TAllocationConfig)
DECLARE_REFCOUNTED_STRUCT(TJobControllerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TJobInputCacheDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TNbdConfig)
DECLARE_REFCOUNTED_STRUCT(TNbdClientConfig)
DECLARE_REFCOUNTED_STRUCT(TJobProxyLoggingConfig)
DECLARE_REFCOUNTED_STRUCT(TJobProxyConfig)
DECLARE_REFCOUNTED_STRUCT(TLogDumpConfig)
DECLARE_REFCOUNTED_STRUCT(TJobProxyLogManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TJobProxyLogManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TExecNodeConfig)
DECLARE_REFCOUNTED_STRUCT(TExecNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
