#pragma once

#include "artifact.h"
#include "public.h"

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/misc/absolute_normalized_path.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

inline const std::string ArtifactMetaSuffix(".artifact");

////////////////////////////////////////////////////////////////////////////////

inline const std::string MountSuffix = "mount";
inline const std::string VolumesName = "volumes";
inline const std::string LayersName = "porto_layers";
inline const std::string LayersMetaName = "layers_meta";
inline const std::string VolumesMetaName = "volumes_meta";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchedulerConnector)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJob)
DECLARE_REFCOUNTED_CLASS(TAllocation)

DECLARE_REFCOUNTED_STRUCT(TSchedulerHeartbeatContext)

DECLARE_REFCOUNTED_STRUCT(TAgentHeartbeatContext)

DECLARE_REFCOUNTED_CLASS(TArtifact)

DECLARE_REFCOUNTED_CLASS(TCacheLocation)

DECLARE_REFCOUNTED_CLASS(TJobFSSecretary)

struct TControllerAgentDescriptor;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ExecNodeLogger, "ExecNode");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, ExecNodeProfiler, "/exec_node");

YT_DEFINE_GLOBAL(const NLogging::TLogger, JobInputCacheLogger, "JobInputCache");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, JobInputCacheProfiler, ExecNodeProfiler().WithPrefix("/job_input_cache"));

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, GpuManagerProfiler, ExecNodeProfiler().WithPrefix("/gpu_manager"));

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, SchedulerConnectorProfiler, ExecNodeProfiler().WithPrefix("/scheduler_connector"));
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, ControllerAgentConnectorProfiler, ExecNodeProfiler().WithPrefix("/controller_agent_connector"));

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, JobEnvironmentProfiler, ExecNodeProfiler().WithPrefix("/job_environment"));

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, HeartbeatOutOfBandAttemptsProfiler, SchedulerConnectorProfiler().WithPrefix("/heartbeat_out_of_band_attempts"));

////////////////////////////////////////////////////////////////////////////////

constexpr int TmpfsRemoveAttemptCount = 5;

////////////////////////////////////////////////////////////////////////////////

struct TShellCommandResult
{
    std::string Stdout;
    std::string Stderr;
    TError Error;
};

////////////////////////////////////////////////////////////////////////////////

struct TNetworkAttributes
{
    ui32 ProjectId;
    std::string HostName;
    std::vector<NJobProxy::TUserJobNetworkAddressPtr> Addresses;
};

////////////////////////////////////////////////////////////////////////////////

struct TGpuCheckOptions
{
    std::string BinaryPath;
    std::vector<std::string> BinaryArgs;
    std::optional<TNetworkAttributes> NetworkAttributes;
    THashMap<std::string, std::string> Environment;
    std::vector<NContainers::TDevice> Devices;
    std::vector<TShellCommandConfigPtr> SetupCommands;
    std::optional<std::string> InfinibandCluster;
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeMount
    : public TRefCounted
{
    std::string VolumeId;
    TAbsoluteNormalizedPath MountPath;
    bool ReadOnly;

    bool operator==(const TVolumeMount& rhs) const;
    bool operator!=(const TVolumeMount& rhs) const;
};

DEFINE_REFCOUNTED_TYPE(TVolumeMount)
DECLARE_REFCOUNTED_STRUCT(TVolumeMount)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
