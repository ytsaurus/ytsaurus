#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchedulerConnector)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJob)
DECLARE_REFCOUNTED_CLASS(TAllocation)

DECLARE_REFCOUNTED_STRUCT(TSchedulerHeartbeatContext)

DECLARE_REFCOUNTED_STRUCT(TAgentHeartbeatContext)

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

struct TShellCommandOutput
{
    TString Stdout;
    TString Stderr;
};

////////////////////////////////////////////////////////////////////////////////

struct TNetworkAttributes
{
    ui32 ProjectId;
    TString HostName;
    std::vector<NJobProxy::TUserJobNetworkAddressPtr> Addresses;
};

////////////////////////////////////////////////////////////////////////////////

struct TGpuCheckOptions
{
    TString BinaryPath;
    std::vector<TString> BinaryArgs;
    std::optional<TNetworkAttributes> NetworkAttributes;
    THashMap<TString, TString> Environment;
    std::vector<NContainers::TDevice> Devices;
    std::vector<TShellCommandConfigPtr> SetupCommands;
    std::optional<TString> InfinibandCluster;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
