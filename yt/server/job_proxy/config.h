#pragma once

#include "public.h"

#include <yt/server/exec_agent/config.h>

#include <yt/server/misc/config.h>

#include <yt/ytlib/cgroup/config.h>

#include <yt/client/file_client/config.h>

#include <yt/ytlib/hydra/config.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/core/bus/tcp/config.h>

#include <yt/core/net/address.h>

#include <yt/core/ytree/node.h>
#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobThrottlerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MinBackoffTime;
    TDuration MaxBackoffTime;
    double BackoffMultiplier;

    TDuration RpcTimeout;

    TJobThrottlerConfig()
    {
        RegisterParameter("min_backoff_time", MinBackoffTime)
            .Default(TDuration::MilliSeconds(100));

        RegisterParameter("max_backoff_time", MaxBackoffTime)
            .Default(TDuration::Seconds(60));

        RegisterParameter("backoff_multiplier", BackoffMultiplier)
            .Default(1.5);

        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TJobThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobCpuMonitorConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableCpuReclaiming;

    TDuration CheckPeriod;

    double SmoothingFactor;

    double RelativeUpperBound;
    double RelativeLowerBound;

    double IncreaseCoefficient;
    double DecreaseCoefficient;

    int VoteWindowSize;
    int VoteDecisionThreshold;

    double MinCpuLimit;

    TJobCpuMonitorConfig()
    {
        RegisterParameter("check_period", CheckPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("smoothing_factor", SmoothingFactor)
            .Default(0.05);

        RegisterParameter("enable_cpu_reclaiming", EnableCpuReclaiming)
            .Default(false);

        RegisterParameter("relative_upper_bound", RelativeUpperBound)
            .Default(0.9);

        RegisterParameter("relative_lower_bound", RelativeLowerBound)
            .Default(0.6);

        RegisterParameter("increase_coefficient", IncreaseCoefficient)
            .Default(1.15);

        RegisterParameter("decrease_coefficient", DecreaseCoefficient)
            .Default(0.85);

        RegisterParameter("vote_window_size", VoteWindowSize)
            .Default(30);

        RegisterParameter("vote_decision_threshold", VoteDecisionThreshold)
            .Default(15);

        RegisterParameter("min_cpu_limit", MinCpuLimit)
            .Default(0.1);
    }
};

DEFINE_REFCOUNTED_TYPE(TJobCpuMonitorConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobProxyConfig
    : public TServerConfig
{
public:
    // Job-specific parameters.
    int SlotIndex = -1;

    TNullable<TString> TmpfsPath;
    std::vector<NExecAgent::TBindConfigPtr> Binds;

    std::vector<TString> GpuDevices;

    //! Path for container root.
    TNullable<TString> RootPath;

    // Job-independent parameters.
    NApi::NNative::TConnectionConfigPtr ClusterConnection;

    NBus::TTcpBusClientConfigPtr SupervisorConnection;
    TDuration SupervisorRpcTimeout;

    TDuration HeartbeatPeriod;
    TDuration InputPipeBlinkerPeriod;

    NYTree::INodePtr JobEnvironment;

    //! Addresses derived from node local descriptor to leverage locality.
    NNodeTrackerClient::TAddressMap Addresses;
    TNullable<TString> Rack;
    TNullable<TString> DataCenter;

    TDuration CoreForwarderTimeout;

    i64 AheadMemoryReserve;

    bool TestRootFS;

    TJobThrottlerConfigPtr JobThrottler;

    TJobCpuMonitorConfigPtr JobCpuMonitor;

    TJobProxyConfig()
    {
        RegisterParameter("slot_index", SlotIndex);

        RegisterParameter("tmpfs_path", TmpfsPath)
            .Default();

        RegisterParameter("root_path", RootPath)
            .Default();

        RegisterParameter("binds", Binds)
            .Default();

        RegisterParameter("gpu_devices", GpuDevices)
            .Default();

        RegisterParameter("cluster_connection", ClusterConnection);

        RegisterParameter("supervisor_connection", SupervisorConnection);

        RegisterParameter("supervisor_rpc_timeout", SupervisorRpcTimeout)
            .Default(TDuration::Seconds(30));

        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));

        RegisterParameter("input_pipe_blinker_period", InputPipeBlinkerPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("job_environment", JobEnvironment);

        RegisterParameter("addresses", Addresses)
            .Default();

        RegisterParameter("rack", Rack)
            .Default(Null);

        RegisterParameter("data_center", DataCenter)
            .Default(Null);

        RegisterParameter("core_forwarder_timeout", CoreForwarderTimeout)
            .Default();

        RegisterParameter("ahead_memory_reserve", AheadMemoryReserve)
            .Default(100_MB);

        RegisterParameter("test_root_fs", TestRootFS)
            .Default(false);

        RegisterParameter("job_throttler", JobThrottler)
            .Default(nullptr);

        RegisterParameter("job_cpu_monitor", JobCpuMonitor)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TJobProxyConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
