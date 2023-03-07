#pragma once

#include <yt/server/lib/scheduler/config.h>

#include <yt/core/ytree/public.h>
#include <yt/core/ytree/yson_serializable.h>

#include <util/system/user.h>

namespace NYT::NSchedulerSimulator {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

template<class TConfig>
TIntrusivePtr<TConfig> LoadConfig(const TString& configFilename)
{
    INodePtr configNode;
    try {
        TIFStream configStream(configFilename);
        configNode = ConvertToNode(&configStream);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading scheduler simulator configuration") << ex;
    }

    auto config = New<TConfig>();
    config->Load(configNode);

    return config;
}

////////////////////////////////////////////////////////////////////////////////

class TNodeResourcesConfig;
typedef TIntrusivePtr<TNodeResourcesConfig> TNodeResourcesConfigPtr;

class TNodeGroupConfig;
typedef TIntrusivePtr<TNodeGroupConfig> TNodeGroupConfigPtr;

class TRemoteEventLogConfig;
typedef TIntrusivePtr<TRemoteEventLogConfig> TRemoteEventLogConfigPtr;

class TSchedulerSimulatorConfig;
typedef TIntrusivePtr<TSchedulerSimulatorConfig> TSchedulerSimulatorConfigPtr;

////////////////////////////////////////////////////////////////////////////////

class TNodeResourcesConfig
    : public NYTree::TYsonSerializable
{
public:
    i64 Memory;
    double Cpu;
    int UserSlots;
    int Network;

    TNodeResourcesConfig()
    {
        RegisterParameter("memory", Memory);
        RegisterParameter("cpu", Cpu);
        RegisterParameter("network", Network);
        RegisterParameter("user_slots", UserSlots);
    }
};

class TNodeGroupConfig
    : public NYTree::TYsonSerializable
{
public:
    int Count;
    TNodeResourcesConfigPtr ResourceLimits;
    THashSet<TString> Tags;

    TNodeGroupConfig()
    {
        RegisterParameter("count", Count);
        RegisterParameter("resource_limits", ResourceLimits);
        RegisterParameter("tags", Tags)
            .Default({});
    }
};

class TRemoteEventLogConfig
    : public NYTree::TYsonSerializable
{
public:
    NEventLog::TEventLogManagerConfigPtr EventLogManager;

    std::optional<TString> ConnectionFilename;
    NApi::NNative::TConnectionConfigPtr Connection;
    TString User;

    TRemoteEventLogConfig()
    {
        RegisterParameter("event_log_manager", EventLogManager);

        RegisterParameter("connection_filename", ConnectionFilename)
            .Default();
        RegisterParameter("connection", Connection)
            .Default(nullptr);
        RegisterParameter("user", User)
            .Optional();

        RegisterPostprocessor([&] {
            YT_VERIFY(EventLogManager->Path);
            if (ConnectionFilename) {
                Connection = LoadConfig<NApi::NNative::TConnectionConfig>(*ConnectionFilename);
            }

            if (!User) {
                User = GetUsername();
            }
        });
    }
};

class TSchedulerSimulatorConfig
    : public TServerConfig
{
public:
    int HeartbeatPeriod;
    TString NodeGroupsFilename;
    TString PoolTreesFilename;
    TString OperationsStatsFilename;
    TString EventLogFilename;
    TString SchedulerConfigFilename;

    bool EnableFullEventLog;

    int CyclesPerFlush;

    int ThreadCount;
    int NodeShardCount;

    std::optional<TDuration> ScheduleJobDelay;

    bool ShiftOperationsToStart;

    TRemoteEventLogConfigPtr RemoteEventLog;

    bool UseClassicScheduler;

    TSchedulerSimulatorConfig()
    {
        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default(5000)
            .GreaterThan(0);

        RegisterParameter("node_groups_file", NodeGroupsFilename);
        RegisterParameter("pool_trees_file", PoolTreesFilename)
            .Alias("pools_file");
        RegisterParameter("operations_stats_file", OperationsStatsFilename);
        RegisterParameter("event_log_file", EventLogFilename);
        RegisterParameter("scheduler_config_file", SchedulerConfigFilename);

        RegisterParameter("enable_full_event_log", EnableFullEventLog)
            .Default(false);

        RegisterParameter("cycles_per_flush", CyclesPerFlush)
            .Default(100000)
            .GreaterThan(0);

        RegisterParameter("thread_count", ThreadCount)
            .Default(1)
            .GreaterThan(0);
        RegisterParameter("node_shard_count", NodeShardCount)
            .Default(1)
            .GreaterThan(0);

        RegisterParameter("schedule_job_delay", ScheduleJobDelay)
            .Default();

        RegisterParameter("shift_operations_to_start", ShiftOperationsToStart)
            .Default(false);

        RegisterParameter("remote_event_log", RemoteEventLog)
            .Default(nullptr);

        RegisterParameter("use_classic_scheduler", UseClassicScheduler)
            .Default(true);

        RegisterPostprocessor([&] () {
            if (EnableFullEventLog && !RemoteEventLog) {
                THROW_ERROR_EXCEPTION("Full event log cannot be written locally. Please specify \"remote_event_log\" parameter");
            }
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
