#pragma once

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/event_log/config.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <util/system/user.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): move to -inl.h

template<class TConfig>
TIntrusivePtr<TConfig> LoadConfig(const TString& configFilename)
{
    NYTree::INodePtr configNode;
    try {
        TIFStream configStream(configFilename);
        configNode = NYTree::ConvertToNode(&configStream);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading scheduler simulator configuration") << ex;
    }

    auto config = ConvertTo<TIntrusivePtr<TConfig>>(configNode);

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
    : public NYTree::TYsonStruct
{
public:
    i64 Memory;
    double Cpu;
    int UserSlots;
    int Network;

    REGISTER_YSON_STRUCT(TNodeResourcesConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("memory", &TThis::Memory);
        registrar.Parameter("cpu", &TThis::Cpu);
        registrar.Parameter("network", &TThis::Network);
        registrar.Parameter("user_slots", &TThis::UserSlots);
    }
};

class TNodeGroupConfig
    : public NYTree::TYsonStruct
{
public:
    int Count;
    TNodeResourcesConfigPtr ResourceLimits;
    THashSet<TString> Tags;

    REGISTER_YSON_STRUCT(TNodeGroupConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("count", &TThis::Count);
        registrar.Parameter("resource_limits", &TThis::ResourceLimits);
        registrar.Parameter("tags", &TThis::Tags)
            .Default({});
    }
};

class TRemoteEventLogConfig
    : public NYTree::TYsonStruct
{
public:
    NEventLog::TEventLogManagerConfigPtr EventLogManager;

    std::optional<TString> ConnectionFilename;
    NApi::NNative::TConnectionCompoundConfigPtr Connection;
    TString User;

    REGISTER_YSON_STRUCT(TRemoteEventLogConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("event_log_manager", &TThis::EventLogManager);

        registrar.Parameter("connection_filename", &TThis::ConnectionFilename)
            .Default();
        registrar.Parameter("connection", &TThis::Connection)
            .Default();
        registrar.Parameter("user", &TThis::User)
            .Optional();

        registrar.Postprocessor([] (TThis* config) {
            YT_VERIFY(config->EventLogManager->Path);
            if (config->ConnectionFilename) {
                config->Connection = LoadConfig<NApi::NNative::TConnectionCompoundConfig>(*config->ConnectionFilename);
            }

            if (!config->User) {
                config->User = GetUsername();
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

    int NodeWorkerCount;
    int NodeWorkerThreadCount;

    int NodeShardCount;

    NScheduler::TDelayConfigPtr ScheduleJobDelay;

    bool ShiftOperationsToStart;

    TRemoteEventLogConfigPtr RemoteEventLog;

    REGISTER_YSON_STRUCT(TSchedulerSimulatorConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
            .Default(5000)
            .GreaterThan(0);

        registrar.Parameter("node_groups_file", &TThis::NodeGroupsFilename);
        registrar.Parameter("pool_trees_file", &TThis::PoolTreesFilename)
            .Alias("pools_file");
        registrar.Parameter("operations_stats_file", &TThis::OperationsStatsFilename);
        registrar.Parameter("event_log_file", &TThis::EventLogFilename);
        registrar.Parameter("scheduler_config_file", &TThis::SchedulerConfigFilename);

        registrar.Parameter("enable_full_event_log", &TThis::EnableFullEventLog)
            .Default(false);

        registrar.Parameter("cycles_per_flush", &TThis::CyclesPerFlush)
            .Default(100000)
            .GreaterThan(0);

        registrar.Parameter("node_worker_count", &TThis::NodeWorkerCount)
            .Default(1)
            .GreaterThan(0);
        registrar.Parameter("node_worker_thread_count", &TThis::NodeWorkerThreadCount)
            .Default(1)
            .GreaterThan(0);

        registrar.Parameter("node_shard_count", &TThis::NodeShardCount)
            .Default(1)
            .InRange(1, NScheduler::MaxNodeShardCount);

        registrar.Parameter("schedule_job_delay", &TThis::ScheduleJobDelay)
            .Default();

        registrar.Parameter("shift_operations_to_start", &TThis::ShiftOperationsToStart)
            .Default(false);

        registrar.Parameter("remote_event_log", &TThis::RemoteEventLog)
            .Default();

        registrar.Postprocessor([] (TThis* config) {
            if (config->EnableFullEventLog && !config->RemoteEventLog) {
                THROW_ERROR_EXCEPTION("Full event log cannot be written locally. Please specify \"remote_event_log\" parameter");
            }
        });

        registrar.Postprocessor([] (TThis* config) {
            if (config->ScheduleJobDelay) {
                config->ScheduleJobDelay->Type = NScheduler::EDelayType::Async;
            }
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
