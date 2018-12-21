#pragma once

#include <yt/server/scheduler/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

class TNodeResourcesConfig;
typedef TIntrusivePtr<TNodeResourcesConfig> TNodeResourcesConfigPtr;

class TNodeGroupConfig;
typedef TIntrusivePtr<TNodeGroupConfig> TNodeGroupConfigPtr;

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

class TSchedulerSimulatorConfig
    : public NYTree::TYsonSerializable
{
public:
    int HeartbeatPeriod;
    TString NodeGroupsFilename;
    TString PoolTreesFilename;
    TString OperationsStatsFilename;
    TString EventLogFilename;
    TString SchedulerConfigFilename;

    NLogging::TLogConfigPtr Logging;
    bool EnableFullEventLog;

    int CyclesPerFlush;

    int ThreadCount;

    bool ShiftOperationsToStart;

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

        RegisterParameter("logging", Logging)
            .DefaultNew();
        RegisterParameter("enable_full_event_log", EnableFullEventLog)
            .Default(false);

        RegisterParameter("cycles_per_flush", CyclesPerFlush)
            .Default(100000)
            .GreaterThan(0);

        RegisterParameter("thread_count", ThreadCount)
            .Default(2)
            .GreaterThan(0);

        RegisterParameter("shift_operations_to_start", ShiftOperationsToStart)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
