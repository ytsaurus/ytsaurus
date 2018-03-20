#pragma once

#include <yt/server/scheduler/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NSchedulerSimulator {

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
        RegisterParameter("user_slots", UserSlots)
            .GreaterThan(0);
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
    TString PoolsFilename;
    TString OperationsStatsFilename;
    TString EventLogFilename;

    NScheduler::TSchedulerConfigPtr SchedulerConfig;
    std::vector<TNodeGroupConfigPtr> NodeGroups;

    NLogging::TLogConfigPtr Logging;
    bool EnableFullEventLog;

    int CyclesPerFlush;

    TSchedulerSimulatorConfig()
    {
        RegisterParameter("node_groups", NodeGroups);

        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default(5000)
            .GreaterThan(0);

        RegisterParameter("pools_file", PoolsFilename);
        RegisterParameter("operations_stats_file", OperationsStatsFilename);
        RegisterParameter("event_log_file", EventLogFilename);

        RegisterParameter("scheduler", SchedulerConfig)
            .DefaultNew();

        RegisterParameter("logging", Logging)
            .DefaultNew();
        RegisterParameter("enable_full_event_log", EnableFullEventLog)
            .Default(false);

        RegisterParameter("cycles_per_flush", CyclesPerFlush)
            .Default(100000)
            .GreaterThan(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT
