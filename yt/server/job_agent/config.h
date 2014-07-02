#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NJobAgent {

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsConfig
    : public NYTree::TYsonSerializable
{
public:
    int UserSlots;
    int Cpu;
    int Network;
    i64 Memory;
    int ReplicationSlots;
    int RemovalSlots;
    int RepairSlots;
    int SealSlots;

    TResourceLimitsConfig()
    {
        // These are some very low default limits.
        // Override for production use.
        RegisterParameter("user_slots", UserSlots)
            .Default(1);
        RegisterParameter("cpu", Cpu)
            .Default(1);
        RegisterParameter("network", Network)
            .Default(100);
        // Very low default, override for production use.
        RegisterParameter("memory", Memory)
            .GreaterThanOrEqual((i64) 1024 * 1024 * 1024)
            .Default((i64)  5 * 1024 * 1024 * 1024);
        RegisterParameter("replication_slots", ReplicationSlots)
            .Default(16);
        RegisterParameter("removal_slots", RemovalSlots)
            .Default(16);
        RegisterParameter("repair_slots", RepairSlots)
            .Default(4);
        RegisterParameter("seal_slots", SealSlots)
            .Default(16);
    }
};


class TJobControllerConfig
    : public NYTree::TYsonSerializable
{
public:
    TResourceLimitsConfigPtr ResourceLimits;

    TJobControllerConfig()
    {
        RegisterParameter("resource_limits", ResourceLimits)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
