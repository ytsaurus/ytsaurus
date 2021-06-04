#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/yson_serializable.h>

#include <optional>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPerfEvents,
    (CpuCycles)
    (Instructions)
    (CacheReferences)
    (CacheMisses)
    (BranchInstructions)
    (BranchMisses)
    (BusCycles)
    (StalledCyclesFrontend)
    (StalledCyclesBackend)
    (RefCpuCycles)
    (CpuClock)
    (TaskClock)
    (ContextSwitches)
    (CpuMigrations)
    (fAlignmentFaults)
    (EmulationFaults)
    (DataTlbReferences)
    (DataTlbMisses)
    (InstructionTlbReferences)
    (InstructionTlbMisses)
    (LocalMemoryReferences)
    (LocalMemoryMisses)
);

class TProfileManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MaxKeepInterval;
    TDuration DequeuePeriod;
    TDuration SampleRateLimit;

    THashMap<TString, TString> GlobalTags;
    THashSet<EPerfEvents> EnabledPerfEvents;

    TProfileManagerConfig();
};

class TProfileManagerDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<THashSet<EPerfEvents>> EnabledPerfEvents;

    TProfileManagerDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TProfileManagerConfig);
DEFINE_REFCOUNTED_TYPE(TProfileManagerDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

