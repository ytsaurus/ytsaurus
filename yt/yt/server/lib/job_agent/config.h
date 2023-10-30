#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/concurrency/config.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsConfig
    : public NYTree::TYsonStruct
{
public:
    int UserSlots;
    double Cpu;
    int Gpu;
    int Network;
    i64 UserMemory;
    i64 SystemMemory;
    int ReplicationSlots;
    i64 ReplicationDataSize;
    i64 MergeDataSize;
    int RemovalSlots;
    int RepairSlots;
    i64 RepairDataSize;
    int SealSlots;
    int MergeSlots;
    int AutotomySlots;
    int ReincarnationSlots;

    REGISTER_YSON_STRUCT(TResourceLimitsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

class TMappedMemoryControllerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration CheckPeriod;
    i64 ReservedMemory;

    REGISTER_YSON_STRUCT(TMappedMemoryControllerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMappedMemoryControllerConfig)

////////////////////////////////////////////////////////////////////////////////

class TMemoryPressureDetectorConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enabled;

    TDuration CheckPeriod;

    // Free memory watermark multiplier will be increased upon reaching this threshold.
    int MajorPageFaultCountThreshold;

    // The value by which free memory watermark multiplier is increased.
    double MemoryWatermarkMultiplierIncreaseStep;

    // Max value of free memory watermark multiplier.
    double MaxMemoryWatermarkMultiplier;

    REGISTER_YSON_STRUCT(TMemoryPressureDetectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryPressureDetectorConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
