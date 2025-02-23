#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

struct TResourceLimitsConfig
    : public NYTree::TYsonStruct
{
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

struct TMappedMemoryControllerConfig
    : public NYTree::TYsonStruct
{
    TDuration CheckPeriod;
    i64 ReservedMemory;

    REGISTER_YSON_STRUCT(TMappedMemoryControllerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMappedMemoryControllerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMemoryPressureDetectorConfig
    : public NYTree::TYsonStruct
{
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

struct TJobResourceManagerConfig
    : public NYTree::TYsonStruct
{
    //! Port set has higher priority than StartPort ans PortCount if it is specified.
    std::optional<THashSet<int>> PortSet;
    int StartPort;
    int PortCount;

    TResourceLimitsConfigPtr ResourceLimits;

    std::optional<double> CpuToVCpuFactor;
    std::optional<TString> CpuModel;

    REGISTER_YSON_STRUCT(TJobResourceManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobResourceManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobResourceManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<double> CpuToVCpuFactor;
    bool EnableCpuToVCpuFactor;

    std::optional<THashMap<TString, double>> CpuModelToCpuToVCpuFactor;

    TDuration ProfilingPeriod;

    i64 FreeMemoryWatermark;

    TMappedMemoryControllerConfigPtr MappedMemoryController;

    NJobAgent::TMemoryPressureDetectorConfigPtr MemoryPressureDetector;

    REGISTER_YSON_STRUCT(TJobResourceManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobResourceManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
