#pragma once

#include "public.h"

#include <yt/core/profiling/profiler.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

struct TGpuInfo
{
    TInstant UpdateTime;
    int Index = -1;
    double UtilizationGpuRate = 0.0;
    double UtilizationMemoryRate = 0.0;
    i64 MemoryUsed = 0;
    TString Name;
};

std::vector<TGpuInfo> GetGpuInfos(TDuration checkTimeout);

struct TGpuDeviceDescriptor
{
    TString DeviceName;
    int DeviceNumber;
};

std::vector<TGpuDeviceDescriptor> ListGpuDevices();

TString GetGpuDeviceName(int deviceNumber);

void ProfileGpuInfo(NProfiling::TProfiler& profiler, const TGpuInfo& gpuInfo, const NProfiling::TTagIdList& tagIds);

TString GetGpuDriverVersion();

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
