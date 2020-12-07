#pragma once

#include "public.h"

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

struct TGpuInfo
{
    TInstant UpdateTime;
    int Index = -1;
    double UtilizationGpuRate = 0.0;
    double UtilizationMemoryRate = 0.0;
    i64 MemoryUsed = 0;
    i64 MemoryTotal = 0;
    double PowerDraw = 0.0;
    double PowerLimit = 0.0;
    i64 ClocksSm = 0;
    i64 ClocksMaxSm = 0;
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

void ProfileGpuInfo(NProfiling::ISensorWriter* writer, const TGpuInfo& gpuInfo);

struct TGpuDriverVersion
{
    std::vector<int> Components;

    static TGpuDriverVersion FromString(TStringBuf driverVersionString);
};

bool operator < (const TGpuDriverVersion& lhs, const TGpuDriverVersion& rhs);

TString GetGpuDriverVersionString();
TString GetDummyGpuDriverVersionString();

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
