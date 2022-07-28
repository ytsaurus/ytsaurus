#pragma once

#include "public.h"

#include "gpu_info_provider.h"

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

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
