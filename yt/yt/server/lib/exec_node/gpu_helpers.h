#pragma once

#include "public.h"

#include <yt/yt/library/gpu/gpu_info_provider.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TGpuDeviceDescriptor
{
    std::string DeviceName;
    int DeviceIndex;
};

std::vector<TGpuDeviceDescriptor> ListNvidiaGpuDevices();

std::string GetNvidiaGpuDeviceName(int deviceIndex);

void ProfileGpuInfo(NProfiling::ISensorWriter* writer, const NGpu::TGpuInfo& gpuInfo);

struct TGpuDriverVersion
{
    // NB(omgronny): empty vector is a dummy GPU driver version.
    std::vector<int> Components;

    static TGpuDriverVersion FromString(TStringBuf driverVersionString);
};

bool operator<(const TGpuDriverVersion& lhs, const TGpuDriverVersion& rhs);

std::string GetNvidiaGpuDriverVersionString();
std::string GetDummyGpuDriverVersionString();

////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> ListInfinibandDevices();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
