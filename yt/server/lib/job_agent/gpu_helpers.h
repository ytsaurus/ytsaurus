#pragma once

#include "public.h"

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

THashSet<int> GetHealthyGpuDeviceNumbers(TDuration checkTimeout);

struct TGpuDeviceDescriptor
{
    TString DeviceName;
    int DeviceNumber;
};

std::vector<TGpuDeviceDescriptor> ListGpuDevices();

TString GetGpuDeviceName(int deviceNumber);

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
