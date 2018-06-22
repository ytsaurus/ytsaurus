#pragma once

#include "public.h"

namespace NYT {
namespace NJobAgent {

////////////////////////////////////////////////////////////////////////////////

class TGpuSlot
{
public:
    TGpuSlot(int deviceNumber);
    TGpuSlot(TGpuSlot&& gpuSlot) = default;

    TString GetDeviceName() const;
    int GetDeviceNumber() const;

private:
    int DeviceNumber_;
};

////////////////////////////////////////////////////////////////////////////////

class TGpuManager
    : public TRefCounted
{
public:
    using TGpuSlotPtr = std::unique_ptr<TGpuSlot, std::function<void(TGpuSlot*)>>;

    TGpuManager();

    static const std::vector<TString>& GetMetaGpuDeviceNames();
    static TString GetDeviceName(int deviceNumber);

    int GetTotalGpuCount() const;
    int GetFreeGpuCount() const;
    const std::vector<TString>& ListGpuDevices() const;
    TGpuSlotPtr AcquireGpuSlot();

private:
    int GpuCount_ = 0;
    TSpinLock SpinLock_;
    std::vector<TGpuSlot> FreeSlots_;

    std::vector<TString> GpuDevices_;
};

DEFINE_REFCOUNTED_TYPE(TGpuManager)

////////////////////////////////////////////////////////////////////////////////

}
}