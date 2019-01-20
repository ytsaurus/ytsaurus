#pragma once

#include "public.h"

#include "config.h"

#include <yt/server/node/cell_node/public.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

class TGpuSlot
{
public:
    explicit TGpuSlot(int deviceNumber);
    explicit TGpuSlot(TGpuSlot&& gpuSlot) = default;

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

    TGpuManager(NCellNode::TBootstrap* bootstrap, TGpuManagerConfigPtr config);
    TGpuManager();

    static const std::vector<TString>& GetMetaGpuDeviceNames();
    static TString GetDeviceName(int deviceNumber);

    int GetTotalGpuCount() const;
    int GetFreeGpuCount() const;
    const std::vector<TString>& ListGpuDevices() const;
    TGpuSlotPtr AcquireGpuSlot();

private:
    const NCellNode::TBootstrap* Bootstrap_;
    const TGpuManagerConfigPtr Config_;
    const bool EnableHealthCheck_ = true;
    NConcurrency::TPeriodicExecutorPtr HealthCheckExecutor_;

    THashSet<int> HealthyGpuDeviceNumbers_;

    TSpinLock SpinLock_;
    std::vector<TGpuSlot> FreeSlots_;

    std::vector<TString> GpuDevices_;
    bool Disabled_ = false;

    void OnHealthCheck();
    void Init();
};

DEFINE_REFCOUNTED_TYPE(TGpuManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
