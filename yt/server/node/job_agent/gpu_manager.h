#pragma once

#include "public.h"

#include <yt/server/lib/job_agent/config.h>

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
    TGpuManager(
        NCellNode::TBootstrap* bootstrap,
        TGpuManagerConfigPtr config);

    int GetTotalGpuCount() const;
    int GetFreeGpuCount() const;
    const std::vector<TString>& ListGpuDevices() const;

    using TGpuSlotPtr = std::unique_ptr<TGpuSlot, std::function<void(TGpuSlot*)>>;
    TGpuSlotPtr AcquireGpuSlot();

private:
    NCellNode::TBootstrap* const Bootstrap_;
    const TGpuManagerConfigPtr Config_;

    std::vector<TString> GpuDevices_;
    NConcurrency::TPeriodicExecutorPtr HealthCheckExecutor_;

    TSpinLock SpinLock_;
    THashSet<int> HealthyGpuDeviceNumbers_;
    std::vector<TGpuSlot> FreeSlots_;
    bool Disabled_ = false;

    void OnHealthCheck();
};

DEFINE_REFCOUNTED_TYPE(TGpuManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
