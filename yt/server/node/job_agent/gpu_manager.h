#pragma once

#include "public.h"

#include <yt/server/lib/job_agent/config.h>
#include <yt/server/lib/job_agent/gpu_helpers.h>

#include <yt/server/node/cell_node/public.h>
#include <yt/server/node/data_node/artifact.h>

#include <yt/client/hydra/public.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

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

struct TGpuStatistics
{
    TInstant LastUpdateTime;
    i64 CumulativeUtilizationGpu = 0;
    i64 CumulativeUtilizationMemory = 0;
    i64 MaxMemoryUsed = 0;
    // Number of microseconds when GPU was busy.
    i64 CumulativeLoad = 0;
};

////////////////////////////////////////////////////////////////////////////////

/*
 * \note
 * Thread affinity: any
 */
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
    THashMap<int, TGpuInfo> GetGpuInfoMap() const;

    using TGpuSlotPtr = std::unique_ptr<TGpuSlot, std::function<void(TGpuSlot*)>>;
    TGpuSlotPtr AcquireGpuSlot();

    std::vector<TShellCommandConfigPtr> GetSetupCommands();
    std::vector<NDataNode::TArtifactKey> GetToppingLayers();
    void VerifyToolkitDriverVersion(const TString& toolkitVersion);

private:
    NCellNode::TBootstrap* const Bootstrap_;
    const TGpuManagerConfigPtr Config_;

    const NConcurrency::TPeriodicExecutorPtr HealthCheckExecutor_;
    const NConcurrency::TPeriodicExecutorPtr FetchDriverLayerExecutor_;

    std::vector<TString> GpuDevices_;

    TSpinLock SpinLock_;
    THashMap<int, TGpuInfo> HealthyGpuInfoMap_;
    std::vector<TGpuSlot> FreeSlots_;
    bool Disabled_ = false;

    NYPath::TYPath DriverLayerPath_;
    NHydra::TRevision DriverLayerRevision_;
    std::optional<NDataNode::TArtifactKey> DriverLayerKey_;
    TString DriverVersionString_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    void OnHealthCheck();
    void OnFetchDriverLayerInfo();
    bool IsDriverLayerMissing() const;
};

DEFINE_REFCOUNTED_TYPE(TGpuManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
