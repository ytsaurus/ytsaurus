#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_agent/config.h>
#include <yt/yt/server/lib/job_agent/gpu_helpers.h>

#include <yt/yt/server/node/cluster_node/public.h>
#include <yt/yt/server/node/data_node/artifact.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NExecNode {

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
    i64 CumulativeMemory = 0;
    i64 CumulativeMemoryMBSec = 0;
    i64 MaxMemoryUsed = 0;
    // Number of microseconds when GPU was busy.
    i64 CumulativeLoad = 0;
    i64 CumulativeUtilizationPower = 0;
    i64 CumulativePower = 0;
    i64 CumulativeUtilizationClocksSm = 0;
    i64 CumulativeSMUtilization = 0;
    i64 CumulativeSMOccupancy = 0;
    i64 MaxStuckDuration = 0;
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
        IBootstrap* bootstrap,
        NJobAgent::TGpuManagerConfigPtr config);

    int GetTotalGpuCount() const;
    int GetFreeGpuCount() const;
    int GetUsedGpuCount() const;
    const std::vector<TString>& GetGpuDevices() const;
    THashMap<int, NJobAgent::TGpuInfo> GetGpuInfoMap() const;

    using TGpuSlotPtr = std::unique_ptr<TGpuSlot, std::function<void(TGpuSlot*)>>;
    TGpuSlotPtr AcquireGpuSlot();

    std::vector<TGpuSlotPtr> AcquireGpuSlots(int slotCount);

    std::vector<NJobAgent::TShellCommandConfigPtr> GetSetupCommands();
    std::vector<NDataNode::TArtifactKey> GetToppingLayers();
    void VerifyCudaToolkitDriverVersion(const TString& toolkitVersion);

private:
    IBootstrap* const Bootstrap_;
    const NJobAgent::TGpuManagerConfigPtr Config_;
    TAtomicObject<NJobAgent::TGpuManagerDynamicConfigPtr> DynamicConfig_;

    const NConcurrency::TPeriodicExecutorPtr HealthCheckExecutor_;
    const NConcurrency::TPeriodicExecutorPtr FetchDriverLayerExecutor_;

    std::vector<TString> GpuDevices_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<int, NJobAgent::TGpuInfo> HealthyGpuInfoMap_;
    THashSet<int> LostGpuDeviceNumbers_;

    THashSet<int> AcquiredGpuDeviceNumbers_;
    std::vector<TGpuSlot> FreeSlots_;

    bool Enabled_ = true;

    // Error for problems with GPU discovery.
    TError Error_;

    // Alerts for concrete GPU devices.
    std::vector<TError> Alerts_;

    TInstant BannedDeadline_ = TInstant::Zero();

    NYPath::TYPath DriverLayerPath_;
    NHydra::TRevision DriverLayerRevision_ = 0;
    std::optional<NDataNode::TArtifactKey> DriverLayerKey_;
    TString DriverVersionString_;
    TAtomicObject<NJobAgent::IGpuInfoProviderPtr> GpuInfoProvider_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& oldNodeConfig,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig);

    TDuration GetHealthCheckTimeout() const;
    TDuration GetHealthCheckFailureBackoff() const;
    THashMap<TString, TString> GetCudaToolkitMinDriverVersion() const;

    void ReleaseGpuSlot(TGpuSlot* slot);

    void OnHealthCheck();
    void OnFetchDriverLayerInfo();
    bool IsDriverLayerMissing() const;
    void PopulateAlerts(std::vector<TError>* alerts) const;
};

DEFINE_REFCOUNTED_TYPE(TGpuManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NExecNode
