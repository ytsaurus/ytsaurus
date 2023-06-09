#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_agent/config.h>
#include <yt/yt/server/lib/job_agent/gpu_helpers.h>
#include <yt/yt/server/lib/job_agent/gpu_info_provider.h>

#include <yt/yt/server/node/cluster_node/public.h>
#include <yt/yt/server/node/cluster_node/node_resource_manager.h>

#include <yt/yt/server/node/data_node/artifact.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TGpuSlot
    : public NClusterNode::ISlot
{
public:
    TGpuSlot(
        TGpuManagerPtr manager,
        int deviceIndex);

    TString GetDeviceName() const;
    int GetDeviceIndex() const;

    ~TGpuSlot();

private:
    const TGpuManagerPtr Manager_;
    const int DeviceIndex_;
};

DEFINE_REFCOUNTED_TYPE(TGpuSlot)

////////////////////////////////////////////////////////////////////////////////

struct TGpuStatistics
{
    TInstant LastUpdateTime;
    i64 CumulativeUtilizationGpu = 0;
    i64 CumulativeUtilizationMemory = 0;
    i64 CumulativeMemory = 0;
    i64 MaxMemoryUsed = 0;
    // Index of microseconds when GPU was busy.
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

    TErrorOr<TGpuSlotPtr> AcquireGpuSlot();

    TErrorOr<std::vector<TGpuSlotPtr>> AcquireGpuSlots(int slotCount);

    std::vector<NJobAgent::TShellCommandConfigPtr> GetSetupCommands();
    std::vector<NDataNode::TArtifactKey> GetToppingLayers();
    void VerifyCudaToolkitDriverVersion(const TString& toolkitVersion);

    void ReleaseGpuSlot(int deviceIndex);

private:
    IBootstrap* const Bootstrap_;
    const NJobAgent::TGpuManagerConfigPtr Config_;
    TAtomicIntrusivePtr<NJobAgent::TGpuManagerDynamicConfig> DynamicConfig_;

    const NConcurrency::TPeriodicExecutorPtr HealthCheckExecutor_;
    const NConcurrency::TPeriodicExecutorPtr FetchDriverLayerExecutor_;

    std::vector<TString> GpuDevices_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<int, NJobAgent::TGpuInfo> HealthyGpuInfoMap_;
    THashSet<int> LostGpuDeviceIndices_;

    THashSet<int> AcquiredGpuDeviceIndices_;
    std::vector<int> FreeSlots_;

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
    TAtomicIntrusivePtr<NJobAgent::IGpuInfoProvider> GpuInfoProvider_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& oldNodeConfig,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig);

    TDuration GetHealthCheckTimeout() const;
    TDuration GetHealthCheckFailureBackoff() const;
    THashMap<TString, TString> GetCudaToolkitMinDriverVersion() const;

    void OnHealthCheck();
    void OnFetchDriverLayerInfo();
    bool IsDriverLayerMissing() const;
    void PopulateAlerts(std::vector<TError>* alerts) const;
};

DEFINE_REFCOUNTED_TYPE(TGpuManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NExecNode
