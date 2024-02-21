#pragma once

#include "public.h"

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/gpu_helpers.h>

#include <yt/yt/server/node/cluster_node/public.h>
#include <yt/yt/server/node/cluster_node/node_resource_manager.h>

#include <yt/yt/server/node/data_node/artifact.h>

#include <yt/yt/library/gpu/gpu_info_provider.h>

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

    void ResetState() override;

    ~TGpuSlot();

private:
    const TGpuManagerPtr Manager_;
    const int DeviceIndex_;
};

DEFINE_REFCOUNTED_TYPE(TGpuSlot)

////////////////////////////////////////////////////////////////////////////////

struct TGpuStatistics
{
    i64 CumulativeUtilizationGpu = 0;
    i64 CumulativeUtilizationMemory = 0;
    i64 CumulativeMemory = 0;
    i64 CumulativeMemoryMBSec = 0;
    i64 MaxMemoryUsed = 0;
    // Index of microseconds when GPU was busy.
    i64 CumulativeLoad = 0;
    i64 CumulativeUtilizationPower = 0;
    i64 CumulativePower = 0;
    i64 CumulativeUtilizationClocksSM = 0;
    i64 CumulativeSMUtilization = 0;
    i64 CumulativeSMOccupancy = 0;
    i64 NvlinkRxBytes = 0;
    i64 NvlinkTxBytes = 0;
    i64 PcieRxBytes = 0;
    i64 PcieTxBytes = 0;
    i64 MaxStuckDuration = 0;
};

void FormatValue(TStringBuilderBase* builder, const TGpuStatistics& gpuStatistics, TStringBuf /*format*/);

////////////////////////////////////////////////////////////////////////////////

struct TRdmaStatistics
{
    i64 RxByteRate = 0.0;
    i64 TxByteRate = 0.0;
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
    explicit TGpuManager(IBootstrap* bootstrap);

    int GetTotalGpuCount() const;
    int GetFreeGpuCount() const;
    int GetUsedGpuCount() const;
    bool HasGpuDevices() const;

    const std::vector<TString>& GetGpuDevices() const;
    THashMap<int, NGpu::TGpuInfo> GetGpuInfoMap() const;

    std::vector<NGpu::TRdmaDeviceInfo> GetRdmaDevices() const;

    TErrorOr<TGpuSlotPtr> AcquireGpuSlot();

    TErrorOr<std::vector<TGpuSlotPtr>> AcquireGpuSlots(int slotCount);

    std::vector<TShellCommandConfigPtr> GetSetupCommands();
    std::vector<NDataNode::TArtifactKey> GetToppingLayers();
    void VerifyCudaToolkitDriverVersion(const TString& toolkitVersion);

    void ReleaseGpuSlot(int deviceIndex);

    NYTree::IYPathServicePtr GetOrchidService() const;

    void OnDynamicConfigChanged(
        const TGpuManagerDynamicConfigPtr& oldConfig,
        const TGpuManagerDynamicConfigPtr& newConfig);

    bool ShouldTestResource() const;
    bool ShouldTestExtraGpuCheckCommandFailure() const;
    bool ShouldTestLayers() const;
    bool ShouldTestSetupCommands() const;

private:
    IBootstrap* const Bootstrap_;
    const TGpuManagerConfigPtr StaticConfig_;
    TAtomicIntrusivePtr<TGpuManagerDynamicConfig> DynamicConfig_;

    const NConcurrency::TPeriodicExecutorPtr HealthCheckExecutor_;
    const NConcurrency::TPeriodicExecutorPtr FetchDriverLayerExecutor_;
    const NConcurrency::TPeriodicExecutorPtr RdmaDeviceInfoUpdateExecutor_;
    const NConcurrency::TPeriodicExecutorPtr TestGpuInfoUpdateExecutor_;

    std::vector<TString> GpuDevices_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<int, NGpu::TGpuInfo> HealthyGpuInfoMap_;
    THashSet<int> LostGpuDeviceIndices_;

    std::vector<NGpu::TRdmaDeviceInfo> RdmaDevices_;

    bool HasGpuDevices_ = false;

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
    TAtomicIntrusivePtr<NGpu::IGpuInfoProvider> GpuInfoProvider_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    TDuration GetHealthCheckTimeout() const;
    TDuration GetHealthCheckFailureBackoff() const;
    THashMap<TString, TString> GetCudaToolkitMinDriverVersion() const;

    void OnHealthCheck();
    void OnFetchDriverLayerInfo();
    bool IsDriverLayerMissing() const;
    void PopulateAlerts(std::vector<TError>* alerts) const;

    void OnRdmaDeviceInfoUpdate();

    void OnTestGpuInfoUpdate();

    void BuildOrchid(NYson::IYsonConsumer* consumer) const;
};

DEFINE_REFCOUNTED_TYPE(TGpuManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
