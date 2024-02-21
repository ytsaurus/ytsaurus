#include "gpu_manager.h"

#include "bootstrap.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/exec_node/helpers.h>

#include <yt/yt/server/lib/exec_node/gpu_helpers.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/library/gpu/config.h>

#include <yt/yt/library/process/subprocess.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/helpers.h>

#include <util/string/strip.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NClusterNode;
using namespace NApi;
using namespace NObjectClient;
using namespace NFileClient;
using namespace NChunkClient;
using namespace NJobAgent;
using namespace NGpu;
using namespace NYTree;
using namespace NCypressClient;
using namespace NDataNode;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TGpuSlot::TGpuSlot(
    TGpuManagerPtr manager,
    int deviceIndex)
    : Manager_(std::move(manager))
    , DeviceIndex_(deviceIndex)
{
    YT_VERIFY(Manager_);
}

TString TGpuSlot::GetDeviceName() const
{
    return NExecNode::GetGpuDeviceName(DeviceIndex_);
}

int TGpuSlot::GetDeviceIndex() const
{
    return DeviceIndex_;
}

void TGpuSlot::ResetState()
{ }

TGpuSlot::~TGpuSlot()
{
    Manager_->ReleaseGpuSlot(DeviceIndex_);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TGpuStatistics& gpuStatistics, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "{CumulativeUtilizationGpu: %v, CumulativeUtilizationMemory: %v, "
        "CumulativeMemory: %v, CumulativeMemoryMBSec: %v, "
        "MaxMemoryUsed: %v, CumulativeLoad: %v, CumulativeUtilizationPower: %v, CumulativePower: %v, "
        "CumulativeUtilizationClocksSM: %v, CumulativeSMUtilization: %v, CumulativeSMOccupancy: %v, "
        "NvlinkRxBytes: %v, NvlinkTxBytes: %v, PcieRxBytes: %v, PcieTxBytes: %v, MaxStuckDuration: %v}",
        gpuStatistics.CumulativeUtilizationGpu,
        gpuStatistics.CumulativeUtilizationMemory,
        gpuStatistics.CumulativeMemory,
        gpuStatistics.CumulativeMemoryMBSec,
        gpuStatistics.MaxMemoryUsed,
        gpuStatistics.CumulativeLoad,
        gpuStatistics.CumulativeUtilizationPower,
        gpuStatistics.CumulativePower,
        gpuStatistics.CumulativeUtilizationClocksSM,
        gpuStatistics.CumulativeSMUtilization,
        gpuStatistics.CumulativeSMOccupancy,
        gpuStatistics.NvlinkRxBytes,
        gpuStatistics.NvlinkTxBytes,
        gpuStatistics.PcieRxBytes,
        gpuStatistics.PcieTxBytes,
        gpuStatistics.MaxStuckDuration);
}

////////////////////////////////////////////////////////////////////////////////

TGpuManager::TGpuManager(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , StaticConfig_(Bootstrap_->GetConfig()->ExecNode->GpuManager)
    , DynamicConfig_(New<TGpuManagerDynamicConfig>())
    , HealthCheckExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TGpuManager::OnHealthCheck, MakeWeak(this)),
        DynamicConfig_.Acquire()->HealthCheckPeriod))
    , FetchDriverLayerExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TGpuManager::OnFetchDriverLayerInfo, MakeWeak(this)),
        DynamicConfig_.Acquire()->DriverLayerFetching))
    , RdmaDeviceInfoUpdateExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TGpuManager::OnRdmaDeviceInfoUpdate, MakeWeak(this)),
        DynamicConfig_.Acquire()->RdmaDeviceInfoUpdatePeriod))
    , TestGpuInfoUpdateExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TGpuManager::OnTestGpuInfoUpdate, MakeWeak(this)),
        StaticConfig_->Testing->TestGpuInfoUpdatePeriod))
    , GpuInfoProvider_(CreateGpuInfoProvider(StaticConfig_->GpuInfoSource))
{
    if (!StaticConfig_->Enable) {
        return;
    }

    std::vector<TGpuDeviceDescriptor> descriptors;
    bool shouldInitializeLayers;

    if (StaticConfig_->Testing->TestResource) {
        for (int index = 0; index < StaticConfig_->Testing->TestGpuCount; ++index) {
            descriptors.push_back(TGpuDeviceDescriptor{Format("/dev/nvidia%v", index), index});
        }
        shouldInitializeLayers = StaticConfig_->Testing->TestLayers;
    } else {
        try {
            descriptors = ListGpuDevices();
        } catch (const std::exception& ex) {
            Error_ = TError(ex);
            descriptors = {};
        }
        shouldInitializeLayers = !descriptors.empty();
    }

    if (shouldInitializeLayers) {
        try {
            DriverVersionString_ = StaticConfig_->DriverVersion ? *StaticConfig_->DriverVersion : GetGpuDriverVersionString();
        } catch (const std::exception& ex) {
            YT_LOG_FATAL(ex, "Cannot determine GPU driver version");
        }
    } else {
        DriverVersionString_ = StaticConfig_->DriverVersion.value_or(GetDummyGpuDriverVersionString());
    }

    if (StaticConfig_->DriverLayerDirectoryPath) {
        DriverLayerPath_ = *StaticConfig_->DriverLayerDirectoryPath + "/" + DriverVersionString_;

        YT_LOG_INFO("GPU driver layer specified (Path: %v, Version: %v)",
            DriverLayerPath_,
            DriverVersionString_);

        if (shouldInitializeLayers) {
            FetchDriverLayerExecutor_->Start();
        } else {
            OnFetchDriverLayerInfo();
        }
    } else {
        YT_LOG_INFO("No GPU driver layer directory specified");
    }

    if (descriptors.empty()) {
        return;
    }

    HasGpuDevices_ = true;

    auto now = TInstant::Now();
    for (const auto& descriptor : descriptors) {
        GpuDevices_.push_back(descriptor.DeviceName);
        FreeSlots_.emplace_back(descriptor.DeviceIndex);

        EmplaceOrCrash(
            HealthyGpuInfoMap_,
            descriptor.DeviceIndex,
            TGpuInfo{
                .UpdateTime = now,
                .Index = descriptor.DeviceIndex
            });
    }

    if (!StaticConfig_->Testing->TestResource) {
        HealthCheckExecutor_->Start();
    } else {
        TestGpuInfoUpdateExecutor_->Start();
    }

    Bootstrap_->SubscribePopulateAlerts(
        BIND(&TGpuManager::PopulateAlerts, MakeStrong(this)));
}

void TGpuManager::OnDynamicConfigChanged(
    const TGpuManagerDynamicConfigPtr& /*oldNodeConfig*/,
    const TGpuManagerDynamicConfigPtr& newConfig)
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_ASSERT(newConfig);

    HealthCheckExecutor_->SetPeriod(newConfig->HealthCheckPeriod);
    RdmaDeviceInfoUpdateExecutor_->SetPeriod(newConfig->RdmaDeviceInfoUpdatePeriod);
    FetchDriverLayerExecutor_->SetOptions(newConfig->DriverLayerFetching);
    if (newConfig->GpuInfoSource) {
        // XXX(ignat): avoid this hack.
        if (!newConfig->GpuInfoSource->NvGpuManagerDevicesCgroupPath) {
            newConfig->GpuInfoSource->NvGpuManagerDevicesCgroupPath = StaticConfig_->GpuInfoSource->NvGpuManagerDevicesCgroupPath;
        }
        GpuInfoProvider_.Store(CreateGpuInfoProvider(newConfig->GpuInfoSource));
    } else {
        GpuInfoProvider_.Store(CreateGpuInfoProvider(StaticConfig_->GpuInfoSource));
    }

    DynamicConfig_.Store(std::move(newConfig));
}

bool TGpuManager::ShouldTestResource() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StaticConfig_->Testing->TestResource;
}

bool TGpuManager::ShouldTestExtraGpuCheckCommandFailure() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StaticConfig_->Testing->TestExtraGpuCheckCommandFailure;
}

bool TGpuManager::ShouldTestLayers() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StaticConfig_->Testing->TestLayers;
}

bool TGpuManager::ShouldTestSetupCommands() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StaticConfig_->Testing->TestSetupCommands;
}

TDuration TGpuManager::GetHealthCheckTimeout() const
{
    return DynamicConfig_.Acquire()->HealthCheckTimeout;
}

TDuration TGpuManager::GetHealthCheckFailureBackoff() const
{
    return DynamicConfig_.Acquire()->HealthCheckFailureBackoff;
}

THashMap<TString, TString> TGpuManager::GetCudaToolkitMinDriverVersion() const
{
    return DynamicConfig_.Acquire()->CudaToolkitMinDriverVersion;
}

void TGpuManager::OnHealthCheck()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (TInstant::Now() < BannedDeadline_) {
        return;
    }

    try {
        auto gpuInfos = GpuInfoProvider_.Acquire()->GetGpuInfos(GetHealthCheckTimeout());

        THashSet<int> deviceIndices;
        for (const auto& info : gpuInfos) {
            deviceIndices.insert(info.Index);
        }

        std::vector<int> freeDeviceIndices;

        YT_LOG_DEBUG("Updating healthy GPU devices (DeviceIndices: %v)",
            deviceIndices);

        {
            auto guard = Guard(SpinLock_);

            auto now = TInstant::Now();

            std::vector<int> deviceIndicesToAdd;
            std::vector<int> deviceIndicesToRemove;
            for (const auto& [index, _] : HealthyGpuInfoMap_) {
                if (deviceIndices.find(index) == deviceIndices.end()) {
                    deviceIndicesToRemove.push_back(index);
                    LostGpuDeviceIndices_.insert(index);
                }
            }

            for (int deviceIndex : deviceIndices) {
                if (LostGpuDeviceIndices_.contains(deviceIndex)) {
                    deviceIndicesToAdd.push_back(deviceIndex);
                }
            }

            for (int deviceIndex : deviceIndicesToRemove) {
                EraseOrCrash(HealthyGpuInfoMap_, deviceIndex);
            }

            std::vector<int> newFreeSlotIndices;
            for (int deviceIndex : deviceIndicesToAdd) {
                if (!AcquiredGpuDeviceIndices_.contains(deviceIndex)) {
                    newFreeSlotIndices.emplace_back(deviceIndex);
                }
                EraseOrCrash(LostGpuDeviceIndices_, deviceIndex);
            }

            for (auto& gpuInfo : gpuInfos) {
                gpuInfo.UpdateTime = now;
                HealthyGpuInfoMap_[gpuInfo.Index] = gpuInfo;
            }

            for (auto index : FreeSlots_) {
                if (!HealthyGpuInfoMap_.contains(index)) {
                    YT_LOG_WARNING("Found lost GPU device (DeviceName: %v)",
                        GetGpuDeviceName(index));
                } else {
                    freeDeviceIndices.push_back(index);
                    newFreeSlotIndices.emplace_back(std::move(index));
                }
            }

            FreeSlots_ = std::move(newFreeSlotIndices);

            std::vector<TError> newAlerts;
            for (auto index : LostGpuDeviceIndices_) {
                newAlerts.push_back(TError("GPU device %v is lost", index));
            }

            Enabled_ = true;
            Error_ = TError();
            Alerts_ = newAlerts;
        }

        std::sort(freeDeviceIndices.begin(), freeDeviceIndices.end());

        YT_LOG_DEBUG(
            "List of healthy GPU devices updated "
            "(HealthyDeviceIndices: %v, FreeDeviceIndices: %v, AcquiredDeviceIndices: %v, LostDeviceIndices: %v)",
            deviceIndices,
            freeDeviceIndices,
            AcquiredGpuDeviceIndices_,
            LostGpuDeviceIndices_);
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to get healthy GPU devices");
        BannedDeadline_ = TInstant::Now() + GetHealthCheckFailureBackoff();

        {
            auto guard = Guard(SpinLock_);
            Enabled_ = false;
            Error_ = TError("All GPU devices are disabled")
                << ex;
        }
    }
}

void TGpuManager::OnFetchDriverLayerInfo()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    try {
        auto fetchedArtifactKey = FetchLayerArtifactKeyIfRevisionChanged(
            DriverLayerPath_,
            DriverLayerRevision_,
            Bootstrap_->GetExecNodeBootstrap(),
            Logger);

        if (fetchedArtifactKey.ContentRevision != DriverLayerRevision_) {
            YT_VERIFY(fetchedArtifactKey.ArtifactKey);
            auto guard = Guard(SpinLock_);
            DriverLayerRevision_ = fetchedArtifactKey.ContentRevision;
            DriverLayerKey_ = std::move(*fetchedArtifactKey.ArtifactKey);
        }
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to fetch GPU layer");
    }
}

void TGpuManager::OnRdmaDeviceInfoUpdate()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    try {
        auto timeout = DynamicConfig_.Acquire()->RdmaDeviceInfoUpdateTimeout;
        auto rdmaDevices = GpuInfoProvider_.Acquire()->GetRdmaDeviceInfos(timeout);

        auto guard = Guard(SpinLock_);
        RdmaDevices_ = std::move(rdmaDevices);
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to fetch RDMA device infos");
    }
}

void TGpuManager::OnTestGpuInfoUpdate()
{
    auto now = TInstant::Now();

    auto guard = Guard(SpinLock_);

    for (auto& [_, gpuInfo] : HealthyGpuInfoMap_) {
        gpuInfo.UtilizationGpuRate = StaticConfig_->Testing->TestUtilizationGpuRate;
        gpuInfo.UpdateTime = now;
    }
}

bool TGpuManager::IsDriverLayerMissing() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return DriverLayerPath_ && !DriverLayerKey_;
}

int TGpuManager::GetTotalGpuCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    return !Enabled_ || IsDriverLayerMissing() ? 0 : HealthyGpuInfoMap_.size();
}

int TGpuManager::GetFreeGpuCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    return !Enabled_ || IsDriverLayerMissing() ? 0 : FreeSlots_.size();
}

int TGpuManager::GetUsedGpuCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    return !Enabled_ || IsDriverLayerMissing() ? 0 : (HealthyGpuInfoMap_.size() - FreeSlots_.size());
}

bool TGpuManager::HasGpuDevices() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return HasGpuDevices_;
}

THashMap<int, TGpuInfo> TGpuManager::GetGpuInfoMap() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    return HealthyGpuInfoMap_;
}

const std::vector<TString>& TGpuManager::GetGpuDevices() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GpuDevices_;
}

std::vector<TRdmaDeviceInfo> TGpuManager::GetRdmaDevices() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    return RdmaDevices_;
}

void TGpuManager::ReleaseGpuSlot(int deviceIndex)
{
    YT_LOG_DEBUG("Released GPU slot (DeviceName: %v)",
        GetGpuDeviceName(deviceIndex));

    auto guard = Guard(SpinLock_);

    if (AcquiredGpuDeviceIndices_.erase(deviceIndex) > 0) {
        if (!HealthyGpuInfoMap_.contains(deviceIndex)) {
            LostGpuDeviceIndices_.insert(deviceIndex);
            YT_LOG_WARNING("Found lost GPU device (DeviceName: %v)",
                deviceIndex);
        } else {
            FreeSlots_.push_back(deviceIndex);
        }
    }
}

NYTree::IYPathServicePtr TGpuManager::GetOrchidService() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return IYPathService::FromProducer(BIND_NO_PROPAGATE(
        &TGpuManager::BuildOrchid,
        MakeStrong(this)));
}

void TGpuManager::BuildOrchid(NYson::IYsonConsumer* consumer) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    BuildYsonFluently(consumer).BeginMap()
        .Item("gpu_infos").Value(GetGpuInfoMap())
    .EndMap();
}

TErrorOr<TGpuSlotPtr> TGpuManager::AcquireGpuSlot()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);

    if (FreeSlots_.empty()) {
        return TError("Cannot find an empty GPU slot");
    }

    auto deviceIndex = FreeSlots_.back();
    FreeSlots_.pop_back();

    InsertOrCrash(AcquiredGpuDeviceIndices_, deviceIndex);

    YT_LOG_DEBUG("Acquired GPU slot (DeviceName: %v)",
        GetGpuDeviceName(deviceIndex));
    return New<TGpuSlot>(MakeStrong(this), deviceIndex);
}

TErrorOr<std::vector<TGpuSlotPtr>> TGpuManager::AcquireGpuSlots(int slotCount)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);

    if (std::ssize(FreeSlots_) < slotCount) {
        return TError("Cannot find enough empty GPU slots")
            << TErrorAttribute("free_slot_count", std::ssize(FreeSlots_))
            << TErrorAttribute("required_slot_count", slotCount);
    }

    // TODO(ignat): use actual topology of GPU-s.
    // nvidia-smi topo -p2p r
    int levelCount = 4;
    int maxLevelIndex = levelCount - 1;

    // NB: std::map used to make the behaviour deterministic.
    std::vector<std::map<int, std::vector<int>>> freeDeviceIndexPerLevelPerGroup(levelCount);
    for (auto index : FreeSlots_) {
        YT_VERIFY(index < (1 << maxLevelIndex));
        for (int levelIndex = 0; levelIndex < levelCount; ++levelIndex) {
            int groupIndex = index / (1 << levelIndex);
            freeDeviceIndexPerLevelPerGroup[levelIndex][groupIndex].push_back(index);
        }
    }

    THashSet<int> resultDeviceIndices;
    bool found = false;
    for (int levelIndex = 0; levelIndex < levelCount && !found; ++levelIndex) {
        for (const auto& [_, slots] : freeDeviceIndexPerLevelPerGroup[levelIndex]) {
            if (std::ssize(slots) >= slotCount) {
                found = true;
                for (int index = 0; index < slotCount; ++index) {
                    YT_VERIFY(resultDeviceIndices.insert(slots[index]).second);
                }
                break;
            }
        }
    }

    YT_VERIFY(found);
    YT_VERIFY(std::ssize(resultDeviceIndices) == slotCount);

    YT_LOG_DEBUG("Acquired GPU slots (DeviceIndices: %v)",
        resultDeviceIndices);

    std::vector<TGpuSlotPtr> resultSlots;
    std::vector<int> remainingSlotIndices;
    for (   auto index : FreeSlots_) {
        if (resultDeviceIndices.contains(index)) {
            resultSlots.push_back(New<TGpuSlot>(MakeStrong(this), index));
        } else {
            remainingSlotIndices.push_back(index);
        }
    }

    for (auto deviceIndex : resultDeviceIndices) {
        InsertOrCrash(AcquiredGpuDeviceIndices_, deviceIndex);
    }

    swap(FreeSlots_, remainingSlotIndices);

    return resultSlots;
}

std::vector<TShellCommandConfigPtr> TGpuManager::GetSetupCommands()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto dynamicConfig = DynamicConfig_.Acquire();
    if (dynamicConfig->JobSetupCommand) {
        return {*dynamicConfig->JobSetupCommand};
    }

    return {};
}

std::vector<TArtifactKey> TGpuManager::GetToppingLayers()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    if (DriverLayerKey_) {
        return {
            *DriverLayerKey_
        };
    } else if (DriverLayerPath_) {
        THROW_ERROR_EXCEPTION(NExecNode::EErrorCode::GpuLayerNotFetched, "GPU layer is not fetched yet");
    } else {
        return {};
    }
}

void TGpuManager::VerifyCudaToolkitDriverVersion(const TString& toolkitVersion)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto cudaToolkitMinDriverVersion = GetCudaToolkitMinDriverVersion();
    auto it = cudaToolkitMinDriverVersion.find(toolkitVersion);
    if (it == cudaToolkitMinDriverVersion.end()) {
        THROW_ERROR_EXCEPTION("Unknown CUDA toolkit version %v", toolkitVersion);
    }

    const auto& minVersionString = it->second;
    auto minVersion = TGpuDriverVersion::FromString(minVersionString);

    auto actualVersion = TGpuDriverVersion::FromString(DriverVersionString_);

    if (actualVersion < minVersion) {
        THROW_ERROR_EXCEPTION("Unsupported GPU driver version for CUDA toolkit %v: required %v, actual %v",
            toolkitVersion,
            minVersionString,
            DriverVersionString_);
    }
}

void TGpuManager::PopulateAlerts(std::vector<TError>* alerts) const
{
    auto guard = Guard(SpinLock_);

    if (!Error_.IsOK()) {
        alerts->push_back(Error_);
    }

    for (const auto& alert : Alerts_) {
        alerts->push_back(alert);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
