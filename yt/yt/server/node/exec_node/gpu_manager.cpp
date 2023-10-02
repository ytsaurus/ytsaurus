#include "gpu_manager.h"

#include "bootstrap.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/exec_node/helpers.h>

#include <yt/yt/server/lib/job_agent/gpu_helpers.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/misc/proc.h>

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
using namespace NYTree;
using namespace NCypressClient;
using namespace NDataNode;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TGpuSlot::TGpuSlot(int deviceNumber)
    : DeviceNumber_(deviceNumber)
{ }

TString TGpuSlot::GetDeviceName() const
{
    return NJobAgent::GetGpuDeviceName(DeviceNumber_);
}

int TGpuSlot::GetDeviceNumber() const
{
    return DeviceNumber_;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TGpuStatistics& gpuStatistics, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "{CumulativeUtilizationGpu: %v, CumulativeUtilizationMemory: %v, "
        "CumulativeMemory: %v, CumulativeMemoryMBSec: %v, "
        "MaxMemoryUsed: %v, CumulativeLoad: %v, CumulativeUtilizationPower: %v, CumulativePower: %v, "
        "CumulativeUtilizationClocksSM: %v, CumulativeSMUtilization: %v, "
        "CumulativeSMOccupancy: %v, MaxStuckDuration: %v}",
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
        gpuStatistics.MaxStuckDuration);
}

////////////////////////////////////////////////////////////////////////////////

TGpuManager::TGpuManager(
    IBootstrap* bootstrap,
    TGpuManagerConfigPtr config)
    : Bootstrap_(bootstrap)
    , Config_(std::move(config))
    , HealthCheckExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TGpuManager::OnHealthCheck, MakeWeak(this)),
        Config_->HealthCheckPeriod))
    , FetchDriverLayerExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetJobInvoker(),
        BIND(&TGpuManager::OnFetchDriverLayerInfo, MakeWeak(this)),
        TPeriodicExecutorOptions{
            .Period = Config_->DriverLayerFetchPeriod,
            .Splay = Config_->DriverLayerFetchPeriodSplay
        }))
    , GpuInfoProvider_(CreateGpuInfoProvider(Config_->GpuInfoSource))
{
    if (!Config_->Enable) {
        return;
    }

    std::vector<TGpuDeviceDescriptor> descriptors;
    bool shouldInitializeLayers;

    if (Config_->TestResource) {
        for (int index = 0; index < Config_->TestGpuCount; ++index) {
            descriptors.push_back(TGpuDeviceDescriptor{Format("/dev/nvidia%v", index), index});
        }
        shouldInitializeLayers = Config_->TestLayers;
    } else {
        try {
            descriptors = NJobAgent::ListGpuDevices();
        } catch (const std::exception& ex) {
            Error_ = TError(ex);
            descriptors = {};
        }
        shouldInitializeLayers = !descriptors.empty();
    }

    if (shouldInitializeLayers) {
        try {
            DriverVersionString_ = Config_->DriverVersion ? *Config_->DriverVersion : GetGpuDriverVersionString();
        } catch (const std::exception& ex) {
            YT_LOG_FATAL(ex, "Cannot determine GPU driver version");
        }
    } else {
        DriverVersionString_ = Config_->DriverVersion.value_or(GetDummyGpuDriverVersionString());
    }

    if (Config_->DriverLayerDirectoryPath) {
        DriverLayerPath_ = *Config_->DriverLayerDirectoryPath + "/" + DriverVersionString_;

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

    auto now = TInstant::Now();
    for (const auto& descriptor : descriptors) {
        GpuDevices_.push_back(descriptor.DeviceName);
        FreeSlots_.emplace_back(descriptor.DeviceNumber);

        TGpuInfo info{
            .UpdateTime = now,
            .Index = descriptor.DeviceNumber
        };
        YT_VERIFY(HealthyGpuInfoMap_.emplace(descriptor.DeviceNumber, info).second);
    }

    if (!Config_->TestResource) {
        HealthCheckExecutor_->Start();
    }

    Bootstrap_->SubscribePopulateAlerts(
        BIND(&TGpuManager::PopulateAlerts, MakeStrong(this)));

    const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
    dynamicConfigManager->SubscribeConfigChanged(BIND(&TGpuManager::OnDynamicConfigChanged, MakeWeak(this)));
}

void TGpuManager::OnDynamicConfigChanged(
    const TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
    const TClusterNodeDynamicConfigPtr& newNodeConfig)
{
    auto gpuManagerConfig = newNodeConfig->ExecNode->JobController->GpuManager;

    DynamicConfig_.Store(gpuManagerConfig);

    if (gpuManagerConfig && gpuManagerConfig->HealthCheckPeriod) {
        HealthCheckExecutor_->SetPeriod(*gpuManagerConfig->HealthCheckPeriod);
    } else {
        HealthCheckExecutor_->SetPeriod(Config_->HealthCheckPeriod);
    }
    if (gpuManagerConfig && gpuManagerConfig->DriverLayerFetchPeriod) {
        FetchDriverLayerExecutor_->SetPeriod(*gpuManagerConfig->DriverLayerFetchPeriod);
    } else {
        FetchDriverLayerExecutor_->SetPeriod(Config_->DriverLayerFetchPeriod);
    }
    if (gpuManagerConfig && gpuManagerConfig->GpuInfoSource) {
        // XXX(ignat): avoid this hack.
        if (!gpuManagerConfig->GpuInfoSource->NvGpuManagerDevicesCgroupPath) {
            gpuManagerConfig->GpuInfoSource->NvGpuManagerDevicesCgroupPath = Config_->GpuInfoSource->NvGpuManagerDevicesCgroupPath;
        }
        GpuInfoProvider_.Store(CreateGpuInfoProvider(gpuManagerConfig->GpuInfoSource));
    } else {
        GpuInfoProvider_.Store(CreateGpuInfoProvider(Config_->GpuInfoSource));
    }
}

TDuration TGpuManager::GetHealthCheckTimeout() const
{
    auto dynamicConfig = DynamicConfig_.Load();
    return dynamicConfig
        ? dynamicConfig->HealthCheckTimeout.value_or(Config_->HealthCheckTimeout)
        : Config_->HealthCheckTimeout;
}

TDuration TGpuManager::GetHealthCheckFailureBackoff() const
{
    auto dynamicConfig = DynamicConfig_.Load();
    return dynamicConfig
        ? dynamicConfig->HealthCheckFailureBackoff.value_or(Config_->HealthCheckFailureBackoff)
        : Config_->HealthCheckFailureBackoff;
}

THashMap<TString, TString> TGpuManager::GetCudaToolkitMinDriverVersion() const
{
    auto dynamicConfig = DynamicConfig_.Load();
    return dynamicConfig
        ? dynamicConfig->CudaToolkitMinDriverVersion.value_or(Config_->CudaToolkitMinDriverVersion)
        : Config_->CudaToolkitMinDriverVersion;
}

void TGpuManager::OnHealthCheck()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (TInstant::Now() < BannedDeadline_) {
        return;
    }

    try {
        auto gpuInfos = GpuInfoProvider_.Load()->GetGpuInfos(GetHealthCheckTimeout());

        THashSet<int> deviceNumbers;
        for (const auto& info : gpuInfos) {
            deviceNumbers.insert(info.Index);
        }

        std::vector<int> freeDeviceNumbers;

        YT_LOG_DEBUG("Updating healthy GPU devices (DeviceNumbers: %v)",
            deviceNumbers);

        {
            auto guard = Guard(SpinLock_);

            auto now = TInstant::Now();

            std::vector<int> deviceNumbersToAdd;
            std::vector<int> deviceNumbersToRemove;
            for (const auto& [number, _] : HealthyGpuInfoMap_) {
                if (deviceNumbers.find(number) == deviceNumbers.end()) {
                    deviceNumbersToRemove.push_back(number);
                    LostGpuDeviceNumbers_.insert(number);
                }
            }

            for (int deviceNumber : deviceNumbers) {
                if (LostGpuDeviceNumbers_.contains(deviceNumber)) {
                    deviceNumbersToAdd.push_back(deviceNumber);
                }
            }

            for (int deviceNumber : deviceNumbersToRemove) {
                EraseOrCrash(HealthyGpuInfoMap_, deviceNumber);
            }

            std::vector<TGpuSlot> newFreeSlots;
            for (int deviceNumber : deviceNumbersToAdd) {
                if (!AcquiredGpuDeviceNumbers_.contains(deviceNumber)) {
                    newFreeSlots.emplace_back(deviceNumber);
                }
                EraseOrCrash(LostGpuDeviceNumbers_, deviceNumber);
            }

            for (auto& gpuInfo : gpuInfos) {
                gpuInfo.UpdateTime = now;
                HealthyGpuInfoMap_[gpuInfo.Index] = gpuInfo;
            }

            for (auto& slot : FreeSlots_) {
                if (!HealthyGpuInfoMap_.contains(slot.GetDeviceNumber())) {
                    YT_LOG_WARNING("Found lost GPU device (DeviceName: %v)",
                        slot.GetDeviceName());
                } else {
                    freeDeviceNumbers.push_back(slot.GetDeviceNumber());
                    newFreeSlots.emplace_back(std::move(slot));
                }
            }

            FreeSlots_ = std::move(newFreeSlots);

            std::vector<TError> newAlerts;
            for (auto number : LostGpuDeviceNumbers_) {
                newAlerts.push_back(TError("GPU device %v is lost", number));
            }

            Enabled_ = true;
            Error_ = TError();
            Alerts_ = newAlerts;
        }

        std::sort(freeDeviceNumbers.begin(), freeDeviceNumbers.end());

        YT_LOG_DEBUG(
            "List of healthy GPU devices updated "
            "(HealthyDeviceNumbers: %v, FreeDeviceNumbers: %v, AcquiredDeviceNumbers: %v, LostDeviceNumbers: %v)",
            deviceNumbers,
            freeDeviceNumbers,
            AcquiredGpuDeviceNumbers_,
            LostGpuDeviceNumbers_);
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

void TGpuManager::ReleaseGpuSlot(TGpuSlot* slot)
{
    YT_LOG_DEBUG("Released GPU slot (DeviceName: %v)",
        slot->GetDeviceName());

    auto guard = Guard(SpinLock_);

    EraseOrCrash(AcquiredGpuDeviceNumbers_, slot->GetDeviceNumber());
    if (!HealthyGpuInfoMap_.contains(slot->GetDeviceNumber())) {
        LostGpuDeviceNumbers_.insert(slot->GetDeviceNumber());
        YT_LOG_WARNING("Found lost GPU device (DeviceName: %v)",
            slot->GetDeviceName());
    } else {
        FreeSlots_.emplace_back(std::move(*slot));
    }
}

TGpuManager::TGpuSlotPtr TGpuManager::AcquireGpuSlot()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto deleter = [this, this_ = MakeStrong(this)] (TGpuSlot* slot) {
        ReleaseGpuSlot(slot);
        delete slot;
    };

    auto guard = Guard(SpinLock_);

    YT_VERIFY(!FreeSlots_.empty());
    TGpuSlotPtr slot(new TGpuSlot(std::move(FreeSlots_.back())), deleter);
    FreeSlots_.pop_back();

    InsertOrCrash(AcquiredGpuDeviceNumbers_, slot->GetDeviceNumber());

    YT_LOG_DEBUG("Acquired GPU slot (DeviceName: %v)",
        slot->GetDeviceName());
    return slot;
}

std::vector<TGpuManager::TGpuSlotPtr> TGpuManager::AcquireGpuSlots(int slotCount)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    YT_VERIFY(std::ssize(FreeSlots_) >= slotCount);

    // TODO(ignat): use actual topology of GPU-s.
    // nvidia-smi topo -p2p r
    int levelCount = 4;
    int maxLevelIndex = levelCount - 1;

    // NB: std::map used to make the behaviour deterministic.
    std::vector<std::map<int, std::vector<int>>> freeDeviceNumberPerLevelPerGroup(levelCount);
    for (const auto& slot : FreeSlots_) {
        int number = slot.GetDeviceNumber();
        YT_VERIFY(number < (1 << maxLevelIndex));
        for (int levelIndex = 0; levelIndex < levelCount; ++levelIndex) {
            int groupIndex = number / (1 << levelIndex);
            freeDeviceNumberPerLevelPerGroup[levelIndex][groupIndex].push_back(number);
        }
    }

    THashSet<int> resultDeviceNumbers;
    bool found = false;
    for (int levelIndex = 0; levelIndex < levelCount && !found; ++levelIndex) {
        for (const auto& [_, slots] : freeDeviceNumberPerLevelPerGroup[levelIndex]) {
            if (std::ssize(slots) >= slotCount) {
                found = true;
                for (int index = 0; index < slotCount; ++index) {
                    YT_VERIFY(resultDeviceNumbers.insert(slots[index]).second);
                }
                break;
            }
        }
    }

    YT_VERIFY(found);
    YT_VERIFY(std::ssize(resultDeviceNumbers) == slotCount);

    YT_LOG_DEBUG("Acquired GPU slots (DeviceNumbers: %v)",
        resultDeviceNumbers);

    auto deleter = [this, this_ = MakeStrong(this)] (TGpuSlot* slot) {
        ReleaseGpuSlot(slot);
        delete slot;
    };

    std::vector<TGpuSlotPtr> resultSlots;
    std::vector<TGpuSlot> remainingSlots;
    for (auto& slot : FreeSlots_) {
        if (resultDeviceNumbers.contains(slot.GetDeviceNumber())) {
            resultSlots.push_back(TGpuSlotPtr(new TGpuSlot(std::move(slot)), deleter));
        } else {
            remainingSlots.push_back(std::move(slot));
        }
    }

    for (auto deviceNumber : resultDeviceNumbers) {
        InsertOrCrash(AcquiredGpuDeviceNumbers_, deviceNumber);
    }

    swap(FreeSlots_, remainingSlots);

    return resultSlots;
}

std::vector<TShellCommandConfigPtr> TGpuManager::GetSetupCommands()
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (Config_->JobSetupCommand) {
        return {*Config_->JobSetupCommand};
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
