#include "gpu_manager.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/helpers.h>

#include <yt/yt/server/lib/job_agent/gpu_helpers.h>

#include <yt/yt/server/lib/exec_agent/config.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/library/process/subprocess.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/helpers.h>

#include <util/string/strip.h>

namespace NYT::NJobAgent {

using namespace NConcurrency;
using namespace NClusterNode;
using namespace NApi;
using namespace NObjectClient;
using namespace NFileClient;
using namespace NChunkClient;
using namespace NYTree;
using namespace NCypressClient;
using namespace NDataNode;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobAgentServerLogger;

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

TGpuManager::TGpuManager(
    TBootstrap* bootstrap,
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
        Config_->DriverLayerFetchPeriod,
        /*splay*/ Config_->DriverLayerFetchPeriod))
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
        descriptors = NJobAgent::ListGpuDevices();
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

    Bootstrap_->GetClusterNodeMasterConnector()->SubscribePopulateAlerts(
        BIND(&TGpuManager::PopulateAlerts, MakeStrong(this)));
}

void TGpuManager::OnHealthCheck()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (TInstant::Now() < BannedDeadline_) {
        return;
    }

    try {
        auto gpuInfos = GetGpuInfos(Config_->HealthCheckTimeout);

        THashSet<int> deviceNumbers;
        for (const auto& info : gpuInfos) {
            deviceNumbers.insert(info.Index);
        }

        YT_LOG_DEBUG("Found healthy GPU devices (DeviceNumbers: %v)",
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
                if (LostGpuDeviceNumbers_.find(deviceNumber) != LostGpuDeviceNumbers_.end()) {
                    deviceNumbersToAdd.push_back(deviceNumber);
                }
            }

            for (int deviceNumber : deviceNumbersToRemove) {
                HealthyGpuInfoMap_.erase(deviceNumber);
            }
            
            std::vector<TGpuSlot> newFreeSlots;
            for (int deviceNumber : deviceNumbersToAdd) {
                if (AcquiredGpuDeviceNumbers_.find(deviceNumber) == AcquiredGpuDeviceNumbers_.end()) {
                    newFreeSlots.emplace_back(deviceNumber);
                }
                LostGpuDeviceNumbers_.erase(deviceNumber);
            }

            for (auto& gpuInfo : gpuInfos) {
                gpuInfo.UpdateTime = now;
                HealthyGpuInfoMap_[gpuInfo.Index] = gpuInfo;
            }

            for (auto& slot : FreeSlots_) {
                if (HealthyGpuInfoMap_.find(slot.GetDeviceNumber()) == HealthyGpuInfoMap_.end()) {
                    YT_LOG_WARNING("Found lost GPU device (DeviceName: %v)",
                        slot.GetDeviceName());
                } else {
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
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to get healthy GPU devices");
        BannedDeadline_ = TInstant::Now() + Config_->HealthCheckFailureBackoff;

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
            Bootstrap_,
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

THashMap<int, TGpuInfo> TGpuManager::GetGpuInfoMap() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    return HealthyGpuInfoMap_;
}

const std::vector<TString>& TGpuManager::ListGpuDevices() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GpuDevices_;
}

void TGpuManager::ReleaseGpuSlot(TGpuSlot* slot)
{
    YT_LOG_DEBUG("Released GPU slot (DeviceName: %v)",
        slot->GetDeviceName());

    auto guard = Guard(SpinLock_);

    AcquiredGpuDeviceNumbers_.erase(slot->GetDeviceNumber());
    if (HealthyGpuInfoMap_.find(slot->GetDeviceNumber()) == HealthyGpuInfoMap_.end()) {
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
    YT_VERIFY(!FreeSlots_.empty());

    auto deleter = [this, this_ = MakeStrong(this)] (TGpuSlot* slot) {
        ReleaseGpuSlot(slot);
        delete slot;
    };

    auto guard = Guard(SpinLock_);
    TGpuSlotPtr slot(new TGpuSlot(std::move(FreeSlots_.back())), deleter);
    FreeSlots_.pop_back();

    YT_LOG_DEBUG("Acquired GPU slot (DeviceName: %v)",
        slot->GetDeviceName());
    return slot;
}

std::vector<TGpuManager::TGpuSlotPtr> TGpuManager::AcquireGpuSlots(int slotCount)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    YT_VERIFY(FreeSlots_.size() >= slotCount);

    // TODO(ignat): use actual topology of GPU-s.
    // nvidia-smi topo -p2p r
    int levelCount = 4;
    // NB: std::map used to make the behaviour deterministic.
    std::vector<std::map<int, std::vector<int>>> freeDeviceNumberPerLevelPerGroup(levelCount);
    for (const auto& slot : FreeSlots_) {
        int number = slot.GetDeviceNumber();
        for (int levelIndex = 0; levelIndex < levelCount; ++levelIndex) {
            int groupIndex = number / (1 << levelIndex);
            freeDeviceNumberPerLevelPerGroup[levelIndex][groupIndex].push_back(number);
        }
    }

    THashSet<int> resultDeviceNumbers;
    bool found = false;
    for (int levelIndex = 0; levelIndex < levelCount && !found; ++levelIndex) {
        for (const auto& [_, slots] : freeDeviceNumberPerLevelPerGroup[levelIndex]) {
            if (slots.size() >= slotCount) {
                found = true;
                for (int index = 0; index < slotCount; ++index) {
                    resultDeviceNumbers.insert(slots[index]);
                }
                break;
            }
        }
    }

    YT_VERIFY(found);
    YT_VERIFY(resultDeviceNumbers.size() == slotCount);

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
        THROW_ERROR_EXCEPTION(NExecAgent::EErrorCode::GpuLayerNotFetched, "GPU layer is not fetched yet");
    } else {
        return {};
    }
}

void TGpuManager::VerifyToolkitDriverVersion(const TString& toolkitVersion)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!Config_->ToolkitMinDriverVersion.contains(toolkitVersion)) {
        THROW_ERROR_EXCEPTION("Unknown toolkit version %v", toolkitVersion);
    }

    auto minVersionString = Config_->ToolkitMinDriverVersion[toolkitVersion];
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

} // namespace NYT::NJobAgent
