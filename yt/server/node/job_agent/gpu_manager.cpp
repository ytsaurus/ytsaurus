#include "gpu_manager.h"
#include "private.h"

#include <yt/server/node/cell_node/bootstrap.h>
#include <yt/server/node/cell_node/config.h>

#include <yt/server/node/data_node/master_connector.h>
#include <yt/server/node/data_node/helpers.h>

#include <yt/server/lib/job_agent/gpu_helpers.h>

#include <yt/server/lib/exec_agent/config.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/finally.h>

#include <yt/core/misc/proc.h>

#include <yt/library/process/subprocess.h>


#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/data_source.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <util/string/strip.h>

namespace NYT::NJobAgent {

using namespace NConcurrency;
using namespace NCellNode;
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

TGpuManager::TGpuManager(TBootstrap* bootstrap, TGpuManagerConfigPtr config)
    : Bootstrap_(bootstrap)
    , Config_(std::move(config))
{
    auto descriptors = NJobAgent::ListGpuDevices();
    bool testGpu = Bootstrap_->GetConfig()->ExecAgent->JobController->TestGpuLayers;
    if (!descriptors.empty() || testGpu) {
        try {
            DriverVersionString_ = Config_->DriverVersion ? *Config_->DriverVersion : GetGpuDriverVersionString();
        } catch (const std::exception& ex) {
            YT_LOG_FATAL(ex, "Cannot determine driver version");
        }

        if (Config_->DriverLayerDirectoryPath) {
            DriverLayerPath_ = *Config_->DriverLayerDirectoryPath + "/" + DriverVersionString_;

            YT_LOG_INFO("GPU driver layer specified (Path: %v, Version: %v)",
                DriverLayerPath_,
                DriverVersionString_);

            FetchDriverLayerExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetControlInvoker(),
                BIND(&TGpuManager::FetchDriverLayerInfo, MakeWeak(this)),
                Config_->DriverLayerFetchPeriod,
                Config_->DriverLayerFetchPeriod /*splay*/);
            FetchDriverLayerExecutor_->Start();
        } else {
            YT_LOG_INFO("No GPU driver layer directory specified");
        }
    }

    if (descriptors.empty()) {
        return;
    }

    auto now = TInstant::Now();
    for (const auto& descriptor : descriptors) {
        GpuDevices_.push_back(descriptor.DeviceName);
        FreeSlots_.emplace_back(descriptor.DeviceNumber);

        TGpuInfo info;
        info.UpdateTime = now;
        YT_VERIFY(HealthyGpuInfoMap_.emplace(descriptor.DeviceNumber, info).second);
    }

    HealthCheckExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND(&TGpuManager::OnHealthCheck, MakeWeak(this)),
        Config_->HealthCheckPeriod);
    HealthCheckExecutor_->Start();
}

void TGpuManager::OnHealthCheck()
{
    try {
        auto gpuInfos = GetGpuInfos(Config_->HealthCheckTimeout);

        THashSet<int> deviceNumbers;
        for (const auto& info : gpuInfos) {
            deviceNumbers.insert(info.Index);
        }

        YT_LOG_DEBUG("Found healthy GPU devices (DeviceNumbers: %v)",
            deviceNumbers);

        std::vector<TError> newAlerts;

        {
            TGuard<TSpinLock> guard(SpinLock_);

            auto now = TInstant::Now();

            std::vector<int> deviceNumbersToRemove;
            for (const auto& [index, _] : HealthyGpuInfoMap_) {
                if (deviceNumbers.find(index) == deviceNumbers.end()) {
                    deviceNumbersToRemove.push_back(index);
                }
            }

            for (int deviceNumber : deviceNumbersToRemove) {
                HealthyGpuInfoMap_.erase(deviceNumber);
            }

            for (auto& gpuInfo : gpuInfos) {
                gpuInfo.UpdateTime = now;
                HealthyGpuInfoMap_[gpuInfo.Index] = gpuInfo;
            }

            std::vector<TGpuSlot> healthySlots;
            for (auto& slot: FreeSlots_) {
                if (HealthyGpuInfoMap_.find(slot.GetDeviceNumber()) == HealthyGpuInfoMap_.end()) {
                    YT_LOG_WARNING("Found lost GPU device (DeviceName: %v)",
                        slot.GetDeviceName());
                    newAlerts.push_back(TError("Found lost GPU device %v",
                        slot.GetDeviceName()));
                } else {
                    healthySlots.emplace_back(std::move(slot));
                }
            }

            FreeSlots_ = std::move(healthySlots);
        }

        for (const auto& alert: newAlerts) {
            Bootstrap_->GetMasterConnector()->RegisterAlert(alert);
        }

    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to get healthy GPU devices");
        Bootstrap_->GetMasterConnector()->RegisterAlert(TError("All GPU devices are disabled")
            << ex);
        HealthCheckExecutor_->Stop();

        TGuard<TSpinLock> guard(SpinLock_);
        Disabled_ = true;
    }
}

void TGpuManager::FetchDriverLayerInfo()
{
    try {
        DoFetchDriverLayerInfo();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to fetch GPU layer");
    }
}

void TGpuManager::DoFetchDriverLayerInfo()
{
    auto fetchedArtifactKey = FetchLayerArtifactKeyIfRevisionChanged(
        DriverLayerPath_,
        DriverLayerRevision_,
        Bootstrap_,
        EMasterChannelKind::Cache,
        Logger);

    if (fetchedArtifactKey.ContentRevision != DriverLayerRevision_) {
        YT_VERIFY(fetchedArtifactKey.ArtifactKey);
        auto guard = Guard(SpinLock_);
        DriverLayerRevision_ = fetchedArtifactKey.ContentRevision;
        DriverLayerKey_ = std::move(*fetchedArtifactKey.ArtifactKey);
    }
}

bool TGpuManager::IsDriverLayerMissing() const
{
    return DriverLayerPath_ && !DriverLayerKey_;
}

int TGpuManager::GetTotalGpuCount() const
{
    auto guard = Guard(SpinLock_);
    return Disabled_ || IsDriverLayerMissing() ? 0 : HealthyGpuInfoMap_.size();
}

int TGpuManager::GetFreeGpuCount() const
{
    auto guard = Guard(SpinLock_);
    return Disabled_ || IsDriverLayerMissing() ? 0 : FreeSlots_.size();
}

THashMap<int, TGpuInfo> TGpuManager::GetGpuInfoMap() const
{
    auto guard = Guard(SpinLock_);
    return HealthyGpuInfoMap_;
}

const std::vector<TString>& TGpuManager::ListGpuDevices() const
{
    return GpuDevices_;
}

TGpuManager::TGpuSlotPtr TGpuManager::AcquireGpuSlot()
{
    YT_VERIFY(!FreeSlots_.empty());

    auto deleter = [this, this_ = MakeStrong(this)] (TGpuSlot* slot) {
        YT_LOG_DEBUG("Released GPU slot (DeviceName: %v)",
            slot->GetDeviceName());

        auto guard = Guard(this_->SpinLock_);
        if (HealthyGpuInfoMap_.find(slot->GetDeviceNumber()) == HealthyGpuInfoMap_.end()) {
            guard.Release();
            YT_LOG_WARNING("Found lost GPU device (DeviceName: %v)",
                slot->GetDeviceName());
            Bootstrap_->GetMasterConnector()->RegisterAlert(TError("Found lost GPU device %v",
                slot->GetDeviceName()));
        } else {
            this_->FreeSlots_.emplace_back(std::move(*slot));
        }

        delete slot;
    };

    auto guard = Guard(SpinLock_);
    TGpuSlotPtr slot(new TGpuSlot(std::move(FreeSlots_.back())), deleter);
    FreeSlots_.pop_back();

    YT_LOG_DEBUG("Acquired GPU slot (DeviceName: %v)",
        slot->GetDeviceName());
    return slot;
}

std::vector<TShellCommandConfigPtr> TGpuManager::GetSetupCommands()
{
    if (Config_->JobSetupCommand) {
        return {*Config_->JobSetupCommand};
    }

    return {};
}

std::vector<TArtifactKey> TGpuManager::GetToppingLayers()
{
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
    if (!Config_->ToolkitMinDriverVersion.contains(toolkitVersion)) {
        THROW_ERROR_EXCEPTION("Unknown toolkit version %v", toolkitVersion);
    }

    auto minVersionString = Config_->ToolkitMinDriverVersion[toolkitVersion];
    auto minVersion = TGpuDriverVersion::FromString(minVersionString);

    auto actualVersion = TGpuDriverVersion::FromString(DriverVersionString_);

    if (actualVersion < minVersion) {
        THROW_ERROR_EXCEPTION("Unsupported driver version for CUDA toolkit %v, required %v, actual %v",
            toolkitVersion,
            minVersionString,
            DriverVersionString_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
