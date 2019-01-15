#include "gpu_manager.h"

#include "private.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/data_node/master_connector.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/finally.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/subprocess.h>

#include <util/folder/iterator.h>
#include <util/string/strip.h>

namespace NYT::NJobAgent {

using namespace NConcurrency;
using namespace NCellNode;

static const auto& Logger = JobTrackerServerLogger;

////////////////////////////////////////////////////////////////////////////////

TGpuSlot::TGpuSlot(int deviceNumber)
    : DeviceNumber_(deviceNumber)
{ }


TString TGpuSlot::GetDeviceName() const
{
    return TGpuManager::GetDeviceName(DeviceNumber_);
}

int TGpuSlot::GetDeviceNumber() const
{
    return DeviceNumber_;
}

////////////////////////////////////////////////////////////////////////////////

static const auto FatalErrorMessage = "Unable to determine";
static const auto MinorNumberMessage = "Minor Number";

THashSet<int> GetHealthyGpuDeviceNumbers(TDuration checkTimeout)
{
    TSubprocess subprocess("nvidia-smi");
    subprocess.AddArguments({ "-q" });

    auto killCookie = TDelayedExecutor::Submit(
        BIND([&] () {
            try {
                subprocess.Kill(SIGKILL);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Failed to kill nvidia-smi process");
            }
        }),
        checkTimeout);

    auto cookieGuard = Finally([&] () {
        TDelayedExecutor::CancelAndClear(killCookie);
    });

    auto nvidiaSmiResult = subprocess.Execute();
    if (!nvidiaSmiResult.Status.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to check healthy GPUs: nvidia-smi exited with an error")
                << nvidiaSmiResult.Status;
    }

    auto output = TString(nvidiaSmiResult.Output.Begin(), nvidiaSmiResult.Output.End());
    if (output.find(FatalErrorMessage) != TString::npos) {
        THROW_ERROR_EXCEPTION("Failed to check healthy GPUs: nvidia-smi exited with fatal error");
    }

    THashSet<int> result;
    size_t pos = 0;
    while (true) {
        pos = output.find(MinorNumberMessage, pos);
        if (pos == TString::npos) {
            break;
        }

        auto semicolonPos = output.find(":", pos);
        auto eolPos = output.find("\n", pos);
        if (semicolonPos == TString::npos || eolPos == TString::npos || eolPos <= semicolonPos) {
            THROW_ERROR_EXCEPTION("Invalid nvidia-smi output format");
        }

        try {
            auto deviceNumberString = output.substr(semicolonPos + 1, eolPos - semicolonPos - 1);
            auto deviceNumber = FromString<int>(Strip(deviceNumberString));
            result.insert(deviceNumber);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to parse GPU minor device number") << ex;
        }

        pos = eolPos;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

static const TString DevNvidia("/dev/nvidia");

std::optional<int> GetDeviceNumber(const TString& deviceName)
{
    int deviceNumber;
    if (TryFromString(deviceName.data() + DevNvidia.length(), deviceNumber)) {
        return deviceNumber;
    } else {
        return std::nullopt;
    }
}

TGpuManager::TGpuManager(TBootstrap* bootstrap, TGpuManagerConfigPtr config)
    : Bootstrap_(bootstrap)
    , Config_(std::move(config))
{
    Init();
}

TGpuManager::TGpuManager()
    : EnableHealthCheck_(false)
{
    Init();
}

void TGpuManager::Init()
{
    int foundMetaDeviceCount = 0;

    auto dirName = TString("/dev");
    TDirIterator dir(dirName, TDirIterator::TOptions().SetMaxLevel(1));
    for (auto file = dir.begin(); file != dir.end(); ++file) {
        if (file->fts_pathlen == file->fts_namelen || file->fts_pathlen <= dirName.length()) {
            continue;
        }

        TStringBuf fileName = file->fts_path + dirName.length() + 1;

        if (fileName.empty() || !fileName.StartsWith("nvidia")) {
            continue;
        }

        auto deviceName = Format("/dev/%v", fileName);

        bool foundMetaDevice = false;
        for (const auto& metaDeviceName : GetMetaGpuDeviceNames()) {
            if (deviceName == metaDeviceName) {
                ++foundMetaDeviceCount;
                foundMetaDevice = true;
                break;
            }
        }

        if (foundMetaDevice) {
            continue;
        }

        if (auto deviceNumber = GetDeviceNumber(deviceName)) {
            YT_LOG_INFO("Found nvidia GPU device %Qv", deviceName);
            FreeSlots_.emplace_back(*deviceNumber);
            GpuDevices_.push_back(deviceName);
            HealthyGpuDeviceNumbers_.insert(*deviceNumber);
        }
    }

    if (foundMetaDeviceCount == GetMetaGpuDeviceNames().size()) {
        YT_LOG_INFO("Found %v nvidia GPUs", GpuDevices_.size());

        if (EnableHealthCheck_) {
            HealthCheckExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetControlInvoker(),
                BIND(&TGpuManager::OnHealthCheck, MakeWeak(this)),
                Config_->HealthCheckPeriod);

            HealthCheckExecutor_->Start();
        }
    } else {
        FreeSlots_.clear();
        GpuDevices_.clear();
        YT_LOG_INFO("Did not find any nvidia GPU, some meta devices were not found (MetaDevices: %v)", GetMetaGpuDeviceNames());
    }
}

void TGpuManager::OnHealthCheck()
{
    try {
        auto healthyGpuDeviceNumbers = GetHealthyGpuDeviceNumbers(Config_->HealthCheckTimeout);
        YT_LOG_DEBUG("Found healthy GPU devices (DeviceNumbers: %v)", healthyGpuDeviceNumbers);

        std::vector<TError> newAlerts;

        {
            TGuard<TSpinLock> guard(SpinLock_);
            HealthyGpuDeviceNumbers_ = std::move(healthyGpuDeviceNumbers);

            std::vector<TGpuSlot> healthySlots;
            for (auto& slot: FreeSlots_) {
                if (HealthyGpuDeviceNumbers_.find(slot.GetDeviceNumber()) == HealthyGpuDeviceNumbers_.end()) {
                    YT_LOG_WARNING("Found lost GPU device (DeviceName: %v)", slot.GetDeviceName());
                    newAlerts.push_back(TError("Found lost GPU device %v", slot.GetDeviceName()));
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
        Bootstrap_->GetMasterConnector()->RegisterAlert(TError("All GPU devices are disabled") << ex);
        HealthCheckExecutor_->Stop();

        TGuard<TSpinLock> guard(SpinLock_);
        Disabled_ = true;
    }
}

int TGpuManager::GetTotalGpuCount() const
{
    auto guard = Guard(SpinLock_);
    return Disabled_ ? 0 : HealthyGpuDeviceNumbers_.size();
}

int TGpuManager::GetFreeGpuCount() const
{
    auto guard = Guard(SpinLock_);
    return Disabled_ ? 0 : FreeSlots_.size();
}

const std::vector<TString>& TGpuManager::ListGpuDevices() const
{
    return GpuDevices_;
}

TGpuManager::TGpuSlotPtr TGpuManager::AcquireGpuSlot()
{
    YCHECK(!FreeSlots_.empty());

    auto deleter = [this, this_ = MakeStrong(this)] (TGpuSlot* slot) {
        YT_LOG_DEBUG("Released GPU slot (DeviceName: %v)", slot->GetDeviceName());
        auto guard = Guard(this_->SpinLock_);


        if (EnableHealthCheck_ && HealthyGpuDeviceNumbers_.find(slot->GetDeviceNumber()) == HealthyGpuDeviceNumbers_.end()) {
            guard.Release();
            YT_LOG_WARNING("Found lost GPU device (DeviceName: %v)", slot->GetDeviceName());
            Bootstrap_->GetMasterConnector()->RegisterAlert(TError("Found lost GPU device %v", slot->GetDeviceName()));
        } else {
            this_->FreeSlots_.emplace_back(std::move(*slot));
        }

        delete slot;
    };

    auto guard = Guard(SpinLock_);
    TGpuSlotPtr slot(new TGpuSlot(std::move(FreeSlots_.back())), deleter);
    FreeSlots_.pop_back();

    YT_LOG_DEBUG("Acquired GPU slot (DeviceName: %v)", slot->GetDeviceName());
    return slot;
}

const std::vector<TString>& TGpuManager::GetMetaGpuDeviceNames()
{
    static std::vector<TString> MetaGpuDevices = { "/dev/nvidiactl", "/dev/nvidia-uvm" };
    return MetaGpuDevices;
}

TString TGpuManager::GetDeviceName(int deviceNumber)
{
    return DevNvidia + ToString(deviceNumber);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
