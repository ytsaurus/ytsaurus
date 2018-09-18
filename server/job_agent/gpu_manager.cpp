#include "gpu_manager.h"

#include "private.h"

#include <util/folder/iterator.h>

namespace NYT {
namespace NJobAgent {

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

static const TString DevNvidia("/dev/nvidia");

TNullable<int> GetDeviceNumber(const TString& deviceName)
{
    int deviceNumber;
    if (TryFromString(deviceName.data() + DevNvidia.length(), deviceNumber)) {
        return deviceNumber;
    } else {
        return Null;
    }
}

TGpuManager::TGpuManager()
{
    int foundMetaDeviceCount = 0;

    auto dirName = TString("/dev");
    TDirIterator dir(dirName, TDirIterator::TOptions().SetMaxLevel(1));
    for (auto file = dir.Begin(); file != dir.End(); ++file) {
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
            LOG_INFO("Found nvidia GPU device %Qv", deviceName);
            FreeSlots_.emplace_back(*deviceNumber);
            GpuDevices_.push_back(deviceName);
        }
    }

    if (foundMetaDeviceCount == GetMetaGpuDeviceNames().size()) {
        LOG_INFO("Found %v nvidia GPUs", GpuDevices_.size());
    } else {
        FreeSlots_.clear();
        GpuDevices_.clear();
        LOG_INFO("Did not find any nvidia GPU, some meta devices were not found (MetaDevices: %v)", GetMetaGpuDeviceNames());
    }
}

int TGpuManager::GetTotalGpuCount() const
{
    return GpuDevices_.size();
}

int TGpuManager::GetFreeGpuCount() const
{
    auto guard = Guard(SpinLock_);
    return FreeSlots_.size();
}

const std::vector<TString>& TGpuManager::ListGpuDevices() const
{
    return GpuDevices_;
}

TGpuManager::TGpuSlotPtr TGpuManager::AcquireGpuSlot()
{
    YCHECK(!FreeSlots_.empty());

    auto deleter = [this_ = MakeStrong(this)] (TGpuSlot* slot) {
        auto guard = Guard(this_->SpinLock_);
        this_->FreeSlots_.emplace_back(std::move(*slot));
        LOG_DEBUG("Released GPU slot (DeviceName: %v)", slot->GetDeviceName());

        delete slot;
    };

    auto guard = Guard(SpinLock_);
    TGpuSlotPtr slot(new TGpuSlot(std::move(FreeSlots_.back())), deleter);
    FreeSlots_.pop_back();

    LOG_DEBUG("Acquired GPU slot (DeviceName: %v)", slot->GetDeviceName());
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

} // namespace NJobAgent
} // namespace NYT