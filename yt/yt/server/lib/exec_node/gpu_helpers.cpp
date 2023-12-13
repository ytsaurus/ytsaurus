#include "gpu_helpers.h"

#include "private.h"

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/library/process/subprocess.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <util/folder/iterator.h>

#include <util/string/strip.h>
#include <util/string/subst.h>
#include <util/string/split.h>
#include <util/string/join.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NGpu;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NJobAgent::JobAgentServerLogger;

static const TString DevNvidiaPath("/dev/nvidia");
static const TString DevPath("/dev");
static const TString NvidiaDevicePrefix("nvidia");
static const TString NvidiaModuleVersionPath("/sys/module/nvidia/version");
static const THashSet<TString> MetaGpuDevices = {
    "/dev/nvidiactl",
    "/dev/nvidia-uvm"
};

////////////////////////////////////////////////////////////////////////////////

std::optional<int> TryParseGpuDeviceNumber(const TString& deviceName)
{
    if (deviceName.length() <= DevNvidiaPath.length()) {
        return std::nullopt;
    }
    int deviceNumber;
    if (!TryFromString(deviceName.data() + DevNvidiaPath.length(), deviceNumber)) {
        return std::nullopt;
    }
    return deviceNumber;
}

std::vector<TGpuDeviceDescriptor> ListGpuDevices()
{
    int foundMetaDeviceCount = 0;
    std::vector<TGpuDeviceDescriptor> result;
    TDirIterator dir(DevPath, TDirIterator::TOptions().SetMaxLevel(1));
    for (auto file = dir.begin(); file != dir.end(); ++file) {
        if (file->fts_pathlen == file->fts_namelen || file->fts_pathlen <= DevPath.length()) {
            continue;
        }

        TStringBuf fileName(file->fts_path + DevPath.length() + 1);
        if (fileName.empty() || !fileName.StartsWith(NvidiaDevicePrefix)) {
            continue;
        }

        auto deviceName = Format("%v/%v", DevPath, fileName);

        if (MetaGpuDevices.contains(deviceName)) {
            YT_LOG_INFO("Nvidia meta GPU device found (Name: %v)",
                deviceName);
            ++foundMetaDeviceCount;
            continue;
        }

        auto deviceNumber = TryParseGpuDeviceNumber(deviceName);
        if (!deviceNumber) {
            continue;
        }

        YT_LOG_INFO("Nvidia GPU device found (Name: %v, Number: %v)",
            deviceName,
            deviceNumber);
        result.push_back({deviceName, *deviceNumber});
    }

    if (foundMetaDeviceCount < std::ssize(MetaGpuDevices)) {
        if (!result.empty()) {
            THROW_ERROR_EXCEPTION("Too few Nvidia meta GPU devices found, but nvidia devices presented");
        }
        YT_LOG_INFO("Too few Nvidia meta GPU devices found; assuming no device is present (Found: %v, Needed: %v)",
            foundMetaDeviceCount,
            MetaGpuDevices.size());
        result.clear();
    }

    return result;
}

TString GetGpuDeviceName(int deviceNumber)
{
    return DevNvidiaPath + ToString(deviceNumber);
}

void ProfileGpuInfo(NProfiling::ISensorWriter* writer, const TGpuInfo& gpuInfo)
{
    writer->AddGauge("/utilization_gpu_rate", gpuInfo.UtilizationGpuRate);
    writer->AddGauge("/utilization_memory_rate", gpuInfo.UtilizationMemoryRate);
    writer->AddGauge("/memory_used", gpuInfo.MemoryUsed);
    writer->AddGauge("/memory_limit", gpuInfo.MemoryTotal);
    writer->AddGauge("/power_used", gpuInfo.PowerDraw);
    writer->AddGauge("/power_limit", gpuInfo.PowerLimit);
    writer->AddGauge("/sm_utilization_rate", gpuInfo.SMUtilizationRate);
    writer->AddGauge("/sm_occupancy_rate", gpuInfo.SMOccupancyRate);
    writer->AddGauge("/stuck", static_cast<double>(gpuInfo.Stuck.Status));
}

TGpuDriverVersion TGpuDriverVersion::FromString(TStringBuf driverVersionString)
{
    std::vector<int> result;
    auto components = StringSplitter(driverVersionString).Split('.');

    try {
        for (const auto& component : components) {
            result.push_back(::FromString<int>(component));
        }
        return {result};
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Unable to parse driver version %v", driverVersionString) << ex;
    }
}

bool operator<(const TGpuDriverVersion& lhs, const TGpuDriverVersion& rhs)
{
    return lhs.Components < rhs.Components;
}

TString GetGpuDriverVersionString()
{
    try {
        TFileInput moduleVersion(NvidiaModuleVersionPath);
        return moduleVersion.ReadLine();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Unable to read GPU module version from %v", NvidiaModuleVersionPath) << ex;
    }
}

TString GetDummyGpuDriverVersionString()
{
    static TString version = "dummy";
    return version;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
