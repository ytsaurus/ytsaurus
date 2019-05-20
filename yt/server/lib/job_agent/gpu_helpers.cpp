#include "gpu_helpers.h"

#include "private.h"

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/finally.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/subprocess.h>

#include <yt/core/concurrency/delayed_executor.h>

#include <util/folder/iterator.h>

#include <util/string/strip.h>

namespace NYT::NJobAgent {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobAgentServerLogger;

static const auto FatalErrorMessage = "Unable to determine";
static const auto MinorNumberMessage = "Minor Number";
static const TString DevNvidiaPath("/dev/nvidia");
static const TString DevPath("/dev");
static const TString NvidiaDevicePrefix("nvidia");
static const THashSet<TString> MetaGpuDevices = {
    "/dev/nvidiactl",
    "/dev/nvidia-uvm"
};

////////////////////////////////////////////////////////////////////////////////

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
            THROW_ERROR_EXCEPTION("Failed to parse minor GPU device number")
                << ex;
        }

        pos = eolPos;
    }

    return result;
}

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

    if (foundMetaDeviceCount < MetaGpuDevices.size()) {
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
