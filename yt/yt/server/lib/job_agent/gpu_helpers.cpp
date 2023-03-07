#include "gpu_helpers.h"

#include "private.h"

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/finally.h>
#include <yt/core/misc/proc.h>
#include <yt/library/process/subprocess.h>

#include <yt/core/concurrency/delayed_executor.h>

#include <util/folder/iterator.h>

#include <util/string/strip.h>
#include <util/string/subst.h>
#include <util/string/split.h>

namespace NYT::NJobAgent {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobAgentServerLogger;

static const auto FatalErrorMessage = "Unable to determine";
static const auto MinorNumberMessage = "Minor Number";
static const auto GpuUuidMessage = "GPU UUID";
static const TString DevNvidiaPath("/dev/nvidia");
static const TString DevPath("/dev");
static const TString NvidiaDevicePrefix("nvidia");
static const TString NvidiaModuleVersionPath("/sys/module/nvidia/version");
static const THashSet<TString> MetaGpuDevices = {
    "/dev/nvidiactl",
    "/dev/nvidia-uvm"
};

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, int> GetGpuIds(TDuration checkTimeout)
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
        THROW_ERROR_EXCEPTION("Failed to check healthy GPUs: 'nvidia-smi -q' exited with an error")
            << nvidiaSmiResult.Status;
    }

    auto output = TString(nvidiaSmiResult.Output.Begin(), nvidiaSmiResult.Output.End());
    if (output.find(FatalErrorMessage) != TString::npos) {
        THROW_ERROR_EXCEPTION("Failed to check healthy GPUs: 'nvidia-smi -q' exited with fatal error");
    }

    THashMap<TString, int> result;

    size_t pos = 0;
    while (true) {
        // Process GPU UUID.
        pos = output.find(GpuUuidMessage, pos);
        if (pos == TString::npos) {
            break;
        }

        TString gpuId;
        int gpuNumber;

        {
            auto semicolonPos = output.find(":", pos);
            auto eolPos = output.find("\n", pos);
            if (semicolonPos == TString::npos || eolPos == TString::npos || eolPos <= semicolonPos) {
                THROW_ERROR_EXCEPTION("Invalid 'nvidia-smi -q' output format: failed to parse GPU UUID");
            }
            gpuId = StripString(output.substr(semicolonPos + 1, eolPos - semicolonPos - 1));

            pos = eolPos;
        }

        // Process GPU Minor Number.
        pos = output.find(MinorNumberMessage, pos);
        if (pos == TString::npos) {
            THROW_ERROR_EXCEPTION("Invalid 'nvidia-smi -q' output format: failed to find Minor Number after GPU UUID");
        }

        {
            auto semicolonPos = output.find(":", pos);
            auto eolPos = output.find("\n", pos);
            if (semicolonPos == TString::npos || eolPos == TString::npos || eolPos <= semicolonPos) {
                THROW_ERROR_EXCEPTION("Invalid 'nvidia-smi -q' output format: failed to parse GPU Minor Number");
            }

            try {
                auto deviceNumberString = output.substr(semicolonPos + 1, eolPos - semicolonPos - 1);
                gpuNumber = FromString<int>(Strip(deviceNumberString));
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Invalid 'nvidia-smi -q' output format: failed to parse GPU Minor Number")
                    << ex;
            }

            pos = eolPos;
        }

        result.emplace(gpuId, gpuNumber);
    }

    return result;
}

std::vector<TGpuInfo> GetGpuInfos(TDuration checkTimeout)
{
    auto startTime = TInstant::Now();

    auto gpuIdToNumber = GetGpuIds(checkTimeout);

    auto remainingTimeout = checkTimeout - std::min((TInstant::Now() - startTime), checkTimeout);

    if (remainingTimeout == TDuration::Zero()) {
        THROW_ERROR_EXCEPTION("Getting gpu information timed out");
    }

    TSubprocess subprocess("nvidia-smi");
    subprocess.AddArguments({"--query-gpu=uuid,name,utilization.gpu,utilization.memory,memory.used", "--format=csv,noheader,nounits"});

    auto killCookie = TDelayedExecutor::Submit(
        BIND([&] () {
            try {
                subprocess.Kill(SIGKILL);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Failed to kill nvidia-smi process");
            }
        }),
        remainingTimeout);

    auto cookieGuard = Finally([&] () {
        TDelayedExecutor::CancelAndClear(killCookie);
    });

    auto nvidiaSmiResult = subprocess.Execute();
    if (!nvidiaSmiResult.Status.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to check healthy GPUs: 'nvidia-smi --query-gpu' exited with an error")
            << nvidiaSmiResult.Status;
    }

    auto output = TString(nvidiaSmiResult.Output.Begin(), nvidiaSmiResult.Output.End());
    if (output.find(FatalErrorMessage) != TString::npos) {
        THROW_ERROR_EXCEPTION("Failed to check healthy GPUs: 'nvidia-smi --query-gpu' exited with fatal error");
    }

    std::vector<TGpuInfo> result;
    for (TStringBuf line : StringSplitter(output).Split('\n').SkipEmpty()) {
        std::vector<TStringBuf> tokens = StringSplitter(line).Split(',');
        if (tokens.size() != 5) {
            THROW_ERROR_EXCEPTION(
                "Invalid 'nvidia-smi --query-gpu' output format: expected 4 comma separated values, but got %Qv",
                line);
        }

        TGpuInfo info;
        auto gpuId = StripString(tokens[0]);
        {
            auto it = gpuIdToNumber.find(gpuId);
            if (it == gpuIdToNumber.end()) {
                THROW_ERROR_EXCEPTION("Invalid 'nvidia-smi --query-gpu' output, gpu id %Qv is not found in 'nvidia-smi -q' output",
                    gpuId);
            }
            info.Index = it->second;
        }
        info.Name = SubstGlobalCopy(to_lower(TString(StripString(tokens[1]))), ' ', '_');
        info.UtilizationGpuRate = FromString<i64>(StripString(tokens[2])) / 100.0;
        info.UtilizationMemoryRate = FromString<i64>(StripString(tokens[3])) / 100.0;
        info.MemoryUsed = FromString<i64>(StripString(tokens[4])) * 1_MB;
        result.emplace_back(std::move(info));
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

void ProfileGpuInfo(NProfiling::TProfiler& profiler, const TGpuInfo& gpuInfo, const NProfiling::TTagIdList& tagIds)
{
    profiler.Enqueue("/utilization_gpu_rate_x1000", gpuInfo.UtilizationGpuRate, NProfiling::EMetricType::Gauge, tagIds);
    profiler.Enqueue("/utilization_memory_rate_x1000", gpuInfo.UtilizationMemoryRate, NProfiling::EMetricType::Gauge, tagIds);
    profiler.Enqueue("/memory_used", gpuInfo.MemoryUsed, NProfiling::EMetricType::Gauge, tagIds);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
