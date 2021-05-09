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

struct TGpuMetricsIndex
{
    TGpuMetricsIndex()
        : Uuid(Register("uuid"))
        , Name(Register("name"))
        , UtilizationGpu(Register("utilization.gpu"))
        , UtilizationMemory(Register("utilization.memory"))
        , MemoryUsed(Register("memory.used"))
        , MemoryTotal(Register("memory.total"))
        , PowerDraw(Register("power.draw"))
        , PowerLimit(Register("power.limit"))
        , ClocksSm(Register("clocks.sm"))
        , ClocksMaxSm(Register("clocks.max.sm"))
    { }

    int Register(const TString& name)
    {
        int index = Names.size();
        Names.push_back(name);
        return index;
    }

    TString GetQueryString() const
    {
        return JoinSeq(",", Names);
    }

    std::vector<TString> Names;

    int Uuid;
    int Name;
    int UtilizationGpu;
    int UtilizationMemory;
    int MemoryUsed;
    int MemoryTotal;
    int PowerDraw;
    int PowerLimit;
    int ClocksSm;
    int ClocksMaxSm;
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

    auto cookieGuard = Finally([&] {
        TDelayedExecutor::Cancel(killCookie);
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
                gpuNumber = FromString<int>(StripInPlace(deviceNumberString));
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

    TGpuMetricsIndex Index;

    TSubprocess subprocess("nvidia-smi");
    subprocess.AddArguments({"--query-gpu=" + Index.GetQueryString(), "--format=csv,noheader,nounits"});

    auto killCookie = TDelayedExecutor::Submit(
        BIND([&] () {
            try {
                subprocess.Kill(SIGKILL);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Failed to kill nvidia-smi process");
            }
        }),
        remainingTimeout);

    auto cookieGuard = Finally([&] {
        TDelayedExecutor::Cancel(killCookie);
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
        if (tokens.size() != Index.Names.size()) {
            THROW_ERROR_EXCEPTION(
                "Invalid 'nvidia-smi --query-gpu' output format: expected %v comma separated values, but got %Qv",
                Index.Names.size(),
                line);
        }

        TGpuInfo info;
        auto gpuId = StripString(tokens[Index.Uuid]);
        {
            auto it = gpuIdToNumber.find(gpuId);
            if (it == gpuIdToNumber.end()) {
                THROW_ERROR_EXCEPTION("Invalid 'nvidia-smi --query-gpu' output, gpu id %Qv is not found in 'nvidia-smi -q' output",
                    gpuId);
            }
            info.Index = it->second;
        }
        info.Name = SubstGlobalCopy(to_lower(TString(StripString(tokens[Index.Name]))), ' ', '_');
        info.UtilizationGpuRate = FromString<i64>(StripString(tokens[Index.UtilizationGpu])) / 100.0;
        info.UtilizationMemoryRate = FromString<i64>(StripString(tokens[Index.UtilizationMemory])) / 100.0;
        info.MemoryUsed = FromString<i64>(StripString(tokens[Index.MemoryUsed])) * 1_MB;
        info.MemoryTotal = FromString<i64>(StripString(tokens[Index.MemoryTotal])) * 1_MB;
        info.PowerDraw = FromString<double>(StripString(tokens[Index.PowerDraw]));
        info.PowerLimit = FromString<double>(StripString(tokens[Index.PowerLimit]));
        info.ClocksSm = FromString<i64>(StripString(tokens[Index.ClocksSm]));
        info.ClocksMaxSm = FromString<i64>(StripString(tokens[Index.ClocksMaxSm]));
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

    if (foundMetaDeviceCount < std::ssize(MetaGpuDevices)) {
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
    writer->AddGauge("/utilization_gpu_rate_x1000", gpuInfo.UtilizationGpuRate);
    writer->AddGauge("/utilization_memory_rate_x1000", gpuInfo.UtilizationMemoryRate);
    writer->AddGauge("/memory_used", gpuInfo.MemoryUsed);
    writer->AddGauge("/memory_limit", gpuInfo.MemoryTotal);
    writer->AddGauge("/power_used", gpuInfo.PowerDraw);
    writer->AddGauge("/power_limit", gpuInfo.PowerLimit);
    writer->AddGauge("/clocks_sm_used", gpuInfo.ClocksSm);
    writer->AddGauge("/clocks_sm_limit", gpuInfo.ClocksMaxSm);
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

} // namespace NYT::NJobAgent
