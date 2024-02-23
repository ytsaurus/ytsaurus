#include "nvidia_smi_gpu_info_provider.h"

#include "gpu_info_provider.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/library/process/subprocess.h>

#include <yt/yt/core/misc/error.h>

#include <util/string/join.h>
#include <util/string/split.h>

namespace NYT::NGpu {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf FatalErrorMessage = "Unable to determine";

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
        , ClocksSM(Register("clocks.sm"))
        , ClocksMaxSM(Register("clocks.max.sm"))
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
    int ClocksSM;
    int ClocksMaxSM;
};

////////////////////////////////////////////////////////////////////////////////

class TNvidiaSmiGpuInfoProvider
    : public IGpuInfoProvider
{
    std::vector<TGpuInfo> GetGpuInfos(TDuration timeout) const override
    {
        auto deadline = TInstant::Now() + timeout;
        auto remainingTimeout = timeout;

        auto checkAndUpdateTimeout = [&] {
            auto now = TInstant::Now();
            if (now > deadline) {
                THROW_ERROR_EXCEPTION("Failed to get GPU information within timeout")
                    << TErrorAttribute("timeout", timeout);
            } else {
                remainingTimeout = deadline - now;
            }
        };

        auto gpuNumbers = GetGpuMinorNumbers(remainingTimeout);
        checkAndUpdateTimeout();

        TGpuMetricsIndex metricsIndex;

        TSubprocess subprocess("nvidia-smi");
        subprocess.AddArguments({"--query-gpu=" + metricsIndex.GetQueryString(), "--format=csv,noheader,nounits"});

        auto nvidiaSmiResult = subprocess.Execute(
            /*input*/ TSharedRef::MakeEmpty(),
            remainingTimeout);
        checkAndUpdateTimeout();
        if (!nvidiaSmiResult.Status.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to check healthy GPUs: 'nvidia-smi --query-gpu' exited with an error")
                << nvidiaSmiResult.Status;
        }

        auto output = nvidiaSmiResult.Output.ToStringBuf();
        if (output.Contains(FatalErrorMessage)) {
            THROW_ERROR_EXCEPTION("Failed to check healthy GPUs: 'nvidia-smi --query-gpu' exited with fatal error");
        }

        std::vector<TGpuInfo> result;
        result.reserve(gpuNumbers.size());
        for (auto line : StringSplitter(output).Split('\n').SkipEmpty()) {
            std::vector<TStringBuf> tokens = StringSplitter(line).Split(',');
            if (tokens.size() != metricsIndex.Names.size()) {
                THROW_ERROR_EXCEPTION(
                    "Invalid 'nvidia-smi --query-gpu' output format: expected %v comma separated values, but got %Qv",
                    metricsIndex.Names.size(),
                    TString(line));
            }

            TGpuInfo gpuInfo;
            auto gpuId = StripString(tokens[metricsIndex.Uuid]);
            int gpuIndex = -1;
            {
                auto it = gpuNumbers.find(gpuId);
                if (it == gpuNumbers.end()) {
                    THROW_ERROR_EXCEPTION("Invalid 'nvidia-smi --query-gpu' output, GPU id %Qv is not found in 'nvidia-smi -q' output",
                        gpuId);
                }
                gpuIndex = it->second;
            }

            auto memoryUsed = FromString<i64>(StripString(tokens[metricsIndex.MemoryUsed])) * 1_MB;
            auto memoryTotal = FromString<i64>(StripString(tokens[metricsIndex.MemoryTotal])) * 1_MB;

            result.push_back(TGpuInfo{
                .Index = gpuIndex,
                .Name = SubstGlobalCopy(to_lower(TString(StripString(tokens[metricsIndex.Name]))), ' ', '_'),
                .UtilizationGpuRate = FromString<i64>(StripString(tokens[metricsIndex.UtilizationGpu])) / 100.0,
                .UtilizationMemoryRate = FromString<i64>(StripString(tokens[metricsIndex.UtilizationMemory])) / 100.0,
                .MemoryUsed = static_cast<i64>(memoryUsed),
                .MemoryTotal = static_cast<i64>(memoryTotal),
                .PowerDraw = FromString<double>(StripString(tokens[metricsIndex.PowerDraw])),
                .PowerLimit = FromString<double>(StripString(tokens[metricsIndex.PowerLimit])),
                .ClocksSM = FromString<i64>(StripString(tokens[metricsIndex.ClocksSM])),
                .ClocksMaxSM = FromString<i64>(StripString(tokens[metricsIndex.ClocksMaxSM])),
            });
        }

        return result;
    }

    std::vector<TRdmaDeviceInfo> GetRdmaDeviceInfos(TDuration /*timeout*/) const override
    {
        // NB(omgronny): RDMA info is not supported in this build.
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

IGpuInfoProviderPtr CreateNvidiaSmiGpuInfoProvider()
{
    return New<TNvidiaSmiGpuInfoProvider>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
