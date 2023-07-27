#include "gpu_info_provider.h"

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/grpc/channel.h>
#include <yt/yt/core/rpc/grpc/config.h>

#include <yt/yt/library/process/subprocess.h>

#include <util/folder/iterator.h>

#include <util/string/strip.h>
#include <util/string/subst.h>
#include <util/string/split.h>
#include <util/string/join.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <infra/rsm/nvgpumanager/api/nvgpu.pb.h>

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

static NRpc::IChannelPtr CreateChannel(const TString& address)
{
    auto channelConfig = New<NRpc::NGrpc::TChannelConfig>();
    channelConfig->Address = address;
    return NRpc::NGrpc::CreateGrpcChannel(channelConfig);
}

////////////////////////////////////////////////////////////////////////////////

static THashMap<TString, int> GetGpuIds(TDuration checkTimeout)
{
    TSubprocess subprocess("nvidia-smi");
    subprocess.AddArguments({ "-q" });

    auto killCookie = TDelayedExecutor::Submit(
        BIND([process=subprocess.GetProcess()] () {
            try {
                process->Kill(SIGKILL);
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

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

void FromProto(TCondition* condition, const nvgpu::Condition& protoCondition)
{
    condition->Status = protoCondition.status();
    if (protoCondition.has_last_transition_time()) {
        condition->LastTransitionTime = NProtoInterop::CastFromProto(protoCondition.last_transition_time());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

using TReqListDevices = nvgpu::Empty;
using TRspListDevices = nvgpu::ListResponse;

class TNvGpuManagerService
    : public NYT::NRpc::TProxyBase
{
public:
    TNvGpuManagerService(NYT::NRpc::IChannelPtr channel, TString serviceName)
        : TProxyBase(std::move(channel), NYT::NRpc::TServiceDescriptor(std::move(serviceName)))
    { }

    DEFINE_RPC_PROXY_METHOD(NJobAgent, ListDevices);
};

void FromProto(TGpuInfo* gpuInfo, int index, const nvgpu::GpuDevice& device)
{
    const auto& spec = device.spec().nvidia();
    const auto& status = device.status().nvidia();

    gpuInfo->Index = index;
    gpuInfo->UtilizationGpuRate = status.gpu_utilization() / 100.0;
    gpuInfo->UtilizationMemoryRate = status.memory_utilization() / 100.0;
    gpuInfo->MemoryUsed = status.memory_used_mb() * 1_MB;
    gpuInfo->MemoryTotal = spec.memory_size_mb() * 1_MB;
    gpuInfo->PowerDraw = status.power();
    gpuInfo->PowerLimit = spec.power();
    gpuInfo->SMUtilizationRate = std::max(0.0, static_cast<double>(status.sm_utilization())) / 100.0;
    gpuInfo->SMOccupancyRate = std::max(0.0, static_cast<double>(status.sm_occupancy())) / 100.0;
    gpuInfo->Name = spec.uuid();
    gpuInfo->Stuck = NYT::FromProto<NDetail::TCondition>(status.stuck());
}

////////////////////////////////////////////////////////////////////////////////

class TNvManagerGpuInfoProvider
    : public IGpuInfoProvider
{
public:
    TNvManagerGpuInfoProvider(const TString& address, const TString serviceName, bool getGpuIndexesFromNvidiaSmi)
        : Channel_(CreateChannel(address))
        , ServiceName_(std::move(serviceName))
        , GetGpuIndexesFromNvidiaSmi_(getGpuIndexesFromNvidiaSmi)
    { }

    virtual std::vector<TGpuInfo> GetGpuInfos(TDuration checkTimeout) override
    {
        // COMPAT(ignat): temporary fix for stable numeration.
        THashMap<TString, int> gpuIdToNumber;
        if (GetGpuIndexesFromNvidiaSmi_) {
            gpuIdToNumber = GetGpuIds(checkTimeout);
        }

        TNvGpuManagerService proxy(Channel_, ServiceName_);
        auto req = proxy.ListDevices();
        auto rsp = WaitFor(
            req->Invoke()
            .WithTimeout(checkTimeout))
            .ValueOrThrow();
        std::vector<TGpuInfo> gpuInfos;
        gpuInfos.reserve(rsp->devices_size());

        int index = 0;
        for (const auto& device : rsp->devices()) {
            if (device.spec().has_nvidia() && device.status().has_nvidia()) {
                int deviceIndex;
                if (GetGpuIndexesFromNvidiaSmi_) {
                    const auto& gpuId = device.spec().nvidia().uuid();
                    auto it = gpuIdToNumber.find(gpuId);
                    if (it == gpuIdToNumber.end()) {
                        THROW_ERROR_EXCEPTION("Invalid 'nvidia-smi --query-gpu' output, gpu id %Qv is not found in 'nvidia-smi -q' output",
                            gpuId);
                    }
                    deviceIndex = it->second;
                } else {
                    deviceIndex = index;
                }

                FromProto(&gpuInfos.emplace_back(), deviceIndex, device);
            }
            ++index;
        }
        return gpuInfos;
    }

private:
    NRpc::IChannelPtr Channel_;
    TString ServiceName_;
    bool GetGpuIndexesFromNvidiaSmi_;
};

////////////////////////////////////////////////////////////////////////////////

class TNvidiaSmiGpuInfoProvider
    : public IGpuInfoProvider
{
public:
    virtual std::vector<TGpuInfo> GetGpuInfos(TDuration checkTimeout) override
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
};

////////////////////////////////////////////////////////////////////////////////

IGpuInfoProviderPtr CreateGpuInfoProvider(const TGpuInfoSourceConfigPtr& gpuInfoSource)
{
    switch (gpuInfoSource->Type) {
        case EGpuInfoSourceType::NvGpuManager:
            return New<TNvManagerGpuInfoProvider>(
                gpuInfoSource->NvGpuManagerServiceAddress,
                gpuInfoSource->NvGpuManagerServiceName,
                gpuInfoSource->GpuIndexesFromNvidiaSmi);
        case EGpuInfoSourceType::NvidiaSmi:
            return New<TNvidiaSmiGpuInfoProvider>();
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
