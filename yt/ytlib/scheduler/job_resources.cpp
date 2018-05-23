#include "job_resources.h"

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NScheduler {

using namespace NYson;
using namespace NYTree;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NProfiling;

using std::round;

////////////////////////////////////////////////////////////////////////////////

//! Nodes having less free memory are considered fully occupied,
//! thus no scheduling attempts will be made.
static const i64 LowWatermarkMemorySize = 256_MB;

////////////////////////////////////////////////////////////////////////////////

i64 TExtendedJobResources::GetMemory() const
{
    return JobProxyMemory_ + UserJobMemory_ + FootprintMemory_;
}

void Serialize(const TExtendedJobResources& resources, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("cpu").Value(resources.GetCpu())
            .Item("gpu").Value(resources.GetGpu())
            .Item("user_slots").Value(resources.GetUserSlots())
            .Item("job_proxy_memory").Value(resources.GetJobProxyMemory())
            .Item("user_job_memory").Value(resources.GetUserJobMemory())
            .Item("footprint_memory").Value(resources.GetFootprintMemory())
            .Item("network").Value(resources.GetNetwork())
        .EndMap();
}

void TExtendedJobResources::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Cpu_);
    Persist(context, Gpu_);
    Persist(context, UserSlots_);
    Persist(context, JobProxyMemory_);
    Persist(context, UserJobMemory_);
    Persist(context, FootprintMemory_);
    Persist(context, Network_);
}

TJobResources::TJobResources(const TNodeResources& resources)
    : TEmptyJobResourcesBase()
#define XX(name, Name) , Name##_(resources.name())
ITERATE_JOB_RESOURCES(XX)
#undef XX
{ }

TNodeResources TJobResources::ToNodeResources() const
{
    TNodeResources result;
    #define XX(name, Name) result.set_##name(static_cast<decltype(result.name())>(Name##_));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

void TJobResources::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    #define XX(name, Name) Persist(context, Name##_);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

TString FormatResource(
    const TJobResources& usage,
    const TJobResources& limits)
{
    return Format(
        "UserSlots: %v/%v, Cpu: %v/%v, Gpu: %v/%v, Memory: %v/%v, Network: %v/%v",
        // User slots
        usage.GetUserSlots(),
        limits.GetUserSlots(),
        // Cpu
        usage.GetCpu(),
        limits.GetCpu(),
        // Gpu
        usage.GetGpu(),
        limits.GetGpu(),
        // Memory (in MB)
        usage.GetMemory() / (1024 * 1024),
        limits.GetMemory() / (1024 * 1024),
        // Network
        usage.GetNetwork(),
        limits.GetNetwork());
}

TString FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits)
{
    return Format("{%v}", FormatResource(usage, limits));
}

TString FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits,
    const NNodeTrackerClient::NProto::TDiskResources& diskInfo)
{
    return Format("{%v, DiskInfo: %v}", FormatResource(usage, limits), NNodeTrackerClient::ToString(diskInfo));
}

TString FormatResources(const TJobResources& resources)
{
    return Format(
        "{UserSlots: %v, Cpu: %v, Gpu: %v, Memory: %v, Network: %v}",
        resources.GetUserSlots(),
        resources.GetCpu(),
        resources.GetGpu(),
        resources.GetMemory() / 1_MB,
        resources.GetNetwork());
}

TString FormatResources(const TJobResourcesWithQuota& resources)
{
    return Format(
        "{UserSlots: %v, Cpu: %v, Gpu: %v, Memory: %v, Network: %v, DiskQuota: %v}",
        resources.GetUserSlots(),
        resources.GetCpu(),
        resources.GetGpu(),
        resources.GetMemory() / 1_MB,
        resources.GetNetwork(),
        resources.GetDiskQuota()
    );
}


TString FormatResources(const TExtendedJobResources& resources)
{
    return Format(
        "{UserSlots: %v, Cpu: %v, Gpu: %v, JobProxyMemory: %v, UserJobMemory: %v, FootprintMemory: %v, Network: %v}",
        resources.GetUserSlots(),
        resources.GetCpu(),
        resources.GetGpu(),
        resources.GetJobProxyMemory() / 1_MB,
        resources.GetUserJobMemory() / 1_MB,
        resources.GetFootprintMemory() / 1_MB,
        resources.GetNetwork());
}

void ProfileResources(
    const TProfiler& profiler,
    const TJobResources& resources,
    const TString& prefix,
    const TTagIdList& tagIds)
{
    #define XX(name, Name) profiler.Enqueue(prefix + "/" #name, static_cast<i64>(resources.Get##Name()), EMetricType::Gauge, tagIds);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

EResourceType GetDominantResource(
    const TJobResources& demand,
    const TJobResources& limits)
{
    auto maxType = EResourceType::Cpu;
    double maxRatio = 0.0;
    auto update = [&] (auto a, auto b, EResourceType type) {
        if (b > 0) {
            double ratio = static_cast<double>(a) / static_cast<double>(b);
            if (ratio > maxRatio) {
                maxRatio = ratio;
                maxType = type;
            }
        }
    };
    #define XX(name, Name) update(demand.Get##Name(), limits.Get##Name(), EResourceType::Name);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return maxType;
}

double GetDominantResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits)
{
    double maxRatio = 0.0;
    auto update = [&] (auto a, auto b, EResourceType type) {
        if (b > 0) {
            double ratio = static_cast<double>(a) / static_cast<double>(b);
            if (ratio > maxRatio) {
                maxRatio = ratio;
            }
        }
    };
    #define XX(name, Name) update(usage.Get##Name(), limits.Get##Name(), EResourceType::Name);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return maxRatio;
}

double GetResource(const TJobResources& resources, EResourceType type)
{
    switch (type) {
        #define XX(name, Name) \
            case EResourceType::Name: \
                return static_cast<double>(resources.Get##Name());
        ITERATE_JOB_RESOURCES(XX)
        #undef XX
        default:
            Y_UNREACHABLE();
    }
}

double GetMinResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator)
{
    double result = std::numeric_limits<double>::infinity();
    auto update = [&] (auto a, auto b) {
        if (b > 0) {
            result = std::min(result, static_cast<double>(a) / static_cast<double>(b));
        }
    };
    #define XX(name, Name) update(nominator.Get##Name(), denominator.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

double GetMaxResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator)
{
    double result = 0.0;
    auto update = [&] (auto a, auto b) {
        if (b > 0) {
            result = std::max(result, static_cast<double>(a) / static_cast<double>(b));
        }
    };
    #define XX(name, Name) update(nominator.Get##Name(), denominator.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources GetAdjustedResourceLimits(
    const TJobResources& demand,
    const TJobResources& limits,
    const TMemoryDistribution& execNodeMemoryDistribution)
{
    auto adjustedLimits = limits;

    // Take memory granularity into account.
    if (demand.GetUserSlots() > 0 && !execNodeMemoryDistribution.empty()) {
        i64 memoryDemandPerJob = demand.GetMemory() / demand.GetUserSlots();
        if (memoryDemandPerJob != 0) {
            i64 newMemoryLimit = 0;
            for (const auto& pair : execNodeMemoryDistribution) {
                const auto memoryLimitPerNode = pair.first;
                const auto nodeCount = pair.second;
                i64 slotsPerNode = memoryLimitPerNode / memoryDemandPerJob;
                i64 adjustedMemoryLimit = slotsPerNode * memoryDemandPerJob * nodeCount;
                newMemoryLimit += adjustedMemoryLimit;
            }
            adjustedLimits.SetMemory(newMemoryLimit);
        }
    }

    return adjustedLimits;
}

const TJobResources& ZeroJobResources()
{
    static auto value = TJobResources();
    return value;
}

const TJobResourcesWithQuota& ZeroJobResourcesWithQuota()
{
    static auto value = TJobResourcesWithQuota();
    return value;
}

TJobResources GetInfiniteResources()
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(std::numeric_limits<decltype(result.Get##Name())>::max() / 4);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResourcesWithQuota GetInfiniteResourcesWithQuota()
{
    return TJobResourcesWithQuota(GetInfiniteResources());
}

const TJobResources& InfiniteJobResources()
{
    static auto result = GetInfiniteResources();
    return result;
}

const TJobResourcesWithQuota& InfiniteJobResourcesWithQuota()
{
    static auto result = GetInfiniteResourcesWithQuota();
    return result;
}

TJobResources operator + (const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(lhs.Get##Name() + rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources& operator += (TJobResources& lhs, const TJobResources& rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() + rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TJobResources operator - (const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(lhs.Get##Name() - rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources& operator -= (TJobResources& lhs, const TJobResources& rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() - rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TJobResources operator * (const TJobResources& lhs, i64 rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(lhs.Get##Name() * rhs);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources operator * (const TJobResources& lhs, double rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(static_cast<decltype(lhs.Get##Name())>(round(lhs.Get##Name() * rhs)));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources& operator *= (TJobResources& lhs, i64 rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() * rhs);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TJobResources& operator *= (TJobResources& lhs, double rhs)
{
    #define XX(name, Name) lhs.Set##Name(static_cast<decltype(lhs.Get##Name())>(round(lhs.Get##Name() * rhs)));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TJobResources  operator - (const TJobResources& resources)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(-resources.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

bool operator == (const TJobResources& lhs, const TJobResources& rhs)
{
    return
    #define XX(name, Name) lhs.Get##Name() == rhs.Get##Name() &&
        ITERATE_JOB_RESOURCES(XX)
    #undef XX
    true;
}

bool operator != (const TJobResources& lhs, const TJobResources& rhs)
{
    return !(lhs == rhs);
}

bool Dominates(const TJobResources& lhs, const TJobResources& rhs)
{
    return
    #define XX(name, Name) lhs.Get##Name() >= rhs.Get##Name() &&
        ITERATE_JOB_RESOURCES(XX)
    #undef XX
    true;
}

TJobResources Max(const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(std::max(lhs.Get##Name(), rhs.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources Min(const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(std::min(lhs.Get##Name(), rhs.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResourcesWithQuota Min(const TJobResourcesWithQuota& a, const TJobResourcesWithQuota& b)
{
    TJobResourcesWithQuota result;
    #define XX(name, Name) result.Set##Name(std::min(a.Get##Name(), b.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    result.SetDiskQuota(std::min(a.GetDiskQuota(), b.GetDiskQuota()));
    return result;
}

void Serialize(const TJobResources& resources, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
    #define XX(name, Name) .Item(#name).Value(resources.Get##Name())
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
            // COMPAT(psushin): fix for a web-face.
            .Item("memory").Value(resources.GetMemory())
        .EndMap();
}

TJobResources GetMinSpareResources()
{
    TJobResources result;
    result.SetUserSlots(1);
    result.SetCpu(1);
    result.SetGpu(0);
    result.SetMemory(LowWatermarkMemorySize);
    return result;
}

const TJobResources& MinSpareNodeResources()
{
    static auto result = GetMinSpareResources();
    return result;
}

bool CanSatisfyDiskRequest(
    const NNodeTrackerClient::NProto::TDiskResources& diskInfo,
    i64 diskRequest)
{
    auto info = diskInfo.disk_reports();
    auto it = info.begin();
    while (it != info.end()) {
        if (diskRequest < it->limit() - it->usage()) {
            return true;
        }
        ++it;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NScheduler::NProto::TJobResources* protoResources, const NScheduler::TJobResources& resources)
{
    protoResources->set_cpu(static_cast<double>(resources.GetCpu()));
    protoResources->set_gpu(resources.GetGpu());
    protoResources->set_user_slots(resources.GetUserSlots());
    protoResources->set_memory(resources.GetMemory());
    protoResources->set_network(resources.GetNetwork());
}

void FromProto(NScheduler::TJobResources* resources, const NScheduler::NProto::TJobResources& protoResources)
{
    resources->SetCpu(TCpuResource(protoResources.cpu()));
    resources->SetGpu(protoResources.gpu());
    resources->SetUserSlots(protoResources.user_slots());
    resources->SetMemory(protoResources.memory());
    resources->SetNetwork(protoResources.network());
}

void ToProto(NScheduler::NProto::TJobResourcesWithQuota* protoResources, const NScheduler::TJobResourcesWithQuota& resources)
{
    protoResources->set_cpu(static_cast<double>(resources.GetCpu()));
    protoResources->set_gpu(resources.GetGpu());
    protoResources->set_user_slots(resources.GetUserSlots());
    protoResources->set_memory(resources.GetMemory());
    protoResources->set_network(resources.GetNetwork());
    protoResources->set_disk_quota(resources.GetDiskQuota());
}

void FromProto(NScheduler::TJobResourcesWithQuota* resources, const NScheduler::NProto::TJobResourcesWithQuota& protoResources)
{
    resources->SetCpu(TCpuResource(protoResources.cpu()));
    resources->SetGpu(protoResources.gpu());
    resources->SetUserSlots(protoResources.user_slots());
    resources->SetMemory(protoResources.memory());
    resources->SetNetwork(protoResources.network());
    resources->SetDiskQuota(protoResources.disk_quota());
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

