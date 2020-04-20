#include "job_resources.h"
#include "config.h"

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

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

TJobResources TJobResources::Infinite()
{
    TJobResources result;
#define XX(name, Name) result.Set##Name(std::numeric_limits<decltype(result.Get##Name())>::max() / 4);
    ITERATE_JOB_RESOURCES(XX)
#undef XX
    return result;
}

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

////////////////////////////////////////////////////////////////////////////////

void TDiskQuota::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, DiskSpacePerMedium);
}

TString ToString(NScheduler::TDiskQuota diskQuota)
{
    return Format(
        "%v",
        MakeFormattableView(diskQuota.DiskSpacePerMedium, [] (TStringBuilderBase* builder, const std::pair<int, i64>& pair) {
            auto [mediumIndex, diskSpace] = pair;
            builder->AppendFormat("{MediumIndex: %v, DiskSpace: %v}", mediumIndex, diskSpace);
        })
    );
}

TDiskQuota CreateDiskQuota(i32 mediumIndex, i64 diskSpace)
{
    TDiskQuota result;
    result.DiskSpacePerMedium.insert(std::make_pair(mediumIndex, diskSpace));
    return result;
}

TDiskQuota Min(TDiskQuota lhs, TDiskQuota rhs)
{
    TDiskQuota result;
    for (auto [mediumIndex, diskSpace] : lhs.DiskSpacePerMedium) {
        auto it = rhs.DiskSpacePerMedium.find(mediumIndex);
        if (it != rhs.DiskSpacePerMedium.end()) {
            result.DiskSpacePerMedium[mediumIndex] = std::min(diskSpace, it->second);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TJobResourcesWithQuota::TJobResourcesWithQuota(const TJobResources& jobResources)
    : TJobResources(jobResources)
{ }

TJobResourcesWithQuota TJobResourcesWithQuota::Infinite()
{
    return TJobResourcesWithQuota(TJobResources::Infinite());
}

TJobResources TJobResourcesWithQuota::ToJobResources() const
{
    return *this;
}

void TJobResourcesWithQuota::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    TJobResources::Persist(context);
    Persist(context, DiskQuota_);
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
    const NNodeTrackerClient::NProto::TDiskResources& diskResources,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
{
    return Format("{%v, DiskResources: %v}",
        FormatResource(usage, limits),
        NNodeTrackerClient::ToString(diskResources, mediumDirectory));
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

TString FormatResources(
    const TJobResourcesWithQuota& resources,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
{
    TStringBuilder stringBuilder;
    for (auto [mediumIndex, diskSpace] : resources.GetDiskQuota().DiskSpacePerMedium) {
        stringBuilder.AppendFormat("{DiskSpace: %v, MediumName: %v}",
            diskSpace,
            mediumDirectory->FindByIndex(mediumIndex)->Name);
    }

    return Format(
        "{UserSlots: %v, Cpu: %v, Gpu: %v, Memory: %v, Network: %v, DiskQuota: %v}",
        resources.GetUserSlots(),
        resources.GetCpu(),
        resources.GetGpu(),
        resources.GetMemory() / 1_MB,
        resources.GetNetwork(),
        stringBuilder.Flush()
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

void ProfileResources(
    NProfiling::TMetricsAccumulator& accumulator,
    const TJobResources& resources,
    const TString& prefix,
    const NProfiling::TTagIdList& tagIds)
{
    #define XX(name, Name) accumulator.Add(prefix + "/" #name, static_cast<i64>(resources.Get##Name()), EMetricType::Gauge, tagIds);
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
            YT_ABORT();
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
            for (const auto& [memoryLimitPerNode, nodeCount] : execNodeMemoryDistribution) {
                i64 slotsPerNode = memoryLimitPerNode / memoryDemandPerJob;
                i64 adjustedMemoryLimit = slotsPerNode * memoryDemandPerJob * nodeCount;
                newMemoryLimit += adjustedMemoryLimit;
            }
            adjustedLimits.SetMemory(newMemoryLimit);
        }
    }

    return adjustedLimits;
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

bool Dominates(const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs)
{
    bool result =
    #define XX(name, Name) lhs.Get##Name() >= rhs.Get##Name() &&
        ITERATE_JOB_RESOURCES(XX)
    #undef XX
    true;
    auto rhsDiskQuota = rhs.GetDiskQuota();
    for (auto [mediumIndex, diskSpace] : lhs.GetDiskQuota().DiskSpacePerMedium) {
        auto it = rhsDiskQuota.DiskSpacePerMedium.find(mediumIndex);
        if (it != rhsDiskQuota.DiskSpacePerMedium.end() && diskSpace < it->second) {
            return false;
        }
    }
    return result;
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

TJobResourcesWithQuota Min(const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs)
{
    TJobResourcesWithQuota result;
    #define XX(name, Name) result.Set##Name(std::min(lhs.Get##Name(), rhs.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    result.SetDiskQuota(Min(lhs.GetDiskQuota(), rhs.GetDiskQuota()));
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


bool CanSatisfyDiskQuotaRequests(
    std::vector<i64> availableDiskSpacePerLocation,
    std::vector<i64> diskSpaceRequests)
{
    std::sort(availableDiskSpacePerLocation.begin(), availableDiskSpacePerLocation.end());
    std::sort(diskSpaceRequests.begin(), diskSpaceRequests.end(), std::greater<i64>());

    for (i64 diskSpace : diskSpaceRequests) {
        auto it = std::lower_bound(availableDiskSpacePerLocation.begin(), availableDiskSpacePerLocation.end(), diskSpace);
        if (it == availableDiskSpacePerLocation.end()) {
            return false;
        }
        *it -= diskSpace;
        while (it != availableDiskSpacePerLocation.begin() && *it < *(it - 1)) {
            std::swap(*it, *(it - 1));
            --it;
        }
    }

    return true;
}

bool CanSatisfyDiskQuotaRequest(
    const std::vector<i64>& availableDiskSpacePerLocation,
    i64 diskSpaceRequest)
{
    for (i64 availableDiskSpace : availableDiskSpacePerLocation) {
        if (diskSpaceRequest <= availableDiskSpace) {
            return true;
        }
    }
    return false;
}

bool CanSatisfyDiskQuotaRequest(
    const NNodeTrackerClient::NProto::TDiskResources& diskResources,
    TDiskQuota diskQuotaRequest)
{
    THashMap<int, std::vector<i64>> availableDiskSpacePerMedium;
    for (const auto& diskLocationResources : diskResources.disk_location_resources()) {
        availableDiskSpacePerMedium[diskLocationResources.medium_index()].push_back(
            diskLocationResources.limit() - diskLocationResources.usage());
    }
    for (auto [mediumIndex, diskSpace] : diskQuotaRequest.DiskSpacePerMedium) {
        if (!CanSatisfyDiskQuotaRequest(availableDiskSpacePerMedium[mediumIndex], diskSpace)) {
            return false;
        }
    }
    return true;
}

bool CanSatisfyDiskQuotaRequests(
    const NNodeTrackerClient::NProto::TDiskResources& diskResources,
    const std::vector<TDiskQuota>& diskQuotaRequests)
{
    THashMap<int, std::vector<i64>> availableDiskSpacePerMedium;
    for (const auto& diskLocationResources : diskResources.disk_location_resources()) {
        availableDiskSpacePerMedium[diskLocationResources.medium_index()].push_back(
            diskLocationResources.limit() - diskLocationResources.usage());
    }

    THashMap<int, std::vector<i64>> diskSpaceRequestsPerMedium;
    for (const auto& diskQuotaRequest : diskQuotaRequests) {
        for (auto [mediumIndex, diskSpace] : diskQuotaRequest.DiskSpacePerMedium) {
            diskSpaceRequestsPerMedium[mediumIndex].push_back(diskSpace);
        }
    }

    for (const auto& [mediumIndex, diskSpaceRequests] : diskSpaceRequestsPerMedium) {
        if (!CanSatisfyDiskQuotaRequests(availableDiskSpacePerMedium[mediumIndex], diskSpaceRequests)) {
            return false;
        }
    }
    return true;
}

TDiskQuota GetMaxAvailableDiskSpace(
    const NNodeTrackerClient::NProto::TDiskResources& diskResources)
{
    TDiskQuota result;
    for (const auto& diskLocationResources : diskResources.disk_location_resources()) {
        result.DiskSpacePerMedium[diskLocationResources.medium_index()] = std::max(
            result.DiskSpacePerMedium[diskLocationResources.medium_index()],
            diskLocationResources.limit() - diskLocationResources.usage());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NScheduler::NProto::TDiskQuota* protoDiskQuota, const NScheduler::TDiskQuota& diskQuota)
{
    for (auto [mediumIndex, diskSpace] : diskQuota.DiskSpacePerMedium) {
        auto* protoDiskLocationQuotaProto = protoDiskQuota->add_disk_location_quota();
        protoDiskLocationQuotaProto->set_medium_index(mediumIndex);
        protoDiskLocationQuotaProto->set_disk_space(diskSpace);
    }
}

void FromProto(NScheduler::TDiskQuota* diskQuota, const NScheduler::NProto::TDiskQuota& protoDiskQuota)
{
    for (const auto& protoDiskLocationQuota : protoDiskQuota.disk_location_quota()) {
        diskQuota->DiskSpacePerMedium.emplace(protoDiskLocationQuota.medium_index(), protoDiskLocationQuota.disk_space());
    }
}

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

    auto diskQuota = resources.GetDiskQuota();
    ToProto(protoResources->mutable_disk_quota(), diskQuota);
    auto defaultMediumIt = diskQuota.DiskSpacePerMedium.find(NChunkClient::DefaultSlotsMediumIndex);
    if (defaultMediumIt != diskQuota.DiskSpacePerMedium.end()) {
        protoResources->set_disk_quota_legacy(defaultMediumIt->second);
    }
}

void FromProto(NScheduler::TJobResourcesWithQuota* resources, const NScheduler::NProto::TJobResourcesWithQuota& protoResources)
{
    resources->SetCpu(TCpuResource(protoResources.cpu()));
    resources->SetGpu(protoResources.gpu());
    resources->SetUserSlots(protoResources.user_slots());
    resources->SetMemory(protoResources.memory());
    resources->SetNetwork(protoResources.network());

    NScheduler::TDiskQuota diskQuota;
    if (protoResources.has_disk_quota()) {
        FromProto(&diskQuota, protoResources.disk_quota());
    } else {
        diskQuota.DiskSpacePerMedium[NChunkClient::DefaultSlotsMediumIndex] = protoResources.disk_quota_legacy();
    }
    resources->SetDiskQuota(diskQuota);
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

