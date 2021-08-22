#include "job_resources.h"
#include "config.h"

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

using namespace NYson;
using namespace NYTree;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NProfiling;

using std::round;

////////////////////////////////////////////////////////////////////////////////

i64 TExtendedJobResources::GetMemory() const
{
    return JobProxyMemory_ + UserJobMemory_ + FootprintMemory_;
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

TDiskQuota::operator bool() const
{
    return !DiskSpacePerMedium.empty() || DiskSpaceWithoutMedium;
}

void TDiskQuota::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, DiskSpacePerMedium);
    Persist(context, DiskSpaceWithoutMedium);
}

void FormatValue(TStringBuilderBase* builder, const TDiskQuota& diskQuota, TStringBuf /* format */)
{
    builder->AppendFormat(
        "%v {DiskSpaceWithoutMedium: %v}",
        MakeFormattableView(diskQuota.DiskSpacePerMedium, [] (TStringBuilderBase* builder, const std::pair<int, i64>& pair) {
            auto [mediumIndex, diskSpace] = pair;
            builder->AppendFormat("{MediumIndex: %v, DiskSpace: %v}", mediumIndex, diskSpace);
        }),
        diskQuota.DiskSpaceWithoutMedium);
}

TDiskQuota CreateDiskQuota(i32 mediumIndex, i64 diskSpace)
{
    TDiskQuota result;
    result.DiskSpacePerMedium.emplace(mediumIndex, diskSpace);
    return result;
}

TDiskQuota CreateDiskQuotaWithoutMedium(i64 diskSpace)
{
    TDiskQuota result;
    result.DiskSpaceWithoutMedium = diskSpace;
    return result;
}

TDiskQuota  operator - (const TDiskQuota& quota)
{
    TDiskQuota result;
    if (quota.DiskSpaceWithoutMedium) {
        result.DiskSpaceWithoutMedium = -*quota.DiskSpaceWithoutMedium;
    }
    for (auto [key, value] : quota.DiskSpacePerMedium) {
        result.DiskSpacePerMedium[key] = -value;
    }
    return result;
}

TDiskQuota  operator + (const TDiskQuota& lhs, const TDiskQuota& rhs)
{
    TDiskQuota result;
    result.DiskSpaceWithoutMedium = lhs.DiskSpaceWithoutMedium.value_or(0) + rhs.DiskSpaceWithoutMedium.value_or(0);
    if (*result.DiskSpaceWithoutMedium == 0) {
        result.DiskSpaceWithoutMedium = std::nullopt;
    }
    for (auto [key, value] : lhs.DiskSpacePerMedium) {
        result.DiskSpacePerMedium[key] += value;
    }
    for (auto [key, value] : rhs.DiskSpacePerMedium) {
        result.DiskSpacePerMedium[key] += value;
    }
    return result;
}

TDiskQuota& operator += (TDiskQuota& lhs, const TDiskQuota& rhs)
{
    lhs = lhs + rhs;
    return lhs;
}

TDiskQuota  operator - (const TDiskQuota& lhs, const TDiskQuota& rhs)
{
    TDiskQuota result;
    result.DiskSpaceWithoutMedium = lhs.DiskSpaceWithoutMedium.value_or(0) - rhs.DiskSpaceWithoutMedium.value_or(0);
    if (*result.DiskSpaceWithoutMedium == 0) {
        result.DiskSpaceWithoutMedium = std::nullopt;
    }
    for (auto [key, value] : lhs.DiskSpacePerMedium) {
        result.DiskSpacePerMedium[key] += value;
    }
    for (auto [key, value] : rhs.DiskSpacePerMedium) {
        result.DiskSpacePerMedium[key] -= value;
    }
    return result;
}

TDiskQuota& operator -= (TDiskQuota& lhs, const TDiskQuota& rhs)
{
    lhs = lhs - rhs;
    return lhs;
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
        usage.GetMemory() / 1_MB,
        limits.GetMemory() / 1_MB,
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
        "{UserSlots: %v, Cpu: %v, Gpu: %v, Memory: %vMB, Network: %v}",
        resources.GetUserSlots(),
        resources.GetCpu(),
        resources.GetGpu(),
        resources.GetMemory() / 1_MB,
        resources.GetNetwork());
}

void FormatValue(TStringBuilderBase* builder, const TJobResources& resources, TStringBuf /* format */)
{
    builder->AppendFormat(
        "{UserSlots: %v, Cpu: %v, Gpu: %v, Memory: %vMB, Network: %v}",
        resources.GetUserSlots(),
        resources.GetCpu(),
        resources.GetGpu(),
        resources.GetMemory() / 1_MB,
        resources.GetNetwork());
}

TString FormatResources(const TJobResourcesWithQuota& resources)
{
    return Format(
        "{UserSlots: %v, Cpu: %v, Gpu: %v, Memory: %vMB, Network: %v, DiskQuota: %v}",
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
        "{UserSlots: %v, Cpu: %v, Gpu: %v, JobProxyMemory: %vMB, UserJobMemory: %vMB, FootprintMemory: %vMB, Network: %v}",
        resources.GetUserSlots(),
        resources.GetCpu(),
        resources.GetGpu(),
        resources.GetJobProxyMemory() / 1_MB,
        resources.GetUserJobMemory() / 1_MB,
        resources.GetFootprintMemory() / 1_MB,
        resources.GetNetwork());
}

////////////////////////////////////////////////////////////////////////////////

void TJobResourcesProfiler::Init(const NProfiling::TProfiler& profiler)
{
#define XX(name, Name) Name = profiler.Gauge("/" #name);
    ITERATE_JOB_RESOURCES(XX)
#undef XX
}

void TJobResourcesProfiler::Reset()
{
#define XX(name, Name) Name = {};
    ITERATE_JOB_RESOURCES(XX)
#undef XX
}

void TJobResourcesProfiler::Update(const TJobResources& resources)
{
#define XX(name, Name) Name.Update(static_cast<double>(resources.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
#undef XX
}

void ProfileResources(
    NProfiling::ISensorWriter* writer,
    const TJobResources& resources,
    const TString& prefix)
{
    #define XX(name, Name) writer->AddGauge(prefix + "/" #name, static_cast<i64>(resources.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

////////////////////////////////////////////////////////////////////////////////

EJobResourceType GetDominantResource(
    const TJobResources& demand,
    const TJobResources& limits)
{
    auto maxType = EJobResourceType::Cpu;
    double maxRatio = 0.0;
    auto update = [&] (auto a, auto b, EJobResourceType type) {
        if (static_cast<double>(b) > 0.0) {
            double ratio = static_cast<double>(a) / static_cast<double>(b);
            if (ratio > maxRatio) {
                maxRatio = ratio;
                maxType = type;
            }
        }
    };
    #define XX(name, Name) update(demand.Get##Name(), limits.Get##Name(), EJobResourceType::Name);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return maxType;
}

double GetDominantResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits)
{
    double maxRatio = 0.0;
    auto update = [&] (auto a, auto b) {
        if (static_cast<double>(b) > 0.0) {
            double ratio = static_cast<double>(a) / static_cast<double>(b);
            if (ratio > maxRatio) {
                maxRatio = ratio;
            }
        }
    };
    #define XX(name, Name) update(usage.Get##Name(), limits.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return maxRatio;
}

double GetResource(const TJobResources& resources, EJobResourceType type)
{
    switch (type) {
        #define XX(name, Name) \
            case EJobResourceType::Name: \
                return static_cast<double>(resources.Get##Name());
        ITERATE_JOB_RESOURCES(XX)
        #undef XX
        default:
            YT_ABORT();
    }
}

void SetResource(TJobResources& resources, EJobResourceType type, double value)
{
    switch (type) {
        #define XX(name, Name) \
            case EJobResourceType::Name: \
                resources.Set##Name(value); \
                break;
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
    double result = std::numeric_limits<double>::max();
    bool updated = false;
    auto update = [&] (auto a, auto b) {
        if (static_cast<double>(b) > 0.0) {
            result = std::min(result, static_cast<double>(a) / static_cast<double>(b));
            updated = true;
        }
    };
    #define XX(name, Name) update(nominator.Get##Name(), denominator.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return updated ? result : 0.0;
}

double GetMaxResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator)
{
    double result = 0.0;
    auto update = [&] (auto a, auto b) {
        if (static_cast<double>(b) > 0.0) {
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

bool HasLocationWithDefaultMedium(const NNodeTrackerClient::NProto::TDiskResources& diskResources)
{
    bool hasLocationWithDefaultMedium = false;
    for (const auto& diskLocationResources : diskResources.disk_location_resources()) {
        if (diskLocationResources.medium_index() == diskResources.default_medium_index()) {
            hasLocationWithDefaultMedium = true;
        }
    }
    return hasLocationWithDefaultMedium;
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

    if (!diskQuotaRequest && !HasLocationWithDefaultMedium(diskResources)) {
        return false;
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
    bool hasEmptyDiskRequest = false;
    for (const auto& diskQuotaRequest : diskQuotaRequests) {
        for (auto [mediumIndex, diskSpace] : diskQuotaRequest.DiskSpacePerMedium) {
            diskSpaceRequestsPerMedium[mediumIndex].push_back(diskSpace);
        }
        if (diskQuotaRequest.DiskSpaceWithoutMedium) {
            diskSpaceRequestsPerMedium[diskResources.default_medium_index()].push_back(*diskQuotaRequest.DiskSpaceWithoutMedium);
        }
        if (!diskQuotaRequest && !diskQuotaRequest.DiskSpaceWithoutMedium) {
            hasEmptyDiskRequest = true;
        }
    }

    if (hasEmptyDiskRequest && !HasLocationWithDefaultMedium(diskResources)) {
        return false;
    }

    for (const auto& [mediumIndex, diskSpaceRequests] : diskSpaceRequestsPerMedium) {
        if (!CanSatisfyDiskQuotaRequests(availableDiskSpacePerMedium[mediumIndex], diskSpaceRequests)) {
            return false;
        }
    }

    return true;
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
    if (diskQuota.DiskSpaceWithoutMedium) {
        auto* protoDiskLocationQuotaProto = protoDiskQuota->add_disk_location_quota();
        protoDiskLocationQuotaProto->set_disk_space(*diskQuota.DiskSpaceWithoutMedium);
    }
}

void FromProto(NScheduler::TDiskQuota* diskQuota, const NScheduler::NProto::TDiskQuota& protoDiskQuota)
{
    for (const auto& protoDiskLocationQuota : protoDiskQuota.disk_location_quota()) {
        if (protoDiskLocationQuota.has_medium_index()) {
            diskQuota->DiskSpacePerMedium.emplace(protoDiskLocationQuota.medium_index(), protoDiskLocationQuota.disk_space());
        } else {
            diskQuota->DiskSpaceWithoutMedium = protoDiskLocationQuota.disk_space();
        }
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
}

void FromProto(NScheduler::TJobResourcesWithQuota* resources, const NScheduler::NProto::TJobResourcesWithQuota& protoResources)
{
    resources->SetCpu(TCpuResource(protoResources.cpu()));
    resources->SetGpu(protoResources.gpu());
    resources->SetUserSlots(protoResources.user_slots());
    resources->SetMemory(protoResources.memory());
    resources->SetNetwork(protoResources.network());

    NScheduler::TDiskQuota diskQuota;
    FromProto(&diskQuota, protoResources.disk_quota());
    resources->SetDiskQuota(diskQuota);
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

