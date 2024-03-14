#include "node_resource_manager.h"
#include "private.h"
#include "bootstrap.h"
#include "config.h"

#include <yt/yt/server/node/tablet_node/bootstrap.h>
#include <yt/yt/server/node/tablet_node/slot_manager.h>

#include <yt/yt/server/node/cellar_node/bootstrap.h>

#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/cellar_node/bundle_dynamic_config_manager.h>
#include <yt/yt/server/node/cellar_node/config.h>

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/library/containers/instance_limits_tracker.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <tcmalloc/malloc_extension.h>

#include <limits>

namespace NYT::NClusterNode {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NProfiling;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ClusterNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TString FormatMemoryUsage(i64 memoryUsage)
{
    TStringBuf prefix = "";
    if (memoryUsage < 0) {
        prefix = "-";
        memoryUsage *= -1;
    }
    if (memoryUsage < static_cast<i64>(1_KB)) {
        return Format("%v%vB", prefix, memoryUsage);
    }
    if (memoryUsage < static_cast<i64>(1_MB)) {
        return Format("%v%vKB", prefix, memoryUsage / 1_KB);
    }
    return Format("%v%vMB", prefix, memoryUsage / 1_MB);
}

TString FormatResources(
    const TJobResources& usage,
    const TJobResources& limits)
{
    return Format(
        "UserSlots: %v/%v, Cpu: %v/%v, Gpu: %v/%v, UserMemory: %v/%v, SystemMemory: %v/%v, Network: %v/%v, "
        "ReplicationSlots: %v/%v, ReplicationDataSize: %v/%v, "
        "RemovalSlots: %v/%v, "
        "RepairSlots: %v/%v, RepairDataSize: %v/%v, "
        "SealSlots: %v/%v, "
        "MergeSlots: %v/%v, MergeDataSize: %v/%v, "
        "AutotomySlots: %v/%v, ReincarnationSlots %v/%v",
        // User slots
        usage.UserSlots,
        limits.UserSlots,
        // Cpu
        usage.Cpu,
        limits.Cpu,
        // Gpu,
        usage.Gpu,
        limits.Gpu,
        // User memory
        FormatMemoryUsage(usage.UserMemory),
        FormatMemoryUsage(limits.UserMemory),
        // System memory
        FormatMemoryUsage(usage.SystemMemory),
        FormatMemoryUsage(limits.SystemMemory),
        // Network
        usage.Network,
        limits.Network,
        // Replication slots
        usage.ReplicationSlots,
        limits.ReplicationSlots,
        // Replication data size
        usage.ReplicationDataSize,
        limits.ReplicationDataSize,
        // Removal slots
        usage.RemovalSlots,
        limits.RemovalSlots,
        // Repair slots
        usage.RepairSlots,
        limits.RepairSlots,
        // Repair data size
        usage.RepairDataSize,
        limits.RepairSlots,
        // Seal slots
        usage.SealSlots,
        limits.SealSlots,
        // Merge slots
        usage.MergeSlots,
        limits.MergeSlots,
        // Merge data size
        usage.MergeDataSize,
        limits.MergeDataSize,
        // Autotomy slots
        usage.AutotomySlots,
        limits.AutotomySlots,
        // Reincarnation slots
        usage.ReincarnationSlots,
        limits.ReincarnationSlots);
}

TString FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits)
{
    return Format("{%v}", FormatResources(usage, limits));
}

TString FormatResources(const TJobResources& resources)
{
    return Format(
        "{"
        "UserSlots: %v, Cpu: %v, Gpu: %v, UserMemory: %v, SystemMemory: %v, Network: %v, "
        "ReplicationSlots: %v, ReplicationDataSize: %v, "
        "RemovalSlots: %v, "
        "RepairSlots: %v, RepairDataSize: %v, "
        "SealSlots: %v, "
        "MergeSlots: %v, MergeDataSize: %v, "
        "AutotomySlots: %v, "
        "ReincarnationSlots: %v"
        "}",
        resources.UserSlots,
        resources.Cpu,
        resources.Gpu,
        FormatMemoryUsage(resources.UserMemory),
        FormatMemoryUsage(resources.SystemMemory),
        resources.Network,
        resources.ReplicationSlots,
        FormatMemoryUsage(resources.ReplicationDataSize),
        resources.RemovalSlots,
        resources.RepairSlots,
        FormatMemoryUsage(resources.RepairDataSize),
        resources.SealSlots,
        resources.MergeSlots,
        FormatMemoryUsage(resources.MergeDataSize),
        resources.AutotomySlots,
        resources.ReincarnationSlots);
}

void ProfileResources(ISensorWriter* writer, const TJobResources& resources)
{
    #define XX(name, Name) writer->AddGauge("/" #name, resources.Name);
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX
}

TJobResources GetZeroJobResources()
{
    TJobResources result;
    #define XX(name, Name) result.Name = 0;
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return result;
}

const TJobResources& ZeroJobResources()
{
    static auto value = GetZeroJobResources();
    return value;
}

TJobResources GetInfiniteJobResources()
{
    TJobResources result;
    #define XX(name, Name) result.Name = (std::numeric_limits<decltype(result.Name)>::max() / 4);
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return result;
}

const TJobResources& InfiniteJobResources()
{
    static auto result = GetInfiniteJobResources();
    return result;
}

NNodeTrackerClient::NProto::TNodeResources ToNodeResources(const TJobResources& jobResources)
{
    TNodeResources result;
    #define XX(name, Name) result.set_##name(jobResources.Name);
    ITERATE_JOB_RESOURCE_PROTO_FIELDS(XX)
    #undef XX
    return result;
}

TJobResources FromNodeResources(const NNodeTrackerClient::NProto::TNodeResources& jobResources)
{
    TJobResources result;
    #define XX(name, Name) result.Name = jobResources.name();
    ITERATE_JOB_RESOURCE_PROTO_FIELDS(XX)
    #undef XX
    return result;
}

TJobResources operator + (const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Name = lhs.Name + rhs.Name;
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return result;
}

TJobResources& operator += (TJobResources& lhs, const TJobResources& rhs)
{
    #define XX(name, Name) lhs.Name = lhs.Name + rhs.Name;
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return lhs;
}

TJobResources operator - (const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Name = lhs.Name - rhs.Name;
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return result;
}

TJobResources& operator -= (TJobResources& lhs, const TJobResources& rhs)
{
    #define XX(name, Name) lhs.Name = lhs.Name - rhs.Name;
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return lhs;
}

TJobResources operator * (const TJobResources& lhs, i64 rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Name = lhs.Name * rhs;
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return result;
}

TJobResources operator * (const TJobResources& lhs, double rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Name = static_cast<decltype(lhs.Name)>(lhs.Name * rhs + 0.5);
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return result;
}

TJobResources& operator *= (TJobResources& lhs, i64 rhs)
{
    #define XX(name, Name) lhs.Name = lhs.Name * rhs;
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return lhs;
}

TJobResources& operator *= (TJobResources& lhs, double rhs)
{
    #define XX(name, Name) lhs.Name = static_cast<decltype(lhs.Name)>(lhs.Name * rhs + 0.5);
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return lhs;
}

TJobResources  operator - (const TJobResources& resources)
{
    TJobResources result;
    #define XX(name, Name) result.Name = -resources.Name;
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return result;
}

bool operator == (const TJobResources& lhs, const TJobResources& rhs)
{
    return
        #define XX(name, Name) lhs.Name == rhs.Name &&
        ITERATE_JOB_RESOURCE_FIELDS(XX)
        #undef XX
        true;
}

TJobResources MakeNonnegative(const TJobResources& resources)
{
    TJobResources result;
    #define XX(name, Name) result.Name = std::max(resources.Name, static_cast<decltype(resources.Name)>(0));
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return result;
}

bool Dominates(const TJobResources& lhs, const TJobResources& rhs)
{
    return
        #define XX(name, Name) lhs.Name >= rhs.Name &&
        ITERATE_JOB_RESOURCE_FIELDS(XX)
        #undef XX
        true;
}

TJobResources Max(const TJobResources& a, const TJobResources& b)
{
    TJobResources result;
    #define XX(name, Name) result.Name = std::max(a.Name, b.Name);
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return result;
}

TJobResources Min(const TJobResources& a, const TJobResources& b)
{
    TJobResources result;
    #define XX(name, Name) result.Name = std::min(a.Name, b.Name);
    ITERATE_JOB_RESOURCE_FIELDS(XX)
    #undef XX

    return result;
}

void Serialize(const TJobResources& resources, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            #define XX(name, Name) .Item(#name).Value(resources.Name)
            ITERATE_JOB_RESOURCE_FIELDS(XX)
            #undef XX
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TNodeResourceManager::TNodeResourceManager(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , UpdateExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND(&TNodeResourceManager::UpdateLimits, MakeWeak(this)),
        Bootstrap_->GetConfig()->ResourceLimitsUpdatePeriod))
{
    const auto& staticLimits = Bootstrap_->GetConfig()->ResourceLimits;
    const auto networkLimit = Bootstrap_->GetConfig()->NetworkBandwidth;
    Limits_.Store(NContainers::TInstanceLimits{
        .Cpu = staticLimits->TotalCpu.value_or(0),
        .Memory = staticLimits->TotalMemory,
        .NetTx = networkLimit,
        .NetRx = networkLimit,
    });
}

void TNodeResourceManager::Start()
{
    UpdateExecutor_->Start();
}

void TNodeResourceManager::OnInstanceLimitsUpdated(const NContainers::TInstanceLimits& limits)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Limits_.Store(limits);
}

IYPathServicePtr TNodeResourceManager::GetOrchidService()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return IYPathService::FromProducer(BIND(&TNodeResourceManager::BuildOrchid, MakeStrong(this)))
        ->Via(Bootstrap_->GetControlInvoker());
}

std::optional<double> TNodeResourceManager::GetCpuGuarantee() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (auto cpu = Limits_.Load().Cpu) {
        return cpu;
    }

    return {};
}

std::optional<double> TNodeResourceManager::GetCpuLimit() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (auto cpu = Limits_.Load().Cpu) {
        return cpu;
    }

    return {};
}

double TNodeResourceManager::GetJobsCpuLimit() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return JobsCpuLimit_;
}

double TNodeResourceManager::GetCpuUsage() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto config = Bootstrap_->GetConfig()->ResourceLimits;
    auto dynamicConfig = Bootstrap_->GetDynamicConfigManager()->GetConfig()->ResourceLimits;

    double cpuUsage = 0;

    // Node dedicated CPU.
    cpuUsage += dynamicConfig->NodeDedicatedCpu.value_or(*config->NodeDedicatedCpu);

    // User jobs.
    cpuUsage += GetJobResourceUsage().Cpu;

    // Tablet cells.
    cpuUsage += GetTabletSlotCpu();

    return cpuUsage;
}

i64 TNodeResourceManager::GetMemoryUsage() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return Bootstrap_->GetMemoryUsageTracker()->GetTotalUsed();
}

double TNodeResourceManager::GetCpuDemand() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto config = Bootstrap_->GetConfig()->ResourceLimits;
    auto dynamicConfig = Bootstrap_->GetDynamicConfigManager()->GetConfig()->ResourceLimits;

    double cpuDemand = 0;

    // Node dedicated CPU.
    cpuDemand += dynamicConfig->NodeDedicatedCpu.value_or(*config->NodeDedicatedCpu);

    // Tablet cells.
    cpuDemand += GetTabletSlotCpu();

    return cpuDemand;
}

i64 TNodeResourceManager::GetMemoryDemand() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto limits = GetMemoryLimits();

    i64 memoryDemand = 0;
    for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
        const auto& limit = limits[category];
        if (limit->Type != EMemoryLimitType::Dynamic) {
            memoryDemand += *limit->Value;
        }
    }

    return memoryDemand;
}

std::optional<i64> TNodeResourceManager::GetNetTxLimit() const
{
    return Limits_.Load().NetTx;
}

std::optional<i64> TNodeResourceManager::GetNetRxLimit() const
{
    return Limits_.Load().NetRx;
}

void TNodeResourceManager::SetResourceLimitsOverride(const TNodeResourceLimitsOverrides& resourceLimitsOverride)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ResourceLimitsOverride_ = resourceLimitsOverride;
}

TNodeResourceLimitsOverrides TNodeResourceManager::ComputeEffectiveResourceLimitsOverrides() const
{
    TNodeResourceLimitsOverrides resourceLimits;
    const auto& dynamicConfigOverrides = Bootstrap_->GetDynamicConfigManager()->GetConfig()->ResourceLimits->Overrides;

    #define XX(name, Name) \
        if (ResourceLimitsOverride_.has_##name()) { \
            resourceLimits.set_##name(ResourceLimitsOverride_.name()); \
        } else if (dynamicConfigOverrides->Name) { \
            resourceLimits.set_##name(*dynamicConfigOverrides->Name); \
        }
    ITERATE_NODE_RESOURCE_LIMITS_DYNAMIC_CONFIG_OVERRIDES(XX)
    #undef XX
    return resourceLimits;
}

void TNodeResourceManager::UpdateLimits()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_LOG_DEBUG("Updating node resource limits");

    UpdateMemoryFootprint();
    UpdateMemoryLimits();
    UpdateJobsCpuLimit();
}

void TNodeResourceManager::UpdateMemoryLimits()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    const auto& config = Bootstrap_->GetConfig()->ResourceLimits;
    const auto& memoryUsageTracker = Bootstrap_->GetMemoryUsageTracker();

    auto limits = GetMemoryLimits();

    auto dynamicConfig = Bootstrap_->GetDynamicConfigManager()->GetConfig()->ResourceLimits;
    i64 freeMemoryWatermark = dynamicConfig->FreeMemoryWatermark.value_or(*config->FreeMemoryWatermark);
    i64 totalMemory = Limits_.Load().Memory;

    if (totalMemory >= freeMemoryWatermark) {
        totalMemory -= freeMemoryWatermark;
    } else {
        YT_LOG_WARNING(
            "Free memory watermark more than total memory (FreeMemoryWatermark: %v, TotalMemory: %v)",
            freeMemoryWatermark,
            totalMemory);
    }

    if (auto oldMemoryLimit = memoryUsageTracker->GetTotalLimit(); totalMemory != oldMemoryLimit) {
        YT_LOG_INFO(
            "Setting new memory limit (OldMemoryLimit: %v, NewMemoryLimit: %v, FreeMemoryWatermark: %v)",
            oldMemoryLimit,
            totalMemory,
            freeMemoryWatermark);

        memoryUsageTracker->SetTotalLimit(totalMemory);
    }

    for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
        const auto& limit = limits[category];
        if (limit->Type == EMemoryLimitType::None) {
            continue;
        }

        auto oldLimit = memoryUsageTracker->GetExplicitLimit(category);
        auto newLimit = *limit->Value;

        if (std::abs(oldLimit - newLimit) > config->MemoryAccountingTolerance) {
            YT_LOG_INFO(
                "Updating memory category limit (Category: %v, OldLimit: %v, NewLimit: %v)",
                category,
                oldLimit,
                newLimit);
            memoryUsageTracker->SetCategoryLimit(category, newLimit);
        }
    }

    auto externalMemory = std::max(
        memoryUsageTracker->GetLimit(EMemoryCategory::UserJobs),
        memoryUsageTracker->GetUsed(EMemoryCategory::UserJobs));
    auto selfMemoryGuarantee = std::max(
        static_cast<i64>(0),
        totalMemory - externalMemory - config->MemoryAccountingGap);
    if (std::abs(selfMemoryGuarantee - SelfMemoryGuarantee_) > config->MemoryAccountingTolerance) {
        SelfMemoryGuarantee_ = selfMemoryGuarantee;
        SelfMemoryGuaranteeUpdated_.Fire(SelfMemoryGuarantee_);
    }
}

void TNodeResourceManager::UpdateMemoryFootprint()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    const auto& memoryUsageTracker = Bootstrap_->GetMemoryUsageTracker();

    i64 bytesUsed = tcmalloc::MallocExtension::GetNumericProperty("generic.current_allocated_bytes").value_or(0);
    i64 bytesCommitted = tcmalloc::MallocExtension::GetNumericProperty("generic.heap_size").value_or(0);
    auto newFragmentation = std::max<i64>(0, bytesCommitted - bytesUsed);

    auto newFootprint = bytesUsed;
    for (auto memoryCategory : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
        if (memoryCategory == EMemoryCategory::UserJobs ||
            memoryCategory == EMemoryCategory::Footprint ||
            memoryCategory == EMemoryCategory::AllocFragmentation ||
            memoryCategory == EMemoryCategory::TmpfsLayers)
        {
            continue;
        }

        newFootprint -= memoryUsageTracker->GetUsed(memoryCategory);
    }
    newFootprint = std::max<i64>(newFootprint, 0);

    auto oldFootprint = memoryUsageTracker->UpdateUsage(EMemoryCategory::Footprint, newFootprint);
    auto oldFragmentation = memoryUsageTracker->UpdateUsage(EMemoryCategory::AllocFragmentation, newFragmentation);

    YT_LOG_INFO("Memory footprint updated (BytesCommitted: %v, BytesUsed: %v, Footprint: %v -> %v, Fragmentation: %v -> %v, Rpc: %v)",
        bytesCommitted,
        bytesUsed,
        oldFootprint,
        newFootprint,
        oldFragmentation,
        newFragmentation,
        memoryUsageTracker->GetUsed(EMemoryCategory::Rpc));
}

void TNodeResourceManager::UpdateJobsCpuLimit()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    double newJobsCpuLimit = 0;

    const auto& effectiveResourceLimits = ComputeEffectiveResourceLimitsOverrides();

    // COPMAT(gritukan)
    if (effectiveResourceLimits.has_cpu()) {
        newJobsCpuLimit = effectiveResourceLimits.cpu();
    } else {
        if (auto cpu = Limits_.Load().Cpu) {
            newJobsCpuLimit = cpu - GetNodeDedicatedCpu();
        } else {
            newJobsCpuLimit = Bootstrap_->GetConfig()->JobResourceManager->ResourceLimits->Cpu;
        }

        newJobsCpuLimit -= GetTabletSlotCpu();
    }
    newJobsCpuLimit = std::max<double>(newJobsCpuLimit, 0);

    JobsCpuLimit_.store(newJobsCpuLimit);
    JobsCpuLimitUpdated_.Fire();
}

TJobResources TNodeResourceManager::GetJobResourceUsage() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return WaitFor(BIND([this, this_ = MakeStrong(this)] {
        const auto& jobResourceManager = Bootstrap_->GetJobResourceManager();
        return jobResourceManager->GetResourceUsage(/*includeWaiting*/ true);
    })
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run())
        .ValueOrThrow();
}

double TNodeResourceManager::GetTabletSlotCpu() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!Bootstrap_->IsTabletNode()) {
        return false;
    }

    const auto& config = Bootstrap_->GetConfig()->ResourceLimits;
    const auto& dynamicConfig = Bootstrap_->GetDynamicConfigManager()->GetConfig()->ResourceLimits;

    if (const auto& tabletSlotManager = Bootstrap_->GetTabletNodeBootstrap()->GetSlotManager()) {
        double cpuPerTabletSlot = dynamicConfig->CpuPerTabletSlot.value_or(*config->CpuPerTabletSlot);
        return tabletSlotManager->GetUsedCpu(cpuPerTabletSlot);
    } else {
        return 0;
    }
}

double TNodeResourceManager::GetNodeDedicatedCpu() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto config = Bootstrap_->GetConfig()->ResourceLimits;
    auto dynamicConfig = Bootstrap_->GetDynamicConfigManager()->GetConfig()->ResourceLimits;

    return dynamicConfig->NodeDedicatedCpu.value_or(*config->NodeDedicatedCpu);
}

TEnumIndexedArray<EMemoryCategory, TMemoryLimitPtr> TNodeResourceManager::GetMemoryLimits() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TEnumIndexedArray<EMemoryCategory, TMemoryLimitPtr> limits;

    const auto& config = Bootstrap_->GetConfig()->ResourceLimits;

    auto dynamicConfig = Bootstrap_->GetDynamicConfigManager()->GetConfig()->ResourceLimits;

    auto bundleDynamicMemoryLimit = Bootstrap_->GetBundleDynamicConfigManager()->GetConfig()
        ->MemoryLimits->AsEnumIndexedVector();

    auto getMemoryLimit = [&] (EMemoryCategory category) {
        if (bundleDynamicMemoryLimit[category]) {
            return bundleDynamicMemoryLimit[category];
        }

        auto memoryLimit = dynamicConfig->MemoryLimits[category];
        if (!memoryLimit) {
            memoryLimit = config->MemoryLimits[category];
        }

        return memoryLimit;
    };

    i64 freeMemoryWatermark = dynamicConfig->FreeMemoryWatermark.value_or(*config->FreeMemoryWatermark);
    i64 totalDynamicMemory = Limits_.Load().Memory - freeMemoryWatermark;

    const auto& memoryUsageTracker = Bootstrap_->GetMemoryUsageTracker();

    int dynamicCategoryCount = 0;
    for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
        auto memoryLimit = getMemoryLimit(category);
        auto& limit = limits[category];
        limit = New<TMemoryLimit>();

        if (!memoryLimit || !memoryLimit->Type || memoryLimit->Type == EMemoryLimitType::None) {
            limit->Type = EMemoryLimitType::None;

            auto categoryLimit = memoryUsageTracker->GetExplicitLimit(category);
            if (categoryLimit == std::numeric_limits<i64>::max()) {
                categoryLimit = memoryUsageTracker->GetUsed(category);
            }

            // NB: Limit may be set via memory tracking caches.
            // Otherwise we fallback to memory usage.
            limit->Value = categoryLimit;

            totalDynamicMemory -= categoryLimit;
        } else if (memoryLimit->Type == EMemoryLimitType::Static) {
            limit->Type = EMemoryLimitType::Static;
            limit->Value = *memoryLimit->Value;
            totalDynamicMemory -= *memoryLimit->Value;
        } else {
            limit->Type = EMemoryLimitType::Dynamic;
            ++dynamicCategoryCount;
        }
    }

    if (dynamicCategoryCount > 0) {
        auto dynamicMemoryPerCategory = std::max<i64>(totalDynamicMemory / dynamicCategoryCount, 0);
        for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
            auto& limit = limits[category];
            if (limit->Type == EMemoryLimitType::Dynamic) {
                limit->Value = dynamicMemoryPerCategory;
            }
        }
    }

    const auto& effectiveResourceLimits = ComputeEffectiveResourceLimitsOverrides();

    if (effectiveResourceLimits.has_system_memory()) {
        limits[EMemoryCategory::SystemJobs]->Value = effectiveResourceLimits.system_memory();
    }
    if (effectiveResourceLimits.has_user_memory()) {
        limits[EMemoryCategory::UserJobs]->Value = effectiveResourceLimits.user_memory();
    }

    return limits;
}

void TNodeResourceManager::BuildOrchid(IYsonConsumer* consumer) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto memoryLimits = GetMemoryLimits();
    auto limits = Limits_.Load();

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("cpu_limit").Value(limits.Cpu)
            .Item("net_tx_limit").Value(limits.NetTx)
            .Item("net_rx_limit").Value(limits.NetRx)
            .Item("total_memory").Value(limits.Memory)
            .Item("jobs_cpu_limit").Value(JobsCpuLimit_)
            .Item("tablet_slot_cpu").Value(GetTabletSlotCpu())
            .Item("node_dedicated_cpu").Value(GetNodeDedicatedCpu())
            .Item("memory_demand").Value(GetMemoryDemand())
            .Item("memory_limit_per_category").DoMapFor(TEnumTraits<EMemoryCategory>::GetDomainValues(),
                [&] (TFluentMap fluent, EMemoryCategory category) {
                    fluent.Item(FormatEnum<EMemoryCategory>(category)).Value(memoryLimits[category]->Value);
                })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
