#include "allocation_statistics.h"

#include "resource_capacities.h"

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

TAllocationStatistics Max(
    const TAllocationStatistics& lhs,
    const TAllocationStatistics& rhs)
{
    TAllocationStatistics result;
    result.Capacities = Max(lhs.Capacities, rhs.Capacities);
    result.Used = lhs.Used | rhs.Used;
    result.UsedExclusively = lhs.UsedExclusively | rhs.UsedExclusively;
    return result;
}

TAllocationStatistics& operator += (
    TAllocationStatistics& lhs,
    const TAllocationStatistics& rhs)
{
    lhs.Capacities += rhs.Capacities;
    lhs.Used |= rhs.Used;
    lhs.UsedExclusively |= rhs.UsedExclusively;
    return lhs;
}

TAllocationStatistics operator + (
    const TAllocationStatistics& lhs,
    const TAllocationStatistics& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

NClient::NApi::NProto::TResourceStatus_TAllocationStatistics ResourceCapacitiesToStatistics(
    const TResourceCapacities& capacities,
    EResourceKind kind)
{
    NClient::NApi::NProto::TResourceStatus_TAllocationStatistics result;
    switch (kind) {
        case EResourceKind::Cpu:
            result.mutable_cpu()->set_capacity(GetCpuCapacity(capacities));
            break;
        case EResourceKind::Disk:
            result.mutable_disk()->set_capacity(GetDiskCapacity(capacities));
            result.mutable_disk()->set_bandwidth(GetDiskBandwidth(capacities));
            break;
        case EResourceKind::Memory:
            result.mutable_memory()->set_capacity(GetMemoryCapacity(capacities));
            break;
        case EResourceKind::Network:
            result.mutable_network()->set_bandwidth(GetNetworkBandwidth(capacities));
            break;
        case EResourceKind::Slot:
            result.mutable_slot()->set_capacity(GetSlotCapacity(capacities));
            break;
        case EResourceKind::Gpu:
            result.mutable_gpu()->set_capacity(GetGpuCapacity(capacities));
            break;
        default:
            YT_ABORT();
    }
    return result;
}

TAllocationStatistics ComputeTotalAllocationStatistics(
    const std::vector<NYP::NClient::NApi::NProto::TResourceStatus_TAllocation>& scheduledAllocations,
    const std::vector<NYP::NClient::NApi::NProto::TResourceStatus_TAllocation>& actualAllocations)
{
    struct TPodStatistics
    {
        TAllocationStatistics Scheduled;
        TAllocationStatistics Actual;
    };

    THashMap<TObjectId, TPodStatistics> podIdToStats;

    for (const auto& allocation : scheduledAllocations) {
        Accumulate(podIdToStats[allocation.pod_id()].Scheduled, allocation);
    }

    for (const auto& allocation : actualAllocations) {
        Accumulate(podIdToStats[allocation.pod_id()].Actual, allocation);
    }

    TAllocationStatistics statistics;
    for (const auto& pair : podIdToStats) {
        const auto& podStatistics = pair.second;
        statistics += Max(podStatistics.Scheduled, podStatistics.Actual);
    }
    return statistics;
}

void Accumulate(
    TAllocationStatistics& statistics,
    const NClient::NApi::NProto::TResourceStatus_TAllocation& allocation)
{
    statistics.Capacities += GetAllocationCapacities(allocation);
    statistics.Used |= true;
    statistics.UsedExclusively = GetAllocationExclusive(allocation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
