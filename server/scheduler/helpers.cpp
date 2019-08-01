#include "helpers.h"

#include <yp/server/objects/helpers.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/resource.h>


namespace NYP::NClient::NApi::NProto {

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TResourceStatus_TAllocation& lhs, const TResourceStatus_TAllocation& rhs)
{
    if (lhs.pod_id() != rhs.pod_id() ||
        lhs.pod_uuid() != rhs.pod_uuid())
    {
        return false;
    }

    if (lhs.has_cpu() != rhs.has_cpu()) {
        return false;
    }
    if (lhs.has_cpu() && lhs.cpu().capacity() != rhs.cpu().capacity()) {
        return false;
    }

    if (lhs.has_memory() != rhs.has_memory()) {
        return false;
    }
    if (lhs.has_memory() && lhs.memory().capacity() != rhs.memory().capacity()) {
        return false;
    }

    if (lhs.has_slot() != rhs.has_slot()) {
        return false;
    }
    if (lhs.has_slot() && lhs.slot().capacity() != rhs.slot().capacity()) {
        return false;
    }

    if (lhs.has_disk() != rhs.has_disk()) {
        return false;
    }
    if (lhs.has_disk() &&
        (lhs.disk().capacity() != rhs.disk().capacity() ||
         lhs.disk().exclusive() != rhs.disk().exclusive() ||
         lhs.disk().volume_id() != rhs.disk().volume_id() ||
         lhs.disk().bandwidth() != rhs.disk().bandwidth()))
    {
        return false;
    }

    return true;
}

bool operator!=(const TResourceStatus_TAllocation& lhs, const TResourceStatus_TAllocation& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NProto

namespace NYP::NServer::NScheduler {

using namespace NServer::NObjects;

////////////////////////////////////////////////////////////////////////////////

TResourceCapacities& operator += (TResourceCapacities& lhs, const TResourceCapacities& rhs)
{
    for (size_t index = 0; index < MaxResourceDimensions; ++index) {
        lhs[index] += rhs[index];
    }
    return lhs;
}

TResourceCapacities operator + (const TResourceCapacities& lhs, const TResourceCapacities& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

bool Dominates(const TResourceCapacities& lhs, const TResourceCapacities& rhs)
{
    for (size_t index = 0; index < MaxResourceDimensions; ++index) {
        if (lhs[index] < rhs[index]) {
            return false;
        }
    }
    return true;
}

TResourceCapacities Max(const TResourceCapacities& a, const TResourceCapacities& b)
{
    TResourceCapacities result = {};
    for (size_t index = 0; index < MaxResourceDimensions; ++index) {
        result[index] = std::max(a[index], b[index]);
    }
    return result;
}

TResourceCapacities SubtractWithClamp(const TResourceCapacities& lhs, const TResourceCapacities& rhs)
{
    TResourceCapacities result = {};
    for (size_t index = 0; index < MaxResourceDimensions; ++index) {
        result[index] = lhs[index] < rhs[index] ? 0 : lhs[index] - rhs[index];
    }
    return result;
}

bool IsHomogeneous(EResourceKind kind)
{
    return
        kind == EResourceKind::Cpu ||
        kind == EResourceKind::Memory ||
        kind == EResourceKind::Slot;
}

TResourceCapacities MakeCpuCapacities(ui64 capacity)
{
    return {{capacity, 0, 0}};
}

TResourceCapacities MakeMemoryCapacities(ui64 capacity)
{
    return {{capacity, 0, 0}};
}

TResourceCapacities MakeSlotCapacities(ui64 capacity)
{
    return {{capacity, 0, 0}};
}

TResourceCapacities MakeDiskCapacities(ui64 capacity, ui64 volumeSlots, ui64 bandwidth)
{
    return {{capacity, volumeSlots, bandwidth}};
}

ui64 GetHomogeneousCapacity(const TResourceCapacities& capacities)
{
    return capacities[0];
}

ui64 GetCpuCapacity(const TResourceCapacities& capacities)
{
    return GetHomogeneousCapacity(capacities);
}

ui64 GetMemoryCapacity(const TResourceCapacities& capacities)
{
    return GetHomogeneousCapacity(capacities);
}

ui64 GetSlotCapacity(const TResourceCapacities& capacities)
{
    return GetHomogeneousCapacity(capacities);
}

ui64 GetDiskCapacity(const TResourceCapacities& capacities)
{
    return capacities[0];
}

ui64 GetDiskBandwidth(const TResourceCapacities& capacities)
{
    return capacities[2];
}

TResourceCapacities GetResourceCapacities(const NClient::NApi::NProto::TResourceSpec& spec)
{
    if (spec.has_cpu()) {
        return MakeCpuCapacities(spec.cpu().total_capacity());
    } else if (spec.has_memory()) {
        return MakeMemoryCapacities(spec.memory().total_capacity());
    } else if (spec.has_slot()) {
        return MakeSlotCapacities(spec.slot().total_capacity());
    } else if (spec.has_disk()) {
        return MakeDiskCapacities(
            spec.disk().total_capacity(),
            spec.disk().total_volume_slots(),
            spec.disk().total_bandwidth());
    } else {
        THROW_ERROR_EXCEPTION("Malformed resource spec");
    }
}

TResourceCapacities GetAllocationCapacities(const NClient::NApi::NProto::TResourceStatus_TAllocation& allocation)
{
    if (allocation.has_cpu()) {
        return MakeCpuCapacities(allocation.cpu().capacity());
    } else if (allocation.has_memory()) {
        return MakeMemoryCapacities(allocation.memory().capacity());
    } else if (allocation.has_slot()) {
        return MakeSlotCapacities(allocation.slot().capacity());
    } else if (allocation.has_disk()) {
        return MakeDiskCapacities(
            allocation.disk().capacity(),
            /*volumeSlots*/ 1,
            allocation.disk().bandwidth());
    } else {
        THROW_ERROR_EXCEPTION("Malformed resource allocation");
    }
}

bool GetAllocationExclusive(const NClient::NApi::NProto::TResourceStatus_TAllocation& allocation)
{
    return allocation.has_disk() && allocation.disk().exclusive();
}

TLocalResourceAllocator::TResource BuildAllocatorResource(
    const TObjectId& resourceId,
    const NClient::NApi::NProto::TResourceSpec& spec,
    const std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation>& scheduledAllocations,
    const std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation>& actualAllocations)
{
    TLocalResourceAllocator::TResource resource;

    resource.Id = resourceId;
    resource.Capacities = GetResourceCapacities(spec);

    if (spec.has_cpu()) {
        resource.Kind = EResourceKind::Cpu;
    } else if (spec.has_memory()) {
        resource.Kind = EResourceKind::Memory;
    } else if (spec.has_slot()) {
        resource.Kind = EResourceKind::Slot;
    } else if (spec.has_disk()) {
        resource.Kind = EResourceKind::Disk;
        resource.ProtoDiskSpec = &spec.disk();
    } else {
        THROW_ERROR_EXCEPTION("Malformed spec of resource %Qlv",
            resourceId);
    }

    auto buildAllocations = [&] (auto* allocations, const auto& protoAllocations) {
        for (const auto& protoAllocation : protoAllocations) {
            allocations->emplace_back();
            auto& allocation = allocations->back();
            allocation.ProtoAllocation = &protoAllocation;
            allocation.PodId = protoAllocation.pod_id();
            allocation.Capacities = GetAllocationCapacities(protoAllocation);
            if (protoAllocation.has_disk()) {
                allocation.Exclusive = protoAllocation.disk().exclusive();
                allocation.RequestId = protoAllocation.disk().volume_id();
            }
        }
    };
    buildAllocations(&resource.ScheduledAllocations, scheduledAllocations);
    buildAllocations(&resource.ActualAllocations, actualAllocations);

    return resource;
}

void BuildProtoResourceAllocation(
    const TObjectId& podId,
    const TLocalResourceAllocator::TResource& resource,
    const TLocalResourceAllocator::TAllocation& allocation,
    NClient::NApi::NProto::TResourceStatus_TAllocation* protoAllocation)
{
    protoAllocation->set_pod_id(podId);
    switch (resource.Kind) {
        case EResourceKind::Cpu:
            protoAllocation->mutable_cpu()->set_capacity(GetCpuCapacity(allocation.Capacities));
            break;

        case EResourceKind::Memory:
            protoAllocation->mutable_memory()->set_capacity(GetMemoryCapacity(allocation.Capacities));
            break;

        case EResourceKind::Slot:
            protoAllocation->mutable_slot()->set_capacity(GetSlotCapacity(allocation.Capacities));
            break;

        case EResourceKind::Disk: {
            auto* protoDisk = protoAllocation->mutable_disk();
            protoDisk->set_capacity(GetDiskCapacity(allocation.Capacities));
            protoDisk->set_exclusive(allocation.Exclusive);
            protoDisk->set_volume_id(allocation.RequestId);
            protoDisk->set_bandwidth(GetDiskBandwidth(allocation.Capacities));
            break;
        }

        default:
            YT_ABORT();
    }
}

ui64 GetDiskVolumeRequestBandwidthGuarantee(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request)
{
    if (request.has_quota_policy()) {
        return request.quota_policy().bandwidth_guarantee();
    } else if (request.has_exclusive_policy()) {
        return request.exclusive_policy().min_bandwidth();
    } else {
        THROW_ERROR_EXCEPTION("Malformed disk volume request");
    }
}

std::optional<ui64> GetDiskVolumeRequestOptionalBandwidthLimit(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request)
{
    if (request.has_quota_policy()) {
        if (request.quota_policy().has_bandwidth_limit()) {
            return request.quota_policy().bandwidth_limit();
        }
        return std::nullopt;
    } else if (request.has_exclusive_policy()) {
        return std::nullopt;
    } else {
        THROW_ERROR_EXCEPTION("Malformed disk volume request");
    }
}

TResourceCapacities GetDiskVolumeRequestCapacities(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request)
{
    if (request.has_quota_policy()) {
        return MakeDiskCapacities(
            request.quota_policy().capacity(),
            /*volumeSlots*/ 1,
            request.quota_policy().bandwidth_guarantee());
    } else if (request.has_exclusive_policy()) {
        return MakeDiskCapacities(
            request.exclusive_policy().min_capacity(),
            /*volumeSlots*/ 1,
            request.exclusive_policy().min_bandwidth());
    } else {
        THROW_ERROR_EXCEPTION("Malformed disk volume request");
    }
}

bool GetDiskVolumeRequestExclusive(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request)
{
    return request.has_exclusive_policy();
}

NClient::NApi::NProto::EDiskVolumePolicy GetDiskVolumeRequestPolicy(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request)
{
    if (request.has_quota_policy()) {
        return NClient::NApi::NProto::DVP_QUOTA;
    } else if (request.has_exclusive_policy()) {
        return NClient::NApi::NProto::DVP_EXCLUSIVE;
    } else {
        THROW_ERROR_EXCEPTION("Malformed disk volume request");
    }
}

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
        case EResourceKind::Slot:
            result.mutable_slot()->set_capacity(GetSlotCapacity(capacities));
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
        podIdToStats[allocation.pod_id()].Scheduled.Accumulate(allocation);
    }

    for (const auto& allocation : actualAllocations) {
        podIdToStats[allocation.pod_id()].Actual.Accumulate(allocation);
    }

    TAllocationStatistics statistics;
    for (const auto& pair : podIdToStats) {
        const auto& podStatistics = pair.second;
        statistics += Max(podStatistics.Scheduled, podStatistics.Actual);
    }
    return statistics;
}

std::vector<TLocalResourceAllocator::TRequest> BuildAllocatorResourceRequests(
    const TObjectId& podId,
    const NObjects::NProto::TPodSpecEtc& spec,
    const NObjects::NProto::TPodStatusEtc& status,
    const std::vector<TLocalResourceAllocator::TResource>& resources)
{
    try {
        std::vector<TLocalResourceAllocator::TRequest> requests;

        const TLocalResourceAllocator::TResource* cpuResource = nullptr;
        const TLocalResourceAllocator::TResource* memoryResource = nullptr;
        const TLocalResourceAllocator::TResource* slotResource = nullptr;
        for (auto& resource : resources) {
            switch (resource.Kind) {
                case EResourceKind::Cpu:
                    cpuResource = &resource;
                    break;

                case EResourceKind::Memory:
                    memoryResource = &resource;
                    break;

                case EResourceKind::Slot:
                    slotResource = &resource;
                    break;

                default:
                    break;
            }
        }

        // Cpu
        const auto& resourceRequests = spec.resource_requests();
        if (resourceRequests.vcpu_guarantee() > 0) {
            TLocalResourceAllocator::TRequest request;
            request.Kind = EResourceKind::Cpu;
            request.Capacities = MakeCpuCapacities(resourceRequests.vcpu_guarantee());
            if (cpuResource) {
                request.MatchingResources.push_back(cpuResource);
            }
            requests.push_back(request);
        }

        // Memory
        if (resourceRequests.memory_limit() > 0) {
            TLocalResourceAllocator::TRequest request;
            request.Kind = EResourceKind::Memory;
            request.Capacities = MakeMemoryCapacities(resourceRequests.memory_limit());
            if (memoryResource) {
                request.MatchingResources.push_back(memoryResource);
            }
            requests.push_back(request);
        }

        // Slot
        if (resourceRequests.slot() > 0) {
            TLocalResourceAllocator::TRequest request;
            request.Kind = EResourceKind::Slot;
            request.Capacities = MakeSlotCapacities(resourceRequests.slot());
            if (slotResource) {
                request.MatchingResources.push_back(slotResource);
            }
            requests.push_back(request);
        }

        // Disk
        THashMap<TString, const NClient::NApi::NProto::TPodStatus_TDiskVolumeAllocation*> idToDiskAllocation;
        for (const auto& allocation : status.disk_volume_allocations()) {
            if (!idToDiskAllocation.emplace(allocation.id(), &allocation).second) {
                THROW_ERROR_EXCEPTION("Duplicate disk volume request %Qv",
                    allocation.id());
            }
        }
        for (int index = 0; index < spec.disk_volume_requests_size(); ++index) {
            const auto& volumeRequest = spec.disk_volume_requests(index);
            auto policy = GetDiskVolumeRequestPolicy(volumeRequest);
            TLocalResourceAllocator::TRequest request;
            request.ProtoVolumeRequest = &volumeRequest;
            request.Kind = EResourceKind::Disk;
            request.Id = volumeRequest.id();
            request.Capacities = GetDiskVolumeRequestCapacities(volumeRequest);
            request.Exclusive = GetDiskVolumeRequestExclusive(volumeRequest);
            auto it = idToDiskAllocation.find(request.Id);
            if (it == idToDiskAllocation.end()) {
                request.AllocationId = GenerateUuid();
            } else {
                const auto* allocation = it->second;
                request.AllocationId = allocation->volume_id();
            }
            for (const auto& resource : resources) {
                if (resource.Kind != EResourceKind::Disk) {
                    continue;
                }
                if (resource.ProtoDiskSpec->storage_class() != volumeRequest.storage_class()) {
                    continue;
                }
                const auto& supportedPolicies = resource.ProtoDiskSpec->supported_policies();
                if (std::find(supportedPolicies.begin(), supportedPolicies.end(), policy) == supportedPolicies.end()) {
                    continue;
                }
                request.MatchingResources.push_back(&resource);
            }
            requests.push_back(request);
        }

        return requests;
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error building allocator resource requests for pod %Qv",
            podId);
    }
}

void UpdatePodDiskVolumeAllocations(
    google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TDiskVolumeAllocation>* allocations,
    const std::vector<TLocalResourceAllocator::TRequest>& allocatorRequests,
    const std::vector<TLocalResourceAllocator::TResponse>& allocatorResponses)
{
    THashMap<TString, const NClient::NApi::NProto::TPodStatus_TDiskVolumeAllocation*> idToAllocation;
    for (const auto& allocation : *allocations) {
        if (!idToAllocation.emplace(allocation.id(), &allocation).second) {
            THROW_ERROR_EXCEPTION("Duplicate volume allocation %Qv",
                allocation.id());
        }
    }

    auto getReadOperationRateFactor = [] (const auto& diskSpec) {
        return diskSpec.has_read_operation_rate_divisor()
            ? 1.0 / diskSpec.read_operation_rate_divisor()
            : 0.0;
    };
    auto getWriteOperationRateFactor = [] (const auto& diskSpec) {
        return diskSpec.has_write_operation_rate_divisor()
            ? 1.0 / diskSpec.write_operation_rate_divisor()
            : 0.0;
    };

    google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TDiskVolumeAllocation> newAllocations;
    for (size_t index = 0; index < allocatorRequests.size(); ++index) {
        const auto& request = allocatorRequests[index];
        const auto& response = allocatorResponses[index];

        if (request.Kind != EResourceKind::Disk) {
            continue;
        }
        const auto& diskSpec = *response.Resource->ProtoDiskSpec;

        const auto& volumeRequest = *request.ProtoVolumeRequest;
        auto* volumeAllocation = newAllocations.Add();
        if (response.ExistingAllocation) {
            auto it = idToAllocation.find(volumeRequest.id());
            YT_VERIFY(it != idToAllocation.end());
            volumeAllocation->CopyFrom(*it->second);
        } else {
            volumeAllocation->set_id(volumeRequest.id());
            volumeAllocation->set_capacity(GetDiskCapacity(request.Capacities));
            volumeAllocation->set_resource_id(response.Resource->Id);
            volumeAllocation->set_volume_id(request.AllocationId);
            volumeAllocation->set_device(diskSpec.device());

            auto volumeBandwidthGuarantee = GetDiskVolumeRequestBandwidthGuarantee(volumeRequest);
            volumeAllocation->set_read_bandwidth_guarantee(
                volumeBandwidthGuarantee * diskSpec.read_bandwidth_factor());
            volumeAllocation->set_write_bandwidth_guarantee(
                volumeBandwidthGuarantee * diskSpec.write_bandwidth_factor());
            volumeAllocation->set_read_operation_rate_guarantee(
                volumeBandwidthGuarantee * getReadOperationRateFactor(diskSpec));
            volumeAllocation->set_write_operation_rate_guarantee(
                volumeBandwidthGuarantee * getWriteOperationRateFactor(diskSpec));
        }

        // For disk resource attributes update without rescheduling is supported only partially.
        if (auto optionalVolumeBandwidthLimit = GetDiskVolumeRequestOptionalBandwidthLimit(volumeRequest)) {
            auto volumeBandwidthLimit = *optionalVolumeBandwidthLimit;
            volumeAllocation->set_read_bandwidth_limit(
                volumeBandwidthLimit * diskSpec.read_bandwidth_factor());
            volumeAllocation->set_write_bandwidth_limit(
                volumeBandwidthLimit * diskSpec.write_bandwidth_factor());
            volumeAllocation->set_read_operation_rate_limit(
                volumeBandwidthLimit * getReadOperationRateFactor(diskSpec));
            volumeAllocation->set_write_operation_rate_limit(
                volumeBandwidthLimit * getWriteOperationRateFactor(diskSpec));
        } else {
            volumeAllocation->clear_read_bandwidth_limit();
            volumeAllocation->clear_write_bandwidth_limit();
            volumeAllocation->clear_read_operation_rate_limit();
            volumeAllocation->clear_write_operation_rate_limit();
        }

        volumeAllocation->mutable_labels()->CopyFrom(volumeRequest.labels());
    }
    allocations->Swap(&newAllocations);
}

void UpdateScheduledResourceAllocations(
    const TObjectId& podId,
    const TObjectId& podUuid,
    google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TResourceAllocation>* scheduledResourceAllocations,
    const std::vector<NObjects::TResource*>& nativeResources,
    const std::vector<TLocalResourceAllocator::TResource>& allocatorResources,
    const std::vector<TLocalResourceAllocator::TRequest>& allocatorRequests,
    const std::vector<TLocalResourceAllocator::TResponse>& allocatorResponses)
{
    std::vector<std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation>> newScheduledAllocationsPerResource(nativeResources.size());

    for (size_t resourceIndex = 0; resourceIndex < nativeResources.size(); ++resourceIndex) {
        auto* resource = nativeResources[resourceIndex];
        auto* scheduledAllocations = resource->Status().ScheduledAllocations().Get();
        for (auto& allocation : *scheduledAllocations) {
            if (allocation.pod_id() != podId) {
                newScheduledAllocationsPerResource[resourceIndex].push_back(std::move(allocation));
            }
        }
    }

    scheduledResourceAllocations->Clear();
    for (size_t index = 0; index < allocatorRequests.size(); ++index) {
        const auto& request = allocatorRequests[index];
        const auto& response = allocatorResponses[index];
        auto resourceIndex = response.Resource - allocatorResources.data();

        auto* scheduledResourceAllocation = scheduledResourceAllocations->Add();
        scheduledResourceAllocation->set_resource_id(response.Resource->Id);

        if (response.ExistingAllocation) {
            newScheduledAllocationsPerResource[resourceIndex].push_back(*response.ExistingAllocation->ProtoAllocation);
        } else {
            newScheduledAllocationsPerResource[resourceIndex].emplace_back();
            auto& allocation = newScheduledAllocationsPerResource[resourceIndex].back();
            allocation.set_pod_id(podId);
            allocation.set_pod_uuid(podUuid);
            switch (request.Kind) {
                case EResourceKind::Cpu:
                    allocation.mutable_cpu()->set_capacity(GetCpuCapacity(request.Capacities));
                    break;
                case EResourceKind::Memory:
                    allocation.mutable_memory()->set_capacity(GetMemoryCapacity(request.Capacities));
                    break;
                case EResourceKind::Slot:
                    allocation.mutable_slot()->set_capacity(GetSlotCapacity(request.Capacities));
                    break;
                case EResourceKind::Disk: {
                    auto* protoDisk = allocation.mutable_disk();
                    protoDisk->set_capacity(GetDiskCapacity(request.Capacities));
                    protoDisk->set_exclusive(request.Exclusive);
                    protoDisk->set_volume_id(request.AllocationId);
                    protoDisk->set_bandwidth(GetDiskBandwidth(request.Capacities));
                    break;
                }
                default:
                    YT_ABORT();
            }
        }
    }

    for (size_t resourceIndex = 0; resourceIndex < nativeResources.size(); ++resourceIndex) {
        auto* resource = nativeResources[resourceIndex];
        resource->Status().ScheduledAllocations()->swap(newScheduledAllocationsPerResource[resourceIndex]);
    }
}

////////////////////////////////////////////////////////////////////////////////


void TAllocationStatistics::Accumulate(const TLocalResourceAllocator::TAllocation& allocation)
{
    Capacities += allocation.Capacities;
    Used |= true;
    UsedExclusively = allocation.Exclusive;
}

void TAllocationStatistics::Accumulate(const TLocalResourceAllocator::TRequest& request)
{
    Capacities += request.Capacities;
    Used |= true;
    UsedExclusively = request.Exclusive;
}

void TAllocationStatistics::Accumulate(const NClient::NApi::NProto::TResourceStatus_TAllocation& allocation)
{
    Capacities += GetAllocationCapacities(allocation);
    Used |= true;
    UsedExclusively = GetAllocationExclusive(allocation);
}

TAllocationStatistics Max(const TAllocationStatistics& lhs, const TAllocationStatistics& rhs)
{
    TAllocationStatistics result;
    result.Capacities = Max(lhs.Capacities, rhs.Capacities);
    result.Used = lhs.Used | rhs.Used;
    result.UsedExclusively = lhs.UsedExclusively | rhs.UsedExclusively;
    return result;
}

TAllocationStatistics& operator+=(TAllocationStatistics& lhs, const TAllocationStatistics& rhs)
{
    lhs.Capacities += rhs.Capacities;
    lhs.Used |= rhs.Used;
    lhs.UsedExclusively |= rhs.UsedExclusively;
    return lhs;
}

TAllocationStatistics operator+(const TAllocationStatistics& lhs, const TAllocationStatistics& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler

