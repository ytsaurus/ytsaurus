#include "helpers.h"

#include <yp/server/objects/pod.h>
#include <yp/server/objects/resource.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

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

bool IsHomogeneous(EResourceKind kind)
{
    return
        kind == EResourceKind::Cpu ||
        kind == EResourceKind::Memory;
}

TResourceCapacities MakeCpuCapacities(ui64 capacity)
{
    return {capacity, 0};
}

TResourceCapacities MakeMemoryCapacities(ui64 capacity)
{
    return {capacity, 0};
}

TResourceCapacities MakeDiskCapacities(ui64 capacity, ui64 volumeSlots)
{
    return {capacity, volumeSlots};
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

ui64 GetDiskCapacity(const TResourceCapacities& capacities)
{
    return capacities[0];
}

TResourceCapacities GetResourceCapacities(const NClient::NApi::NProto::TResourceSpec& spec)
{
    if (spec.has_cpu()) {
        return MakeCpuCapacities(spec.cpu().total_capacity());
    } else if (spec.has_memory()) {
        return MakeMemoryCapacities(spec.memory().total_capacity());
    } else if (spec.has_disk()) {
        return MakeDiskCapacities(spec.disk().total_capacity(), spec.disk().total_volume_slots());
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
    } else if (allocation.has_disk()) {
        return MakeDiskCapacities(allocation.disk().capacity(), 1);
    } else {
        THROW_ERROR_EXCEPTION("Malformed resource allocation");
    }
}

bool GetAllocationExclusive(const NClient::NApi::NProto::TResourceStatus_TAllocation& allocation)
{
    return allocation.has_disk() && allocation.disk().exclusive();
}

void ValidateDiskVolumeRequests(
    const google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest>& requests)
{
    THashSet<TString> ids;
    for (const auto& request : requests) {
        if (!ids.insert(request.id()).second) {
            THROW_ERROR_EXCEPTION("Duplicate disk volume request %Qv",
                request.id());
        }
        if (!request.has_quota_policy() &&
            !request.has_exclusive_policy())
        {
            THROW_ERROR_EXCEPTION("Missing policy in disk volume request %Qv",
                request.id());
        }
    }
}

void ValidateDiskVolumeRequestsUpdate(
    const google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest>& oldRequests,
    const google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest>& newRequests)
{
    THashMap<TString, const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest*> idToRequest;
    for (const auto& request : oldRequests) {
        idToRequest.emplace(request.id(), &request);
    }
    for (const auto& newRequest : newRequests) {
        auto it = idToRequest.find(newRequest.id());
        if (it == idToRequest.end()) {
            continue;
        }
        const auto& oldRequest = *it->second;
        if (!TScalarAttributeTraits<NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest>::Equals(oldRequest, newRequest)) {
            THROW_ERROR_EXCEPTION("Cannot change disk volume request %Qv since pod is already assigned to node",
                oldRequest.id());
        }
    }
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

        case EResourceKind::Disk: {
            auto* protoDisk = protoAllocation->mutable_disk();
            protoDisk->set_capacity(GetDiskCapacity(allocation.Capacities));
            protoDisk->set_exclusive(allocation.Exclusive);
            protoDisk->set_volume_id(allocation.RequestId);
            break;
        }

        default:
            Y_UNREACHABLE();
    }
}

TResourceCapacities GetDiskVolumeRequestCapacities(
    const NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest& request)
{
    if (request.has_quota_policy()) {
        return MakeDiskCapacities(request.quota_policy().capacity(), 1);
    } else if (request.has_exclusive_policy()) {
        return MakeDiskCapacities(request.exclusive_policy().min_capacity(), 1);
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

std::vector<TLocalResourceAllocator::TRequest> BuildAllocatorResourceRequests(
    const TObjectId& podId,
    const NObjects::NProto::TPodSpecOther& spec,
    const NObjects::NProto::TPodStatusOther& status,
    const std::vector<TLocalResourceAllocator::TResource>& resources)
{
    std::vector<TLocalResourceAllocator::TRequest> requests;

    const TLocalResourceAllocator::TResource* cpuResource = nullptr;
    const TLocalResourceAllocator::TResource* memoryResource = nullptr;
    for (auto& resource : resources) {
        switch (resource.Kind) {
            case EResourceKind::Cpu:
                cpuResource = &resource;
                break;

            case EResourceKind::Memory:
                memoryResource = &resource;
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
        request.Capacities = MakeCpuCapacities(resourceRequests.memory_limit());
        if (memoryResource) {
            request.MatchingResources.push_back(memoryResource);
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
            request.AllocationId = ToString(TGuid::Create());
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
}

void UpdatePodDiskVolumeAllocations(
    google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TDiskVolumeAllocation>* allocations,
    const std::vector<NObjects::TResource*>& nativeResources,
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

    google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TDiskVolumeAllocation> newAllocations;
    for (size_t index = 0; index < allocatorRequests.size(); ++index) {
        const auto& request = allocatorRequests[index];
        const auto& response = allocatorResponses[index];
        if (request.Kind != EResourceKind::Disk) {
            continue;
        }

        const auto& volumeRequest = *request.ProtoVolumeRequest;
        auto* volumeAllocation = newAllocations.Add();
        if (response.ExistingAllocation) {
            auto it = idToAllocation.find(volumeRequest.id());
            YCHECK(it != idToAllocation.end());
            volumeAllocation->MergeFrom(*it->second);
        } else {
            volumeAllocation->set_id(volumeRequest.id());
            volumeAllocation->set_capacity(GetDiskCapacity(request.Capacities));
            volumeAllocation->set_resource_id(response.Resource->Id);
            volumeAllocation->set_volume_id(request.AllocationId);
            volumeAllocation->set_device(response.Resource->ProtoDiskSpec->device());
        }
        volumeAllocation->mutable_labels()->MergeFrom(volumeRequest.labels());
    }
    allocations->Swap(&newAllocations);
}

void UpdateScheduledResourceAllocations(
    const TObjectId& podId,
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
            switch (request.Kind) {
                case EResourceKind::Cpu:
                    allocation.mutable_cpu()->set_capacity(GetCpuCapacity(request.Capacities));
                    break;
                case EResourceKind::Memory:
                    allocation.mutable_memory()->set_capacity(GetMemoryCapacity(request.Capacities));
                    break;
                case EResourceKind::Disk: {
                    auto* protoDisk = allocation.mutable_disk();
                    protoDisk->set_capacity(GetDiskCapacity(request.Capacities));
                    protoDisk->set_exclusive(request.Exclusive);
                    protoDisk->set_volume_id(request.AllocationId);
                    break;
                }
                default:
                    Y_UNREACHABLE();
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

} // namespace NScheduler
} // namespace NServer
} // namespace NYP

