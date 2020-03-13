#include "helpers.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/server/objects/helpers.h>
#include <yp/server/objects/resource.h>

#include <yp/server/lib/cluster/allocation_statistics.h>

#include <yt/core/misc/error.h>

namespace NYP::NClient::NApi::NProto {

////////////////////////////////////////////////////////////////////////////////

bool operator == (
    const TResourceStatus_TAllocation& lhs,
    const TResourceStatus_TAllocation& rhs)
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

    if (lhs.has_network() != rhs.has_network()) {
        return false;
    }
    if (lhs.has_network() && lhs.network().bandwidth() != rhs.network().bandwidth())
    {
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

    if (lhs.has_gpu() != rhs.has_gpu()) {
        return false;
    }
    if (lhs.has_gpu() && lhs.gpu().capacity() != rhs.gpu().capacity()) {
        return false;
    }

    return true;
}

bool operator != (
    const TResourceStatus_TAllocation& lhs,
    const TResourceStatus_TAllocation& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NProto

namespace NYP::NServer::NScheduler {

using namespace NServer::NObjects;

////////////////////////////////////////////////////////////////////////////////

namespace {

THashMap<TObjectId, const NClient::NApi::NProto::TPodStatus_TDiskVolumeAllocation*> GetDiskVolumeRequestIdToAllocationMapping(
    const ::google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TDiskVolumeAllocation>& allocations)
{
    THashMap<TObjectId, const NClient::NApi::NProto::TPodStatus_TDiskVolumeAllocation*> requestIdToDiskAllocation;
    for (const auto& allocation : allocations) {
        // NB! Actually /pod/status/disk_volume_allocations/*/id means corresponding request id.
        if (!requestIdToDiskAllocation.emplace(allocation.id(), &allocation).second) {
            THROW_ERROR_EXCEPTION("Disk volume allocation %Qv refers to duplicate disk volume request %Qv",
                allocation.volume_id(),
                allocation.id());
        }
    }
    return requestIdToDiskAllocation;
}

THashMap<TObjectId, const NClient::NApi::NProto::TPodStatus_TGpuAllocation*> GetGpuRequestIdToAllocationMapping(
    const ::google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TGpuAllocation>& allocations)
{
    THashMap<TObjectId, const NClient::NApi::NProto::TPodStatus_TGpuAllocation*> requestIdToGpuAllocation;
    for (const auto& allocation : allocations) {
        if (!requestIdToGpuAllocation.emplace(allocation.request_id(), &allocation).second) {
            THROW_ERROR_EXCEPTION("GPU allocation %Qv refers to duplicate GPU request %Qv",
                allocation.id(),
                allocation.request_id());
        }
    }
    return requestIdToGpuAllocation;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TLocalResourceAllocator::TResource BuildAllocatorResource(
    const TObjectId& resourceId,
    const NClient::NApi::NProto::TResourceSpec& spec,
    const std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation>& scheduledAllocations,
    const std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation>& actualAllocations)
{
    TLocalResourceAllocator::TResource resource;

    resource.Id = resourceId;
    resource.Capacities = NCluster::GetResourceCapacities(spec);

    if (spec.has_cpu()) {
        resource.Kind = EResourceKind::Cpu;
    } else if (spec.has_memory()) {
        resource.Kind = EResourceKind::Memory;
    } else if (spec.has_network()) {
        resource.Kind = EResourceKind::Network;
    } else if (spec.has_slot()) {
        resource.Kind = EResourceKind::Slot;
    } else if (spec.has_disk()) {
        resource.Kind = EResourceKind::Disk;
        resource.ProtoDiskSpec = &spec.disk();
    } else if (spec.has_gpu()) {
        resource.Kind = EResourceKind::Gpu;
        resource.ProtoGpuSpec = &spec.gpu();
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
            allocation.Capacities = NCluster::GetAllocationCapacities(protoAllocation);
            if (protoAllocation.has_disk()) {
                allocation.Id = protoAllocation.disk().volume_id();
                allocation.Exclusive = protoAllocation.disk().exclusive();
            } else if (protoAllocation.has_gpu()) {
                allocation.Id = protoAllocation.gpu().allocation_id();
            }
        }
    };
    buildAllocations(&resource.ScheduledAllocations, scheduledAllocations);
    buildAllocations(&resource.ActualAllocations, actualAllocations);

    return resource;
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
        const TLocalResourceAllocator::TResource* networkResource = nullptr;
        const TLocalResourceAllocator::TResource* slotResource = nullptr;
        for (auto& resource : resources) {
            switch (resource.Kind) {
                case EResourceKind::Cpu:
                    cpuResource = &resource;
                    break;

                case EResourceKind::Memory:
                    memoryResource = &resource;
                    break;

                case EResourceKind::Network:
                    networkResource = &resource;
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
            request.Capacities = NCluster::MakeCpuCapacities(resourceRequests.vcpu_guarantee());
            if (cpuResource) {
                request.MatchingResources.push_back(cpuResource);
            }
            requests.push_back(request);
        }

        // Memory
        if (resourceRequests.memory_limit() > 0) {
            TLocalResourceAllocator::TRequest request;
            request.Kind = EResourceKind::Memory;
            request.Capacities = NCluster::MakeMemoryCapacities(resourceRequests.memory_limit());
            if (memoryResource) {
                request.MatchingResources.push_back(memoryResource);
            }
            requests.push_back(request);
        }

        // Network
        if (resourceRequests.network_bandwidth_guarantee() > 0) {
            TLocalResourceAllocator::TRequest request;
            request.Kind = EResourceKind::Network;
            request.Capacities = NCluster::MakeNetworkCapacities(resourceRequests.network_bandwidth_guarantee());
            if (networkResource) {
                request.MatchingResources.push_back(networkResource);
            }
            requests.push_back(request);
        }

        // Slot
        if (resourceRequests.slot() > 0) {
            TLocalResourceAllocator::TRequest request;
            request.Kind = EResourceKind::Slot;
            request.Capacities = NCluster::MakeSlotCapacities(resourceRequests.slot());
            if (slotResource) {
                request.MatchingResources.push_back(slotResource);
            }
            requests.push_back(request);
        }

        // Disk
        auto requestIdToDiskAllocation = GetDiskVolumeRequestIdToAllocationMapping(status.disk_volume_allocations());
        for (int index = 0; index < spec.disk_volume_requests_size(); ++index) {
            const auto& volumeRequest = spec.disk_volume_requests(index);
            auto policy = NCluster::GetDiskVolumeRequestPolicy(volumeRequest);
            TLocalResourceAllocator::TRequest request;
            request.ProtoVolumeRequest = &volumeRequest;
            request.Kind = EResourceKind::Disk;
            request.Id = volumeRequest.id();
            request.Capacities = NCluster::GetDiskVolumeRequestCapacities(volumeRequest);
            request.Exclusive = NCluster::GetDiskVolumeRequestExclusive(volumeRequest);
            auto it = requestIdToDiskAllocation.find(request.Id);
            if (it == requestIdToDiskAllocation.end()) {
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

        // Gpu
        auto requestIdToGpuAllocation = GetGpuRequestIdToAllocationMapping(status.gpu_allocations());
        for (int index = 0; index < spec.gpu_requests_size(); ++index) {
            const auto& gpuRequest = spec.gpu_requests(index);
            TLocalResourceAllocator::TRequest request;
            request.Id = gpuRequest.id();
            request.ProtoGpuRequest = &gpuRequest;
            request.Kind = EResourceKind::Gpu;
            request.Capacities = NCluster::GetGpuRequestCapacities(gpuRequest);
            auto it = requestIdToGpuAllocation.find(request.Id);
            if (it == requestIdToGpuAllocation.end()) {
                request.AllocationId = GenerateUuid();
            } else {
                const auto* allocation = it->second;
                request.AllocationId = allocation->id();
            }
            for (const auto& resource : resources) {
                if (resource.Kind != EResourceKind::Gpu) {
                    continue;
                }
                if (resource.ProtoGpuSpec->model() != gpuRequest.model()) {
                    continue;
                }
                if (gpuRequest.has_min_memory()
                    && resource.ProtoGpuSpec->total_memory() < gpuRequest.min_memory())
                {
                    continue;
                }
                if (gpuRequest.has_max_memory()
                    && resource.ProtoGpuSpec->total_memory() > gpuRequest.max_memory())
                {
                    continue;
                }
                request.MatchingResources.push_back(&resource);
            }
            // NB: we sort GPUs to ensure that greedy allocation
            // that is done in TryAllocate() is optimal.
            std::sort(
                request.MatchingResources.begin(),
                request.MatchingResources.end(),
                [] (const TLocalResourceAllocator::TResource* lhs,
                    const TLocalResourceAllocator::TResource* rhs)
                    {
                        return lhs->ProtoGpuSpec->total_memory() < rhs->ProtoGpuSpec->total_memory();
                    });
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
    auto requestIdToAllocation = GetDiskVolumeRequestIdToAllocationMapping(*allocations);

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
            auto it = requestIdToAllocation.find(volumeRequest.id());
            YT_VERIFY(it != requestIdToAllocation.end());
            volumeAllocation->CopyFrom(*it->second);
        } else {
            volumeAllocation->set_id(volumeRequest.id());
            volumeAllocation->set_capacity(NCluster::GetDiskCapacity(request.Capacities));
            volumeAllocation->set_resource_id(response.Resource->Id);
            volumeAllocation->set_volume_id(request.AllocationId);
            volumeAllocation->set_device(diskSpec.device());

            auto volumeBandwidthGuarantee = NCluster::GetDiskVolumeRequestBandwidthGuarantee(volumeRequest);
            volumeAllocation->set_read_bandwidth_guarantee(
                volumeBandwidthGuarantee * diskSpec.read_bandwidth_factor());
            volumeAllocation->set_write_bandwidth_guarantee(
                volumeBandwidthGuarantee * diskSpec.write_bandwidth_factor());
            volumeAllocation->set_read_operation_rate_guarantee(
                volumeBandwidthGuarantee * getReadOperationRateFactor(diskSpec));
            volumeAllocation->set_write_operation_rate_guarantee(
                volumeBandwidthGuarantee * getWriteOperationRateFactor(diskSpec));
        }

        if (auto optionalVolumeBandwidthLimit = NCluster::GetDiskVolumeRequestOptionalBandwidthLimit(volumeRequest)) {
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

void UpdatePodGpuAllocations(
    TPodGpuAllocations* allocations,
    const std::vector<TLocalResourceAllocator::TRequest>& allocatorRequests,
    const std::vector<TLocalResourceAllocator::TResponse>& allocatorResponses)
{
    auto requestIdToAllocation = GetGpuRequestIdToAllocationMapping(*allocations);

    TPodGpuAllocations newAllocations;
    for (size_t index = 0; index < allocatorRequests.size(); ++index) {
        const auto& request = allocatorRequests[index];
        const auto& response = allocatorResponses[index];

        if (request.Kind != EResourceKind::Gpu) {
            continue;
        }
        const auto& gpuSpec = *response.Resource->ProtoGpuSpec;

        const auto& gpuRequest = *request.ProtoGpuRequest;
        auto* gpuAllocation = newAllocations.Add();

        if (response.ExistingAllocation) {
            auto it = requestIdToAllocation.find(gpuRequest.id());
            YT_VERIFY(it != requestIdToAllocation.end());
            gpuAllocation->CopyFrom(*it->second);
        } else {
            gpuAllocation->set_id(request.AllocationId);
            gpuAllocation->set_request_id(gpuRequest.id());
            gpuAllocation->set_resource_id(response.Resource->Id);
            gpuAllocation->set_device_uuid(gpuSpec.uuid());
        }
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
                    allocation.mutable_cpu()->set_capacity(NCluster::GetCpuCapacity(request.Capacities));
                    break;
                case EResourceKind::Memory:
                    allocation.mutable_memory()->set_capacity(NCluster::GetMemoryCapacity(request.Capacities));
                    break;
                case EResourceKind::Network: {
                    auto* protoNetwork = allocation.mutable_network();
                    protoNetwork->set_bandwidth(NCluster::GetNetworkBandwidth(request.Capacities));
                    break;
                }
                case EResourceKind::Slot:
                    allocation.mutable_slot()->set_capacity(NCluster::GetSlotCapacity(request.Capacities));
                    break;
                case EResourceKind::Disk: {
                    auto* protoDisk = allocation.mutable_disk();
                    protoDisk->set_capacity(NCluster::GetDiskCapacity(request.Capacities));
                    protoDisk->set_exclusive(request.Exclusive);
                    protoDisk->set_volume_id(request.AllocationId);
                    protoDisk->set_bandwidth(NCluster::GetDiskBandwidth(request.Capacities));
                    break;
                }
                case EResourceKind::Gpu: {
                    auto* protoGpu = allocation.mutable_gpu();
                    protoGpu->set_allocation_id(request.AllocationId);
                    protoGpu->set_capacity(NCluster::GetGpuCapacity(request.Capacities));
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

bool IsSingletonResource(EResourceKind kind)
{
    return
        kind == EResourceKind::Cpu ||
        kind == EResourceKind::Memory ||
        kind == EResourceKind::Network ||
        kind == EResourceKind::Slot;
}

////////////////////////////////////////////////////////////////////////////////

void Accumulate(
    NCluster::TAllocationStatistics& statistics,
    const TLocalResourceAllocator::TAllocation& allocation)
{
    using NCluster::operator +=;
    statistics.Capacities += allocation.Capacities;
    statistics.Used |= true;
    statistics.UsedExclusively = allocation.Exclusive;
}

void Accumulate(
    NCluster::TAllocationStatistics& statistics,
    const TLocalResourceAllocator::TRequest& request)
{
    using NCluster::operator +=;
    statistics.Capacities += request.Capacities;
    statistics.Used |= true;
    statistics.UsedExclusively = request.Exclusive;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
