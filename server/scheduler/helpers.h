#pragma once

#include "local_resource_allocator.h"

#include <yp/client/api/proto/data_model.pb.h>

namespace NYP::NClient::NApi::NProto {

////////////////////////////////////////////////////////////////////////////////

bool operator == (
    const TResourceStatus_TAllocation& lhs,
    const TResourceStatus_TAllocation& rhs);

bool operator != (
    const TResourceStatus_TAllocation& lhs,
    const TResourceStatus_TAllocation& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NProto

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TLocalResourceAllocator::TResource BuildAllocatorResource(
    const TObjectId& resourceId,
    const NClient::NApi::NProto::TResourceSpec& spec,
    const std::vector<NYP::NClient::NApi::NProto::TResourceStatus_TAllocation>& scheduledAllocations,
    const std::vector<NYP::NClient::NApi::NProto::TResourceStatus_TAllocation>& actualAllocations);

std::vector<TLocalResourceAllocator::TRequest> BuildAllocatorResourceRequests(
    const TObjectId& podId,
    const NObjects::NProto::TPodSpecEtc& spec,
    const NObjects::NProto::TPodStatusEtc& status,
    const std::vector<TLocalResourceAllocator::TResource>& resources);

void UpdatePodDiskVolumeAllocations(
    google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TDiskVolumeAllocation>* allocations,
    const std::vector<TLocalResourceAllocator::TRequest>& allocatorRequests,
    const std::vector<TLocalResourceAllocator::TResponse>& allocatorResponses);

void UpdatePodGpuAllocations(
    google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TGpuAllocation>* allocations,
    const std::vector<TLocalResourceAllocator::TRequest>& allocatorRequests,
    const std::vector<TLocalResourceAllocator::TResponse>& allocatorResponses);

void UpdateScheduledResourceAllocations(
    const TObjectId& podId,
    const TObjectId& podUuid,
    google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TResourceAllocation>* scheduledResourceAllocations,
    const std::vector<NObjects::TResource*>& nativeResources,
    const std::vector<TLocalResourceAllocator::TResource>& allocatorResources,
    const std::vector<TLocalResourceAllocator::TRequest>& allocatorRequests,
    const std::vector<TLocalResourceAllocator::TResponse>& allocatorResponses);

////////////////////////////////////////////////////////////////////////////////

void Accumulate(
    NCluster::TAllocationStatistics& statistics,
    const TLocalResourceAllocator::TAllocation& allocation);

void Accumulate(
    NCluster::TAllocationStatistics& statistics,
    const TLocalResourceAllocator::TRequest& request);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
