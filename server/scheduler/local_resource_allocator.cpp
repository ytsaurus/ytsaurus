#include "local_resource_allocator.h"

#include "helpers.h"
#include "resource_traits.h"

#include <yt/core/ytree/convert.h>

namespace NYP::NServer::NScheduler {

using namespace NCluster;

using NObjects::GenerateUuid;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsReallocationAllowed(
    EResourceKind resourceKind,
    const TResourceCapacities& oldCapacities,
    const TResourceCapacities& newCapacities)
{
    if (IsAnonymousResource(resourceKind)) {
        return true;
    }
    switch (resourceKind) {
        case EResourceKind::Disk:
            return IsDiskVolumeReallocationAllowed(oldCapacities, newCapacities);
        case EResourceKind::Gpu:
            return false;
        default:
            YT_UNIMPLEMENTED();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool TLocalResourceAllocator::TryAllocate(
    const TObjectId& podId,
    const std::vector<TRequest>& requests,
    const std::vector<TResource>& resources,
    std::vector<TResponse>* responses,
    std::vector<TError>* errors)
{
    ResourceStatistics_.clear();

    // Indexed by allocation id.
    THashMap<TObjectId, std::pair<const TResource*, const TAllocation*>> existingAllocations;

    // Cf. ComputeTotalAllocationStatistics.
    struct TPodStatistics
    {
        TAllocationStatistics Scheduled;
        TAllocationStatistics Actual;
    };

    THashMap<TObjectId, TPodStatistics> podIdToStats;
    for (const auto& resource : resources) {
        ResourceStatistics_.emplace_back();
        auto& resourceStatistics = ResourceStatistics_.back();

        podIdToStats.clear();

        for (const auto& allocation : resource.ScheduledAllocations) {
            if (allocation.PodId == podId) {
                if (allocation.Id) {
                    if (!existingAllocations.emplace(allocation.Id, std::make_pair(&resource, &allocation)).second) {
                        THROW_ERROR_EXCEPTION("Duplicate allocation id %Qv found while examining scheduled allocations",
                            allocation.Id);
                    }
                }
            } else {
                Accumulate(podIdToStats[allocation.PodId].Scheduled, allocation);
            }
        }

        for (const auto& allocation : resource.ActualAllocations) {
            if (allocation.PodId != podId) {
                Accumulate(podIdToStats[allocation.PodId].Actual, allocation);
            }
        }

        for (const auto& pair : podIdToStats) {
            const auto& podStatistics = pair.second;
            resourceStatistics += Max(podStatistics.Scheduled, podStatistics.Actual);
        }
    }

    const auto canAllocateWithoutOvercommit = [] (
        const TResource& resource,
        const NCluster::TAllocationStatistics& statistics,
        const TRequest& request,
        std::vector<TError>* errors)
    {
        if (statistics.UsedExclusively) {
            return false;
        }

        if (statistics.Used && request.Exclusive) {
            return false;
        }

        if (!Dominates(resource.Capacities, statistics.Capacities + request.Capacities)) {
            // For simplicity provide an error for homogeneous resources only.
            if (IsHomogeneousResource(request.Kind)) {
                if (errors) {
                    errors->push_back(TError(
                        "%Qlv resource %Qv limit exceeded: allocated %v, requested %v, total %v",
                        request.Kind,
                        resource.Id,
                        GetHomogeneousCapacity(statistics.Capacities),
                        GetHomogeneousCapacity(request.Capacities),
                        GetHomogeneousCapacity(resource.Capacities)));
                }
            }
            return false;
        }

        return true;
    };

    responses->clear();
    responses->resize(requests.size());

    // Handle relationship between non-anonymous resources and existing allocations.
    for (size_t requestIndex = 0; requestIndex < requests.size(); ++requestIndex) {
        const auto& request = requests[requestIndex];
        auto& response = (*responses)[requestIndex];

        if (IsAnonymousResource(request.Kind)) {
            YT_VERIFY(!request.ExistingAllocationId);
            continue;
        }

        // No existing allocation found, allocate a new one.
        if (!request.ExistingAllocationId) {
            response.AllocationId = GenerateUuid();
            continue;
        }

        auto it = existingAllocations.find(request.ExistingAllocationId);
        YT_VERIFY(it != existingAllocations.end());
        const auto* resource = it->second.first;
        const auto* allocation = it->second.second;
        auto resourceIndex = resource - resources.data();
        auto& resourceStatistics = ResourceStatistics_[resourceIndex];

        YT_VERIFY(allocation->PodId == podId);

        if (request.Exclusive != allocation->Exclusive) {
            if (errors) {
                errors->push_back(TError(
                    "Reallocation within resource %Qv is forbidden "
                    "due to mismatched exclusiveness of request %v and allocation %Qv",
                    resource->Id,
                    FormatRequest(request),
                    allocation->Id));
            }
            return false;
        }

        // Reuse allocation.
        if (request.Capacities == allocation->Capacities) {
            response.Resource = resource;
            response.ExistingAllocation = allocation;
            // Resource overcommit check is skipped intentionally to allow
            // pod updates in case of already existing overcommit.
            Accumulate(resourceStatistics, request);
            continue;
        }

        if (!IsReallocationAllowed(
            request.Kind,
            /* oldCapacities */ allocation->Capacities,
            /* newCapacities */ request.Capacities))
        {
            if (errors) {
                errors->push_back(TError(
                    "Reallocation within resource %Qv is forbidden "
                    "due to mismatched capacities of request %v and allocation %Qv",
                    resource->Id,
                    FormatRequest(request),
                    allocation->Id)
                    << TErrorAttribute("allocation_capacities", allocation->Capacities)
                    << TErrorAttribute("request_capacities", request.Capacities));
            }
            return false;
        }

        std::vector<TError> innerErrors;
        if (canAllocateWithoutOvercommit(*resource, resourceStatistics, request, &innerErrors)) {
            response.Resource = resource;
            response.AllocationId = GenerateUuid();
            Accumulate(resourceStatistics, request);
        } else {
            if (errors) {
                errors->push_back(TError(
                    "Reallocation of request %v within resource %Qv is forbidden due to resource overcommit",
                    FormatRequest(request),
                    resource->Id)
                    << std::move(innerErrors));
            }
            return false;
        }
    }

    // Building new allocations.
    bool allSatisfied = true;
    for (size_t requestIndex = 0; requestIndex < requests.size(); ++requestIndex) {
        const auto& request = requests[requestIndex];
        auto& response = (*responses)[requestIndex];

        // Request is already satisfied by an existing allocation or reallocated near it.
        if (response.Resource) {
            continue;
        }

        if (request.MatchingResources.empty()) {
            if (errors) {
                errors->push_back(TError("Found no matching resource for %v",
                    FormatRequest(request)));
            }
            allSatisfied = false;
            continue;
        }

        std::vector<TError> innerErrors;

        bool satisified = false;
        for (const auto* resource : request.MatchingResources) {
            auto resourceIndex = resource - resources.data();
            auto& statistics = ResourceStatistics_[resourceIndex];

            if (canAllocateWithoutOvercommit(*resource, statistics, request, &innerErrors)) {
                response.Resource = resource;
                Accumulate(statistics, request);
                satisified = true;
                break;
            }
        }

        if (!satisified) {
            allSatisfied = false;
            if (errors) {
                errors->push_back(TError("Cannot satisfy %v",
                    FormatRequest(request))
                    << std::move(innerErrors));
            }
        }
    }

    return allSatisfied;
}

TString TLocalResourceAllocator::FormatRequest(const TRequest& request)
{
    TStringBuilder builder;
    if (request.Kind == EResourceKind::Disk) {
        if (request.Exclusive) {
            builder.AppendString("exclusive ");
        } else {
            builder.AppendString("non-exclusive ");
        }
    }

    builder.AppendFormat("%Qlv request", request.Kind);

    if (request.Id) {
        builder.AppendFormat(" %Qv", request.Id);
    }

    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler

