#include "local_resource_allocator.h"

#include "helpers.h"

#include <yp/server/objects/resource_helpers.h>

#include <yt/core/ytree/convert.h>

namespace NYP::NServer::NScheduler {

using namespace NCluster;

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

    responses->clear();

    // Initialize responses and reuse existing allocations.
    for (const auto& request : requests) {
        responses->emplace_back();

        if (!request.AllocationId) {
            // Anonymous resource allocation cannot be reused.
            continue;
        }

        auto it = existingAllocations.find(request.AllocationId);
        if (it == existingAllocations.end()) {
            // There is no allocation to reuse.
            continue;
        }

        auto* resource = it->second.first;
        auto* allocation = it->second.second;
        if (request.Capacities != allocation->Capacities) {
            THROW_ERROR_EXCEPTION("Mismatching capacities in resource request %Qv and scheduled allocation of resource %Qv",
                request.AllocationId,
                resource->Id)
                << TErrorAttribute("request_capacities", request.Capacities)
                << TErrorAttribute("allocation_capacities", allocation->Capacities);
        }

        if (request.Exclusive != allocation->Exclusive) {
            if (request.Exclusive && !allocation->Exclusive) {
                THROW_ERROR_EXCEPTION("Found an exclusive request %Qv satisfied by a non-exclusive scheduled allocation of resource %Qv",
                    request.AllocationId,
                    resource->Id);
            }
            YT_VERIFY(!request.Exclusive);
        }

        if (allocation->PodId != podId) {
            THROW_ERROR_EXCEPTION("Allocation %Qv of resource %Qv belongs to a different pod: expected %Qv, found %Qv",
                allocation->Id,
                resource->Id,
                podId,
                allocation->PodId);
        }

        responses->back().Resource = resource;
        responses->back().ExistingAllocation = allocation;

        auto resourceIndex = resource - resources.data();
        auto& resourceStatus = ResourceStatistics_[resourceIndex];
        Accumulate(resourceStatus, request);
    }

    // Building new allocations.
    bool allSatisfied = true;
    for (size_t requestIndex = 0; requestIndex < requests.size(); ++requestIndex) {
        const auto& request = requests[requestIndex];
        auto& response = (*responses)[requestIndex];

        if (response.Resource) {
            // Request is already satisfied by an existing allocation.
            continue;
        }

        if (request.MatchingResources.empty()) {
            if (errors) {
                errors->push_back(TError("Found no matching resource for %v",
                    FormatRequest(request)));
            }
            allSatisfied = false;
            break;
        }

        bool satisified = false;
        for (const auto* resource : request.MatchingResources) {
            auto resourceIndex = resource - resources.data();
            auto& statistics = ResourceStatistics_[resourceIndex];

            if (statistics.UsedExclusively) {
                continue;
            }

            if (statistics.Used && request.Exclusive) {
                continue;
            }

            if (!Dominates(resource->Capacities, statistics.Capacities + request.Capacities)) {
                if (IsSingletonResource(request.Kind)) {
                    if (errors) {
                        errors->push_back(TError(
                            "%Qlv capacity limit exceeded at node: allocated %v, requested %v, total %v",
                            request.Kind,
                            GetHomogeneousCapacity(statistics.Capacities),
                            GetHomogeneousCapacity(request.Capacities),
                            GetHomogeneousCapacity(resource->Capacities)));
                    }
                }
                continue;
            }

            response.Resource = resource;
            Accumulate(statistics, request);
            satisified = true;
            break;
        }

        if (!satisified) {
            allSatisfied = false;
            if (errors) {
                errors->push_back(TError("Cannot satisfy %v",
                    FormatRequest(request)));
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

