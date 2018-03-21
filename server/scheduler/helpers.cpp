#include "helpers.h"
#include "cluster.h"
#include "pod.h"
#include "pod_set.h"
#include "node.h"
#include "node_segment.h"
#include "label_filter_cache.h"

#include <yt/core/misc/error.h>
#include <yt/core/misc/protobuf_helpers.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

using namespace NServer::NObjects;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

bool IsHomogeneous(EResourceKind kind)
{
    return
        kind == EResourceKind::Cpu ||
        kind == EResourceKind::Memory;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TLocalResourceAllocator::TRequest> TLocalResourceAllocator::BuildRequests(
    const NClient::NApi::NProto::TPodSpec_TResourceRequests& protoRequests)
{
    std::vector<TLocalResourceAllocator::TRequest> requests;

    // Cpu
    if (protoRequests.vcpu_guarantee() > 0) {
        requests.push_back({
            EResourceKind::Cpu,
            protoRequests.vcpu_guarantee()
        });        
    }

    // Memory
    if (protoRequests.memory_guarantee() > 0) {
        requests.push_back({
            EResourceKind::Memory,
            protoRequests.memory_guarantee()
        });        
    }

    return requests;
}

bool TLocalResourceAllocator::ComputeAllocations(
    const NObjects::TObjectId& podId,
    const std::vector<TRequest>& requests,
    const std::vector<TResource>& resources,
    std::vector<TAllocation>* allocations,
    std::vector<TError>* errors)
{
    TEnumIndexedVector<int, EResourceKind> homogeneousResourceIndexes;
    std::fill(homogeneousResourceIndexes.begin(), homogeneousResourceIndexes.end(), -1);

    TEnumIndexedVector<ui64, EResourceKind> totalHomogeneousCapacity{};
    TEnumIndexedVector<ui64, EResourceKind> allocatedHomogeneousCapacity{};

    THashSet<TObjectId> otherPodIds;
    THashMap<TObjectId, ui64> podIdToScheduledCapacity;
    THashMap<TObjectId, ui64> podIdToActualCapacity;

    errors->clear();
    for (int resourceIndex = 0; resourceIndex < static_cast<int>(resources.size()); ++resourceIndex) {
        const auto& resource = resources[resourceIndex];
        const auto& spec = *resource.Spec;

        auto kind = EResourceKind(spec.kind());
        if (!IsHomogeneous(kind)) {
            // TODO(babenko)
            continue;
        }

        if (homogeneousResourceIndexes[kind] >= 0) {
            errors->push_back(TError("Duplicate homogeneous resource %Qlv",
                kind));
            break;
        }

        homogeneousResourceIndexes[kind] = resourceIndex;

        totalHomogeneousCapacity[kind] = spec.total_capacity();

        auto processAllocations = [&] (auto* map, const auto& allocations) {
            map->clear();
            for (const auto& allocation : allocations) {
                auto allocationPodId = FromProto<TObjectId>(allocation.pod_id());
                // NB: Skip allocations already belonging to this particular pod.
                if (allocationPodId != podId) {
                    otherPodIds.insert(allocationPodId);
                    (*map)[allocationPodId] += allocation.capacity();
                }
            }
        };
        otherPodIds.clear();
        processAllocations(&podIdToScheduledCapacity, *resource.ScheduledAllocations);
        processAllocations(&podIdToActualCapacity, *resource.ActualAllocations);

        for (const auto& podId : otherPodIds) {
            allocatedHomogeneousCapacity[kind] += std::max(podIdToScheduledCapacity[podId], podIdToActualCapacity[podId]);
        }
    }

    if (!errors->empty()) {
        return false;
    }

    allocations->clear();
    allocations->resize(resources.size());
    for (const auto& request : requests) {
        auto kind = request.Kind;
        auto requestedCapacity = request.Capacity;

        if (!IsHomogeneous(kind)) {
            // TODO(babenko)
            errors->push_back(TError("%Qlv resources are not currently supported",
                kind));
            continue;
        }

        auto resourceIndex = homogeneousResourceIndexes[kind];
        if (resourceIndex < 0) {
            errors->push_back(TError("%Qlv resource requested but not found at node",
                kind));
            continue;
        }

        if (allocatedHomogeneousCapacity[kind] + requestedCapacity > totalHomogeneousCapacity[kind]) {
            errors->push_back(TError("%Qlv capacity limit exceeded at node: allocated %v, requested %v, total %v",
                kind,
                allocatedHomogeneousCapacity[kind],
                requestedCapacity,
                totalHomogeneousCapacity[kind]));
            continue;
        }

        allocatedHomogeneousCapacity[kind] += requestedCapacity;

        auto& allocation = (*allocations)[resourceIndex];
        allocation.NewAllocations.emplace_back();
        auto& newResourceAllocation = allocation.NewAllocations.back();
        ToProto(newResourceAllocation.mutable_pod_id(), podId);
        newResourceAllocation.set_capacity(requestedCapacity);
    }

    return errors->empty();
}

////////////////////////////////////////////////////////////////////////////////

TGlobalResourceAllocator::TGlobalResourceAllocator(const TClusterPtr& cluster)
    : Cluster_(std::move(cluster))
{ }

TErrorOr<TNode*> TGlobalResourceAllocator::ComputeAllocation(TPod* pod)
{
    auto* nodeSegment = pod->GetPodSet()->GetNodeSegment();
    if (!nodeSegment) {
        return nullptr;
    }

    // TODO(babenko): do something meaningful
    const auto& nodesOrError = nodeSegment->GetNodeLabelFilterCache()->GetFilteredObjects(pod->SpecOther().node_filter());
    if (!nodesOrError.IsOK()) {
        return TError("Error applying pod node filter")
            << TError(nodesOrError);
    }
    const auto& nodes = nodesOrError.Value();
    if (nodes.empty()) {
        return nullptr;
    }

    TLocalResourceAllocator localAllocator;
    auto requests = localAllocator.BuildRequests(pod->SpecOther().resource_requests());

    auto tryAllocation = [&] (TNode* node) {
        // Check.
        for (const auto& request : requests) {
            auto* status = node->GetHomegeneousResourceStatus(request.Kind);
            if (!status->CanAllocateCapacity(request.Capacity)) {
                return false;
            }
        }

        if (!node->CanAcquireAntiaffinityVacancies(pod)) {
            return false;
        }

        // Commit.
        for (const auto& request : requests) {
            auto* status = node->GetHomegeneousResourceStatus(request.Kind);
            YCHECK(status->TryAllocateCapacity(request.Capacity));
        }

        node->AcquireAntiaffinityVacancies(pod);

        return true;
    };

    const int MaxAttempts = 10;
    for (int attempt = 0; attempt < MaxAttempts; ++attempt) {
        auto* node = nodes[RandomNumber(nodes.size())];
        if (tryAllocation(node)) {
            return node;
        }
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP

