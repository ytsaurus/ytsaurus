#include "global_resource_allocator.h"
#include "cluster.h"
#include "pod.h"
#include "pod_set.h"
#include "node.h"
#include "node_segment.h"
#include "label_filter_cache.h"
#include "helpers.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TGlobalResourceAllocator::TAllocationContext
{
public:
    TAllocationContext(TNode* node, TPod* pod)
        : CpuResource_(node->CpuResource())
        , MemoryResource_(node->MemoryResource())
        , DiskResources_(node->DiskResources())
        , Node_(node)
        , Pod_(pod)
    { }

    bool TryAcquireAntiaffinityVacancies()
    {
        return Node_->CanAcquireAntiaffinityVacancies(Pod_);
    }

    void Commit()
    {
        Node_->AcquireAntiaffinityVacancies(Pod_);
        Node_->CpuResource() = CpuResource_;
        Node_->MemoryResource() = MemoryResource_;
        Node_->DiskResources() = DiskResources_;
    }

    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, CpuResource);
    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, MemoryResource);
    DEFINE_BYREF_RW_PROPERTY(TNode::TDiskResources, DiskResources);

private:
    TNode* const Node_;
    TPod* const Pod_;
};


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

    const auto& cache = nodeSegment->GetNodeLabelFilterCache();

    const auto& allSegmentNodesOrError = cache->GetFilteredObjects(TString());
    YCHECK(allSegmentNodesOrError.IsOK());
    const auto& allSegmentNodes = allSegmentNodesOrError.Value();
    if (allSegmentNodes.empty()) {
        return TError("No alive nodes in segment %Qv",
            nodeSegment->GetId());
    }

    const auto& nodeFilter = pod->SpecOther().node_filter();
    const auto& nodesOrError = cache->GetFilteredObjects(nodeFilter);
    if (!nodesOrError.IsOK()) {
        return TError("Error applying pod node filter %Qv",
            nodeFilter)
            << TError(nodesOrError);
    }

    const auto& nodes = nodesOrError.Value();
    if (nodes.empty()) {
        return TError("No alive node in segment %Qv matches pod filter %Qv",
            nodeSegment->GetId(),
            nodeFilter)
            << TError(nodesOrError);
    }

    const int MaxAttempts = 10;
    for (int attempt = 0; attempt < MaxAttempts; ++attempt) {
        auto* node = nodes[RandomNumber(nodes.size())];
        if (TryAllocation(node, pod)) {
            return node;
        }
    }

    return TError("No matching alive node could be allocated for pod");
}

bool TGlobalResourceAllocator::TryAllocation(TNode* node, TPod* pod)
{
    TAllocationContext context(node, pod);

    if (!context.TryAcquireAntiaffinityVacancies()) {
        return false;
    }

    const auto& resourceRequests = pod->SpecOther().resource_requests();

    if (resourceRequests.vcpu_guarantee() > 0) {
        if (!context.CpuResource().TryAllocate(MakeCpuCapacities(resourceRequests.vcpu_guarantee()))) {
            return false;
        }
    }

    if (resourceRequests.memory_guarantee() > 0) {
        if (!context.MemoryResource().TryAllocate(MakeMemoryCapacities(resourceRequests.memory_guarantee()))) {
            return false;
        }
    }

    for (const auto& volumeRequest : pod->SpecOther().disk_volume_requests()) {
        const auto& storageClass = volumeRequest.storage_class();
        auto policy = GetDiskVolumeRequestPolicy(volumeRequest);
        auto capacities = GetDiskVolumeRequestCapacities(volumeRequest);
        bool exclusive = GetDiskVolumeRequestExclusive(volumeRequest);
        bool satisfied = false;
        for (auto& diskResource : context.DiskResources()) {
            if (diskResource.TryAllocate(exclusive, storageClass, policy, capacities)) {
                satisfied = true;
                break;
            }
        }
        if (!satisfied) {
            return false;
        }
    }

    context.Commit();

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NScheduler
} // namespace NYP

