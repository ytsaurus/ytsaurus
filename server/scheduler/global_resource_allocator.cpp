#include "global_resource_allocator.h"
#include "cluster.h"
#include "internet_address.h"
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

class TInternetAddressAllocationContext
{
public:
    TInternetAddressAllocationContext(
        const TNode* node,
        const TPod* pod,
        THashMap<TString, size_t>* networkModuleIdToFreeAddressCount)
        : Node_(node)
        , Pod_(pod)
        , NetworkModuleIdToFreeAddressCount_(networkModuleIdToFreeAddressCount)
    { }

    bool TryAllocate()
    {
        ReleaseAddresses();
        return TryAcquireAddresses();
    }

    void Commit()
    {
        AllocationSize_ = 0;
    }

    ~TInternetAddressAllocationContext()
    {
        ReleaseAddresses();
    }

private:
    void ReleaseAddresses()
    {
        if (AllocationSize_ > 0) {
            (*NetworkModuleIdToFreeAddressCount_)[NetworkModuleId_] += AllocationSize_;
            AllocationSize_ = 0;
        }
    }

    bool TryAcquireAddresses()
    {
        size_t allocationSize = 0;

        for (const auto& addressRequest : Pod_->SpecOther().ip6_address_requests()) {
            if (addressRequest.enable_internet()) {
                ++allocationSize;
            }
        }

        if (allocationSize == 0) {
            return true;
        }

        const auto& networkModuleId = Node_->Spec().network_module_id();
        if (!NetworkModuleIdToFreeAddressCount_->has(networkModuleId)) {
            return false;
        }

        auto& freeAddressCount = (*NetworkModuleIdToFreeAddressCount_)[networkModuleId];
        if (freeAddressCount < allocationSize) {
            return false;
        }
        freeAddressCount -= allocationSize;

        NetworkModuleId_ = networkModuleId;
        AllocationSize_ = allocationSize;

        return true;
    }

    const TNode* Node_;
    const TPod* Pod_;

    THashMap<TString, size_t>* NetworkModuleIdToFreeAddressCount_;
    TString NetworkModuleId_;
    size_t AllocationSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeAllocationContext
{
public:
    TNodeAllocationContext(TNode* node, TPod* pod)
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

void TGlobalResourceAllocator::ReconcileState(
    const TClusterPtr& cluster)
{
    Cluster_ = std::move(cluster);

    NetworkModuleIdToFreeAddressCount_.clear();
    for (auto* address : cluster->GetInternetAddresses()) {
        if (!address->Status().has_pod_id()) {
            const auto& networkModuleId = address->Spec().network_module_id();
            ++NetworkModuleIdToFreeAddressCount_[networkModuleId];
        }
    }
}

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
    TNodeAllocationContext nodeAllocationContext(node, pod);
    TInternetAddressAllocationContext internetAddressAllocationContext(node, pod, &NetworkModuleIdToFreeAddressCount_);

    if (!nodeAllocationContext.TryAcquireAntiaffinityVacancies()) {
        return false;
    }

    if (!internetAddressAllocationContext.TryAllocate()) {
        return false;
    }

    const auto& resourceRequests = pod->SpecOther().resource_requests();

    if (resourceRequests.vcpu_guarantee() > 0) {
        if (!nodeAllocationContext.CpuResource().TryAllocate(MakeCpuCapacities(resourceRequests.vcpu_guarantee()))) {
            return false;
        }
    }

    if (resourceRequests.memory_limit() > 0) {
        if (!nodeAllocationContext.MemoryResource().TryAllocate(MakeMemoryCapacities(resourceRequests.memory_limit()))) {
            return false;
        }
    }

    for (const auto& volumeRequest : pod->SpecOther().disk_volume_requests()) {
        const auto& storageClass = volumeRequest.storage_class();
        auto policy = GetDiskVolumeRequestPolicy(volumeRequest);
        auto capacities = GetDiskVolumeRequestCapacities(volumeRequest);
        bool exclusive = GetDiskVolumeRequestExclusive(volumeRequest);
        bool satisfied = false;
        for (auto& diskResource : nodeAllocationContext.DiskResources()) {
            if (diskResource.TryAllocate(exclusive, storageClass, policy, capacities)) {
                satisfied = true;
                break;
            }
        }
        if (!satisfied) {
            return false;
        }
    }

    nodeAllocationContext.Commit();
    internetAddressAllocationContext.Commit();

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NScheduler
} // namespace NYP

