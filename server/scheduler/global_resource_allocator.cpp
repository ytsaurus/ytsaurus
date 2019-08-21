#include "global_resource_allocator.h"
#include "cluster.h"
#include "internet_address.h"
#include "network_module.h"
#include "pod.h"
#include "pod_set.h"
#include "node.h"
#include "node_segment.h"
#include "label_filter_cache.h"
#include "helpers.h"
#include "config.h"
#include "pod_node_score.h"

#include <yt/core/misc/random.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAllocatorResourceType,
    (AntiaffinityVacancy)
    (Cpu)
    (Memory)
    (Disk)
    (IP6AddressVlan)
    (IP6AddressIP4Tunnel)
    (IP6Subnet)
    (Slot)
);

////////////////////////////////////////////////////////////////////////////////

class TGlobalResourceAllocatorStatistics
{
public:
    void RegisterUnsatisfiedResource(EAllocatorResourceType resourceType)
    {
        ++UnsatisfiedResourceCounts_[resourceType];
    }

    const TEnumIndexedVector<EAllocatorResourceType, int>& GetUnsatisfiedResourceCounts() const
    {
        return UnsatisfiedResourceCounts_;
    }

private:
    TEnumIndexedVector<EAllocatorResourceType, int> UnsatisfiedResourceCounts_;
};

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TGlobalResourceAllocatorStatistics& statistics,
    TStringBuf /*format*/)
{
    builder->AppendFormat("UnsatisfiedResourceCounts: %v",
        statistics.GetUnsatisfiedResourceCounts());
}

////////////////////////////////////////////////////////////////////////////////

class TInternetAddressAllocationContext
{
public:
    explicit TInternetAddressAllocationContext(TNetworkModule* networkModule)
        : NetworkModule_(networkModule)
    { }

    bool TryAllocate(int allocationSize)
    {
        ReleaseAddresses();
        return TryAcquireAddresses(allocationSize);
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
    TNetworkModule* const NetworkModule_;

    int AllocationSize_ = 0;

private:
    void ReleaseAddresses()
    {
        if (AllocationSize_ > 0) {
            YT_VERIFY(NetworkModule_);
            YT_VERIFY(NetworkModule_->AllocatedInternetAddressCount() >= AllocationSize_);
            NetworkModule_->AllocatedInternetAddressCount() -= AllocationSize_;
            AllocationSize_ = 0;
        }
    }

    bool TryAcquireAddresses(int allocationSize)
    {
        YT_VERIFY(allocationSize >= 0);

        if (allocationSize == 0) {
            return true;
        }

        if (!NetworkModule_) {
            return false;
        }

        if (NetworkModule_->AllocatedInternetAddressCount() + allocationSize > NetworkModule_->InternetAddressCount()) {
            return false;
        }

        NetworkModule_->AllocatedInternetAddressCount() += allocationSize;
        AllocationSize_ = allocationSize;

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNodeAllocationContext
{
public:
    TNodeAllocationContext(TNode* node, TPod* pod, const TClusterPtr& cluster)
        : CpuResource_(node->CpuResource())
        , MemoryResource_(node->MemoryResource())
        , SlotResource_(node->SlotResource())
        , DiskResources_(node->DiskResources())
        , Node_(node)
        , Pod_(pod)
        , InternetAddressAllocationContext_(cluster->FindNetworkModule(node->Spec().network_module_id()))
    { }

    bool TryAcquireIP6Addresses(TGlobalResourceAllocatorStatistics* statistics)
    {
        bool result = true;
        int internetAddressCount = 0;
        for (const auto& addressRequest : Pod_->IP6AddressRequests()) {
            if (!Node_->HasIP6SubnetInVlan(addressRequest.vlan_id()) && result) {
                statistics->RegisterUnsatisfiedResource(EAllocatorResourceType::IP6AddressVlan);
                result = false;
            }
            if (addressRequest.enable_internet() || !addressRequest.ip4_address_pool_id().empty()) {
                ++internetAddressCount;
            }
        }
        if (!InternetAddressAllocationContext_.TryAllocate(internetAddressCount)) {
            statistics->RegisterUnsatisfiedResource(EAllocatorResourceType::IP6AddressIP4Tunnel);
            result = false;
        }
        return result;
    }

    bool TryAcquireIP6Subnets()
    {
        // NB! Assumming this resource has infinite capacity we do not need to acquire anything.
        for (const auto& subnetRequest : Pod_->IP6SubnetRequests()) {
            if (!Node_->HasIP6SubnetInVlan(subnetRequest.vlan_id())) {
                return false;
            }
        }
        return true;
    }

    bool TryAcquireAntiaffinityVacancies()
    {
        // NB! Just checking: actual acquisition is deferred to the commit stage.
        return Node_->CanAcquireAntiaffinityVacancies(Pod_);
    }

    void Commit()
    {
        Node_->AcquireAntiaffinityVacancies(Pod_);

        Node_->CpuResource() = CpuResource_;
        Node_->MemoryResource() = MemoryResource_;
        Node_->SlotResource() = SlotResource_;
        Node_->DiskResources() = DiskResources_;

        InternetAddressAllocationContext_.Commit();
    }

    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, CpuResource);
    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, MemoryResource);
    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, SlotResource);
    DEFINE_BYREF_RW_PROPERTY(TNode::TDiskResources, DiskResources);

private:
    TNode* const Node_;
    TPod* const Pod_;

    TInternetAddressAllocationContext InternetAddressAllocationContext_;
};

DEFINE_ENUM(EAllocatorNodeSelectionStrategy,
    (Every)
    (Random)
);

////////////////////////////////////////////////////////////////////////////////

class TBasicGlobalResourceAllocator
    : public IGlobalResourceAllocator
{
public:
    TBasicGlobalResourceAllocator(
        EAllocatorNodeSelectionStrategy nodeSelectionStrategy,
        TPodNodeScoreConfigPtr podNodeScoreConfig)
        : NodeSelectionStrategy_(nodeSelectionStrategy)
        , PodNodeScore_(CreatePodNodeScore(std::move(podNodeScoreConfig)))
    { }

    virtual void ReconcileState(const TClusterPtr& cluster) override
    {
        Cluster_ = cluster;
    }

    virtual TErrorOr<TNode*> ComputeAllocation(TPod* pod) override
    {
        YT_LOG_DEBUG("Started computing pod allocation via basic global resource allocator (PodId: %v, NodeSelectionStrategy: %v)",
            pod->GetId(),
            NodeSelectionStrategy_);

        auto* nodeSegment = pod->GetPodSet()->GetNodeSegment();
        const auto& cache = nodeSegment->GetSchedulableNodeLabelFilterCache();

        const auto& allSegmentNodesOrError = cache->GetFilteredObjects(TString());
        YT_VERIFY(allSegmentNodesOrError.IsOK());
        const auto& allSegmentNodes = allSegmentNodesOrError.Value();
        if (allSegmentNodes.empty()) {
            return TError("No schedulable nodes in segment %Qv",
                nodeSegment->GetId());
        }

        auto* podSet = pod->GetPodSet();
        // FIXME(YP-803): pod set could be abscent due to race condition.
        const auto& nodeFilter = pod->NodeFilter().Empty() && podSet
            ? podSet->NodeFilter()
            : pod->NodeFilter();

        const auto& nodesOrError = cache->GetFilteredObjects(nodeFilter);
        if (!nodesOrError.IsOK()) {
            return TError("Error applying pod node filter %Qv",
                nodeFilter)
                << TError(nodesOrError);
        }

        const auto& nodes = nodesOrError.Value();
        if (nodes.empty()) {
            return TError("No alive nodes in segment %Qv match filter %Qv",
                nodeSegment->GetId(),
                nodeFilter)
                << TError(nodesOrError);
        }

        TGlobalResourceAllocatorStatistics statistics;
        switch (NodeSelectionStrategy_) {
            case EAllocatorNodeSelectionStrategy::Random: {
                const int SampleSize = 10;

                std::vector<TNode*> sampledNodes;
                sampledNodes.reserve(SampleSize);
                NYT::RandomSampleN(
                    nodes.cbegin(),
                    nodes.cend(),
                    std::back_inserter(sampledNodes),
                    SampleSize,
                    [] (size_t max) { return RandomNumber(max); });

                auto* resultNode = AllocateMinimumScoreNode(pod, sampledNodes, &statistics);
                if (resultNode) {
                    return resultNode;
                }

                return TError("No matching node from a random sample of size %v could be allocated for pod (%v)",
                    sampledNodes.size(),
                    statistics);
            }
            case EAllocatorNodeSelectionStrategy::Every: {
                auto* resultNode = AllocateMinimumScoreNode(pod, nodes, &statistics);
                if (resultNode) {
                    return resultNode;
                }

                return TError("No matching alive node (from %v in total after filtering) could be allocated for pod (%v)",
                    nodes.size(),
                    statistics);
            }
            default:
                YT_UNIMPLEMENTED();
        }
    }

private:
    const EAllocatorNodeSelectionStrategy NodeSelectionStrategy_;
    const IPodNodeScorePtr PodNodeScore_;

    TClusterPtr Cluster_;

    TNode* AllocateMinimumScoreNode(
        TPod* pod,
        const std::vector<TNode*>& nodes,
        TGlobalResourceAllocatorStatistics* statistics)
    {
        TNode* resultNode = nullptr;
        TPodNodeScoreValue resultScore{};
        for (auto* node : nodes) {
            if (TryAllocation(node, pod, statistics)) {
                auto score = PodNodeScore_->Compute(node, pod);
                if (!resultNode || score < resultScore) {
                    resultNode = node;
                    resultScore = score;
                }
            }
        }
        if (resultNode) {
            YT_LOG_DEBUG("Found node for pod with minimum score (Score: %v, Pod: %v, Node: %v)",
                resultScore,
                pod->GetId(),
                resultNode->GetId());
            Allocate(resultNode, pod);
        }
        return resultNode;
    }

    void Allocate(TNode* node, TPod* pod)
    {
        TGlobalResourceAllocatorStatistics statistics;
        TNodeAllocationContext allocationContext(node, pod, Cluster_);
        if (!TryAllocation(&allocationContext, pod, &statistics)) {
            THROW_ERROR_EXCEPTION("Could not allocate resources for pod %Qv on node %Qv",
                pod->GetId(),
                node->GetId());
        }
        allocationContext.Commit();
    }

    bool TryAllocation(
        TNode* node,
        TPod* pod,
        TGlobalResourceAllocatorStatistics* statistics)
    {
        TNodeAllocationContext allocationContext(node, pod, Cluster_);
        return TryAllocation(&allocationContext, pod, statistics);
    }

    bool TryAllocation(
        TNodeAllocationContext* nodeAllocationContext,
        TPod* pod,
        TGlobalResourceAllocatorStatistics* statistics)
    {
        bool result = true;

        if (!nodeAllocationContext->TryAcquireIP6Addresses(statistics)) {
            result = false;
        }

        if (!nodeAllocationContext->TryAcquireIP6Subnets()) {
            result = false;
            statistics->RegisterUnsatisfiedResource(EAllocatorResourceType::IP6Subnet);
        }

        if (!nodeAllocationContext->TryAcquireAntiaffinityVacancies()) {
            result = false;
            statistics->RegisterUnsatisfiedResource(EAllocatorResourceType::AntiaffinityVacancy);
        }

        const auto& resourceRequests = pod->ResourceRequests();

        if (resourceRequests.vcpu_guarantee() > 0) {
            if (!nodeAllocationContext->CpuResource().TryAllocate(MakeCpuCapacities(resourceRequests.vcpu_guarantee()))) {
                result = false;
                statistics->RegisterUnsatisfiedResource(EAllocatorResourceType::Cpu);
            }
        }

        if (resourceRequests.memory_limit() > 0) {
            if (!nodeAllocationContext->MemoryResource().TryAllocate(MakeMemoryCapacities(resourceRequests.memory_limit()))) {
                result = false;
                statistics->RegisterUnsatisfiedResource(EAllocatorResourceType::Memory);
            }
        }

        if (resourceRequests.slot() > 0) {
            if (!nodeAllocationContext->SlotResource().TryAllocate(MakeSlotCapacities(resourceRequests.slot()))) {
                result = false;
                statistics->RegisterUnsatisfiedResource(EAllocatorResourceType::Slot);
            }
        }

        bool allDiskVolumeRequestsSatisfied = true;
        for (const auto& volumeRequest : pod->DiskVolumeRequests()) {
            const auto& storageClass = volumeRequest.storage_class();
            auto policy = GetDiskVolumeRequestPolicy(volumeRequest);
            auto capacities = GetDiskVolumeRequestCapacities(volumeRequest);
            bool exclusive = GetDiskVolumeRequestExclusive(volumeRequest);
            bool satisfied = false;
            for (auto& diskResource : nodeAllocationContext->DiskResources()) {
                if (diskResource.TryAllocate(exclusive, storageClass, policy, capacities)) {
                    satisfied = true;
                    break;
                }
            }
            if (!satisfied) {
                allDiskVolumeRequestsSatisfied = false;
                break;
            }
        }

        if (!allDiskVolumeRequestsSatisfied) {
            result = false;
            statistics->RegisterUnsatisfiedResource(EAllocatorResourceType::Disk);
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeGlobalResourceAllocator
    : public IGlobalResourceAllocator
{
public:
    explicit TCompositeGlobalResourceAllocator(TGlobalResourceAllocatorConfigPtr config)
        : Config_(std::move(config))
        , RandomNodeSelectionAllocator_(
            New<TBasicGlobalResourceAllocator>(
                EAllocatorNodeSelectionStrategy::Random,
                Config_->PodNodeScore))
        , EveryNodeSelectionAllocator_(
            New<TBasicGlobalResourceAllocator>(
                EAllocatorNodeSelectionStrategy::Every,
                Config_->PodNodeScore))
    {
        YT_VERIFY(Config_->EveryNodeSelectionStrategy->Enable);
    }

    virtual void ReconcileState(const TClusterPtr& cluster) override
    {
        VERIFY_THREAD_AFFINITY(SchedulerThread);

        YT_LOG_DEBUG("Started reconciling state of the global resource allocator");

        RandomNodeSelectionAllocator_->ReconcileState(cluster);
        EveryNodeSelectionAllocator_->ReconcileState(cluster);

        std::vector<TObjectId> expiredPodIds;
        int removedPodCount = 0;
        int assignedPodCount = 0;
        for (const auto& pair : PodComputeAllocationHistory_) {
            const auto& podId = pair.first;
            const auto& history = pair.second;

            const auto* pod = cluster->FindPod(podId);

            // We compare uuids here to overcome possible pod ids collision:
            // current pod could be removed and another pod could be created with the same pod_id
            // between consecutive state reconcilations.
            if (!pod || pod->Uuid() != history->Uuid) {
                ++removedPodCount;
                expiredPodIds.push_back(podId);
            } else if (pod->GetNode()) {
                ++assignedPodCount;
                expiredPodIds.push_back(podId);
            }
        }

        YT_LOG_DEBUG("Erasing expired pods from the global resource allocator history (RemovedPodCount: %v, AssignedPodCount: %v, HistorySize: %v)",
            removedPodCount,
            assignedPodCount,
            PodComputeAllocationHistory_.size());

        for (const auto& podId : expiredPodIds) {
            YT_VERIFY(PodComputeAllocationHistory_.erase(podId));
        }

        YT_LOG_DEBUG("State of the global resource allocator reconciled");
    }

    virtual TErrorOr<TNode*> ComputeAllocation(TPod* pod) override
    {
        VERIFY_THREAD_AFFINITY(SchedulerThread);

        auto* history = GetOrCreatePodComputeAllocationHistory(pod);

        TErrorOr<TNode*> nodeOrError;
        if (history->IterationCountToEveryNodeSelection == 0) {
            history->IterationCountToEveryNodeSelection = GenerateIterationCountToEveryNodeSelection();

            nodeOrError = EveryNodeSelectionAllocator_->ComputeAllocation(pod);
            if (!nodeOrError.IsOK()) {
                history->StrategyToLastError[EAllocatorNodeSelectionStrategy::Every] = nodeOrError;
            }
        } else {
            --history->IterationCountToEveryNodeSelection;

            nodeOrError = RandomNodeSelectionAllocator_->ComputeAllocation(pod);
            if (!nodeOrError.IsOK()) {
                history->StrategyToLastError[EAllocatorNodeSelectionStrategy::Random] = nodeOrError;
            }
        }

        if (nodeOrError.IsOK()) {
            YT_VERIFY(PodComputeAllocationHistory_.erase(pod->GetId()));
            return nodeOrError;
        }

        TError combinedError("Could not compute pod allocation");
        for (auto nodeSelectionStrategy : TEnumTraits<EAllocatorNodeSelectionStrategy>::GetDomainValues()) {
            const auto& error = history->StrategyToLastError[nodeSelectionStrategy];
            if (!error.IsOK()) {
                combinedError.InnerErrors().push_back(error);
            }
        }

        return combinedError;
    }

private:
    const TGlobalResourceAllocatorConfigPtr Config_;

    IGlobalResourceAllocatorPtr RandomNodeSelectionAllocator_;
    IGlobalResourceAllocatorPtr EveryNodeSelectionAllocator_;

    DECLARE_THREAD_AFFINITY_SLOT(SchedulerThread);

    struct TPodComputeAllocationHistory
    {
        // Last error occurred while computing allocation per node selection strategy.
        TEnumIndexedVector<EAllocatorNodeSelectionStrategy, TError> StrategyToLastError;

        // Loop iteration count before next usage of every node selection strategy.
        int IterationCountToEveryNodeSelection;

        // Used to track pod identity.
        TObjectId Uuid;
    };

    THashMap<TObjectId, std::unique_ptr<TPodComputeAllocationHistory>> PodComputeAllocationHistory_;

    int GenerateIterationCountToEveryNodeSelection() const
    {
        const auto& config = Config_->EveryNodeSelectionStrategy;
        auto result = std::max(config->IterationPeriod, 1) - 1;
        if (config->IterationSplay > 0) {
            result += RandomNumber(static_cast<size_t>(config->IterationSplay));
        }
        return result;
    }

    TPodComputeAllocationHistory* GetOrCreatePodComputeAllocationHistory(TPod* pod)
    {
        const auto& podId = pod->GetId();
        auto it = PodComputeAllocationHistory_.find(podId);
        if (it == PodComputeAllocationHistory_.end()) {
            auto history = std::make_unique<TPodComputeAllocationHistory>();
            history->IterationCountToEveryNodeSelection = GenerateIterationCountToEveryNodeSelection();
            history->Uuid = pod->Uuid();

            it = PodComputeAllocationHistory_.emplace(podId, std::move(history)).first;
        } else {
            const auto& history = it->second;

            YT_VERIFY(history->Uuid == pod->Uuid());
        }
        return it->second.get();
    }
};

////////////////////////////////////////////////////////////////////////////////

IGlobalResourceAllocatorPtr CreateGlobalResourceAllocator(TGlobalResourceAllocatorConfigPtr config)
{
    if (config->EveryNodeSelectionStrategy->Enable) {
        return New<TCompositeGlobalResourceAllocator>(std::move(config));
    }
    return New<TBasicGlobalResourceAllocator>(
        EAllocatorNodeSelectionStrategy::Random,
        std::move(config->PodNodeScore));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
