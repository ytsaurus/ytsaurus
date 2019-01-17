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

class TInternetAddressAllocationContext
{
public:
    TInternetAddressAllocationContext(
        TNetworkModule* networkModule,
        const TPod* pod)
        : NetworkModule_(networkModule)
        , Pod_(pod)
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
    TNetworkModule* const NetworkModule_;
    const TPod* const Pod_;

    int AllocationSize_ = 0;

private:
    void ReleaseAddresses()
    {
        if (AllocationSize_ > 0) {
            YCHECK(NetworkModule_);
            YCHECK(NetworkModule_->AllocatedInternetAddressCount() >= AllocationSize_);
            NetworkModule_->AllocatedInternetAddressCount() -= AllocationSize_;
            AllocationSize_ = 0;
        }
    }

    bool TryAcquireAddresses()
    {
        auto allocationSize = GetPodRequestedInternetAddressCount(Pod_);

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

    static int GetPodRequestedInternetAddressCount(const TPod* pod)
    {
        int result = 0;

        for (const auto& addressRequest : pod->SpecOther().ip6_address_requests()) {
            if (addressRequest.enable_internet()) {
                ++result;
            }
        }

        return result;
    }
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

DEFINE_ENUM(EAllocationErrorType,
    (AntiaffinityUnsatisfied)
    (InternetAddressUnsatisfied)
    (CpuUnsatisfied)
    (MemoryUnsatisfied)
    (DiskUnsatisfied)
);

////////////////////////////////////////////////////////////////////////////////

class TGlobalResourceAllocatorStatistics
{
public:
    void RegisterError(EAllocationErrorType errorType)
    {
        ++ErrorCountPerType_[errorType];
    }

    TString FormatErrors() const
    {
        return Format("%v", ErrorCountPerType_);
    }

private:
    TEnumIndexedVector<int, EAllocationErrorType> ErrorCountPerType_;
};

////////////////////////////////////////////////////////////////////////////////

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
        YCHECK(allSegmentNodesOrError.IsOK());
        const auto& allSegmentNodes = allSegmentNodesOrError.Value();
        if (allSegmentNodes.empty()) {
            return TError("No schedulable nodes in segment %Qv",
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

                return TError("No matching node from a random sample of size %v could be allocated for pod due to errors %v",
                    sampledNodes.size(),
                    statistics.FormatErrors());
            }
            case EAllocatorNodeSelectionStrategy::Every: {
                auto* resultNode = AllocateMinimumScoreNode(pod, nodes, &statistics);
                if (resultNode) {
                    return resultNode;
                }

                return TError("No matching alive node (from %v in total after filtering) could be allocated for pod due to errors %v",
                    nodes.size(),
                    statistics.FormatErrors());
            }
            default:
                Y_UNIMPLEMENTED();
        }
    }

private:
    const EAllocatorNodeSelectionStrategy NodeSelectionStrategy_;
    const IPodNodeScorePtr PodNodeScore_;

    TClusterPtr Cluster_;

    struct TAllocationContext
    {
        TAllocationContext(TNode* node, TPod* pod, const TClusterPtr& cluster)
            : NodeAllocationContext(node, pod)
            , InternetAddressAllocationContext(
                cluster->FindNetworkModule(node->Spec().network_module_id()),
                pod)
        { }

        TNodeAllocationContext NodeAllocationContext;
        TInternetAddressAllocationContext InternetAddressAllocationContext;
    };

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
        TAllocationContext allocationContext(node, pod, Cluster_);
        if (!TryAllocation(&allocationContext, pod, &statistics)) {
            THROW_ERROR_EXCEPTION("Could not allocate resources for pod %Qv on node %Qv",
                pod->GetId(),
                node->GetId());
        }
        CommitAllocation(&allocationContext);
    }

    bool TryAllocation(
        TNode* node,
        TPod* pod,
        TGlobalResourceAllocatorStatistics* statistics)
    {
        TAllocationContext allocationContext(node, pod, Cluster_);
        return TryAllocation(&allocationContext, pod, statistics);
    }

    bool TryAllocation(
        TAllocationContext* allocationContext,
        TPod* pod,
        TGlobalResourceAllocatorStatistics* statistics)
    {
        auto& nodeAllocationContext = allocationContext->NodeAllocationContext;
        auto& internetAddressAllocationContext = allocationContext->InternetAddressAllocationContext;

        bool result = true;

        if (!nodeAllocationContext.TryAcquireAntiaffinityVacancies()) {
            result = false;
            statistics->RegisterError(EAllocationErrorType::AntiaffinityUnsatisfied);
        }

        if (!internetAddressAllocationContext.TryAllocate()) {
            result = false;
            statistics->RegisterError(EAllocationErrorType::InternetAddressUnsatisfied);
        }

        const auto& resourceRequests = pod->SpecOther().resource_requests();

        if (resourceRequests.vcpu_guarantee() > 0) {
            if (!nodeAllocationContext.CpuResource().TryAllocate(MakeCpuCapacities(resourceRequests.vcpu_guarantee()))) {
                result = false;
                statistics->RegisterError(EAllocationErrorType::CpuUnsatisfied);
            }
        }

        if (resourceRequests.memory_limit() > 0) {
            if (!nodeAllocationContext.MemoryResource().TryAllocate(MakeMemoryCapacities(resourceRequests.memory_limit()))) {
                result = false;
                statistics->RegisterError(EAllocationErrorType::MemoryUnsatisfied);
            }
        }

        bool allDiskVolumeRequestsSatisfied = true;
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
                allDiskVolumeRequestsSatisfied = false;
                break;
            }
        }

        if (!allDiskVolumeRequestsSatisfied) {
            result = false;
            statistics->RegisterError(EAllocationErrorType::DiskUnsatisfied);
        }

        return result;
    }

    static void CommitAllocation(TAllocationContext* allocationContext)
    {
        allocationContext->NodeAllocationContext.Commit();
        allocationContext->InternetAddressAllocationContext.Commit();
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
        YCHECK(Config_->EveryNodeSelectionStrategy->Enable);
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
            if (!pod || pod->MetaOther().uuid() != history->Uuid) {
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
            YCHECK(PodComputeAllocationHistory_.erase(podId));
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
            YCHECK(PodComputeAllocationHistory_.erase(pod->GetId()));
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
        TEnumIndexedVector<TError, EAllocatorNodeSelectionStrategy> StrategyToLastError;

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
            history->Uuid = pod->MetaOther().uuid();

            it = PodComputeAllocationHistory_.emplace(podId, std::move(history)).first;
        } else {
            const auto& history = it->second;

            YCHECK(history->Uuid == pod->MetaOther().uuid());
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
} // namespace NYP::NScheduler::NObjects
