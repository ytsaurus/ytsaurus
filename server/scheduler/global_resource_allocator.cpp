#include "global_resource_allocator.h"

#include "config.h"
#include "node_score.h"
#include "pod_node_score.h"

#include <yp/server/lib/cluster/allocator.h>
#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/node_segment.h>
#include <yp/server/lib/cluster/object_filter_cache.h>
#include <yp/server/lib/cluster/pod.h>
#include <yp/server/lib/cluster/pod_set.h>

#include <yp/server/lib/objects/object_filter.h>

#include <yt/core/misc/random.h>
#include <yt/core/misc/string_builder.h>

namespace NYP::NServer::NScheduler {

using namespace NCluster;

////////////////////////////////////////////////////////////////////////////////

namespace {

TString GetHumanReadableDescription(const TAllocatorDiagnostics& diagnostics)
{
    TStringBuilder builder;

    const auto& unsatisfiedConstraintCounters = diagnostics.GetUnsatisfiedConstraintCounters();

    builder.AppendString("Number of nodes with unsatisfied constraints: {");

    TDelimitedStringBuilderWrapper delimitedBuilder(&builder);
    for (auto kind : TEnumTraits<EAllocatorConstraintKind>::GetDomainValues()) {
        auto count = unsatisfiedConstraintCounters[kind];
        if (count > 0) {
            delimitedBuilder->AppendFormat("%v: %v", kind, count);
        }
    }

    builder.AppendString("}");

    return builder.Flush();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAllocatorNodeSelectionStrategy,
    (Every)
    (Random)
);

////////////////////////////////////////////////////////////////////////////////

// This class holds allocator state shared between several allocator instances.
//
// It helps to prevent duplicate reconciliations in the hierarchy of allocators
// by enabling reconciliation only for the root allocator.
class TGlobalResourceAllocatorSharedState
{
public:
    explicit TGlobalResourceAllocatorSharedState(INodeScorePtr nodeScore)
        : State_(New<TState>())
    {
        State_->NodeScore = std::move(nodeScore);
    }

    TGlobalResourceAllocatorSharedState(TGlobalResourceAllocatorSharedState&& other) noexcept = default;
    TGlobalResourceAllocatorSharedState& operator = (TGlobalResourceAllocatorSharedState&& other) noexcept = default;

    const TClusterPtr& GetCluster() const
    {
        return State_->Cluster;
    }

    const INodeScorePtr& GetNodeScore() const
    {
        return State_->NodeScore;
    }

    void ReconcileState(const TClusterPtr& cluster)
    {
        if (!EnabledReconciliation_) {
            return;
        }

        State_->Cluster = cluster;
        State_->NodeScore->ReconcileState(cluster);
    }

    TGlobalResourceAllocatorSharedState WithDisabledReconciliation() const
    {
        auto result = *this;
        result.EnabledReconciliation_ = false;
        return result;
    }

private:
    struct TState
        : public TIntrinsicRefCounted
    {
        TClusterPtr Cluster;
        INodeScorePtr NodeScore;
    };

    // Hide copy constructors. State can be shared only with disabled reconciliation.
    // NB! We do not use MoveOnly mixin because we want to use copy constructors privately.
    TGlobalResourceAllocatorSharedState(const TGlobalResourceAllocatorSharedState& other) = default;
    TGlobalResourceAllocatorSharedState& operator = (const TGlobalResourceAllocatorSharedState& other) = default;


    // Store a pointer to the state such that changes in one instance will affect other instances.
    TIntrusivePtr<TState> State_;

    bool EnabledReconciliation_ = true;
};

////////////////////////////////////////////////////////////////////////////////

struct TScoreValue
{
    TNodeScoreValue Node = 0;
    TPodNodeScoreValue PodNode = 0;
};

bool operator < (TScoreValue lhs, TScoreValue rhs)
{
    if (lhs.Node < rhs.Node) {
        return true;
    }
    if (lhs.Node > rhs.Node) {
        return false;
    }
    return lhs.PodNode < rhs.PodNode;
}

void FormatValue(TStringBuilderBase* builder, TScoreValue score, TStringBuf /*format*/)
{
    builder->AppendString("{");

    TDelimitedStringBuilderWrapper delimitedBuilder(builder);

    delimitedBuilder->AppendFormat("Node: %v",
        score.Node);

    delimitedBuilder->AppendFormat("PodNode: %v",
        score.PodNode);

    builder->AppendString("}");
}

////////////////////////////////////////////////////////////////////////////////

class TBasicGlobalResourceAllocator
    : public IGlobalResourceAllocator
{
public:
    TBasicGlobalResourceAllocator(
        EAllocatorNodeSelectionStrategy nodeSelectionStrategy,
        TPodNodeScoreConfigPtr podNodeScoreConfig,
        TGlobalResourceAllocatorSharedState sharedState)
        : NodeSelectionStrategy_(nodeSelectionStrategy)
        , PodNodeScore_(CreatePodNodeScore(std::move(podNodeScoreConfig)))
        , SharedState_(std::move(sharedState))
    { }

    virtual void ReconcileState(const TClusterPtr& cluster) override
    {
        SharedState_.ReconcileState(cluster);
    }

    virtual TErrorOr<TNode*> ComputeAllocation(TPod* pod) override
    {
        YT_LOG_DEBUG("Started computing pod allocation via basic global resource allocator (PodId: %v, NodeSelectionStrategy: %v)",
            pod->GetId(),
            NodeSelectionStrategy_);

        if (auto error = pod->GetSchedulingAttributesValidationError(); !error.IsOK()) {
            return error;
        }

        auto* nodeSegment = pod->GetPodSet()->GetNodeSegment();
        auto* cache = nodeSegment->GetSchedulableNodeFilterCache();

        const auto& allSegmentNodesOrError = cache->Get(NObjects::TObjectFilter{});
        YT_VERIFY(allSegmentNodesOrError.IsOK());
        const auto& allSegmentNodes = allSegmentNodesOrError.Value();
        if (allSegmentNodes.empty()) {
            return TError("No schedulable nodes in segment %Qv",
                nodeSegment->GetId());
        }

        const auto& nodeFilter = pod->GetEffectiveNodeFilter();

        const auto& nodesOrError = cache->Get(NObjects::TObjectFilter{nodeFilter});
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

        TAllocator allocator(SharedState_.GetCluster());
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

                auto* resultNode = AllocateMinimumScoreNode(pod, sampledNodes, &allocator);
                if (resultNode) {
                    return resultNode;
                }

                return TError("No matching node from a random sample of size %v could be allocated for pod. %v",
                    sampledNodes.size(),
                    GetHumanReadableDescription(allocator.GetDiagnostics()));
            }
            case EAllocatorNodeSelectionStrategy::Every: {
                auto* resultNode = AllocateMinimumScoreNode(pod, nodes, &allocator);
                if (resultNode) {
                    return resultNode;
                }

                return TError("No matching alive node (from %v in total after filtering) could be allocated for pod. %v",
                    nodes.size(),
                    GetHumanReadableDescription(allocator.GetDiagnostics()));
            }
            default:
                YT_UNIMPLEMENTED();
        }
    }

private:
    const EAllocatorNodeSelectionStrategy NodeSelectionStrategy_;
    const IPodNodeScorePtr PodNodeScore_;

    TGlobalResourceAllocatorSharedState SharedState_;

    TScoreValue ComputeScore(TNode* node, TPod* pod)
    {
        auto nodeScore = SharedState_.GetNodeScore()->Compute(node);
        auto podNodeScore = PodNodeScore_->Compute(node, pod);
        return TScoreValue{nodeScore, podNodeScore};
    }

    TNode* AllocateMinimumScoreNode(
        TPod* pod,
        const std::vector<TNode*>& nodes,
        TAllocator* allocator)
    {
        TNode* resultNode = nullptr;
        TScoreValue resultScore;
        for (auto* node : nodes) {
            if (allocator->CanAllocate(node, pod)) {
                auto score = ComputeScore(node, pod);
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
            allocator->Allocate(resultNode, pod);
        }
        return resultNode;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeGlobalResourceAllocator
    : public IGlobalResourceAllocator
{
public:
    TCompositeGlobalResourceAllocator(
        TGlobalResourceAllocatorConfigPtr config,
        TGlobalResourceAllocatorSharedState sharedState)
        : Config_(std::move(config))
        , SharedState_(std::move(sharedState))
        , RandomNodeSelectionAllocator_(
            New<TBasicGlobalResourceAllocator>(
                EAllocatorNodeSelectionStrategy::Random,
                Config_->PodNodeScore,
                SharedState_.WithDisabledReconciliation()))
        , EveryNodeSelectionAllocator_(
            New<TBasicGlobalResourceAllocator>(
                EAllocatorNodeSelectionStrategy::Every,
                Config_->PodNodeScore,
                SharedState_.WithDisabledReconciliation()))
    {
        YT_VERIFY(Config_->EveryNodeSelectionStrategy->Enable);
    }

    virtual void ReconcileState(const TClusterPtr& cluster) override
    {
        VERIFY_THREAD_AFFINITY(SchedulerThread);

        YT_LOG_DEBUG("Started reconciling state of the global resource allocator");

        SharedState_.ReconcileState(cluster);

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

    TGlobalResourceAllocatorSharedState SharedState_;

    const IGlobalResourceAllocatorPtr RandomNodeSelectionAllocator_;
    const IGlobalResourceAllocatorPtr EveryNodeSelectionAllocator_;

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

IGlobalResourceAllocatorPtr CreateGlobalResourceAllocator(
    TGlobalResourceAllocatorConfigPtr config,
    IObjectFilterEvaluatorPtr nodeFilterEvaluator)
{
    TGlobalResourceAllocatorSharedState sharedState(CreateNodeScore(
        config->NodeScore, // NB! Do not move NodeScore out of the config.
        std::move(nodeFilterEvaluator)));

    if (config->EveryNodeSelectionStrategy->Enable) {
        return New<TCompositeGlobalResourceAllocator>(
            std::move(config),
            std::move(sharedState));
    }

    return New<TBasicGlobalResourceAllocator>(
        EAllocatorNodeSelectionStrategy::Random,
        config->PodNodeScore, // NB! Do not move PodNodeScore out of the config.
        std::move(sharedState));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
