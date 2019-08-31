#include "allocation_plan.h"

#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/pod.h>

namespace NYP::NServer::NScheduler {

using namespace NCluster;

////////////////////////////////////////////////////////////////////////////////

void TAllocationPlan::Clear()
{
    NodeToRequests_.clear();
    NodeCount_ = 0;
}

void TAllocationPlan::AssignPodToNode(TPod* pod, TNode* node)
{
    EmplaceRequest(node, TPodRequest{pod, EAllocationPlanPodRequestType::AssignPodToNode});
}

void TAllocationPlan::RevokePodFromNode(TPod* pod)
{
    auto* node = pod->GetNode();
    YT_ASSERT(node);
    EmplaceRequest(node, TPodRequest{pod, EAllocationPlanPodRequestType::RevokePodFromNode});
}

void TAllocationPlan::RemoveOrphanedAllocations(TNode* node)
{
    EmplaceRequest(node, TNodeRequest{EAllocationPlanNodeRequestType::RemoveOrphanedResourceScheduledAllocations});
}

void TAllocationPlan::RecordFailure(TPod* pod, const TError& error)
{
    Failures_.push_back(TFailure{pod, error});
}

std::optional<TAllocationPlan::TPerNodePlan> TAllocationPlan::TryExtractPerNodePlan()
{
    if (NodeToRequests_.empty()) {
        return std::nullopt;
    }

    TPerNodePlan plan;
    plan.Node = NodeToRequests_.begin()->first;
    auto range = NodeToRequests_.equal_range(plan.Node);
    for (auto it = range.first; it != range.second; ++it) {
        plan.Requests.push_back(it->second);
    }
    NodeToRequests_.erase(plan.Node);
    return plan;
}

const std::vector<TAllocationPlan::TFailure>& TAllocationPlan::GetFailures() const
{
    return Failures_;
}

int TAllocationPlan::GetPodCount() const
{
    return static_cast<int>(NodeToRequests_.size());
}

int TAllocationPlan::GetNodeCount() const
{
    return NodeCount_;
}

void TAllocationPlan::EmplaceRequest(TNode* node, const TRequest& request)
{
    auto range = NodeToRequests_.equal_range(node);
    if (range.first == range.second) {
        ++NodeCount_;
    }
    NodeToRequests_.emplace(node, request);
}

void FormatValue(TStringBuilderBase* builder, const TAllocationPlan::TPodRequest& podRequest, TStringBuf /* format */)
{
    builder->AppendFormat("PodId: %v%v", podRequest.Type == EAllocationPlanPodRequestType::AssignPodToNode ? "+" : "-", podRequest.Pod->GetId());
}

void FormatValue(TStringBuilderBase* builder, const TAllocationPlan::TNodeRequest& /* nodeRequest */, TStringBuf /* format */)
{
    builder->AppendString("Remove orphaned scheduled allocations");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
