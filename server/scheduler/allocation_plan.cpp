#include "allocation_plan.h"
#include "pod.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

void TAllocationPlan::Clear()
{
    NodeToRequests_.clear();
    NodeCount_ = 0;
}

void TAllocationPlan::AssignPodToNode(TPod* pod, TNode* node)
{
    EmplaceRequest(node, TPodRequest{pod, true});
}

void TAllocationPlan::RevokePodFromNode(TPod* pod)
{
    auto* node = pod->GetNode();
    Y_ASSERT(node);
    EmplaceRequest(node, TPodRequest{pod, false});
}

void TAllocationPlan::RecordFailure(TPod* pod, const TError& error)
{
    Failures_.push_back(TFailure{pod, error});
}

TNullable<TAllocationPlan::TPerNodePlan> TAllocationPlan::TryExtractPerNodePlan()
{
    if (NodeToRequests_.empty()) {
        return Null;
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

void TAllocationPlan::EmplaceRequest(TNode* node, const TPodRequest& request)
{
    auto range = NodeToRequests_.equal_range(node);
    if (range.first != range.second) {
        ++NodeCount_;
    }
    NodeToRequests_.emplace(node, request);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NScheduler
} // namespace NYP

