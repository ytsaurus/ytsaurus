#pragma once

#include "pod.h"
#include "private.h"

#include <yt/core/misc/enum.h>

#include <optional>
#include <variant>
#include <vector>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAllocationPlanPodRequestType,
    ((AssignPodToNode)             (  0))
    ((RevokePodFromNode)           (100))
);

DEFINE_ENUM(EAllocationPlanNodeRequestType,
    ((RemoveOrphanedResourceScheduledAllocations)   (  0))
);

class TAllocationPlan
{
public:
    void Clear();
    void AssignPodToNode(TPod* pod, TNode* node);
    void RevokePodFromNode(TPod* pod);
    void RemoveOrphanedAllocations(TNode* node);
    void RecordFailure(TPod* pod, const TError& error);

    struct TPodRequest
    {
        TPod* Pod;
        EAllocationPlanPodRequestType Type;
    };

    struct TNodeRequest
    {
        EAllocationPlanNodeRequestType Type;
    };

    using TRequest = std::variant<TPodRequest, TNodeRequest>;

    struct TPerNodePlan
    {
        TNode* Node;
        std::vector<TRequest> Requests;
    };

    std::optional<TPerNodePlan> TryExtractPerNodePlan();

    struct TFailure
    {
        TPod* Pod;
        TError Error;
    };

    const std::vector<TFailure>& GetFailures() const;

    int GetPodCount() const;
    int GetNodeCount() const;

private:
    THashMultiMap<TNode*, TRequest> NodeToRequests_;
    std::vector<TFailure> Failures_;
    int NodeCount_ = 0;

    void EmplaceRequest(TNode* node, const TRequest& request);
};

inline void FormatValue(TStringBuilder* builder, const TAllocationPlan::TPodRequest& podRequest, TStringBuf /* format */) {
    builder->AppendFormat("PodId: %v%v", podRequest.Type == EAllocationPlanPodRequestType::AssignPodToNode ? "+" : "-", podRequest.Pod->GetId());
}

inline void FormatValue(TStringBuilder* builder, const TAllocationPlan::TNodeRequest& /* nodeRequest */, TStringBuf /* format */) {
    builder->AppendString("Remove orphaned scheduled allocations");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
