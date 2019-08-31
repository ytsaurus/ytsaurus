#pragma once

#include "public.h"

#include <yt/core/misc/enum.h>
#include <yt/core/misc/error.h>

#include <optional>
#include <variant>

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
    void AssignPodToNode(NCluster::TPod* pod, NCluster::TNode* node);
    void RevokePodFromNode(NCluster::TPod* pod);
    void RemoveOrphanedAllocations(NCluster::TNode* node);
    void RecordFailure(NCluster::TPod* pod, const TError& error);

    struct TPodRequest
    {
        NCluster::TPod* Pod;
        EAllocationPlanPodRequestType Type;
    };

    struct TNodeRequest
    {
        EAllocationPlanNodeRequestType Type;
    };

    using TRequest = std::variant<TPodRequest, TNodeRequest>;

    struct TPerNodePlan
    {
        NCluster::TNode* Node;
        std::vector<TRequest> Requests;
    };

    std::optional<TPerNodePlan> TryExtractPerNodePlan();

    struct TFailure
    {
        NCluster::TPod* Pod;
        TError Error;
    };

    const std::vector<TFailure>& GetFailures() const;

    int GetPodCount() const;
    int GetNodeCount() const;

private:
    THashMultiMap<NCluster::TNode*, TRequest> NodeToRequests_;
    std::vector<TFailure> Failures_;
    int NodeCount_ = 0;

    void EmplaceRequest(NCluster::TNode* node, const TRequest& request);
};

void FormatValue(TStringBuilderBase* builder, const TAllocationPlan::TPodRequest& podRequest, TStringBuf format);

void FormatValue(TStringBuilderBase* builder, const TAllocationPlan::TNodeRequest& nodeRequest, TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
