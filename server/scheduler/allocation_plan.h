#pragma once

#include "private.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TAllocationPlan
{
public:
    void Clear();
    void AssignPodToNode(TPod* pod, TNode* node);
    void RevokePodFromNode(TPod* pod);
    void RecordFailure(TPod* pod, const TError& error);

    struct TPodRequest
    {
        TPod* Pod;
        bool Assign;
    };

    struct TPerNodePlan
    {
        TNode* Node;
        std::vector<TPodRequest> Requests;
    };

    TNullable<TPerNodePlan> TryExtractPerNodePlan();

    struct TFailure
    {
        TPod* Pod;
        TError Error;
    };

    const std::vector<TFailure>& GetFailures() const;

    int GetPodCount() const;
    int GetNodeCount() const;

private:
    THashMultiMap<TNode*, TPodRequest> NodeToRequests_;
    std::vector<TFailure> Failures_;
    int NodeCount_ = 0;

    void EmplaceRequest(TNode* node, const TPodRequest& request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
