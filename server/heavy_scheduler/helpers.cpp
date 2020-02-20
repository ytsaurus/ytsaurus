#include "helpers.h"

#include "private.h"
#include "disruption_throttler.h"

#include <yp/server/lib/cluster/allocator.h>
#include <yp/server/lib/cluster/cluster.h>
#include <yp/server/lib/cluster/pod_set.h>
#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/node_segment.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TObjectCompositeId& compositeId,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{Id: %v, Uuid: %v}",
        compositeId.Id,
        compositeId.Uuid);
}

TObjectCompositeId GetCompositeId(const TPod* pod)
{
    return TObjectCompositeId{
        pod->GetId(),
        pod->Uuid()};
}

TPod* FindPod(const TClusterPtr& cluster, const TObjectCompositeId& compositeId)
{
    auto* pod = cluster->FindPod(compositeId.Id);
    if (!pod) {
        return nullptr;
    }
    if (pod->Uuid() != compositeId.Uuid) {
        return nullptr;
    }
    return pod;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TPod*> GetNodeSegmentSchedulablePods(const TClusterPtr& cluster, const TObjectId& nodeSegment)
{
    auto result = cluster->GetSchedulablePods();
    result.erase(
        std::remove_if(
            result.begin(),
            result.end(),
            [&] (auto* pod) {
                return nodeSegment != pod->GetPodSet()->GetNodeSegment()->GetId();
            }),
        result.end());
    return result;
}

std::vector<TNode*> FindSuitableNodes(
    TPod* pod,
    const std::vector<TNode*>& nodes,
    std::optional<int> limit)
{
    std::vector<TNode*> result;
    if (limit) {
        YT_VERIFY(*limit >= 0);
        result.reserve(*limit);
    }
    TAllocator allocator;
    for (auto* node : nodes) {
        if (limit && static_cast<int>(result.size()) >= *limit) {
            break;
        }
        if (allocator.CanAllocate(node, pod)) {
            result.push_back(node);
        }
    }
    return result;
}

TErrorOr<std::vector<TNode*>> FindSuitableNodes(
    TPod* pod,
    std::optional<int> limit)
{
    const auto& nodesOrError = GetFilteredNodes(pod);
    if (!nodesOrError.IsOK()) {
        return TError("Error filtering nodes")
            << nodesOrError;
    }
    return FindSuitableNodes(pod, nodesOrError.Value(), limit);
}

const TErrorOr<std::vector<TNode*>>& GetFilteredNodes(TPod* pod)
{
    auto* nodeSegmentCache = pod->GetPodSet()->GetNodeSegment()->GetSchedulableNodeFilterCache();
    return nodeSegmentCache->Get(NObjects::TObjectFilter{pod->GetEffectiveNodeFilter()});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
