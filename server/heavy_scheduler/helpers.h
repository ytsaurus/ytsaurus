#include "public.h"

#include <yp/server/lib/cluster/pod.h>

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TObjectCompositeId
{
    TObjectId Id;
    TObjectId Uuid;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TObjectCompositeId& compositeId,
    TStringBuf format);

TObjectCompositeId GetCompositeId(const NCluster::TPod* pod);

NCluster::TPod* FindPod(
    const NCluster::TClusterPtr& cluster,
    const TObjectCompositeId& compositeId);

////////////////////////////////////////////////////////////////////////////////

std::vector<NCluster::TPod*> GetNodeSegmentSchedulablePods(
    const NCluster::TClusterPtr& cluster,
    const TObjectId& nodeSegment);

std::vector<NCluster::TNode*> FindSuitableNodes(
    NCluster::TPod* pod,
    const std::vector<NCluster::TNode*>& nodes,
    std::optional<int> limit);

TErrorOr<std::vector<NCluster::TNode*>> FindSuitableNodes(
    NCluster::TPod* pod,
    std::optional<int> limit);

const TErrorOr<std::vector<NCluster::TNode*>>& GetFilteredNodes(NCluster::TPod* pod);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
