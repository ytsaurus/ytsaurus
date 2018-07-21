#include "node_segment.h"
#include "label_filter_cache.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

using namespace NObjects;

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TNodeSegment::TNodeSegment(
    const TObjectId& id,
    TYsonString labels,
    std::vector<TNode*> allNodes,
    std::vector<TNode*> schedulableNodes,
    std::unique_ptr<TLabelFilterCache<TNode>> schedulableNodeLabelFilterCache)
    : TObject(id, std::move(labels))
    , AllNodes_(std::move(allNodes))
    , SchedulableNodes_(std::move(schedulableNodes))
    , SchedulableNodeLabelFilterCache_(std::move(schedulableNodeLabelFilterCache))
{ }

TLabelFilterCache<TNode>* TNodeSegment::GetSchedulableNodeLabelFilterCache() const
{
    return SchedulableNodeLabelFilterCache_.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NScheduler
} // namespace NYP

