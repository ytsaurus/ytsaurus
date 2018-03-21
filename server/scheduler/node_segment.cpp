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
    std::unique_ptr<TLabelFilterCache<TNode>> nodeLabelFilterCache)
    : TObject(id, std::move(labels))
    , NodeLabelFilterCache_(std::move(nodeLabelFilterCache))
{ }

TLabelFilterCache<TNode>* TNodeSegment::GetNodeLabelFilterCache() const
{
    return NodeLabelFilterCache_.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NScheduler
} // namespace NYP

