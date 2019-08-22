#include "node_segment.h"
#include "label_filter_cache.h"

namespace NYP::NServer::NScheduler {

using namespace NObjects;

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TNodeSegment::TNodeSegment(
    TObjectId id,
    TYsonString labels,
    TString nodeFilter)
    : TObject(std::move(id), std::move(labels))
    , NodeFilter_(std::move(nodeFilter))
{ }

void TNodeSegment::SetSchedulableNodeLabelFilterCache(std::unique_ptr<TLabelFilterCache<TNode>> cache)
{
    SchedulableNodeLabelFilterCache_ = std::move(cache);
}

TLabelFilterCache<TNode>* TNodeSegment::GetSchedulableNodeLabelFilterCache() const
{
    return SchedulableNodeLabelFilterCache_.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
