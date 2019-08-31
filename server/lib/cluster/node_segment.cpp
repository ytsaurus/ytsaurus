#include "node_segment.h"

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

TNodeSegment::TNodeSegment(
    TObjectId id,
    NYT::NYson::TYsonString labels,
    TString nodeFilter)
    : TObject(std::move(id), std::move(labels))
    , NodeFilter_(std::move(nodeFilter))
{ }

void TNodeSegment::SetSchedulableNodeFilterCache(std::unique_ptr<TObjectFilterCache<TNode>> cache)
{
    SchedulableNodeFilterCache_ = std::move(cache);
}

TObjectFilterCache<TNode>* TNodeSegment::GetSchedulableNodeFilterCache() const
{
    return SchedulableNodeFilterCache_.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
