#pragma once

#include "label_filter_cache.h"
#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TNodeSegment
    : public TObject
    , public NYT::TRefTracked<TNodeSegment>
{
public:
    TNodeSegment(
        TObjectId id,
        NYT::NYson::TYsonString labels,
        TString nodeFilter);

    DEFINE_BYREF_RO_PROPERTY(TString, NodeFilter);

    DEFINE_BYREF_RW_PROPERTY(std::vector<TNode*>, Nodes);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TNode*>, SchedulableNodes);

    void SetSchedulableNodeLabelFilterCache(std::unique_ptr<TLabelFilterCache<TNode>> cache);
    TLabelFilterCache<TNode>* GetSchedulableNodeLabelFilterCache() const;

private:
    std::unique_ptr<TLabelFilterCache<TNode>> SchedulableNodeLabelFilterCache_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
