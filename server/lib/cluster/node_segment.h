#pragma once

#include "object.h"
#include "object_filter_cache.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

class TNodeSegment
    : public TObject
    , public NYT::TRefTracked<TNodeSegment>
{
public:
    static constexpr EObjectType Type = EObjectType::NodeSegment;

    TNodeSegment(
        TObjectId id,
        NYT::NYson::TYsonString labels,
        TString nodeFilter);

    DEFINE_BYREF_RO_PROPERTY(TString, NodeFilter);

    DEFINE_BYREF_RW_PROPERTY(std::vector<TNode*>, Nodes);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TNode*>, SchedulableNodes);

    void SetNodeFilterCache(std::unique_ptr<TObjectFilterCache<TNode>> cache);
    TObjectFilterCache<TNode>* GetNodeFilterCache() const;

    void SetSchedulableNodeFilterCache(std::unique_ptr<TObjectFilterCache<TNode>> cache);
    TObjectFilterCache<TNode>* GetSchedulableNodeFilterCache() const;

private:
    std::unique_ptr<TObjectFilterCache<TNode>> NodeFilterCache_;
    std::unique_ptr<TObjectFilterCache<TNode>> SchedulableNodeFilterCache_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
