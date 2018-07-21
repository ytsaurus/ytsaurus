#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TNodeSegment
    : public TObject
    , public NYT::TRefTracked<TNodeSegment>
{
public:
    TNodeSegment(
        const TObjectId& id,
        NYT::NYson::TYsonString labels,
        std::vector<TNode*> allNodes,
        std::vector<TNode*> schedulableNodes,
        std::unique_ptr<TLabelFilterCache<TNode>> schedulableNodeLabelFilterCache);

    TLabelFilterCache<TNode>* GetSchedulableNodeLabelFilterCache() const;

    DEFINE_BYREF_RO_PROPERTY(std::vector<TNode*>, AllNodes);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TNode*>, SchedulableNodes);

private:
    const std::unique_ptr<TLabelFilterCache<TNode>> SchedulableNodeLabelFilterCache_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
