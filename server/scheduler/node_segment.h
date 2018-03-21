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
        std::unique_ptr<TLabelFilterCache<TNode>> nodeLabelFilterCache);

    TLabelFilterCache<TNode>* GetNodeLabelFilterCache() const;

private:
    const std::unique_ptr<TLabelFilterCache<TNode>> NodeLabelFilterCache_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
