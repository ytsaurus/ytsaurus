#pragma once

#include "common.h"
#include "node_detail.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

template <class TBase, ERuntimeNodeType::EDomain runtimeType>
class TVirtualizedNode
    : public TBase
{
public:
    typedef TVirtualizedNode<TBase, runtimeType> TThis;

    explicit TVirtualizedNode(const TBranchedNodeId& id)
        : TMapNode(id)
    { }

    TVirtualizedNode(const TBranchedNodeId& id, const TMapNode& other)
        : TBase(id, other)
    { }

    virtual TAutoPtr<ICypressNode> Clone() const
    {
        return new TThis(this->Id, *this);
    }

    ERuntimeNodeType GetRuntimeType() const
    {
        return runtimeType;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
