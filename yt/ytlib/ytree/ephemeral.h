#pragma once

#include "common.h"
#include "ytree.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! An ephemeral (non-persistent, in memory) implementation
//! of the YTree.
class TEphemeralNodeFactory
    : public INodeFactory
{
public:
    static INodeFactory* Get();

    virtual IStringNode::TPtr CreateString();
    virtual IInt64Node::TPtr CreateInt64();
    virtual IDoubleNode::TPtr CreateDouble();
    virtual IMapNode::TPtr CreateMap();
    virtual IListNode::TPtr CreateList();
    virtual IEntityNode::TPtr CreateEntity();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

