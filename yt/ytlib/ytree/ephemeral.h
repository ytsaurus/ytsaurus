#pragma once

#include "common.h"
#include "node.h"
#include "ypath_detail.h"

#include "../misc/hash.h"

namespace NYT {
namespace NYTree {
namespace NEphemeral {

////////////////////////////////////////////////////////////////////////////////

class TNodeFactory
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

} // namespace NEphemeral
} // namespace NYTree
} // namespace NYT

