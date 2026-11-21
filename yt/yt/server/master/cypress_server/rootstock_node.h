#pragma once

#include "node_detail.h"

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TRootstockNode
    : public TCypressNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TNodeId, ScionId);

public:
    using TCypressNode::TCypressNode;

    NYTree::ENodeType GetNodeType() const override;

private:
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TRootstockNode, 0x84431620);
};

DEFINE_MASTER_OBJECT_TYPE(TRootstockNode)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
