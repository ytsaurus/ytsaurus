#pragma once

#include "node.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TLinkNode
    : public TCypressNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, TargetPath);

public:
    using TCypressNode::TCypressNode;

    NYTree::ENodeType GetNodeType() const override;

private:
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TLinkNode, 0x3db6051b);
};

DEFINE_MASTER_OBJECT_TYPE(TLinkNode)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
