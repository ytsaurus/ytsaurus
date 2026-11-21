#pragma once

#include "node_detail.h"

#include <library/cpp/yt/misc/property.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TPortalEntranceNode
    : public TCypressNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, RemovalStarted);
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, ExitCellTag);

public:
    using TCypressNode::TCypressNode;

    NYTree::ENodeType GetNodeType() const override;

private:
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TPortalEntranceNode, 0x534f317e);
};

DEFINE_MASTER_OBJECT_TYPE(TPortalEntranceNode)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
