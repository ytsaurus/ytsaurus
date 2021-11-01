#pragma once

#include "node_detail.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TDocumentNode
    : public TCypressNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NYTree::INodePtr, Value);

public:
    using TCypressNode::TCypressNode;
    explicit TDocumentNode(TVersionedNodeId id);

    NYTree::ENodeType GetNodeType() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
