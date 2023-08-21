#pragma once

#include "node_detail.h"

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TRootstockNode
    : public TCypressNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TObjectId, ScionId);

public:
    using TCypressNode::TCypressNode;

    NYTree::ENodeType GetNodeType() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
