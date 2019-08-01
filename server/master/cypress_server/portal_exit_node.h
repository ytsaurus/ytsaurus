#pragma once

#include "node_detail.h"

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TPortalExitNode
    : public TMapNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, EntranceCellTag);
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, Path);

public:
    using TMapNode::TMapNode;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
