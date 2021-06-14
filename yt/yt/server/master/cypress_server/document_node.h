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
    explicit TDocumentNode(TVersionedNodeId id);

    virtual NYTree::ENodeType GetNodeType() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
