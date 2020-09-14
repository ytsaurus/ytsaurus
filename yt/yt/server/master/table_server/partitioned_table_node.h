#pragma once

#include "public.h"

#include <yt/server/master/cypress_server/node.h>

#include <yt/client/cypress_client/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TPartitionedTableNode
    : public NCypressServer::TCypressNode
{
public:
    using TCypressNode::TCypressNode;

    virtual NYTree::ENodeType GetNodeType() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
