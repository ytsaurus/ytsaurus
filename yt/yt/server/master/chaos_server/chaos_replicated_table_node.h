#pragma once

#include "public.h"

#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TChaosReplicatedTableNode
    : public NCypressServer::TCypressNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TReplicationCardId, ReplicationCardId)

public:
    using TCypressNode::TCypressNode;

    NYTree::ENodeType GetNodeType() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer

