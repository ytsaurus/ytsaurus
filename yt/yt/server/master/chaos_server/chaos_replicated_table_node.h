#pragma once

#include "public.h"

#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chaos_server/public.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TChaosReplicatedTableNode
    : public NCypressServer::TCypressNode
{
public:
    DEFINE_BYREF_RW_PROPERTY(NChaosServer::TChaosCellBundlePtr, ChaosCellBundle);
    DEFINE_BYVAL_RW_PROPERTY(TReplicationCardId, ReplicationCardId)
    DEFINE_BYVAL_RW_PROPERTY(bool, OwnsReplicationCard)

public:
    using TCypressNode::TCypressNode;

    TChaosReplicatedTableNode* GetTrunkNode();
    const TChaosReplicatedTableNode* GetTrunkNode() const;

    NYTree::ENodeType GetNodeType() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer

