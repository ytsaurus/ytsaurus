#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chaos_server/public.h>

#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/server/master/table_server/schemaful_node.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TChaosReplicatedTableNode
    : public NCypressServer::TCypressNode
    , public NTableServer::ISchemafulNode
{
public:
    DEFINE_BYREF_RW_PROPERTY(NChaosServer::TChaosCellBundlePtr, ChaosCellBundle);
    DEFINE_BYVAL_RW_PROPERTY(TReplicationCardId, ReplicationCardId);
    DEFINE_BYVAL_RW_PROPERTY(bool, OwnsReplicationCard);

public:
    using TCypressNode::TCypressNode;

    TChaosReplicatedTableNode* GetTrunkNode();
    const TChaosReplicatedTableNode* GetTrunkNode() const;

    NYTree::ENodeType GetNodeType() const override;

    virtual NTableServer::TMasterTableSchema* GetSchema() const override;
    virtual void SetSchema(NTableServer::TMasterTableSchema* schema) override;
    virtual NSecurityServer::TAccount* GetAccount() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

private:
    NTableServer::TMasterTableSchema* Schema_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer

