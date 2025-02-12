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
    , public NTableServer::TSchemafulNode
{
public:
    DEFINE_BYREF_RW_PROPERTY(NChaosServer::TChaosCellBundlePtr, ChaosCellBundle);
    DEFINE_BYVAL_RW_PROPERTY(TReplicationCardId, ReplicationCardId);
    DEFINE_BYVAL_RW_PROPERTY(bool, OwnsReplicationCard);
    DEFINE_BYVAL_RW_PROPERTY(bool, TreatAsQueueConsumer);
    DEFINE_BYVAL_RW_PROPERTY(bool, TreatAsQueueProducer);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<std::string>, QueueAgentStage);

public:
    using TCypressNode::TCypressNode;

    TChaosReplicatedTableNode* GetTrunkNode();
    const TChaosReplicatedTableNode* GetTrunkNode() const;

    NYTree::ENodeType GetNodeType() const override;

    NSecurityServer::TAccount* GetAccount() const override;

    // COMPAT(h0pless): This is a temporary workaround until schemaful node typehandler is introduced.
    NObjectClient::TCellTag GetExternalCellTag() const override;
    bool IsExternal() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    void CheckInvariants(NCellMaster::TBootstrap* bootstrap) const override;

    bool IsQueue() const;
    bool IsTrackedQueueObject() const;
    bool IsQueueConsumer() const;
    bool IsTrackedQueueConsumerObject() const;
    bool IsQueueProducer() const;
    bool IsTrackedQueueProducerObject() const;
    bool IsSorted() const;
    bool HasNonEmptySchema() const;
};

DEFINE_MASTER_OBJECT_TYPE(TChaosReplicatedTableNode)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer

