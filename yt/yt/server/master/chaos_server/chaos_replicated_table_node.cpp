#include "chaos_replicated_table_node.h"

#include "chaos_cell_bundle.h"

#include "chaos_manager.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/table_server/master_table_schema.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NChaosServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTableServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TChaosReplicatedTableNode* TChaosReplicatedTableNode::GetTrunkNode()
{
    return TCypressNode::GetTrunkNode()->As<TChaosReplicatedTableNode>();
}

const TChaosReplicatedTableNode* TChaosReplicatedTableNode::GetTrunkNode() const
{
    return TCypressNode::GetTrunkNode()->As<TChaosReplicatedTableNode>();
}

ENodeType TChaosReplicatedTableNode::GetNodeType() const
{
    return ENodeType::Entity;
}

TAccount* TChaosReplicatedTableNode::GetAccount() const
{
    return TCypressNode::Account().Get();
}

TCellTag TChaosReplicatedTableNode::GetExternalCellTag() const
{
    return TCypressNode::GetExternalCellTag();
}

bool TChaosReplicatedTableNode::IsExternal() const
{
    return TCypressNode::IsExternal();
}

void TChaosReplicatedTableNode::Save(TSaveContext& context) const
{
    TCypressNode::Save(context);
    TSchemafulNode::Save(context);

    using NYT::Save;
    Save(context, ChaosCellBundle_);
    Save(context, ReplicationCardId_);
    Save(context, OwnsReplicationCard_);
    Save(context, TreatAsQueueConsumer_);
    Save(context, TreatAsQueueProducer_);
    Save(context, QueueAgentStage_);
}

void TChaosReplicatedTableNode::Load(TLoadContext& context)
{
    TCypressNode::Load(context);

    // COMPAT(h0pless): AddSchemafulNodeTypeHandler
    if (context.GetVersion() >= EMasterReign::AddSchemafulNodeTypeHandler) {
        TSchemafulNode::Load(context);
    }

    using NYT::Load;
    Load(context, ChaosCellBundle_);
    Load(context, ReplicationCardId_);
    Load(context, OwnsReplicationCard_);
    if (context.GetVersion() < EMasterReign::AddSchemafulNodeTypeHandler) {
        Load(context, Schema_);
    }

    // COMPAT(cherepashka)
    if (context.GetVersion() >= EMasterReign::ChaosReplicatedConsumersFix) {
        Load(context, TreatAsQueueConsumer_);
    }

    // COMPAT(apachee): Remove user attributes conflicting with new producer attributes.
    // DropLegacyClusterNodeMap is the start of 24.2 reigns.
    if ((context.GetVersion() >= EMasterReign::QueueProducers_24_1 && context.GetVersion() < EMasterReign::DropLegacyClusterNodeMap) ||
        context.GetVersion() >= EMasterReign::QueueProducers)
    {
        Load(context, TreatAsQueueProducer_);
    } else if (Attributes_) {
        static constexpr std::array producerRelatedAttributes = {
            EInternedAttributeKey::TreatAsQueueProducer,
            EInternedAttributeKey::QueueProducerStatus,
            EInternedAttributeKey::QueueProducerPartitions,
        };
        for (const auto& attribute : producerRelatedAttributes) {
            Attributes_->Remove(attribute.Unintern());
        }
    }

    // COMPAT(nadya73): Remove queue related attributes for old reigns.
    if (context.GetVersion() >= EMasterReign::QueueAgentStageForChaos) {
        Load(context, QueueAgentStage_);
    } else if (Attributes_) {
        static const std::vector<TInternedAttributeKey> queueRelatedAttributes = {
            EInternedAttributeKey::QueueStatus,
            EInternedAttributeKey::QueuePartitions,
            EInternedAttributeKey::QueueConsumerStatus,
            EInternedAttributeKey::QueueConsumerPartitions,
            EInternedAttributeKey::QueueAgentStage,
            EInternedAttributeKey::TreatAsQueueConsumer,
        };

        for (const auto& attribute : queueRelatedAttributes) {
            Attributes_->Remove(attribute.Unintern());
        }
    }
}

void TChaosReplicatedTableNode::CheckInvariants(NCellMaster::TBootstrap* bootstrap) const
{
    NCypressServer::TCypressNode::CheckInvariants(bootstrap);

    if (IsObjectAlive(this)) {
        // NB: Const-cast due to const-correctness rabbit-hole, which led to TChaosReplicatedTableNode* being stored in the set.
        YT_VERIFY(bootstrap->GetChaosManager()->GetQueues().contains(const_cast<TChaosReplicatedTableNode*>(this)) == IsTrackedQueueObject());
        YT_VERIFY(bootstrap->GetChaosManager()->GetQueueConsumers().contains(const_cast<TChaosReplicatedTableNode*>(this)) == IsTrackedQueueConsumerObject());
        YT_VERIFY(bootstrap->GetChaosManager()->GetQueueProducers().contains(const_cast<TChaosReplicatedTableNode*>(this)) == IsTrackedQueueProducerObject());
    }
}

bool TChaosReplicatedTableNode::IsSorted() const
{
    return HasNonEmptySchema() && GetSchema()->AsTableSchema()->IsSorted();
}

// Chaos Replicated Tables are always dynamic.
bool TChaosReplicatedTableNode::IsQueue() const
{
    return HasNonEmptySchema() && !IsSorted();
}

// Chaos Replicated Tables are always native.
bool TChaosReplicatedTableNode::IsTrackedQueueObject() const
{
    return IsNative() && IsTrunk() && IsQueue();
}

// Chaos Replicated Tables are always dynamic.
bool TChaosReplicatedTableNode::IsQueueConsumer() const
{
    return GetTreatAsQueueConsumer();
}

// Chaos Replicated Tables are always native.
bool TChaosReplicatedTableNode::IsTrackedQueueConsumerObject() const
{
    return IsNative() && IsTrunk() && IsQueueConsumer();
}

// Chaos Replicated Tables are always dynamic.
bool TChaosReplicatedTableNode::IsQueueProducer() const
{
    return GetTreatAsQueueProducer();
}

// Chaos Replicated Tables are always native.
bool TChaosReplicatedTableNode::IsTrackedQueueProducerObject() const
{
    return IsNative() && IsTrunk() && IsQueueProducer();
}

bool TChaosReplicatedTableNode::HasNonEmptySchema() const
{
    const auto& schema = GetSchema();
    return schema && !schema->AsTableSchema()->IsEmpty();
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
