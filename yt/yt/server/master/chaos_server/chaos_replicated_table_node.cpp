#include "chaos_replicated_table_node.h"

#include "chaos_cell_bundle.h"

#include "chaos_manager.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/table_server/master_table_schema.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NChaosServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTableServer;
using namespace NYTree;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

TChaosReplicatedTableNode* TChaosReplicatedTableNode::GetTrunkNode()
{
    return TCypressNode::GetTrunkNode()->As<TChaosReplicatedTableNode>();
}

const TChaosReplicatedTableNode* TChaosReplicatedTableNode::GetTrunkNode() const
{
    return TCypressNode::GetTrunkNode()->As<TChaosReplicatedTableNode>();
}

TChaosCellBundleRawPtr TChaosReplicatedTableNode::GetChaosCellBundle() const
{
    return GetTrunkNode()->ChaosCellBundle_.Get();
}

void TChaosReplicatedTableNode::SetChaosCellBundle(TChaosCellBundle* cellBundle)
{
    YT_VERIFY(IsTrunk());
    ChaosCellBundle_.Assign(cellBundle);
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

void TChaosReplicatedTableNode::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TCypressNode>();
    registrar.template BaseType<TSchemafulNode>();

    PHOENIX_REGISTER_FIELD(1, ChaosCellBundle_);
    PHOENIX_REGISTER_FIELD(2, ReplicationCardId_);
    PHOENIX_REGISTER_FIELD(3, OwnsReplicationCard_);
    PHOENIX_REGISTER_FIELD(4, TreatAsQueueConsumer_);
    PHOENIX_REGISTER_FIELD(5, TreatAsQueueProducer_);
    PHOENIX_REGISTER_FIELD(6, QueueAgentStage_);
}

PHOENIX_DEFINE_TYPE(TChaosReplicatedTableNode);

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
    return HasNonEmptySchema() && GetSchema()->AsCompactTableSchema()->IsSorted();
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
    return schema && !schema->AsCompactTableSchema()->IsEmpty();
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
