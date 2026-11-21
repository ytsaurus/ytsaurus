#include "replicated_table_node.h"
#include "table_node_type_handler_detail.h"

#include <yt/yt/server/master/tablet_server/table_replica.h>

#include <yt/yt/server/lib/tablet_server/config.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NTableServer {

using namespace NObjectClient;
using namespace NCypressServer;
using namespace NCellMaster;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

TReplicatedTableNode::TReplicatedTableNode(TVersionedNodeId id)
    : TTableNode(id)
    , ReplicatedTableOptions_(New<TReplicatedTableOptions>())
{ }

void TReplicatedTableNode::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTableNode>();

    PHOENIX_REGISTER_FIELD(1, Replicas_);
    PHOENIX_REGISTER_FIELD(2, ReplicatedTableOptions_,
        .template Serializer<TDerefSerializer<>>());
}

PHOENIX_DEFINE_TYPE(TReplicatedTableNode);

const TReplicatedTableNode::TReplicaSet& TReplicatedTableNode::Replicas() const
{
    return const_cast<TReplicatedTableNode*>(this)->Replicas();
}

TReplicatedTableNode::TReplicaSet& TReplicatedTableNode::Replicas()
{
    if (!IsTrunk()) {
        return GetTrunkNode()->As<TReplicatedTableNode>()->Replicas();
    }
    return Replicas_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

