#include "replicated_table_node.h"
#include "table_node_type_handler_detail.h"

#include <yt/yt/server/master/tablet_server/table_replica.h>

#include <yt/yt/server/lib/tablet_server/config.h>

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

void TReplicatedTableNode::Save(TSaveContext& context) const
{
    TTableNode::Save(context);

    using NYT::Save;
    Save(context, Replicas_);
    Save(context, *ReplicatedTableOptions_);
}

void TReplicatedTableNode::Load(TLoadContext& context)
{
    TTableNode::Load(context);

    using NYT::Load;
    Load(context, Replicas_);
    Load(context, *ReplicatedTableOptions_);
}

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

