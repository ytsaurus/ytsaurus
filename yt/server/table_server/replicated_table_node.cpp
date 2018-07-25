#include "replicated_table_node.h"
#include "table_node_type_handler_detail.h"

#include <yt/server/tablet_server/table_replica.h>

namespace NYT {
namespace NTableServer {

using namespace NObjectClient;
using namespace NCypressServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TReplicatedTableNode::TReplicatedTableNode(const TVersionedNodeId& id)
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
    if (context.GetVersion() >= 717) {
        Load(context, *ReplicatedTableOptions_);
    }
}

const TReplicatedTableNode::TReplicaSet& TReplicatedTableNode::Replicas() const
{
    return const_cast<TReplicatedTableNode*>(this)->Replicas();
}

TReplicatedTableNode::TReplicaSet& TReplicatedTableNode::Replicas()
{
    Y_ASSERT(IsTrunk());
    return Replicas_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

