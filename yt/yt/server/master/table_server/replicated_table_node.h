#pragma once

#include "table_node.h"

#include <yt/yt/server/master/tablet_server/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableNode
    : public TTableNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NTabletClient::TReplicatedTableOptionsPtr, ReplicatedTableOptions);

public:
    using TTableNode::TTableNode;
    explicit TReplicatedTableNode(NCypressServer::TVersionedNodeId id);

    using TReplicaSet = THashSet<NTabletServer::TTableReplicaRawPtr>;
    const TReplicaSet& Replicas() const;
    TReplicaSet& Replicas();

private:
    TReplicaSet Replicas_;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TReplicatedTableNode, 0xa8169058);
};

DEFINE_MASTER_OBJECT_TYPE(TReplicatedTableNode)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
