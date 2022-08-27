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

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    using TReplicaSet = THashSet<NTabletServer::TTableReplica*>;
    const TReplicaSet& Replicas() const;
    TReplicaSet& Replicas();

private:
    TReplicaSet Replicas_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
