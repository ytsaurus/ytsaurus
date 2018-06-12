#pragma once

#include "table_node.h"

#include <yt/server/tablet_server/public.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableOptions
    : public NYTree::TYsonSerializable
{
public:
    bool EnableReplicatedTableManager;
    int SyncReplicas;

    // TODO(aozeritsky): add check list and other options here

    TReplicatedTableOptions()
    {
        RegisterParameter("enable_replicated_table_manager", EnableReplicatedTableManager)
            .Default(false);
        RegisterParameter("sync_replicas", SyncReplicas)
            .Default(1);
    }
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableOptions)

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableNode
    : public TTableNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TReplicatedTableOptionsPtr, ReplicatedTableOptions);

public:
    explicit TReplicatedTableNode(const NCypressServer::TVersionedNodeId& id);

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    using TReplicaSet = THashSet<NTabletServer::TTableReplica*>;
    const TReplicaSet& Replicas() const;
    TReplicaSet& Replicas();

private:
    TReplicaSet Replicas_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

