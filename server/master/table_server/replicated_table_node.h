#pragma once

#include "table_node.h"

#include <yt/server/master/tablet_server/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableOptions
    : public NYTree::TYsonSerializable
{
public:
    bool EnableReplicatedTableTracker;
    std::optional<int> MaxSyncReplicaCount;
    std::optional<int> MinSyncReplicaCount;

    TReplicatedTableOptions()
    {
        RegisterParameter("enable_replicated_table_tracker", EnableReplicatedTableTracker)
            .Default(false);
        RegisterParameter("max_sync_replica_count", MaxSyncReplicaCount)
            .Alias("sync_replica_count")
            .Optional();
        RegisterParameter("min_sync_replica_count", MinSyncReplicaCount)
            .Optional();

        RegisterPostprocessor([&] {
            if (!MaxSyncReplicaCount && !MinSyncReplicaCount) {
                MaxSyncReplicaCount = 1;
            }
            if (MaxSyncReplicaCount && MinSyncReplicaCount && *MinSyncReplicaCount > *MaxSyncReplicaCount) {
                THROW_ERROR_EXCEPTION("\"min_sync_replica_count\" must be less or equal to \"max_sync_replica_count\"");
            }
        });
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

} // namespace NYT::NTableServer

