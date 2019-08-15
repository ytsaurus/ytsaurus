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
    std::optional<int> MaxSyncReplicaCount_;
    std::optional<int> MinSyncReplicaCount_;

    TDuration TabletCellBundleNameTtl;
    TDuration RetryOnFailureInterval;

    TReplicatedTableOptions()
    {
        RegisterParameter("enable_replicated_table_tracker", EnableReplicatedTableTracker)
            .Default(false);
        RegisterParameter("max_sync_replica_count", MaxSyncReplicaCount_)
            .Alias("sync_replica_count")
            .Optional();
        RegisterParameter("min_sync_replica_count", MinSyncReplicaCount_)
            .Optional();

        RegisterParameter("tablet_cell_bundle_name_ttl", TabletCellBundleNameTtl)
            .Default(TDuration::Seconds(300));
        RegisterParameter("tablet_cell_bundle_name_failure_interval", RetryOnFailureInterval)
            .Default(TDuration::Seconds(60));

        RegisterPostprocessor([&] {
            if (MaxSyncReplicaCount_ && MinSyncReplicaCount_ && *MinSyncReplicaCount_ > *MaxSyncReplicaCount_) {
                THROW_ERROR_EXCEPTION("\"min_sync_replica_count\" must be less or equal to \"max_sync_replica_count\"");
            }
        });
    }

    std::tuple<int, int> GetEffectiveMinMaxReplicaCount(int totalReplicas) const
    {
        int maxSyncReplicas = 0;
        int minSyncReplicas = 0;

        if (!MaxSyncReplicaCount_ && !MinSyncReplicaCount_) {
            maxSyncReplicas = 1;
        } else {
            maxSyncReplicas = MaxSyncReplicaCount_.value_or(totalReplicas);
        }

        minSyncReplicas = MinSyncReplicaCount_.value_or(maxSyncReplicas);

        return std::make_tuple(minSyncReplicas, maxSyncReplicas);
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

