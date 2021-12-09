#pragma once

#include "table_node.h"

#include <yt/yt/server/master/tablet_server/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableOptions
    : public NYTree::TYsonSerializable
{
public:
    bool EnableReplicatedTableTracker;

    std::optional<int> MaxSyncReplicaCount;
    std::optional<int> MinSyncReplicaCount;

    TDuration SyncReplicaLagThreshold;

    TDuration TabletCellBundleNameTtl;
    TDuration RetryOnFailureInterval;

    bool EnablePreloadStateCheck;

    std::optional<std::vector<TString>> PreferredSyncReplicaClusters;

    TReplicatedTableOptions()
    {
        RegisterParameter("enable_replicated_table_tracker", EnableReplicatedTableTracker)
            .Default(false);

        RegisterParameter("max_sync_replica_count", MaxSyncReplicaCount)
            .Alias("sync_replica_count")
            .Optional();
        RegisterParameter("min_sync_replica_count", MinSyncReplicaCount)
            .Optional();

        RegisterParameter("sync_replica_lag_threshold", SyncReplicaLagThreshold)
            .Default(TDuration::Minutes(10));

        RegisterParameter("tablet_cell_bundle_name_ttl", TabletCellBundleNameTtl)
            .Default(TDuration::Seconds(300));
        RegisterParameter("tablet_cell_bundle_name_failure_interval", RetryOnFailureInterval)
            .Default(TDuration::Seconds(60));

        RegisterParameter("enable_preload_state_check", EnablePreloadStateCheck)
            .Default(false)
            .DontSerializeDefault();

        RegisterParameter("preferred_sync_replica_clusters", PreferredSyncReplicaClusters)
            .Default(std::nullopt)
            .DontSerializeDefault();

        RegisterPostprocessor([&] {
            if (MaxSyncReplicaCount && MinSyncReplicaCount && *MinSyncReplicaCount > *MaxSyncReplicaCount) {
                THROW_ERROR_EXCEPTION("\"min_sync_replica_count\" must be less or equal to \"max_sync_replica_count\"");
            }
        });
    }

    std::tuple<int, int> GetEffectiveMinMaxReplicaCount(int totalReplicas) const
    {
        int maxSyncReplicas = 0;
        int minSyncReplicas = 0;

        if (!MaxSyncReplicaCount && !MinSyncReplicaCount) {
            maxSyncReplicas = 1;
        } else {
            maxSyncReplicas = MaxSyncReplicaCount.value_or(totalReplicas);
        }

        minSyncReplicas = MinSyncReplicaCount.value_or(maxSyncReplicas);

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
