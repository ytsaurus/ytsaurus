#include "config.h"

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

TReplicatedTableOptions::TReplicatedTableOptions()
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
        .Default(false);
    RegisterParameter("incomplete_preload_grace_period", IncompletePreloadGracePeriod)
        .Default(TDuration::Minutes(5));

    RegisterParameter("preferred_sync_replica_clusters", PreferredSyncReplicaClusters)
        .Default(std::nullopt);

    RegisterPostprocessor([&] {
        if (MaxSyncReplicaCount && MinSyncReplicaCount && *MinSyncReplicaCount > *MaxSyncReplicaCount) {
            THROW_ERROR_EXCEPTION("\"min_sync_replica_count\" must be less or equal to \"max_sync_replica_count\"");
        }
    });
}

std::tuple<int, int> TReplicatedTableOptions::GetEffectiveMinMaxReplicaCount(int replicaCount) const
{
    int maxSyncReplicas = 0;
    int minSyncReplicas = 0;

    if (!MaxSyncReplicaCount && !MinSyncReplicaCount) {
        maxSyncReplicas = 1;
    } else {
        maxSyncReplicas = MaxSyncReplicaCount.value_or(replicaCount);
    }

    minSyncReplicas = MinSyncReplicaCount.value_or(maxSyncReplicas);

    return std::make_tuple(minSyncReplicas, maxSyncReplicas);
}

////////////////////////////////////////////////////////////////////////////////

TDynamicReplicatedTableTrackerConfig::TDynamicReplicatedTableTrackerConfig()
{
    RegisterParameter("enable_replicated_table_tracker", EnableReplicatedTableTracker)
        .Default(true);
    RegisterParameter("use_new_replicated_table_tracker", UseNewReplicatedTableTracker)
        .Default(false);
    RegisterParameter("check_period", CheckPeriod)
        .Default(TDuration::Seconds(3));
    RegisterParameter("update_period", UpdatePeriod)
        .Default(TDuration::Seconds(3));
    RegisterParameter("general_check_timeout", GeneralCheckTimeout)
        .Default(TDuration::Minutes(1));
    RegisterParameter("replicator_hint", ReplicatorHint)
        .DefaultNew();
    RegisterParameter("bundle_health_cache", BundleHealthCache)
        .DefaultNew();
    RegisterParameter("cluster_state_cache", ClusterStateCache)
        .DefaultNew();
    RegisterParameter("cluster_directory_synchronizer", ClusterDirectorySynchronizer)
        .DefaultNew();
    RegisterParameter("max_iterations_without_acceptable_bundle_health", MaxIterationsWithoutAcceptableBundleHealth)
        .Default(1);
    RegisterParameter("max_action_queue_size", MaxActionQueueSize)
        .Default(10000)
        .GreaterThanOrEqual(0);
    RegisterParameter("client_expiration_time", ClientExpirationTime)
        .Default(TDuration::Minutes(15));

    RegisterPreprocessor([&] {
        ClusterStateCache->RefreshTime = CheckPeriod;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
