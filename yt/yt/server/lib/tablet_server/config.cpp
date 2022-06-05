#include "config.h"

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

void TReplicatedTableOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_replicated_table_tracker", &TThis::EnableReplicatedTableTracker)
        .Default(false);

    registrar.Parameter("max_sync_replica_count", &TThis::MaxSyncReplicaCount)
        .Alias("sync_replica_count")
        .Optional();
    registrar.Parameter("min_sync_replica_count", &TThis::MinSyncReplicaCount)
        .Optional();

    registrar.Parameter("sync_replica_lag_threshold", &TThis::SyncReplicaLagThreshold)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("tablet_cell_bundle_name_ttl", &TThis::TabletCellBundleNameTtl)
        .Default(TDuration::Seconds(300));
    registrar.Parameter("tablet_cell_bundle_name_failure_interval", &TThis::RetryOnFailureInterval)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("enable_preload_state_check", &TThis::EnablePreloadStateCheck)
        .Default(false);
    registrar.Parameter("incomplete_preload_grace_period", &TThis::IncompletePreloadGracePeriod)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("preferred_sync_replica_clusters", &TThis::PreferredSyncReplicaClusters)
        .Default(std::nullopt);

    registrar.Postprocessor([] (TThis* config) {
        if (config->MaxSyncReplicaCount && config->MinSyncReplicaCount && *config->MinSyncReplicaCount > *config->MaxSyncReplicaCount) {
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

void TDynamicReplicatedTableTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_replicated_table_tracker", &TThis::EnableReplicatedTableTracker)
        .Default(true);
    registrar.Parameter("use_new_replicated_table_tracker", &TThis::UseNewReplicatedTableTracker)
        .Default(false);
    registrar.Parameter("check_period", &TThis::CheckPeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("general_check_timeout", &TThis::GeneralCheckTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("replicator_hint", &TThis::ReplicatorHint)
        .DefaultNew();
    registrar.Parameter("bundle_health_cache", &TThis::BundleHealthCache)
        .DefaultNew();
    registrar.Parameter("cluster_state_cache", &TThis::ClusterStateCache)
        .DefaultNew();
    registrar.Parameter("cluster_directory_synchronizer", &TThis::ClusterDirectorySynchronizer)
        .DefaultNew();
    registrar.Parameter("max_iterations_without_acceptable_bundle_health", &TThis::MaxIterationsWithoutAcceptableBundleHealth)
        .Default(1);
    registrar.Parameter("max_action_queue_size", &TThis::MaxActionQueueSize)
        .Default(10000)
        .GreaterThanOrEqual(0);
    registrar.Parameter("client_expiration_time", &TThis::ClientExpirationTime)
        .Default(TDuration::Minutes(15));

    registrar.Preprocessor([] (TThis* config) {
        config->ClusterStateCache->RefreshTime = config->CheckPeriod;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
