#include "config.h"

namespace NYT::NTabletServer {

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
