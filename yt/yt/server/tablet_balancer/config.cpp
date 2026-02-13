#include "config.h"
#include "private.h"

#include <yt/yt/server/lib/tablet_balancer/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/security_client/public.h>

namespace NYT::NTabletBalancer {

const TTimeFormula DefaultTabletBalancerSchedule = MakeTimeFormula("minutes % 5 == 0");
const TString DefaultParameterizedMetricFormula = "double([/performance_counters/dynamic_row_write_data_weight_10m_rate])";
const TString StatisticsTableDefaultPath = "//sys/tablet_balancer/performance_counters";

////////////////////////////////////////////////////////////////////////////////

void TStandaloneTabletBalancerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("period", &TThis::Period)
        .Default(TDuration::Minutes(2));
    registrar.Parameter("worker_thread_pool_size", &TThis::WorkerThreadPoolSize)
        .Default(3);
    registrar.Parameter("pivot_picker_thread_pool_size", &TThis::PivotPickerThreadPoolSize)
        .Default(3);

    registrar.Parameter("parameterized_timeout_on_start", &TThis::ParameterizedTimeoutOnStart)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("parameterized_timeout", &TThis::ParameterizedTimeout)
        .Default(TDuration::Minutes(9));
}

////////////////////////////////////////////////////////////////////////////////

void TTabletBalancerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("enable_everywhere", &TThis::EnableEverywhere)
        .Default(false);

    registrar.Parameter("max_parameterized_move_action_count", &TThis::MaxParameterizedMoveActionCount)
        .Default(50)
        .GreaterThanOrEqual(0);
    registrar.Parameter("max_parameterized_move_action_hard_limit", &TThis::MaxParameterizedMoveActionHardLimit)
        .Default(2000)
        .GreaterThanOrEqual(0);
    registrar.Parameter("parameterized_node_deviation_threshold", &TThis::ParameterizedNodeDeviationThreshold)
        .Default(0.1)
        .GreaterThanOrEqual(0);
    registrar.Parameter("parameterized_cell_deviation_threshold", &TThis::ParameterizedCellDeviationThreshold)
        .Default(0.1)
        .GreaterThanOrEqual(0);
    registrar.Parameter("parameterized_min_relative_metric_improvement", &TThis::ParameterizedMinRelativeMetricImprovement)
        .Default(0.0)
        .GreaterThanOrEqual(0);
    registrar.Parameter("default_parameterized_metric", &TThis::DefaultParameterizedMetric)
        .Default(DefaultParameterizedMetricFormula)
        .NonEmpty();
    registrar.Parameter("parameterized_factors", &TThis::ParameterizedFactors)
        .DefaultCtor([] {
            return TComponentFactorConfig::MakeDefaultIdentity();
        });

    registrar.Parameter("schedule", &TThis::Schedule)
        .Default(DefaultTabletBalancerSchedule);
    registrar.Parameter("period", &TThis::Period)
        .Default();
    registrar.Parameter("parameterized_timeout_on_start", &TThis::ParameterizedTimeoutOnStart)
        .Default();
    registrar.Parameter("parameterized_timeout", &TThis::ParameterizedTimeout)
        .Default();

    registrar.Parameter("bundle_errors_ttl", &TThis::BundleErrorsTtl)
        .Default(TDuration::Days(1));
    registrar.Parameter("statistics_table_path", &TThis::StatisticsTablePath)
        .Default(StatisticsTableDefaultPath)
        .NonEmpty();
    registrar.Parameter("use_statistics_reporter", &TThis::UseStatisticsReporter)
        .Default(false);

    registrar.Parameter("fetch_tablet_cells_from_secondary_masters", &TThis::FetchTabletCellsFromSecondaryMasters)
        .Default(false);
    registrar.Parameter("enable_parameterized_reshard_by_default", &TThis::EnableParameterizedReshardByDefault)
        .Default(false);
    registrar.Parameter("pick_reshard_pivot_keys", &TThis::PickReshardPivotKeys)
        .Default(true);
    registrar.Parameter("cancel_action_if_pick_pivot_keys_fails", &TThis::CancelActionIfPickPivotKeysFails)
        .Default(true);
    registrar.Parameter("enable_reshard_verbose_logging", &TThis::EnableReshardVerboseLogging)
        .Default(false);
    registrar.Parameter("ignore_tablet_to_cell_ratio", &TThis::IgnoreTabletToCellRatio)
        .Default(false);
    registrar.Parameter("reshard_slicing_accuracy", &TThis::ReshardSlicingAccuracy)
        .Default();
    registrar.Parameter("enable_smooth_movement", &TThis::EnableSmoothMovement)
        .Default();

    registrar.Parameter("allowed_replica_clusters", &TThis::AllowedReplicaClusters)
        .Default();

    registrar.Parameter("min_desired_tablet_size", &TThis::MinDesiredTabletSize)
        .Default(5_MB);

    registrar.Parameter("max_actions_per_group", &TThis::MaxActionsPerGroup)
        .Default(300)
        .GreaterThan(0);
    registrar.Parameter("max_actions_per_reshard_type", &TThis::MaxActionsPerReshardType)
        .Default(150)
        .GreaterThan(0);
    registrar.Parameter("action_manager", &TThis::ActionManager)
        .DefaultNew();
    registrar.Parameter("cluster_state_provider", &TThis::ClusterStateProvider)
        .DefaultNew();
    registrar.Parameter("bundle_state_provider", &TThis::BundleStateProvider)
        .DefaultNew();

    registrar.Parameter("clusters_for_bundle_health_check", &TThis::ClustersForBundleHealthCheck)
        .Default();
    registrar.Parameter("max_unhealthy_bundles_on_replica_cluster", &TThis::MaxUnhealthyBundlesOnReplicaCluster)
        .Default(5);

    registrar.Parameter("master_request_throttler", &TThis::MasterRequestThrottler)
        .DefaultCtor([] {
            auto throttler = New<NConcurrency::TThroughputThrottlerConfig>();
            throttler->Limit = 300;
            return throttler;
        });

    registrar.Postprocessor([] (TThis* config) {
        if (config->Schedule.IsEmpty()) {
            THROW_ERROR_EXCEPTION("Schedule cannot be empty");
        }

        auto updateIfEmpty = [] (auto* field, const auto& value) {
            if (field->empty()) {
                *field = value;
            }
        };

        config->BundleStateProvider->UseStatisticsReporter |= config->UseStatisticsReporter;
        config->BundleStateProvider->FetchTabletCellsFromSecondaryMasters |= config->FetchTabletCellsFromSecondaryMasters;
        updateIfEmpty(&config->BundleStateProvider->StatisticsTablePath, config->StatisticsTablePath);

        updateIfEmpty(&config->ClusterStateProvider->ClustersForBundleHealthCheck, config->ClustersForBundleHealthCheck);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TActionManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("create_action_batch_size_limit", &TThis::CreateActionBatchSizeLimit)
        .Default(500);
    registrar.Parameter("tablet_action_polling_period", &TThis::TabletActionPollingPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("tablet_action_creation_timeout", &TThis::TabletActionCreationTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("tablet_action_expiration_timeout", &TThis::TabletActionExpirationTimeout)
        .Default(TDuration::Minutes(20));
    registrar.Parameter("max_tablet_count_per_action", &TThis::MaxTabletCountPerAction)
        .Default(200)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

void TClusterStateProviderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("clusters_for_bundle_health_check", &TThis::ClustersForBundleHealthCheck)
        .Default();
    registrar.Parameter("meta_cluster_for_banned_replicas", &TThis::MetaClusterForBannedReplicas)
        .Default();

    registrar.Parameter("fetch_planner_period", &TThis::FetchPlannerPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("worker_thread_pool_size", &TThis::WorkerThreadPoolSize)
        .Default(3);

    registrar.Parameter("fetch_tablet_actions_bundle_attribute", &TThis::FetchTabletActionsBundleAttribute)
        .Default(false);

    registrar.Parameter("bundles_freshness_time", &TThis::BundlesFreshnessTime)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("nodes_freshness_time", &TThis::NodesFreshnessTime)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("unhealthy_bundles_freshness_time", &TThis::UnhealthyBundlesFreshnessTime)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("banned_replicas_freshness_time", &TThis::BannedReplicasFreshnessTime)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("bundles_fetch_period", &TThis::BundlesFetchPeriod)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("nodes_fetch_period", &TThis::NodesFetchPeriod)
        .Default(TDuration::Seconds(40));
    registrar.Parameter("unhealthy_bundles_fetch_period", &TThis::UnhealthyBundlesFetchPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("banned_replicas_fetch_period", &TThis::BannedReplicasFetchPeriod)
        .Default(TDuration::Seconds(40));
}

////////////////////////////////////////////////////////////////////////////////

void TBundleStateProviderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("fetch_tablet_cells_from_secondary_masters", &TThis::FetchTabletCellsFromSecondaryMasters)
        .Default(false);
    registrar.Parameter("use_statistics_reporter", &TThis::UseStatisticsReporter)
        .Default(false);
    registrar.Parameter("statistics_table_path", &TThis::StatisticsTablePath)
        .Default(StatisticsTableDefaultPath)
        .NonEmpty();

    registrar.Parameter("fetch_planner_period", &TThis::FetchPlannerPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("state_freshness_time", &TThis::StateFreshnessTime)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("statistics_freshness_time", &TThis::StatisticsFreshnessTime)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("performance_counters_freshness_time", &TThis::PerformanceCountersFreshnessTime)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("config_freshness_time", &TThis::ConfigFreshnessTime)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("state_fetch_period", &TThis::StateFetchPeriod)
        .Default();
    registrar.Parameter("statistics_fetch_period", &TThis::StatisticsFetchPeriod)
        .Default();
    registrar.Parameter("performance_counters_fetch_period", &TThis::PerformanceCountersFetchPeriod)
        .Default();

    registrar.Parameter("chunk_invariants", &TThis::CheckInvariants)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TTabletBalancerBootstrapConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("tablet_balancer", &TThis::TabletBalancer)
        .DefaultNew();
    registrar.Parameter("cluster_user", &TThis::ClusterUser)
        .Default(NSecurityClient::TabletBalancerUserName);
    registrar.Parameter("root_path", &TThis::RootPath)
        .Default(DefaultTabletBalancerRootPath);
    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultNew();
    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();
    registrar.Parameter("dynamic_config_path", &TThis::DynamicConfigPath)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (auto& lockPath = config->ElectionManager->LockPath; lockPath.empty()) {
            lockPath = config->RootPath + "/leader_lock";
        }
        if (auto& dynamicConfigPath = config->DynamicConfigPath; dynamicConfigPath.empty()) {
            dynamicConfigPath = config->RootPath + "/config";
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TTabletBalancerProgramConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
