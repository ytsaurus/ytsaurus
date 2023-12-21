#include "config.h"

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

    registrar.Parameter("parameterized_timeout_on_start", &TThis::ParameterizedTimeoutOnStart)
        .Default(TDuration::Minutes(10));
    registrar.Parameter("parameterized_timeout", &TThis::ParameterizedTimeout)
        .Default(TDuration::Minutes(10));
}

////////////////////////////////////////////////////////////////////////////////

void TTabletBalancerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("enable_everywhere", &TThis::EnableEverywhere)
        .Default(false);

    registrar.Parameter("enable_swaps", &TThis::EnableSwaps)
        .Default(true);
    registrar.Parameter("max_parameterized_move_action_count", &TThis::MaxParameterizedMoveActionCount)
        .Default(5)
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

    registrar.Parameter("schedule", &TThis::Schedule)
        .Default(DefaultTabletBalancerSchedule);
    registrar.Parameter("period", &TThis::Period)
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
    registrar.Parameter("pick_reshard_pivot_keys", &TThis::PickReshardPivotKeys)
        .Default(true);
    registrar.Parameter("cancel_action_if_pick_pivot_keys_fails", &TThis::CancelActionIfPickPivotKeysFails)
        .Default(false);
    registrar.Parameter("enable_reshard_verbose_logging", &TThis::EnableReshardVerboseLogging)
        .Default(false);
    registrar.Parameter("reshard_slicing_accuracy", &TThis::ReshardSlicingAccuracy)
        .Default();

    registrar.Parameter("min_desired_tablet_size", &TThis::MinDesiredTabletSize)
        .Default(5_MB);

    registrar.Parameter("max_actions_per_group", &TThis::MaxActionsPerGroup)
        .Default(300)
        .GreaterThan(0);
    registrar.Parameter("action_manager", &TThis::ActionManager)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        if (config->Schedule.IsEmpty()) {
            THROW_ERROR_EXCEPTION("Schedule cannot be empty");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TActionManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("create_action_batch_size_limit", &TThis::CreateActionBatchSizeLimit)
        .Default(100);
    registrar.Parameter("tablet_action_polling_period", &TThis::TabletActionPollingPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("tablet_action_creation_timeout", &TThis::TabletActionCreationTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("tablet_action_expiration_timeout", &TThis::TabletActionExpirationTimeout)
        .Default(TDuration::Minutes(20));
}

////////////////////////////////////////////////////////////////////////////////

void TTabletBalancerServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("tablet_balancer", &TThis::TabletBalancer)
        .DefaultNew();
    registrar.Parameter("cluster_user", &TThis::ClusterUser)
        .Default(NSecurityClient::TabletBalancerUserName);
    registrar.Parameter("root_path", &TThis::RootPath)
        .Default("//sys/tablet_balancer");
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

} // namespace NYT::NTabletBalancer
