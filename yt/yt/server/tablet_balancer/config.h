#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/server_program/config.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TStandaloneTabletBalancerConfig
    : public NYTree::TYsonStruct
{
    bool AbortOnUnrecognizedOptions;

    TDuration Period;

    int WorkerThreadPoolSize;
    i64 PivotPickerThreadPoolSize;

    TDuration ParameterizedTimeoutOnStart;
    TDuration ParameterizedTimeout;

    REGISTER_YSON_STRUCT(TStandaloneTabletBalancerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStandaloneTabletBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTabletBalancerDynamicConfig
    : public TSingletonsDynamicConfig
{
    // Enable standalone tablet balancer. The balancer will not work at all if set to false.
    bool Enable;

    // Ignore bundle attribute EnableStandaloneTabletBalancer and balance each bundle.
    bool EnableEverywhere;

    std::optional<TDuration> Period;
    std::optional<TDuration> ParameterizedTimeoutOnStart;
    std::optional<TDuration> ParameterizedTimeout;

    NYPath::TYPath StatisticsTablePath;
    bool UseStatisticsReporter;

    int MaxParameterizedMoveActionCount;
    int MaxParameterizedMoveActionHardLimit;
    double ParameterizedNodeDeviationThreshold;
    double ParameterizedCellDeviationThreshold;
    double ParameterizedMinRelativeMetricImprovement;
    TString DefaultParameterizedMetric;
    TComponentFactorConfigPtr ParameterizedFactors;

    TTimeFormula Schedule;
    TDuration BundleErrorsTtl;

    bool FetchTabletCellsFromSecondaryMasters;
    bool EnableParameterizedReshardByDefault;
    bool PickReshardPivotKeys;
    bool CancelActionIfPickPivotKeysFails;
    bool EnableReshardVerboseLogging;
    bool IgnoreTabletToCellRatio;
    std::optional<double> ReshardSlicingAccuracy;
    std::optional<bool> EnableSmoothMovement;

    THashSet<std::string> AllowedReplicaClusters;

    i64 MinDesiredTabletSize;

    int MaxActionsPerGroup;
    int MaxActionsPerReshardType;

    TActionManagerConfigPtr ActionManager;
    TClusterStateProviderConfigPtr ClusterStateProvider;
    TBundleStateProviderConfigPtr BundleStateProvider;

    std::vector<std::string> ClustersForBundleHealthCheck;
    int MaxUnhealthyBundlesOnReplicaCluster;

    NConcurrency::TThroughputThrottlerConfigPtr MasterRequestThrottler;

    REGISTER_YSON_STRUCT(TTabletBalancerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TActionManagerConfig
    : public NYTree::TYsonStruct
{
    int CreateActionBatchSizeLimit;
    TDuration TabletActionPollingPeriod;
    TDuration TabletActionCreationTimeout;
    TDuration TabletActionExpirationTimeout;
    int MaxTabletCountPerAction;

    REGISTER_YSON_STRUCT(TActionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TActionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TClusterStateProviderConfig
    : public NYTree::TYsonStruct
{
    std::vector<std::string> ClustersForBundleHealthCheck;
    std::string MetaClusterForBannedReplicas;

    TDuration FetchPlannerPeriod;
    int WorkerThreadPoolSize;

    bool FetchTabletActionsBundleAttribute;

    TDuration BundlesFreshnessTime;
    TDuration NodesFreshnessTime;
    TDuration UnhealthyBundlesFreshnessTime;
    TDuration BannedReplicasFreshnessTime;

    TDuration BundlesFetchPeriod;
    TDuration NodesFetchPeriod;
    TDuration UnhealthyBundlesFetchPeriod;
    TDuration BannedReplicasFetchPeriod;

    REGISTER_YSON_STRUCT(TClusterStateProviderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClusterStateProviderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBundleStateProviderConfig
    : public NYTree::TYsonStruct
{
    bool FetchTabletCellsFromSecondaryMasters;
    bool UseStatisticsReporter;
    NYPath::TYPath StatisticsTablePath;

    TDuration FetchPlannerPeriod;

    TDuration StateFreshnessTime;
    TDuration StatisticsFreshnessTime;
    TDuration PerformanceCountersFreshnessTime;
    TDuration ConfigFreshnessTime;

    std::optional<TDuration> StateFetchPeriod;
    std::optional<TDuration> StatisticsFetchPeriod;
    std::optional<TDuration> PerformanceCountersFetchPeriod;

    bool CheckInvariants;

    REGISTER_YSON_STRUCT(TBundleStateProviderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleStateProviderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTabletBalancerBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
    bool AbortOnUnrecognizedOptions;

    TStandaloneTabletBalancerConfigPtr TabletBalancer;

    std::string ClusterUser;

    NYPath::TYPath RootPath;

    NCypressElection::TCypressElectionManagerConfigPtr ElectionManager;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    REGISTER_YSON_STRUCT(TTabletBalancerBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTabletBalancerProgramConfig
    : public TTabletBalancerBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TTabletBalancerProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerProgramConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
