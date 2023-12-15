#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/library/dynamic_config/config.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

class TStandaloneTabletBalancerConfig
    : public NYTree::TYsonStruct
{
public:
    bool AbortOnUnrecognizedOptions;

    TDuration Period;

    i64 WorkerThreadPoolSize;

    TDuration ParameterizedTimeoutOnStart;
    TDuration ParameterizedTimeout;

    REGISTER_YSON_STRUCT(TStandaloneTabletBalancerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStandaloneTabletBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerDynamicConfig
    : public TNativeSingletonsDynamicConfig
{
public:
    // Enable standalone tablet balancer. The balancer will not work at all if set to false.
    bool Enable;

    // Ignore bundle attribute EnableStandaloneTabletBalancer and balance each bundle.
    bool EnableEverywhere;

    std::optional<TDuration> Period;

    NYPath::TYPath StatisticsTablePath;
    bool UseStatisticsReporter;

    bool EnableSwaps;
    int MaxParameterizedMoveActionCount;
    double ParameterizedNodeDeviationThreshold;
    double ParameterizedCellDeviationThreshold;
    double ParameterizedMinRelativeMetricImprovement;
    TString DefaultParameterizedMetric;

    TTimeFormula Schedule;
    TDuration BundleErrorsTtl;

    bool FetchTabletCellsFromSecondaryMasters;
    bool PickReshardPivotKeys;
    bool CancelActionIfPickPivotKeysFails;
    bool EnableReshardVerboseLogging;
    std::optional<double> ReshardSlicingAccuracy;

    i64 MinDesiredTabletSize;

    int MaxActionsPerGroup;

    TActionManagerConfigPtr ActionManager;

    REGISTER_YSON_STRUCT(TTabletBalancerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TActionManagerConfig
    : public NYTree::TYsonStruct
{
public:
    int CreateActionBatchSizeLimit;
    TDuration TabletActionPollingPeriod;
    TDuration TabletActionCreationTimeout;
    TDuration TabletActionExpirationTimeout;

    REGISTER_YSON_STRUCT(TActionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TActionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerServerConfig
    : public TNativeServerConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    TStandaloneTabletBalancerConfigPtr TabletBalancer;

    TString ClusterUser;

    NYPath::TYPath RootPath;

    NCypressElection::TCypressElectionManagerConfigPtr ElectionManager;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    REGISTER_YSON_STRUCT(TTabletBalancerServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
