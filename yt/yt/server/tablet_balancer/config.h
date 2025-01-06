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

class TStandaloneTabletBalancerConfig
    : public NYTree::TYsonStruct
{
public:
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

class TTabletBalancerDynamicConfig
    : public TSingletonsDynamicConfig
{
public:
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
    std::optional<double> ReshardSlicingAccuracy;

    i64 MinDesiredTabletSize;

    int MaxActionsPerGroup;
    int MaxActionsPerReshardType;

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

class TTabletBalancerBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    TStandaloneTabletBalancerConfigPtr TabletBalancer;

    TString ClusterUser;

    NYPath::TYPath RootPath;

    NCypressElection::TCypressElectionManagerConfigPtr ElectionManager;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    REGISTER_YSON_STRUCT(TTabletBalancerBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerProgramConfig
    : public TTabletBalancerBootstrapConfig
    , public TServerProgramConfig
{
public:
    REGISTER_YSON_STRUCT(TTabletBalancerProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerProgramConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
