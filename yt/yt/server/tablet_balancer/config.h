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

    TDuration TabletActionExpirationTime;
    TDuration TabletActionPollingPeriod;

    REGISTER_YSON_STRUCT(TStandaloneTabletBalancerConfig)

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

    int MaxParameterizedMoveActionCount;

    double ParameterizedDeviationThreshold;

    TTimeFormula Schedule;

    REGISTER_YSON_STRUCT(TTabletBalancerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerServerConfig
    : public TServerConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    TStandaloneTabletBalancerConfigPtr TabletBalancer;

    NApi::NNative::TConnectionConfigPtr ClusterConnection;

    TString ClusterUser;

    NYPath::TYPath RootPath;

    NCypressElection::TCypressElectionManagerConfigPtr ElectionManager;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    REGISTER_YSON_STRUCT(TTabletBalancerServerConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
