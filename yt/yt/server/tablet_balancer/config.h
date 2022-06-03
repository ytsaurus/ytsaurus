#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

class TStandaloneTabletBalancerConfig
    : public NYTree::TYsonStruct
{
public:
    bool AbortOnUnrecognizedOptions;

    TDuration Period;

    i64 WorkerThreadPoolSize;

    REGISTER_YSON_STRUCT(TStandaloneTabletBalancerConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStandaloneTabletBalancerConfig)

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

    REGISTER_YSON_STRUCT(TTabletBalancerServerConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
