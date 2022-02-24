#pragma once

#include "private.h"

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerConfig
    : public NYTree::TYsonStruct
{
public:
    bool AbortOnUnrecognizedOptions;

    TDuration Period;

    REGISTER_YSON_STRUCT(TTabletBalancerConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerServerConfig
    : public TServerConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    TTabletBalancerConfigPtr TabletBalancer;

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
