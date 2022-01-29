#pragma once

#include "private.h"

#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/node_tracker_client/public.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerConfig
    : public NYTree::TYsonStruct
{
public:
    NTabletServer::TDynamicTabletManagerConfigPtr TabletManager;

    REGISTER_YSON_STRUCT(TCellBalancerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerMasterConnectorConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration ConnectRetryBackoffTime;

    REGISTER_YSON_STRUCT(TCellBalancerMasterConnectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerBootstrapConfig
    : public TServerConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    NApi::NNative::TConnectionConfigPtr ClusterConnection;

    NCypressElection::TCypressElectionManagerConfigPtr ElectionManager;
    TCellBalancerConfigPtr CellBalancer;
    TCellBalancerMasterConnectorConfigPtr MasterConnector;

    NNodeTrackerClient::TNetworkAddressList Addresses;

    REGISTER_YSON_STRUCT(TCellBalancerBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
