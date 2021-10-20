#pragma once

#include "private.h"

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/node_tracker_client/public.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerConfig
    : public NYTree::TYsonSerializable
{
public:
    TCellBalancerConfig();
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerMasterConnectorConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration ConnectRetryBackoffTime;

    TCellBalancerMasterConnectorConfig();
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

    TCellBalancerBootstrapConfig();
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
