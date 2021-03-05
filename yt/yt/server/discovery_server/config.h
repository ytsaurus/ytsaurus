#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/discovery_server/config.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

class TClusterDiscoveryServerConfig
    : public TServerConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    int WorkerThreadPoolSize;

    NBus::TTcpBusConfigPtr BusClient;

    NDiscoveryServer::TDiscoveryServerConfigPtr DiscoveryServer;

    TClusterDiscoveryServerConfig();
};

DEFINE_REFCOUNTED_TYPE(TClusterDiscoveryServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
