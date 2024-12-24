#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/discovery_server/config.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

class TClusterDiscoveryServerConfig
    : public TServerConfig
    , public TServerProgramConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    int WorkerThreadPoolSize;

    NBus::TBusConfigPtr BusClient;

    NDiscoveryServer::TDiscoveryServerConfigPtr DiscoveryServer;

    REGISTER_YSON_STRUCT(TClusterDiscoveryServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClusterDiscoveryServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
