#include "config.h"

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

TClusterDiscoveryServerConfig::TClusterDiscoveryServerConfig()
{
    RegisterParameter("abort_on_unrecognized_options", AbortOnUnrecognizedOptions)
        .Default(false);

    RegisterParameter("worker_thread_pool_size", WorkerThreadPoolSize)
        .Default(4);

    RegisterParameter("bus_client", BusClient)
        .DefaultNew();

    RegisterParameter("discovery_server", DiscoveryServer)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
