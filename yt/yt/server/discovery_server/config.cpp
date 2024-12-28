#include "config.h"

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryServerBootstrapConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("worker_thread_pool_size", &TThis::WorkerThreadPoolSize)
        .Default(4);

    registrar.Parameter("bus_client", &TThis::BusClient)
        .DefaultNew();

    registrar.Parameter("discovery_server", &TThis::DiscoveryServer)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryServerProgramConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
