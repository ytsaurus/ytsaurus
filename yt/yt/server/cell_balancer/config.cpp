#include "config.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

void TCellBalancerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_manager", &TThis::TabletManager)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TCellBalancerMasterConnectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("connect_retry_backoff_time", &TThis::ConnectRetryBackoffTime)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

void TCellBalancerBootstrapConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("cluster_connection", &TThis::ClusterConnection);

    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultNew();
    registrar.Parameter("cell_balancer", &TThis::CellBalancer)
        .DefaultNew();
    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();

    registrar.Parameter("addresses", &TThis::Addresses)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
