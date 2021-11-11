#include "config.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

TCellBalancerConfig::TCellBalancerConfig()
{
    RegisterParameter("tablet_manager", TabletManager)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TCellBalancerMasterConnectorConfig::TCellBalancerMasterConnectorConfig()
{
    RegisterParameter("connect_retry_backoff_time", ConnectRetryBackoffTime)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

TCellBalancerBootstrapConfig::TCellBalancerBootstrapConfig()
{
    RegisterParameter("abort_on_unrecognized_options", AbortOnUnrecognizedOptions)
        .Default(false);

    RegisterParameter("cluster_connection", ClusterConnection);

    RegisterParameter("election_manager", ElectionManager)
        .DefaultNew();
    RegisterParameter("cell_balancer", CellBalancer)
        .DefaultNew();
    RegisterParameter("master_connector", MasterConnector)
        .DefaultNew();

    RegisterParameter("addresses", Addresses)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
