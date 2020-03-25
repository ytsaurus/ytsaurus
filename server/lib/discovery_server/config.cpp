#include "config.h"

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

TDiscoveryServerConfig::TDiscoveryServerConfig()
{
    RegisterParameter("server_addresses", ServerAddresses)
        .NonEmpty();
    RegisterParameter("gossip_period", GossipPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("attributes_update_period", AttributesUpdatePeriod)
        .Default(TDuration::Seconds(5));
    RegisterParameter("max_members_per_gossip", MaxMembersPerGossip)
        .Default(1000);
    RegisterParameter("gossip_batch_size", GossipBatchSize)
        .Default(100);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer

