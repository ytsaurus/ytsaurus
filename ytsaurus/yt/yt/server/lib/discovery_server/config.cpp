#include "config.h"

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("server_addresses", &TThis::ServerAddresses)
        .NonEmpty();
    registrar.Parameter("gossip_period", &TThis::GossipPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("attributes_update_period", &TThis::AttributesUpdatePeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("max_members_per_gossip", &TThis::MaxMembersPerGossip)
        .Default(1000);
    registrar.Parameter("gossip_batch_size", &TThis::GossipBatchSize)
        .Default(100);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer

