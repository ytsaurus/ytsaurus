#include "config.h"

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryClientBaseConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("server_addresses", &TThis::ServerAddresses);
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("server_ban_timeout", &TThis::ServerBanTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

void TMemberClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("attribute_update_period", &TThis::AttributeUpdatePeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("lease_timeout", &TThis::LeaseTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("max_failed_heartbeats_on_startup", &TThis::MaxFailedHeartbeatsOnStartup)
        .Default(10);
    registrar.Parameter("write_quorum", &TThis::WriteQuorum)
        .Default(2);
}

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("read_quorum", &TThis::ReadQuorum)
        .Default(2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

