#include "config.h"

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

TDiscoveryClientBaseConfig::TDiscoveryClientBaseConfig()
{
    RegisterParameter("server_addresses", ServerAddresses);
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(5));
    RegisterParameter("server_ban_timeout", ServerBanTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

TMemberClientConfig::TMemberClientConfig()
{
    RegisterParameter("heartbeat_period", HeartbeatPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("attribute_update_period", AttributeUpdatePeriod)
        .Default(TDuration::Seconds(5));
    RegisterParameter("lease_timeout", LeaseTimeout)
        .Default(TDuration::Seconds(5));
    RegisterParameter("max_failed_heartbeats_on_startup", MaxFailedHeartbeatsOnStartup)
        .Default(10);
    RegisterParameter("write_quorum", WriteQuorum)
        .Default(2);
}

////////////////////////////////////////////////////////////////////////////////

TDiscoveryClientConfig::TDiscoveryClientConfig()
{
    RegisterParameter("read_quorum", ReadQuorum)
        .Default(2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

