#include "config.h"

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

TMemberClientConfig::TMemberClientConfig()
{
    RegisterParameter("server_addresses", ServerAddresses)
        .NonEmpty();
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(5));
    RegisterParameter("heartbeat_period", HeartbeatPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("attribute_update_period", AttributeUpdatePeriod)
        .Default(TDuration::Seconds(5));
    RegisterParameter("lease_timeout", LeaseTimeout)
        .Default(TDuration::Seconds(5));

    RegisterParameter("write_quorum", WriteQuorum)
        .Default(2);
    RegisterParameter("server_ban_timeout", ServerBanTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

TDiscoveryClientConfig::TDiscoveryClientConfig()
{
    RegisterParameter("server_addresses", ServerAddresses)
        .NonEmpty();
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(5));

    RegisterParameter("read_quorum", ReadQuorum)
        .Default(2);
    RegisterParameter("server_ban_timeout", ServerBanTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

