#include "config.h"

namespace NYP::NClient::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

TAuthenticationConfig::TAuthenticationConfig()
{
    RegisterParameter("user", User);
    RegisterParameter("token", Token)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TConnectionConfig::TConnectionConfig()
{
    RegisterParameter("grpc_channel", GrpcChannel);

    RegisterParameter("discovery_period", DiscoveryPeriod)
        .Default(TDuration::Seconds(60));
    RegisterParameter("discovery_timeout", DiscoveryTimeout)
        .Default(TDuration::Seconds(60));

    RegisterParameter("authentication", Authentication)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TClientConfig::TClientConfig()
{
    RegisterParameter("connection", Connection)
        .DefaultNew();

    RegisterParameter("timeout", Timeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
