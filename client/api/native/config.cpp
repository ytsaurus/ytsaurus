#include "config.h"

namespace NYP::NClient::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

namespace {

TString InferGrpcAddress(const TString& address)
{
    static const TString GrpcPort = "8090";
    static const TString BalancerDomain = ".yp.yandex.net";
    if (!address.Contains('.') &&
        !address.Contains(':') &&
        address != "localhost")
    {
        return address + BalancerDomain + ":" + GrpcPort;
    }
    return address;
}

} // namespace

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

    RegisterPostprocessor([&] {
        GrpcChannel->Address = InferGrpcAddress(GrpcChannel->Address);
    });
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
