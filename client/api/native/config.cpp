#include "config.h"

#include <yt/core/crypto/config.h>
#include <yt/core/rpc/grpc/config.h>

#include <library/resource/resource.h>

namespace NYP::NClient::NApi::NNative {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

namespace {

void PatchAddress(const NRpc::NGrpc::TChannelConfigPtr& channelConfig)
{
    static const TString GrpcPort = "8090";
    static const TString BalancerDomain = ".yp.yandex.net";

    auto& address = channelConfig->Address;
    if (!address.Contains('.') && !address.Contains(':') && address != "localhost") {
        address = address + BalancerDomain + ":" + GrpcPort;
    }
}

void PatchSecurityOptions(bool secure, const NRpc::NGrpc::TChannelConfigPtr& channelConfig)
{
    if (secure) {
        if (!channelConfig->Credentials) {
            channelConfig->Credentials = New<NRpc::NGrpc::TChannelCredentialsConfig>();
        }
        auto& credentials = channelConfig->Credentials;
        if (!credentials->PemRootCerts) {
            credentials->PemRootCerts = New<NCrypto::TPemBlobConfig>();
            credentials->PemRootCerts->Value = NResource::Find("YandexInternalRootCA.crt");
        }
    } else {
        if (channelConfig->Credentials) {
            THROW_ERROR_EXCEPTION("Value of \"secure\" is false, but \"grpc_channel/credentials\" are given");
        }
    }
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
    RegisterParameter("secure", Secure)
        .Default(true);
    RegisterParameter("grpc_channel", GrpcChannel);

    RegisterParameter("discovery_period", DiscoveryPeriod)
        .Default(TDuration::Seconds(60));
    RegisterParameter("discovery_timeout", DiscoveryTimeout)
        .Default(TDuration::Seconds(60));

    RegisterParameter("authentication", Authentication)
        .DefaultNew();

    RegisterPostprocessor([&] {
        PatchAddress(GrpcChannel);
        PatchSecurityOptions(Secure, GrpcChannel);
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
