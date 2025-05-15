
#include <util/system/env.h>

#include <yt/yt/library/auth/auth.h>

#include <yt/systest/rpc_client.h>

namespace NYT::NTest {

namespace {

class TRpcConfig : public TSingletonsConfig
{
};
DEFINE_REFCOUNTED_TYPE(TRpcConfig);

}  // namespace

NApi::IClientPtr CreateRpcClient(const TNetworkConfig& config) {
    auto proxyAddress = GetEnv("YT_PROXY");
    if (proxyAddress.empty()) {
        THROW_ERROR_EXCEPTION("YT_PROXY environment variable must be set");
    }
    auto connectionConfig = New<NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->ClusterUrl = proxyAddress;
    connectionConfig->ProxyListUpdatePeriod = TDuration::Seconds(5);

    auto singletonsConfig = New<TRpcConfig>();
    if (config.Ipv4) {
        auto addressResolverConfig = singletonsConfig->GetSingletonConfig<NNet::TAddressResolverConfig>();
        addressResolverConfig->EnableIPv4 = true;
        addressResolverConfig->EnableIPv6 = false;
    }
    ConfigureSingletons(singletonsConfig);

    auto connection = NApi::NRpcProxy::CreateConnection(connectionConfig);

    auto token = NAuth::LoadToken();
    if (!token) {
        THROW_ERROR_EXCEPTION("YT_TOKEN environment variable must be set");
    }

    NApi::TClientOptions clientOptions = NApi::TClientOptions::FromToken(*token);
    return connection->CreateClient(clientOptions);
}

}  // namespace NYT::NTest
