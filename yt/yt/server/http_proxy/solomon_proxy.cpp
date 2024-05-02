#include "solomon_proxy.h"
#include "config.h"

namespace NYT::NHttpProxy {

using namespace NApi;
using namespace NHttp;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

auto GetLabel(bool value)
{
    return value ? "1" : "0";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYTComponentEndpointProvider::TYTComponentEndpointProvider(
    NApi::IClientPtr client,
    TYTComponentEndpointProviderConfigPtr config)
    : TComponentDiscoverer(
        std::move(client),
        TMasterReadOptions{.ReadFrom = EMasterChannelKind::MasterCache},
        config->ProxyDeathAge)
    , Config_(std::move(config))
{ }

TString TYTComponentEndpointProvider::GetComponentName() const
{
    return FormatEnum(Config_->ComponentType);
}

std::vector<IEndpointProvider::TEndpoint> TYTComponentEndpointProvider::GetEndpoints() const
{
    auto instances = GetInstances(Config_->ComponentType);

    std::vector<TEndpoint> endpoints;
    endpoints.reserve(instances.size() * Config_->Shards.size());

    for (const auto& instance : instances) {
        TEndpoint endpoint;

        // Plain hostname without any specified port.
        auto hostname = instance.Address.substr(0, instance.Address.find_last_of(':'));

        endpoint.Name = Config_->IncludePortInInstanceName ? instance.Address : hostname;
        endpoint.Labels["version"] = instance.Version;
        endpoint.Labels["banned"] = GetLabel(instance.Banned);
        endpoint.Labels["online"] = GetLabel(instance.Online);
        endpoint.Labels["state"] = instance.State;

        for (const auto& shard : Config_->Shards) {
            endpoint.Address = Format("http://%v:%v/solomon/%v", hostname, Config_->MonitoringPort, shard);
            endpoint.Labels["shard"] = shard;
            endpoints.push_back(endpoint);
        }
    }

    return endpoints;
}

////////////////////////////////////////////////////////////////////////////////

TSolomonProxyPtr CreateSolomonProxy(
    const NHttpProxy::TSolomonProxyConfigPtr& config,
    const NApi::IClientPtr& client,
    NConcurrency::IPollerPtr poller)
{
    auto solomonProxy = New<TSolomonProxy>(config, std::move(poller));
    for (const auto& endpointProviderConfig : config->EndpointProviders) {
        solomonProxy->RegisterEndpointProvider(New<TYTComponentEndpointProvider>(client, endpointProviderConfig));
    }
    return solomonProxy;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
