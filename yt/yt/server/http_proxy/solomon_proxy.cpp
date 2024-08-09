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

//! Works for all components present in the `EClusterComponentType` enum.
class TProfilingEndpointProvider
    : public NProfiling::IEndpointProvider
    , public TComponentDiscoverer
{
public:
    TProfilingEndpointProvider(
        NApi::NNative::IClientPtr client,
        TProfilingEndpointProviderConfigPtr config,
        TComponentDiscoveryOptions componentDiscoveryOptions)
        : TComponentDiscoverer(
            std::move(client),
            TMasterReadOptions{.ReadFrom = EMasterChannelKind::MasterCache},
            std::move(componentDiscoveryOptions))
        , Config_(std::move(config))
    { }

    TString GetComponentName() const override
    {
        return FormatEnum(Config_->ComponentType);
    }

    // TODO(achulkov2): Introduce client-side caching, either in the endpoint provider hierarchy, or in the component discoverer itself.
    std::vector<TEndpoint> GetEndpoints() const override
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

private:
    const TProfilingEndpointProviderConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

TSolomonProxyPtr CreateSolomonProxy(
    const NHttpProxy::TSolomonProxyConfigPtr& config,
    const TComponentDiscoveryOptions& componentDiscoveryOptions,
    const NApi::NNative::IClientPtr& client,
    NConcurrency::IPollerPtr poller)
{
    auto solomonProxy = New<TSolomonProxy>(config, std::move(poller));
    for (const auto& endpointProviderConfig : config->EndpointProviders) {
        solomonProxy->RegisterEndpointProvider(New<TProfilingEndpointProvider>(client, endpointProviderConfig, componentDiscoveryOptions));
    }
    return solomonProxy;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
