#include "dynamic_config_manager.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/server/lib/rpc_proxy/proxy_coordinator.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/net/local_address.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

class TDynamicConfigManager
    : public IDynamicConfigManager
{
public:
    TDynamicConfigManager(
        TProxyConfigPtr config,
        IProxyCoordinatorPtr proxyCoordinator,
        NApi::NNative::IConnectionPtr connection,
        IInvokerPtr controlInvoker)
        : IDynamicConfigManager(
            TDynamicConfigManagerOptions{
                .ConfigPath = config->DynamicConfigPath,
                .Name = "RpcProxy",
                .ConfigIsTagged = config->UseTaggedDynamicConfig
            },
            config->DynamicConfigManager,
            connection->CreateNativeClient(NApi::TClientOptions::FromUser(NRpc::RootUserName)),
            controlInvoker)
        , Config_(std::move(config))
        , ProxyCoordinator_(std::move(proxyCoordinator))
    {
        auto proxyAddress = NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort);
        BaseTags_.push_back(proxyAddress);

        OnProxyRoleChanged(std::nullopt);
    }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ProxyCoordinator_->SubscribeOnProxyRoleChanged(BIND_NO_PROPAGATE(&TDynamicConfigManager::OnProxyRoleChanged, MakeWeak(this)));
    }

private:
    const TProxyConfigPtr Config_;
    const IProxyCoordinatorPtr ProxyCoordinator_;

    std::vector<std::string> BaseTags_;

    NThreading::TAtomicObject<std::string> ProxyRole_;

    std::vector<std::string> GetInstanceTags() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto tags = BaseTags_;
        tags.push_back(ProxyRole_.Load());

        return tags;
    }

    void OnProxyRoleChanged(const std::optional<std::string>& newRole)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ProxyRole_.Store(newRole.value_or(NApi::DefaultRpcProxyRole));
    }
};

////////////////////////////////////////////////////////////////////////////////

IDynamicConfigManagerPtr CreateDynamicConfigManager(
    TProxyConfigPtr config,
    IProxyCoordinatorPtr proxyCoordinator,
    NApi::NNative::IConnectionPtr connection,
    IInvokerPtr controlInvoker)
{
    return New<TDynamicConfigManager>(
        std::move(config),
        std::move(proxyCoordinator),
        std::move(connection),
        std::move(controlInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
