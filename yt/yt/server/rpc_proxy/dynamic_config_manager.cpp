#include "dynamic_config_manager.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/server/lib/rpc_proxy/proxy_coordinator.h>

#include <yt/yt/core/net/local_address.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

class TDynamicConfigManager
    : public IDynamicConfigManager
{
public:
    explicit TDynamicConfigManager(TBootstrap* bootstrap)
        : IDynamicConfigManager(
            TDynamicConfigManagerOptions{
                .ConfigPath = bootstrap->GetConfig()->DynamicConfigPath,
                .Name = "RpcProxy",
                .ConfigIsTagged = bootstrap->GetConfig()->UseTaggedDynamicConfig
            },
            bootstrap->GetConfig()->DynamicConfigManager,
            bootstrap->GetNativeClient(),
            bootstrap->GetControlInvoker())
        , Bootstrap_(bootstrap)
    {
        auto proxyAddress = NNet::BuildServiceAddress(NNet::GetLocalHostName(), bootstrap->GetConfig()->RpcPort);
        BaseTags_.push_back(proxyAddress);

        OnProxyRoleChanged(std::nullopt);
    }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& coordinator = Bootstrap_->GetProxyCoordinator();
        coordinator->SubscribeOnProxyRoleChanged(BIND(&TDynamicConfigManager::OnProxyRoleChanged, MakeWeak(this)));
    }

private:
    TBootstrap* Bootstrap_;

    std::vector<TString> BaseTags_;

    TAtomicObject<TString> ProxyRole_;

    std::vector<TString> GetInstanceTags() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto tags = BaseTags_;
        tags.push_back(ProxyRole_.Load());

        return tags;
    }

    void OnProxyRoleChanged(const std::optional<TString>& newRole)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (newRole) {
            ProxyRole_.Store(*newRole);
        } else {
            ProxyRole_.Store("default");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IDynamicConfigManagerPtr CreateDynamicConfigManager(TBootstrap* bootstrap)
{
    return New<TDynamicConfigManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
