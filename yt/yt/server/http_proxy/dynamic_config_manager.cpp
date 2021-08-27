#include "dynamic_config_manager.h"

#include "bootstrap.h"
#include "config.h"
#include "coordinator.h"

namespace NYT::NHttpProxy {

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
                .Name = "HttpProxy",
                .ConfigIsTagged = bootstrap->GetConfig()->UseTaggedDynamicConfig
            },
            bootstrap->GetConfig()->DynamicConfigManager,
            bootstrap->GetRootClient(),
            bootstrap->GetControlInvoker())
    {
        const auto& coordinator = bootstrap->GetCoordinator();
        coordinator->SubscribeOnSelfRoleChanged(BIND(&TDynamicConfigManager::OnProxyRoleChanged, MakeWeak(this)));

        BaseTags_.push_back(coordinator->GetSelf()->Endpoint);
        ProxyRole_.Store(coordinator->GetSelf()->Role);
    }

private:
    std::vector<TString> BaseTags_;

    TAtomicObject<TString> ProxyRole_;

    std::vector<TString> GetInstanceTags() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto tags = BaseTags_;
        tags.push_back(ProxyRole_.Load());

        return tags;
    }

    void OnProxyRoleChanged(const TString& newRole)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ProxyRole_.Store(newRole);
    }
};

////////////////////////////////////////////////////////////////////////////////

IDynamicConfigManagerPtr CreateDynamicConfigManager(TBootstrap* bootstrap)
{
    return New<TDynamicConfigManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
