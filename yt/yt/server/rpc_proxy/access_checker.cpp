#include "access_checker.h"

#include "bootstrap.h"
#include "config.h"
#include "dynamic_config_manager.h"

#include <yt/yt/server/lib/rpc_proxy/access_checker.h>
#include <yt/yt/server/lib/rpc_proxy/proxy_coordinator.h>
#include <yt/yt/server/lib/rpc_proxy/private.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NRpcProxy {

using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = RpcProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TAccessChecker
    : public IAccessChecker
{
public:
    TAccessChecker(
        TAccessCheckerConfigPtr config,
        IProxyCoordinatorPtr proxyCoordinator,
        NApi::NNative::IConnectionPtr connection,
        IDynamicConfigManagerPtr dynamicConfigManager)
        : Config_(std::move(config))
        , Cache_(New<TPermissionCache>(
            Config_->Cache,
            connection,
            RpcProxyProfiler().WithPrefix("/access_checker_cache")))
        , Enabled_(Config_->Enabled)
    {
        dynamicConfigManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TAccessChecker::OnDynamicConfigChanged, MakeWeak(this)));
        proxyCoordinator->SubscribeOnProxyRoleChanged(BIND_NO_PROPAGATE(&TAccessChecker::OnProxyRoleChanged, MakeWeak(this)));
    }

    TError CheckAccess(const std::string& user) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (!Enabled_.load()) {
            return TError();
        }

        auto proxyRole = ProxyRole_.Load();
        if (!proxyRole) {
            return TError();
        }

        auto path = Config_->UseAccessControlObjects
            ? Format("%v/%v/principal", Config_->PathPrefix, *proxyRole)
            : Format("%v/%v", Config_->PathPrefix, *proxyRole);
        auto error = WaitForFast(Cache_->Get(TPermissionKey{
            .Object = path,
            .User = user,
            .Permission = EPermission::Use,
        }));

        if (error.IsOK()) {
            return TError();
        }

        if (error.FindMatching(NSecurityClient::EErrorCode::AuthorizationError)) {
            return TError("User %Qv is not allowed to use RPC proxies with role %Qv", user, proxyRole)
                << error;
        }

        YT_LOG_INFO(error, "Failed to check if user is allowed to use RPC proxy (User: %v, Role: %v)",
            user,
            proxyRole);

        return TError();
    }

private:
    const TAccessCheckerConfigPtr Config_;

    const TPermissionCachePtr Cache_;

    std::atomic<bool> Enabled_;

    NThreading::TAtomicObject<std::optional<std::string>> ProxyRole_;

    void OnDynamicConfigChanged(
        const TProxyDynamicConfigPtr& /*oldConfig*/,
        const TProxyDynamicConfigPtr& newConfig)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        Enabled_.store(newConfig->AccessChecker->Enabled.value_or(Config_->Enabled));
    }

    void OnProxyRoleChanged(const std::optional<std::string>& newRole)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        ProxyRole_.Store(newRole);
    }
};

////////////////////////////////////////////////////////////////////////////////

IAccessCheckerPtr CreateAccessChecker(
    TAccessCheckerConfigPtr config,
    IProxyCoordinatorPtr proxyCoordinator,
    NApi::NNative::IConnectionPtr connection,
    IDynamicConfigManagerPtr dynamicConfigManager)
{
    return New<TAccessChecker>(
        std::move(config),
        std::move(proxyCoordinator),
        std::move(connection),
        std::move(dynamicConfigManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
