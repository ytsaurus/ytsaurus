#include "access_checker.h"

#include "private.h"
#include "bootstrap.h"
#include "config.h"
#include "dynamic_config_manager.h"

#include <yt/yt/server/lib/rpc_proxy/access_checker.h>
#include <yt/yt/server/lib/rpc_proxy/proxy_coordinator.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NRpcProxy {

using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcProxyLogger;

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
            RpcProxyProfiler.WithPrefix("/access_checker_cache")))
        , Enabled_(Config_->Enabled)
    {
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TAccessChecker::OnDynamicConfigChanged, MakeWeak(this)));
        proxyCoordinator->SubscribeOnProxyRoleChanged(BIND(&TAccessChecker::OnProxyRoleChanged, MakeWeak(this)));
    }

    TError ValidateAccess(const TString& user) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!Enabled_.load()) {
            return TError();
        }

        auto proxyRole = ProxyRole_.Load();
        if (!proxyRole) {
            return TError();
        }

        auto path = Format("%v/%v", Config_->PathPrefix, *proxyRole);
        auto error = WaitFor(Cache_->Get(TPermissionKey{
            .Object = path,
            .User = user,
            .Permission = EPermission::Use
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

    TAtomicObject<std::optional<TString>> ProxyRole_;

    void OnDynamicConfigChanged(
        const TProxyDynamicConfigPtr& /*oldConfig*/,
        const TProxyDynamicConfigPtr& newConfig)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Enabled_.store(newConfig->AccessChecker->Enabled.value_or(Config_->Enabled));
    }

    void OnProxyRoleChanged(const std::optional<TString>& newRole)
    {
        VERIFY_THREAD_AFFINITY_ANY();

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
