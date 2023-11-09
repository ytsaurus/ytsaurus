#include "access_checker.h"

#include "bootstrap.h"
#include "config.h"
#include "coordinator.h"
#include "dynamic_config_manager.h"

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NHttpProxy {

using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("AccessChecker");

////////////////////////////////////////////////////////////////////////////////

class TAccessChecker
    : public IAccessChecker
{
public:
    explicit TAccessChecker(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->AccessChecker)
        , Cache_(New<TPermissionCache>(
            Config_->Cache,
            Bootstrap_->GetNativeConnection(),
            HttpProxyProfiler.WithPrefix("/access_checker_cache")))
        , ProxyRole_(Bootstrap_->GetCoordinator()->GetSelf()->Role)
        , Enabled_(Config_->Enabled)
    {
        const auto& coordinator = Bootstrap_->GetCoordinator();
        coordinator->SubscribeOnSelfRoleChanged(BIND(&TAccessChecker::OnProxyRoleUpdated, MakeWeak(this)));

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TAccessChecker::OnDynamicConfigChanged, MakeWeak(this)));
    }

    TError CheckAccess(const TString& user) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!Enabled_.load()) {
            return TError();
        }

        auto proxyRole = Bootstrap_->GetCoordinator()->GetSelf()->Role;
        auto path = Config_->UseAccessControlObjects
            ? Format("%v/%v/principal", Config_->PathPrefix, proxyRole)
            : Format("%v/%v", Config_->PathPrefix, proxyRole);
        auto error = WaitFor(Cache_->Get(TPermissionKey{
            .Object = path,
            .User = user,
            .Permission = EPermission::Use
        }));

        if (error.IsOK()) {
            return TError();
        }

        if (error.FindMatching(NSecurityClient::EErrorCode::AuthorizationError)) {
            return TError("User %Qv is not allowed to use HTTP proxies with role %Qv", user, proxyRole)
                << error;
        }

        YT_LOG_INFO(error, "Failed to check if user is allowed to use HTTP proxy (User: %v, Role: %v)",
            user,
            proxyRole);

        return TError();
    }

private:
    TBootstrap const* Bootstrap_;
    const TAccessCheckerConfigPtr Config_;

    const TPermissionCachePtr Cache_;

    TAtomicObject<TString> ProxyRole_;

    std::atomic<bool> Enabled_;

    void OnProxyRoleUpdated(const TString& newRole)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ProxyRole_.Store(newRole);
    }

    void OnDynamicConfigChanged(
        const TProxyDynamicConfigPtr& /*oldConfig*/,
        const TProxyDynamicConfigPtr& newConfig)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Enabled_.store(newConfig->AccessChecker->Enabled.value_or(Config_->Enabled));
    }
};

////////////////////////////////////////////////////////////////////////////////

IAccessCheckerPtr CreateAccessChecker(TBootstrap* bootstrap)
{
    return New<TAccessChecker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
