#include "access_checker.h"

#include "private.h"
#include "bootstrap.h"
#include "config.h"
#include "proxy_coordinator.h"

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NRpcProxy {

using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NYTree;

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
            RpcProxyProfiler.WithPrefix("/access_checker_cache")))
        , Enabled_(Config_->Enabled)
    {
        Bootstrap_->GetProxyCoordinator()->SubscribeOnDynamicConfigChanged(BIND(&TAccessChecker::OnDynamicConfigChanged, MakeWeak(this)));
    }

    virtual TError ValidateAccess(const TString& user) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!Enabled_.load()) {
            return TError();
        }

        auto proxyRole = Bootstrap_->GetProxyCoordinator()->GetProxyRole();
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

        if (Config_->AllowAccessIfNodeDoesNotExist && error.FindMatching(NYTree::EErrorCode::ResolveError)) {
            return TError();
        }

        return TError("User %Qv is not allowed to use RPC proxies with role %Qv", user, proxyRole)
            << error;
    }

private:
    TBootstrap const* Bootstrap_;
    const TAccessCheckerConfigPtr Config_;

    const TPermissionCachePtr Cache_;

    std::atomic<bool> Enabled_;

    void OnDynamicConfigChanged(const TDynamicProxyConfigPtr& newConfig)
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

} // namespace NYT::NRpcProxy
