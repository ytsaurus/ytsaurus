#include "access_checker.h"

#include "bootstrap.h"
#include "config.h"
#include "coordinator.h"

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NHttpProxy {

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
            HttpProxyProfiler.WithPrefix("/access_checker_cache")))
        , ProxyRole_(Bootstrap_->GetCoordinator()->GetSelf()->Role)
    {
        const auto& coordinator = Bootstrap_->GetCoordinator();
        coordinator->SubscribeOnSelfRoleChanged(BIND(&TAccessChecker::OnProxyRoleUpdated, MakeWeak(this)));
    }

    virtual TError ValidateAccess(const TString& user) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!Config_->Enabled) {
            return TError();
        }

        auto proxyRole = Bootstrap_->GetCoordinator()->GetSelf()->Role;
        auto path = Format("%v/%v", Config_->PathPrefix, proxyRole);
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

        return TError("User %Qv is not allowed to use HTTP proxies with role %Qv", user, proxyRole)
            << error;
    }

private:
    TBootstrap const* Bootstrap_;
    const TAccessCheckerConfigPtr Config_;

    const TPermissionCachePtr Cache_;

    TAtomicObject<TString> ProxyRole_;

    void OnProxyRoleUpdated(TString newRole)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ProxyRole_.Store(newRole);
    }
};

////////////////////////////////////////////////////////////////////////////////

IAccessCheckerPtr CreateAccessChecker(TBootstrap* bootstrap)
{
    return New<TAccessChecker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
