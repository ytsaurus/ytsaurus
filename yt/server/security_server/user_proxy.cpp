#include "stdafx.h"
#include "user_proxy.h"
#include "user.h"
#include "security_manager.h"
#include "subject_proxy_detail.h"

#include <ytlib/security_client/user_ypath.pb.h>

namespace NYT {
namespace NSecurityServer {

using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TUserProxy
    : public TSubjectProxy<TUser>
{
public:
    TUserProxy(NCellMaster::TBootstrap* bootstrap, TUser* user)
        : TBase(bootstrap, user)
    { }

private:
    typedef TSubjectProxy<TUser> TBase;

    virtual void ValidateRemoval() override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        if (GetThisTypedImpl() == securityManager->GetRootUser() ||
            GetThisTypedImpl() == securityManager->GetGuestUser())
        {
            THROW_ERROR_EXCEPTION("Cannot remove a built-in user");
        }
    }

};

IObjectProxyPtr CreateUserProxy(
    NCellMaster::TBootstrap* bootstrap,
    TUser* user)
{
    return New<TUserProxy>(bootstrap, user);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

