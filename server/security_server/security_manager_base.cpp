#include "security_manager_base.h"

namespace NYT::NSecurityServer {

///////////////////////////////////////////////////////////////////////////////

TAuthenticatedUserGuardBase::TAuthenticatedUserGuardBase(
    ISecurityManagerPtr securityManager,
    const std::optional<TString>& user)
{
    if (user) {
        securityManager->SetAuthenticatedUserByNameOrThrow(*user);
        SecurityManager_ = std::move(securityManager);
    }
}

TAuthenticatedUserGuardBase::~TAuthenticatedUserGuardBase()
{
    if (SecurityManager_) {
        SecurityManager_->ResetAuthenticatedUser();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
