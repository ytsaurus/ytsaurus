#include "security_manager_base.h"

namespace NYT {
namespace NSecurityServer {

///////////////////////////////////////////////////////////////////////////////

TAuthenticatedUserGuardBase::TAuthenticatedUserGuardBase(
    ISecurityManagerBasePtr securityManager,
    const TNullable<TString>& user)
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

} // namespace NSecurityServer
} // namespace NYT
