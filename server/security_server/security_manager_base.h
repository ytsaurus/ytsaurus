#pragma once

#include "public.h"

#include <yt/core/misc/optional.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! A simple RAII guard for setting the authenticated user.
/*!
 *  \see #TSecurityManagerBase::SetAuthenticatedUserByName
 *  \see #TSecurityManagerBase::ResetAuthenticatedUser
 */
class TAuthenticatedUserGuardBase
    : private TNonCopyable
{
public:
    TAuthenticatedUserGuardBase(ISecurityManagerPtr securityManager, const std::optional<TString>& userName);
    ~TAuthenticatedUserGuardBase();

protected:
    ISecurityManagerPtr SecurityManager_;
};

////////////////////////////////////////////////////////////////////////////////

struct ISecurityManager
    : public virtual TRefCounted
{
    //! Sets the authenticated user by user name.
    virtual void SetAuthenticatedUserByNameOrThrow(const TString& userName) = 0;

    //! Resets the authenticated user.
    virtual void ResetAuthenticatedUser() = 0;

    //! Returns the current user or null if there's no one.
    virtual std::optional<TString> GetAuthenticatedUserName() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISecurityManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
