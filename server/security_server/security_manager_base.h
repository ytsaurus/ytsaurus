#pragma once

#include "public.h"

#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NSecurityServer {

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
    TAuthenticatedUserGuardBase(ISecurityManagerPtr securityManager, const TNullable<TString>& userName);
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

    //! Returns the current user or Null if there's no one.
    virtual TNullable<TString> GetAuthenticatedUserName() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISecurityManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
