#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct TLoginResult
{
    //! Authenticated (possibly normalized) login.
    std::string Login;
    //! Which backend performed the authentication.
    EAuthSource Source;
};

struct TLoginCredentials
{
    std::string User;
    std::string Password;
};

////////////////////////////////////////////////////////////////////////////////

//! Verifies username + password credentials.
//! Returns TLoginResult on success, throws on failure.
struct ILoginAuthenticator
    : public virtual TRefCounted
{
    virtual TFuture<TLoginResult> Authenticate(const TLoginCredentials& credentials) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILoginAuthenticator)

////////////////////////////////////////////////////////////////////////////////

//! Tries each authenticator in order; returns on first success.
//! Collects errors from all failed attempts and throws a combined error
//! if all authenticators fail.
ILoginAuthenticatorPtr CreateCompositeLoginAuthenticator(
    std::vector<ILoginAuthenticatorPtr> authenticators);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
