#pragma once

#include "login_authenticator.h"

#include <yt/yt/client/api/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

//! Creates a login authenticator that verifies credentials against
//! the Cypress user store (hashed password + salt).
ILoginAuthenticatorPtr CreateCypressLoginAuthenticator(NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
