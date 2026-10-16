#pragma once

#include "login_authenticator.h"
#include "public.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

ILoginAuthenticatorPtr CreateLdapLoginAuthenticator(
    TLdapServiceConfigPtr config,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
