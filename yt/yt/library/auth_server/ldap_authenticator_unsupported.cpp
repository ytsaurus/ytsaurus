#include "ldap_authenticator.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

ILoginAuthenticatorPtr CreateLdapLoginAuthenticator(
    TLdapServiceConfigPtr /*config*/,
    IInvokerPtr /*invoker*/)
{
    THROW_ERROR_EXCEPTION("LDAP authentication is not supported in this build");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
