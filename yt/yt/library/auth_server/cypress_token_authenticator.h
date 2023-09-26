#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

ITokenAuthenticatorPtr CreateCypressTokenAuthenticator(
    TCypressTokenAuthenticatorConfigPtr config,
    NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
