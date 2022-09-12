#pragma once

#include "public.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

//! Creates an authenticator for native connections between clusters. It requires a TVM ticket from one of the allowed
//! sources as specified by sourceValidator. If the authentification is successful, then the request is fully trusted,
//! and user login is just taken from the headers without further validation.
NRpc::IAuthenticatorPtr CreateNativeAuthenticator(
    ITvmServicePtr tvmService,
    std::function<bool(TTvmId)> sourceValidator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
