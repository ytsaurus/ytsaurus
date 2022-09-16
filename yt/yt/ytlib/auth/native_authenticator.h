#pragma once

#include "public.h"

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

//! Creates an authenticator for native connections between clusters. It requires a TVM ticket from one of the allowed
//! sources as specified by sourceValidator. If the authentification is successful, then the request is fully trusted,
//! and user login is just taken from the headers without further validation.
//!
//! If authenticationManager->IsValidationEnabled() is false, requests just pass without any authentification.
NRpc::IAuthenticatorPtr CreateNativeAuthenticator(std::function<bool(TTvmId)> sourceValidator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
