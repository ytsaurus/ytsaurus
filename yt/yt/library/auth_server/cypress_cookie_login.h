#pragma once

#include "login_authenticator.h"
#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/http/http.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

//! Creates an HTTP handler for the /login endpoint.
//!
//! The handler parses Basic auth credentials and tries each authenticator
//! in order. On first success, a Cypress cookie is issued.
//!
//! The caller is responsible for building the authenticator list.
//! Use CreateCypressLoginAuthenticator to include Cypress password auth.
NHttp::IHttpHandlerPtr CreateCypressCookieLoginHandler(
    TCypressCookieGeneratorConfigPtr config,
    NApi::IClientPtr client,
    ICypressCookieStorePtr cookieStore,
    std::vector<ILoginAuthenticatorPtr> authenticators);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
