#pragma once

#include "public.h"

#include <yt/yt/ytlib/auth/public.h>

#include <yt/yt/ytlib/api/public.h>

#include <yt/yt/core/http/http.h>

#include <yt/yt/core/rpc/authenticator.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

struct TAuthenticationResultAndToken
{
    NAuth::TAuthenticationResult Result;
    TString TokenHash;
};

void SetStatusFromAuthError(const NHttp::IResponseWriterPtr& req, const TError& error);

////////////////////////////////////////////////////////////////////////////////

class THttpAuthenticator
    : public NHttp::IHttpHandler
{
public:
    THttpAuthenticator(
        NAuth::TAuthenticationManagerConfigPtr config,
        NAuth::ITokenAuthenticatorPtr tokenAuthenticator,
        NAuth::ICookieAuthenticatorPtr cookieAuthenticator,
        TCoordinatorPtr coordinator);

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

    TErrorOr<TAuthenticationResultAndToken> Authenticate(
        const NHttp::IRequestPtr& request,
        bool disableCsrfTokenCheck = false);

private:
    const NAuth::TAuthenticationManagerConfigPtr Config_;
    const NAuth::ITokenAuthenticatorPtr TokenAuthenticator_;
    const NAuth::ICookieAuthenticatorPtr CookieAuthenticator_;
    const TCoordinatorPtr Coordinator_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
