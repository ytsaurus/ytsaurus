#pragma once

#include "public.h"

#include <yt/ytlib/auth/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/http/http.h>

#include <yt/core/rpc/authenticator.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class THttpAuthenticator
    : public NHttp::IHttpHandler
{
public:
    THttpAuthenticator(
        NAuth::TAuthenticationManagerConfigPtr config,
        NAuth::ITokenAuthenticatorPtr tokenAuthenticator,
        NAuth::ICookieAuthenticatorPtr cookieAuthenticator);

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

    TErrorOr<NAuth::TAuthenticationResult> Authenticate(
        const NHttp::IRequestPtr& request,
        bool disableCsrfTokenCheck = false);

private:
    const NAuth::TAuthenticationManagerConfigPtr Config_;

    NAuth::ITokenAuthenticatorPtr TokenAuthenticator_;
    NAuth::ICookieAuthenticatorPtr CookieAuthenticator_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
