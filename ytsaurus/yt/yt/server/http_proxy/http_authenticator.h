#pragma once

#include "public.h"

#include <yt/yt/library/auth_server/public.h>

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
        TBootstrap* bootstrap,
        const NAuth::TAuthenticationManagerConfigPtr& authManagerConfig,
        const NAuth::IAuthenticationManagerPtr& authManager);

    void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

    TErrorOr<TAuthenticationResultAndToken> Authenticate(
        const NHttp::IRequestPtr& request,
        bool disableCsrfTokenCheck = false);

    const NAuth::ITokenAuthenticatorPtr& GetTokenAuthenticator() const;

private:
    TBootstrap* Bootstrap_;

    const NAuth::TAuthenticationManagerConfigPtr Config_;
    const NAuth::IAuthenticationManagerPtr AuthenticationManager_;
    const NAuth::ITokenAuthenticatorPtr TokenAuthenticator_;
    const NAuth::ICookieAuthenticatorPtr CookieAuthenticator_;
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeHttpAuthenticator
    : public NHttp::IHttpHandler
{
public:
    explicit TCompositeHttpAuthenticator(const THashMap<int, THttpAuthenticatorPtr>& portAuthenticators);

    void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

    TErrorOr<TAuthenticationResultAndToken> Authenticate(
        const NHttp::IRequestPtr& request,
        bool disableCsrfTokenCheck = false);

    const NAuth::ITokenAuthenticatorPtr& GetTokenAuthenticatorOrThrow(int port) const;

private:
    THashMap<int, THttpAuthenticatorPtr> PortAuthenticators_;

    TErrorOr<THttpAuthenticatorPtr> GetPortAuthenticator(int port) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
