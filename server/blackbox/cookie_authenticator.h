#pragma once

#include "public.h"
#include "blackbox_service.h"
#include "config.h"

namespace NYT {
namespace NBlackbox {

////////////////////////////////////////////////////////////////////////////////

struct ICookieAuthenticator
    : public virtual TRefCounted
{
    virtual TFuture<TAuthenticationResult> Authenticate(
        const TCookieCredentials& credentials) = 0;

    TFuture<TAuthenticationResult> Authenticate(
        const TString& sessionId,
        const TString& sslSessionId,
        const TString& host,
        const TString& userIP)
    {
        TCookieCredentials credentials;
        credentials.SessionId = sessionId;
        credentials.SslSessionId = sslSessionId;
        credentials.Host = host;
        credentials.UserIP = userIP;
        return Authenticate(credentials);
    }
};

DEFINE_REFCOUNTED_TYPE(ICookieAuthenticator)

ICookieAuthenticatorPtr CreateCookieAuthenticator(
    TCookieAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackbox);

ICookieAuthenticatorPtr CreateCachingCookieAuthenticator(
    TExpiringCacheConfigPtr config,
    ICookieAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT
