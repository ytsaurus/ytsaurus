#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/rpc/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

TString SignCsrfToken(const TString& userId, const TString& key, TInstant now);
TError CheckCsrfToken(const TString& csrfToken, const TString& userId, const TString& key, TInstant expirationTime);

////////////////////////////////////////////////////////////////////////////////

struct ICookieAuthenticator
    : public virtual TRefCounted
{
    virtual TFuture<TAuthenticationResult> Authenticate(
        const TCookieCredentials& credentials) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICookieAuthenticator)

////////////////////////////////////////////////////////////////////////////////

ICookieAuthenticatorPtr CreateBlackboxCookieAuthenticator(
    TBlackboxCookieAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackboxService);

ICookieAuthenticatorPtr CreateCachingCookieAuthenticator(
    TCachingCookieAuthenticatorConfigPtr config,
    ICookieAuthenticatorPtr authenticator,
    NProfiling::TProfiler profiler = {});

NRpc::IAuthenticatorPtr CreateCookieAuthenticatorWrapper(
    ICookieAuthenticatorPtr underlying);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
