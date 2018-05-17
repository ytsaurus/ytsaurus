#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

struct ICookieAuthenticator
    : public virtual TRefCounted
{
    virtual TFuture<TAuthenticationResult> Authenticate(
        const TCookieCredentials& credentials) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICookieAuthenticator)

ICookieAuthenticatorPtr CreateCookieAuthenticator(
    TCookieAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackbox);

ICookieAuthenticatorPtr CreateCachingCookieAuthenticator(
    TAsyncExpiringCacheConfigPtr config,
    ICookieAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
