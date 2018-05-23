#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

struct ITokenAuthenticator
    : public virtual TRefCounted
{
    virtual TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITokenAuthenticator)

ITokenAuthenticatorPtr CreateBlackboxTokenAuthenticator(
    TTokenAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackbox);

ITokenAuthenticatorPtr CreateCachingTokenAuthenticator(
    TAsyncExpiringCacheConfigPtr config,
    ITokenAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
