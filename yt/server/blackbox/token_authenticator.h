#pragma once

#include "public.h"
#include "blackbox_service.h"
#include "config.h"

namespace NYT {
namespace NBlackbox {

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

} // namespace NBlackbox
} // namespace NYT
