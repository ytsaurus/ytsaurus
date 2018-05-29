#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

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

////////////////////////////////////////////////////////////////////////////////

ITokenAuthenticatorPtr CreateBlackboxTokenAuthenticator(
    TBlackboxTokenAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackbox);

ITokenAuthenticatorPtr CreateCypressTokenAuthenticator(
    TCypressTokenAuthenticatorConfigPtr config,
    NApi::IClientPtr client);

ITokenAuthenticatorPtr CreateCachingTokenAuthenticator(
    TAsyncExpiringCacheConfigPtr config,
    ITokenAuthenticatorPtr authenticator);

ITokenAuthenticatorPtr CreateCompositeTokenAuthenticator(
    std::vector<ITokenAuthenticatorPtr> authenticators);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
