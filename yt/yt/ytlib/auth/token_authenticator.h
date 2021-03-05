#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/profiling/profiler.h>

namespace NYT::NAuth {

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
    IBlackboxServicePtr blackboxService,
    NProfiling::TRegistry profiler = {});

ITokenAuthenticatorPtr CreateCypressTokenAuthenticator(
    TCypressTokenAuthenticatorConfigPtr config,
    NApi::IClientPtr client);

ITokenAuthenticatorPtr CreateCachingTokenAuthenticator(
    TCachingTokenAuthenticatorConfigPtr config,
    ITokenAuthenticatorPtr authenticator,
    NProfiling::TRegistry profiler = {});

ITokenAuthenticatorPtr CreateCompositeTokenAuthenticator(
    std::vector<ITokenAuthenticatorPtr> authenticators);

ITokenAuthenticatorPtr CreateNoopTokenAuthenticator();

NRpc::IAuthenticatorPtr CreateTokenAuthenticatorWrapper(
    ITokenAuthenticatorPtr underlying);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
