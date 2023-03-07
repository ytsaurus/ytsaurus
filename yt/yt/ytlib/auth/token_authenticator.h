#pragma once

#include "public.h"

#include <yt/client/api/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/profiling/profiler.h>

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
    NProfiling::TProfiler profiler = {});

ITokenAuthenticatorPtr CreateCypressTokenAuthenticator(
    TCypressTokenAuthenticatorConfigPtr config,
    NApi::IClientPtr client);

ITokenAuthenticatorPtr CreateCachingTokenAuthenticator(
    TCachingTokenAuthenticatorConfigPtr config,
    ITokenAuthenticatorPtr authenticator,
    NProfiling::TProfiler profiler = {});

ITokenAuthenticatorPtr CreateCompositeTokenAuthenticator(
    std::vector<ITokenAuthenticatorPtr> authenticators);

ITokenAuthenticatorPtr CreateNoopTokenAuthenticator();

NRpc::IAuthenticatorPtr CreateTokenAuthenticatorWrapper(
    ITokenAuthenticatorPtr underlying);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
