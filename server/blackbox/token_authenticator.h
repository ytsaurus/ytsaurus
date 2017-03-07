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
        const Stroka& token,
        const Stroka& userIP) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITokenAuthenticator);

ITokenAuthenticatorPtr CreateTokenAuthenticator(
    TTokenAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackbox);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT
