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
        const Stroka& sessionId,
        const Stroka& sslSessionId,
        const Stroka& host,
        const Stroka& userIP) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICookieAuthenticator)

ICookieAuthenticatorPtr CreateCookieAuthenticator(
    TCookieAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackbox);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT
