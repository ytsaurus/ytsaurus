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
        const TString& sessionId,
        const TString& sslSessionId,
        const TString& host,
        const TString& userIP) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICookieAuthenticator)

ICookieAuthenticatorPtr CreateCookieAuthenticator(
    TCookieAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackbox);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT
