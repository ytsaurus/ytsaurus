#pragma once

#include <yt/core/misc/ref_counted.h>

#include <util/generic/stroka.h>

namespace NYT {
namespace NBlackbox {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDefaultBlackboxServiceConfig)
DECLARE_REFCOUNTED_CLASS(TTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCookieAuthenticatorConfig)

DECLARE_REFCOUNTED_STRUCT(IBlackboxService)

struct TTokenCredentials
{
    Stroka Token;
    Stroka UserIp;
};

struct TAuthenticationResult
{
    Stroka Login;
    Stroka Realm;
};

DECLARE_REFCOUNTED_STRUCT(ICookieAuthenticator)
DECLARE_REFCOUNTED_STRUCT(ITokenAuthenticator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT
