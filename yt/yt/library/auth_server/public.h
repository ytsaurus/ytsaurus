#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/library/tvm/service/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TAuthCacheConfig)
DECLARE_REFCOUNTED_CLASS(TBlackboxServiceConfig)
DECLARE_REFCOUNTED_CLASS(TBlackboxTicketAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TBlackboxTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingBlackboxTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCypressTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingCypressTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TBlackboxCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingBlackboxCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TDefaultSecretVaultServiceConfig)
DECLARE_REFCOUNTED_CLASS(TBatchingSecretVaultServiceConfig)
DECLARE_REFCOUNTED_CLASS(TCachingSecretVaultServiceConfig)
DECLARE_REFCOUNTED_CLASS(TAuthenticationManagerConfig)

DECLARE_REFCOUNTED_CLASS(TOAuthAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TOAuthCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TOAuthTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingOAuthCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingOAuthTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TStringReplacementConfig)
DECLARE_REFCOUNTED_CLASS(TOAuthServiceConfig)
DECLARE_REFCOUNTED_CLASS(TCypressUserManagerConfig)
DECLARE_REFCOUNTED_CLASS(TCachingCypressUserManagerConfig)

DECLARE_REFCOUNTED_STRUCT(TCypressCookie)

DECLARE_REFCOUNTED_STRUCT(TCypressCookieStoreConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressCookieGeneratorConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressCookieManagerConfig)

DECLARE_REFCOUNTED_STRUCT(ICypressCookieStore)
DECLARE_REFCOUNTED_STRUCT(ICypressCookieManager)
DECLARE_REFCOUNTED_STRUCT(ICypressUserManager)

DECLARE_REFCOUNTED_STRUCT(IAuthenticationManager)

DECLARE_REFCOUNTED_STRUCT(IBlackboxService)
DECLARE_REFCOUNTED_STRUCT(IOAuthService)

DECLARE_REFCOUNTED_STRUCT(ICookieAuthenticator)
DECLARE_REFCOUNTED_STRUCT(ITokenAuthenticator)
DECLARE_REFCOUNTED_STRUCT(ITicketAuthenticator)

DECLARE_REFCOUNTED_STRUCT(ISecretVaultService)

////////////////////////////////////////////////////////////////////////////////

// See https://doc.yandex-team.ru/blackbox/reference/method-sessionid-response-json.xml for reference.
DEFINE_ENUM_WITH_UNDERLYING_TYPE(EBlackboxStatus, i64,
    ((Valid)    (0))
    ((NeedReset)(1))
    ((Expired)  (2))
    ((NoAuth)   (3))
    ((Disabled) (4))
    ((Invalid)  (5))
);

// See https://doc.yandex-team.ru/blackbox/concepts/blackboxErrors.xml
DEFINE_ENUM_WITH_UNDERLYING_TYPE(EBlackboxException, i64,
    ((Ok)                (0))
    ((Unknown)           (1))
    ((InvalidParameters) (2))
    ((DBFetchFailed)     (9))
    ((DBException)      (10))
    ((AccessDenied)     (21))
);

DEFINE_ENUM(ESecretVaultErrorCode,
    ((UnknownError)           (18000))
    ((MalformedResponse)      (18001))
    ((NonexistentEntityError) (18002))
    ((DelegationAccessError)  (18003))
    ((DelegationTokenRevoked) (18004))
    ((UnexpectedStatus)       (18005))
);

DEFINE_ENUM(EBlackboxCacheKeyMode,
    (Credentials)
    (CredentialsAndUserAddressProjectId)
    (CredentialsAndUserAddress)
);

////////////////////////////////////////////////////////////////////////////////

struct TTokenCredentials;
struct TCookieCredentials;
struct TTicketCredentials;
struct TServiceTicketCredentials;

struct TAuthenticationResult;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf BlackboxSessionIdCookieName = "Session_id";
constexpr TStringBuf BlackboxSslSessionIdCookieName = "sessionid2";
constexpr TStringBuf CypressCookieName = "YTCypressCookie";
constexpr TStringBuf OAuthAccessTokenCookieName = "access_token";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
