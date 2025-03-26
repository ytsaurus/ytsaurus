#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/library/tvm/service/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TAuthCacheConfig)
DECLARE_REFCOUNTED_STRUCT(TBlackboxServiceConfig)
DECLARE_REFCOUNTED_STRUCT(TBlackboxTicketAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TBlackboxTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TCachingBlackboxTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TCachingTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TCachingCypressTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TBlackboxCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TCachingCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TCachingBlackboxCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TDefaultSecretVaultServiceConfig)
DECLARE_REFCOUNTED_STRUCT(TBatchingSecretVaultServiceConfig)
DECLARE_REFCOUNTED_STRUCT(TCachingSecretVaultServiceConfig)
DECLARE_REFCOUNTED_STRUCT(TAuthenticationManagerConfig)

DECLARE_REFCOUNTED_STRUCT(TOAuthAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TOAuthCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TOAuthTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TCachingOAuthCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TCachingOAuthTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TYCIAMTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_STRUCT(TStringReplacementConfig)
DECLARE_REFCOUNTED_STRUCT(TOAuthServiceConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressUserManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TCachingCypressUserManagerConfig)

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

YT_DEFINE_ERROR_ENUM(
    // User error.
    ((InvalidUserCredentials)     (30000))
    // YT communication error with YC IAM service.
    ((YCIAMProtocolError)           (30001))
    // Server error.
    ((YCIAMRetryableServerError)    (30002))
    // Unexpected errors.
    ((UnexpectedClientYCIAMError)   (30003))
    ((UnexpectedServerYCIAMError)   (30004))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
