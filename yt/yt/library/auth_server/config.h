#pragma once

#include "public.h"

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/https/public.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/library/re2/public.h>

#include <yt/yt/library/tvm/service/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct TAuthCacheConfig
    : public virtual NYTree::TYsonStruct
{
    //! Time between last update time and entry update for correct entries.
    TDuration CacheTtl;
    //! Time between last access time and entry eviction.
    TDuration OptimisticCacheTtl;
    //! Time between last update time and entry update for error entries.
    TDuration ErrorTtl;

    REGISTER_YSON_STRUCT(TAuthCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAuthCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBlackboxServiceConfig
    : public virtual NYTree::TYsonStruct
{
    NHttps::TClientConfigPtr HttpClient;
    TString Host;
    int Port;
    bool Secure;
    TString BlackboxServiceId;

    int ConcurrencyLimit;
    TDuration RequestTimeout;
    TDuration AttemptTimeout;
    TDuration BackoffTimeout;
    bool UseLowercaseLogin;

    REGISTER_YSON_STRUCT(TBlackboxServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlackboxServiceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBlackboxTokenAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
    TString Scope;
    bool EnableScopeCheck;
    bool GetUserTicket;

    REGISTER_YSON_STRUCT(TBlackboxTokenAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlackboxTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBlackboxTicketAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
    THashSet<TString> Scopes;
    bool EnableScopeCheck;

    REGISTER_YSON_STRUCT(TBlackboxTicketAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlackboxTicketAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCachingTokenAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
    TAuthCacheConfigPtr Cache;
    EBlackboxCacheKeyMode CacheKeyMode;

    REGISTER_YSON_STRUCT(TCachingTokenAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCachingBlackboxTokenAuthenticatorConfig
    : public TBlackboxTokenAuthenticatorConfig
    , public TCachingTokenAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TCachingBlackboxTokenAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingBlackboxTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressTokenAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
    std::optional<NYPath::TYPath> RootPath;
    TString Realm;

    bool Secure;

    REGISTER_YSON_STRUCT(TCypressTokenAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCachingCypressTokenAuthenticatorConfig
    : public TCachingTokenAuthenticatorConfig
    , public TCypressTokenAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TCachingCypressTokenAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingCypressTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TOAuthAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
    //! Creates a new user if it doesn't exist. User name is taken from the "Login" field.
    bool CreateUserIfNotExists;

    REGISTER_YSON_STRUCT(TOAuthAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOAuthAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TOAuthTokenAuthenticatorConfig
    : public TOAuthAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TOAuthTokenAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TOAuthTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCachingOAuthTokenAuthenticatorConfig
    : public TOAuthTokenAuthenticatorConfig
    , public TCachingTokenAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TCachingOAuthTokenAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingOAuthTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

static const auto DefaultCsrfTokenTtl = TDuration::Days(7);

struct TBlackboxCookieAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
    TString Domain;

    std::optional<TString> CsrfSecret;
    TDuration CsrfTokenTtl;

    bool GetUserTicket;

    REGISTER_YSON_STRUCT(TBlackboxCookieAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlackboxCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TOAuthCookieAuthenticatorConfig
    : public TOAuthAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TOAuthCookieAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TOAuthCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): Replace with polymorphic YSON struct in 24.2+.
struct TStringReplacementConfig
    : public NYTree::TYsonStruct
{
    //! NB: The three transformations options below are mutually exclusive.

    //! If set, replaces all non-overlapping matches of this pattern with the replacement string.
    NRe2::TRe2Ptr MatchPattern;
    TString Replacement;

    //! If set, uppercase characters are replaced with lowercase.
    bool ToLower;

    //! If true, lowercase characters are replaced with uppercase.
    bool ToUpper;

    REGISTER_YSON_STRUCT(TStringReplacementConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStringReplacementConfig)

////////////////////////////////////////////////////////////////////////////////

struct TOAuthServiceConfig
    : public virtual NYTree::TYsonStruct
{
    NHttp::TRetryingClientConfigPtr RetryingClient;
    NHttps::TClientConfigPtr HttpClient;

    TString Host;
    int Port;
    bool Secure;

    TString AuthorizationHeaderPrefix;
    TString UserInfoEndpoint;
    TString UserInfoLoginField;
    std::optional<TString> UserInfoSubjectField;
    std::optional<TString> UserInfoErrorField;

    //! Configures a list of transformations to be applied to the contents of the login field.
    //! Transformations are applied consecutively in order they are listed.
    //! Each transformation will replace all non-overlapping matches of its match pattern
    //! with the replacement string. You can use \1-\9 for captured match groups in the
    //! replacement string, and \0 for the whole match.
    //! Regex must follow RE2 syntax, which is a subset of that accepted by PCRE, roughly
    //! speaking, and with various caveats.
    std::vector<TStringReplacementConfigPtr> LoginTransformations;

    REGISTER_YSON_STRUCT(TOAuthServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOAuthServiceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressUserManagerConfig
    : public virtual NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TCypressUserManagerConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressUserManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCachingCookieAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
    TAuthCacheConfigPtr Cache;
    EBlackboxCacheKeyMode CacheKeyMode;

    REGISTER_YSON_STRUCT(TCachingCookieAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCachingCypressUserManagerConfig
    : public TCypressUserManagerConfig
{
    TAuthCacheConfigPtr Cache;

    REGISTER_YSON_STRUCT(TCachingCypressUserManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingCypressUserManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCachingBlackboxCookieAuthenticatorConfig
    : public TBlackboxCookieAuthenticatorConfig
    , public TCachingCookieAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TCachingBlackboxCookieAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingBlackboxCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCachingOAuthCookieAuthenticatorConfig
    : public TOAuthCookieAuthenticatorConfig
    , public TCachingCookieAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TCachingOAuthCookieAuthenticatorConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingOAuthCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDefaultSecretVaultServiceConfig
    : public virtual NYT::NYTree::TYsonStruct
{
    TString Host;
    int Port;
    bool Secure;
    NHttps::TClientConfigPtr HttpClient;
    TDuration RequestTimeout;
    TString VaultServiceId;
    TString Consumer;
    bool EnableRevocation;
    std::optional<TTvmId> DefaultTvmIdForNewTokens;
    std::optional<TTvmId> DefaultTvmIdForExistingTokens;

    REGISTER_YSON_STRUCT(TDefaultSecretVaultServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDefaultSecretVaultServiceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBatchingSecretVaultServiceConfig
    : public virtual NYT::NYTree::TYsonStruct
{
    TDuration BatchDelay;
    int MaxSubrequestsPerRequest;
    NConcurrency::TThroughputThrottlerConfigPtr RequestsThrottler;

    REGISTER_YSON_STRUCT(TBatchingSecretVaultServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBatchingSecretVaultServiceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCachingSecretVaultServiceConfig
    : public TAsyncExpiringCacheConfig
{
    TAsyncExpiringCacheConfigPtr Cache;

    REGISTER_YSON_STRUCT(TCachingSecretVaultServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingSecretVaultServiceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressCookieStoreConfig
    : public NYTree::TYsonStruct
{
    //! Store will renew cookie list with this frequency.
    TDuration FullFetchPeriod;

    //! Errors are cached for this period of time.
    TDuration ErrorEvictionTime;

    REGISTER_YSON_STRUCT(TCypressCookieStoreConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressCookieStoreConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressCookieGeneratorConfig
    : public NYTree::TYsonStruct
{
    //! Used to form ExpiresAt parameter.
    TDuration CookieExpirationTimeout;

    //! If cookie will expire within this period,
    //! authenticator will try to renew it.
    TDuration CookieRenewalPeriod;

    //! Controls Secure parameter of a cookie.
    //! If true, cookie will be used by user only
    //! in https requests which prevents cookie
    //! stealing because of unsecured connection,
    //! so this field should be set to true in production
    //! environments.
    bool Secure;

    //! Controls HttpOnly parameter of a cookie.
    bool HttpOnly;

    //! Domain parameter of generated cookies.
    std::optional<TString> Domain;

    //! Path parameter of generated cookies.
    TString Path;

    //! If set and if cookie is generated via login page,
    //! will redirect user to this page.
    std::optional<TString> RedirectUrl;

    REGISTER_YSON_STRUCT(TCypressCookieGeneratorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressCookieGeneratorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressCookieManagerConfig
    : public NYTree::TYsonStruct
{
    TCypressCookieStoreConfigPtr CookieStore;
    TCypressCookieGeneratorConfigPtr CookieGenerator;
    TCachingBlackboxCookieAuthenticatorConfigPtr CookieAuthenticator;

    REGISTER_YSON_STRUCT(TCypressCookieManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressCookieManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TYCIAMTokenAuthenticatorConfig
    : public NYTree::TYsonStruct
{
    NHttp::TRetryingClientConfigPtr RetryingClient;
    NHttps::TClientConfigPtr HttpClient;

    TString Host;
    int Port;
    bool Secure;

    bool CheckUserExists;
    bool CreateUserIfNotExists;

    bool RetryAllServerErrors;
    std::vector<int> RetryStatusCodes;

    std::string AuthenticateLoginField;

    REGISTER_YSON_STRUCT(TYCIAMTokenAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYCIAMTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TAuthenticationManagerConfig
    : public virtual NYT::NYTree::TYsonStruct
{
    bool RequireAuthentication;
    TCachingBlackboxTokenAuthenticatorConfigPtr BlackboxTokenAuthenticator;
    TCachingBlackboxCookieAuthenticatorConfigPtr BlackboxCookieAuthenticator;
    TBlackboxServiceConfigPtr BlackboxService;
    TCachingCypressTokenAuthenticatorConfigPtr CypressTokenAuthenticator;
    TTvmServiceConfigPtr TvmService;
    TBlackboxTicketAuthenticatorConfigPtr BlackboxTicketAuthenticator;
    TCachingOAuthCookieAuthenticatorConfigPtr OAuthCookieAuthenticator;
    TCachingOAuthTokenAuthenticatorConfigPtr OAuthTokenAuthenticator;
    TOAuthServiceConfigPtr OAuthService;
    TYCIAMTokenAuthenticatorConfigPtr YCIAMTokenAuthenticator;

    TCypressCookieManagerConfigPtr CypressCookieManager;
    TCachingCypressUserManagerConfigPtr CypressUserManager;

    TString GetCsrfSecret() const;

    TInstant GetCsrfTokenExpirationTime() const;

    REGISTER_YSON_STRUCT(TAuthenticationManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAuthenticationManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
