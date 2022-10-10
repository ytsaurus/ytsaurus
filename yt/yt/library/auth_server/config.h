#pragma once

#include "public.h"

#include <yt/yt/core/https/public.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

class TAuthCacheConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TDuration CacheTtl;
    TDuration OptimisticCacheTtl;
    TDuration ErrorTtl;

    REGISTER_YSON_STRUCT(TAuthCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAuthCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlackboxServiceConfig
    : public virtual NYTree::TYsonStruct
{
public:
    NHttps::TClientConfigPtr HttpClient;
    TString Host;
    int Port;
    bool Secure;
    TString BlackboxServiceId;

    TDuration RequestTimeout;
    TDuration AttemptTimeout;
    TDuration BackoffTimeout;
    bool UseLowercaseLogin;

    REGISTER_YSON_STRUCT(TBlackboxServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlackboxServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TTvmServiceConfig
    : public virtual NYTree::TYsonStruct
{
public:
    bool UseTvmTool;

    // TvmClient settings
    TTvmId ClientSelfId = 0;
    TString ClientDiskCacheDir;

    TString TvmHost;
    ui16 TvmPort = 0;

    bool ClientEnableUserTicketChecking = false;
    TString ClientBlackboxEnv;

    bool ClientEnableServiceTicketFetching = false;
    TString ClientSelfSecret;
    THashMap<TString, ui32> ClientDstMap;

    bool ClientEnableServiceTicketChecking = false;

    TString TvmToolSelfAlias;
    int TvmToolPort = 0;
    TString TvmToolAuthToken;

    //! For testing only. If enabled, then a mock instead of a real TVM service will be used.
    bool EnableMock = false;

    REGISTER_YSON_STRUCT(TTvmServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTvmServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTokenAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TString Scope;
    bool EnableScopeCheck;
    bool GetUserTicket;

    REGISTER_YSON_STRUCT(TBlackboxTokenAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlackboxTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTicketAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    THashSet<TString> Scopes;
    bool EnableScopeCheck;

    REGISTER_YSON_STRUCT(TBlackboxTicketAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlackboxTicketAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingTokenAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TAuthCacheConfigPtr Cache;

    REGISTER_YSON_STRUCT(TCachingTokenAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingBlackboxTokenAuthenticatorConfig
    : public TBlackboxTokenAuthenticatorConfig
    , public TCachingTokenAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TCachingBlackboxTokenAuthenticatorConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TCachingBlackboxTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCypressTokenAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    NYPath::TYPath RootPath;
    TString Realm;

    bool Secure;

    REGISTER_YSON_STRUCT(TCypressTokenAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingCypressTokenAuthenticatorConfig
    : public TCachingTokenAuthenticatorConfig
    , public TCypressTokenAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TCachingCypressTokenAuthenticatorConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TCachingCypressTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

static const auto DefaultCsrfTokenTtl = TDuration::Days(7);

class TBlackboxCookieAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TString Domain;

    std::optional<TString> CsrfSecret;
    TDuration CsrfTokenTtl;

    bool GetUserTicket;

    REGISTER_YSON_STRUCT(TBlackboxCookieAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlackboxCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingCookieAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TAuthCacheConfigPtr Cache;

    REGISTER_YSON_STRUCT(TCachingCookieAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingBlackboxCookieAuthenticatorConfig
    : public TBlackboxCookieAuthenticatorConfig
    , public TCachingCookieAuthenticatorConfig
{
    REGISTER_YSON_STRUCT(TCachingBlackboxCookieAuthenticatorConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TCachingBlackboxCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TDefaultSecretVaultServiceConfig
    : public virtual NYT::NYTree::TYsonStruct
{
public:
    TString Host;
    int Port;
    bool Secure;
    NHttps::TClientConfigPtr HttpClient;
    TDuration RequestTimeout;
    TString VaultServiceId;
    TString Consumer;

    REGISTER_YSON_STRUCT(TDefaultSecretVaultServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDefaultSecretVaultServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TBatchingSecretVaultServiceConfig
    : public virtual NYT::NYTree::TYsonStruct
{
public:
    TDuration BatchDelay;
    int MaxSubrequestsPerRequest;
    NConcurrency::TThroughputThrottlerConfigPtr RequestsThrottler;

    REGISTER_YSON_STRUCT(TBatchingSecretVaultServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBatchingSecretVaultServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingSecretVaultServiceConfig
    : public TAsyncExpiringCacheConfig
{
public:
    TAsyncExpiringCacheConfigPtr Cache;

    REGISTER_YSON_STRUCT(TCachingSecretVaultServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingSecretVaultServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TAuthenticationManagerConfig
    : public virtual NYT::NYTree::TYsonStruct
{
public:
    bool RequireAuthentication;
    NAuth::TCachingBlackboxTokenAuthenticatorConfigPtr BlackboxTokenAuthenticator;
    NAuth::TCachingBlackboxCookieAuthenticatorConfigPtr BlackboxCookieAuthenticator;
    NAuth::TBlackboxServiceConfigPtr BlackboxService;
    NAuth::TCachingCypressTokenAuthenticatorConfigPtr CypressTokenAuthenticator;
    NAuth::TTvmServiceConfigPtr TvmService;
    NAuth::TBlackboxTicketAuthenticatorConfigPtr BlackboxTicketAuthenticator;

    TString GetCsrfSecret() const;

    TInstant GetCsrfTokenExpirationTime() const;

    REGISTER_YSON_STRUCT(TAuthenticationManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAuthenticationManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
