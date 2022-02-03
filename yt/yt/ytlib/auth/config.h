#pragma once

#include "public.h"

#include <yt/yt/core/https/public.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

class TAuthCacheConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDuration CacheTtl;
    TDuration OptimisticCacheTtl;
    TDuration ErrorTtl;

    TAuthCacheConfig();
};

DEFINE_REFCOUNTED_TYPE(TAuthCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TDefaultBlackboxServiceConfig
    : public virtual NYTree::TYsonSerializable
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

    TDefaultBlackboxServiceConfig();
};

DEFINE_REFCOUNTED_TYPE(TDefaultBlackboxServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TDefaultTvmServiceConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    bool UseTvmTool;

    // TvmClient settings
    ui32 ClientSelfId = 0;
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

    TDefaultTvmServiceConfig();
};

DEFINE_REFCOUNTED_TYPE(TDefaultTvmServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTokenAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TString Scope;
    bool EnableScopeCheck;
    bool GetUserTicket;

    TBlackboxTokenAuthenticatorConfig();
};

DEFINE_REFCOUNTED_TYPE(TBlackboxTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTicketAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    THashSet<TString> Scopes;
    bool EnableScopeCheck;

    TBlackboxTicketAuthenticatorConfig();
};

DEFINE_REFCOUNTED_TYPE(TBlackboxTicketAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingTokenAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TAuthCacheConfigPtr Cache;

    TCachingTokenAuthenticatorConfig();
};

DEFINE_REFCOUNTED_TYPE(TCachingTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingBlackboxTokenAuthenticatorConfig
    : public TBlackboxTokenAuthenticatorConfig
    , public TCachingTokenAuthenticatorConfig
{ };

DEFINE_REFCOUNTED_TYPE(TCachingBlackboxTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCypressTokenAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    NYPath::TYPath RootPath;
    TString Realm;

    bool Secure;

    TCypressTokenAuthenticatorConfig();
};

DEFINE_REFCOUNTED_TYPE(TCypressTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingCypressTokenAuthenticatorConfig
    : public TCachingTokenAuthenticatorConfig
    , public TCypressTokenAuthenticatorConfig
{ };

DEFINE_REFCOUNTED_TYPE(TCachingCypressTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

static const auto DefaultCsrfTokenTtl = TDuration::Days(7);

class TBlackboxCookieAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TString Domain;

    std::optional<TString> CsrfSecret;
    TDuration CsrfTokenTtl;

    bool GetUserTicket;

    TBlackboxCookieAuthenticatorConfig();
};

DEFINE_REFCOUNTED_TYPE(TBlackboxCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingCookieAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TAuthCacheConfigPtr Cache;

    TCachingCookieAuthenticatorConfig();
};

DEFINE_REFCOUNTED_TYPE(TCachingCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingBlackboxCookieAuthenticatorConfig
    : public TBlackboxCookieAuthenticatorConfig
    , public TCachingCookieAuthenticatorConfig
{ };

DEFINE_REFCOUNTED_TYPE(TCachingBlackboxCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TDefaultSecretVaultServiceConfig
    : public virtual NYT::NYTree::TYsonSerializable
{
public:
    TString Host;
    int Port;
    bool Secure;
    NHttps::TClientConfigPtr HttpClient;
    TDuration RequestTimeout;
    TString VaultServiceId;
    TString Consumer;

    TDefaultSecretVaultServiceConfig();
};

DEFINE_REFCOUNTED_TYPE(TDefaultSecretVaultServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TBatchingSecretVaultServiceConfig
    : public virtual NYT::NYTree::TYsonSerializable
{
public:
    TDuration BatchDelay;
    int MaxSubrequestsPerRequest;
    NConcurrency::TThroughputThrottlerConfigPtr RequestsThrottler;

    TBatchingSecretVaultServiceConfig();
};

DEFINE_REFCOUNTED_TYPE(TBatchingSecretVaultServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingSecretVaultServiceConfig
    : public TAsyncExpiringCacheConfig
{
public:
    TAsyncExpiringCacheConfigPtr Cache;

    TCachingSecretVaultServiceConfig();
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
    NAuth::TDefaultBlackboxServiceConfigPtr BlackboxService;
    NAuth::TCachingCypressTokenAuthenticatorConfigPtr CypressTokenAuthenticator;
    NAuth::TDefaultTvmServiceConfigPtr TvmService;
    NAuth::TBlackboxTicketAuthenticatorConfigPtr BlackboxTicketAuthenticator;

    TString GetCsrfSecret() const;

    TInstant GetCsrfTokenExpirationTime() const;

    REGISTER_YSON_STRUCT(TAuthenticationManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAuthenticationManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
