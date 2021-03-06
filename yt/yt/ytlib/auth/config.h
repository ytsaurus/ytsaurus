#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/https/config.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

class TAuthCacheConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDuration CacheTtl;
    TDuration OptimisticCacheTtl;
    TDuration ErrorTtl;

    TAuthCacheConfig()
    {
        RegisterParameter("cache_ttl", CacheTtl)
            .Default(TDuration::Minutes(5));
        RegisterParameter("optimistic_cache_ttl", OptimisticCacheTtl)
            .Default(TDuration::Hours(1));
        RegisterParameter("error_ttl", ErrorTtl)
            .Default(TDuration::Seconds(15));
    }
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

    TDefaultBlackboxServiceConfig()
    {
        RegisterParameter("http_client", HttpClient)
            .DefaultNew();
        RegisterParameter("host", Host)
            .Default("blackbox.yandex-team.ru");
        RegisterParameter("port", Port)
            .Default(443);
        RegisterParameter("secure", Secure)
            .Default(true);
        RegisterParameter("blackbox_service_id", BlackboxServiceId)
            .Default("blackbox");
        RegisterParameter("request_timeout", RequestTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("attempt_timeout", AttemptTimeout)
            .Default(TDuration::Seconds(10));
        RegisterParameter("backoff_timeout", BackoffTimeout)
            .Default(TDuration::Seconds(1));
        RegisterParameter("use_lowercase_login", UseLowercaseLogin)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TDefaultBlackboxServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TDefaultTvmServiceConfig
    : public virtual NYTree::TYsonSerializable
{
public:
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

    TDefaultTvmServiceConfig()
    {
        RegisterParameter("client_self_id", ClientSelfId)
            .Default(0);
        RegisterParameter("client_disk_cache_dir", ClientDiskCacheDir)
            .Optional();
        RegisterParameter("tvm_host", TvmHost)
            .Optional();
        RegisterParameter("tvm_port", TvmPort)
            .Optional();
        RegisterParameter("client_enable_user_ticket_checking", ClientEnableUserTicketChecking)
            .Default(false);
        RegisterParameter("client_blackbox_env", ClientBlackboxEnv)
            .Default("ProdYateam");
        RegisterParameter("client_enable_service_ticket_fetching", ClientEnableServiceTicketFetching)
            .Default(false);
        RegisterParameter("client_self_secret", ClientSelfSecret)
            .Optional();
        RegisterParameter("client_dst_map", ClientDstMap)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TDefaultTvmServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTokenAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TString Scope;
    bool EnableScopeCheck;

    TBlackboxTokenAuthenticatorConfig()
    {
        RegisterParameter("scope", Scope);
        RegisterParameter("enable_scope_check", EnableScopeCheck)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TBlackboxTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTicketAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    THashSet<TString> Scopes;
    bool EnableScopeCheck;

    TBlackboxTicketAuthenticatorConfig()
    {
        RegisterParameter("scopes", Scopes)
            .Optional();
        RegisterParameter("enable_scope_check", EnableScopeCheck)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TBlackboxTicketAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingTokenAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TAuthCacheConfigPtr Cache;

    TCachingTokenAuthenticatorConfig()
    {
        RegisterParameter("cache", Cache)
            .DefaultNew();
    }
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

    TCypressTokenAuthenticatorConfig()
    {
        RegisterParameter("root_path", RootPath)
            .Default("//sys/tokens");
        RegisterParameter("realm", Realm)
            .Default("cypress");

        RegisterParameter("secure", Secure)
            .Default(false);
    }
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

    TBlackboxCookieAuthenticatorConfig()
    {
        RegisterParameter("domain", Domain)
            .Default("yt.yandex-team.ru");

        RegisterParameter("csrf_secret", CsrfSecret)
            .Default();
        RegisterParameter("csrf_token_ttl", CsrfTokenTtl)
            .Default(DefaultCsrfTokenTtl);
    }
};

DEFINE_REFCOUNTED_TYPE(TBlackboxCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingCookieAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TAuthCacheConfigPtr Cache;

    TCachingCookieAuthenticatorConfig()
    {
        RegisterParameter("cache", Cache)
            .DefaultNew();
    }
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

    TDefaultSecretVaultServiceConfig()
    {
        RegisterParameter("host", Host)
            .Default("vault-api.passport.yandex.net");
        RegisterParameter("port", Port)
            .Default(443);
        RegisterParameter("secure", Secure)
            .Default(true);
        RegisterParameter("http_client", HttpClient)
            .DefaultNew();
        RegisterParameter("request_timeout", RequestTimeout)
            .Default(TDuration::Seconds(3));
        RegisterParameter("vault_service_id", VaultServiceId)
            .Default("yav");
        RegisterParameter("consumer", Consumer)
            .Optional();
    }
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

    TBatchingSecretVaultServiceConfig()
    {
        RegisterParameter("batch_delay", BatchDelay)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("max_subrequests_per_request", MaxSubrequestsPerRequest)
            .Default(100)
            .GreaterThan(0);
        RegisterParameter("requests_throttler", RequestsThrottler)
            .DefaultNew();

        RegisterPreprocessor([&] {
            RequestsThrottler->Limit = 100;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TBatchingSecretVaultServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingSecretVaultServiceConfig
    : public TAsyncExpiringCacheConfig
{
public:
    TAsyncExpiringCacheConfigPtr Cache;

    TCachingSecretVaultServiceConfig()
    {
        RegisterParameter("cache", Cache)
            .DefaultNew();

        RegisterPreprocessor([&] {
            Cache->RefreshTime = std::nullopt;
            Cache->ExpireAfterAccessTime = TDuration::Seconds(60);
            Cache->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(60);
            Cache->ExpireAfterFailedUpdateTime = TDuration::Seconds(60);
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TCachingSecretVaultServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TAuthenticationManagerConfig
    : public virtual NYT::NYTree::TYsonSerializable
{
public:
    bool RequireAuthentication;
    NAuth::TCachingBlackboxTokenAuthenticatorConfigPtr BlackboxTokenAuthenticator;
    NAuth::TCachingBlackboxCookieAuthenticatorConfigPtr BlackboxCookieAuthenticator;
    NAuth::TDefaultBlackboxServiceConfigPtr BlackboxService;
    NAuth::TCachingCypressTokenAuthenticatorConfigPtr CypressTokenAuthenticator;
    NAuth::TDefaultTvmServiceConfigPtr TvmService;
    NAuth::TBlackboxTicketAuthenticatorConfigPtr BlackboxTicketAuthenticator;

    TAuthenticationManagerConfig()
    {
        // COMPAT(prime@)
        RegisterParameter("require_authentication", RequireAuthentication)
            .Alias("enable_authentication")
            .Default(true);
        RegisterParameter("blackbox_token_authenticator", BlackboxTokenAuthenticator)
            .Alias("token_authenticator")
            .Optional();
        RegisterParameter("blackbox_cookie_authenticator", BlackboxCookieAuthenticator)
            .Alias("cookie_authenticator")
            .DefaultNew();
        RegisterParameter("blackbox_service", BlackboxService)
            .Alias("blackbox")
            .DefaultNew();
        RegisterParameter("cypress_token_authenticator", CypressTokenAuthenticator)
            .Optional();
        RegisterParameter("tvm_service", TvmService)
            .Optional();
        RegisterParameter("blackbox_ticket_authenticator", BlackboxTicketAuthenticator)
            .Optional();
    }

    TString GetCsrfSecret() const
    {
        if (BlackboxCookieAuthenticator &&
            BlackboxCookieAuthenticator->CsrfSecret)
        {
            return *BlackboxCookieAuthenticator->CsrfSecret;
        }

        return TString();
    }

    TInstant GetCsrfTokenExpirationTime() const
    {
        if (BlackboxCookieAuthenticator) {
            return TInstant::Now() - BlackboxCookieAuthenticator->CsrfTokenTtl;
        }

        return TInstant::Now() - DefaultCsrfTokenTtl;
    }
};

DEFINE_REFCOUNTED_TYPE(TAuthenticationManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
