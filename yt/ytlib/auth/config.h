#pragma once

#include "public.h"

#include <yt/core/ypath/public.h>

#include <yt/core/misc/config.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

class TDefaultBlackboxServiceConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDefaultBlackboxServiceConfig()
    {
        RegisterParameter("host", Host)
            .Default("blackbox.yandex-team.ru");
        RegisterParameter("port", Port)
            .Default(443);
        RegisterParameter("secure", Secure)
            .Default(true);
        RegisterParameter("request_timeout", RequestTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("attempt_timeout", AttemptTimeout)
            .Default(TDuration::Seconds(10));
        RegisterParameter("backoff_timeout", BackoffTimeout)
            .Default(TDuration::Seconds(1));
        RegisterParameter("use_lowercase_login", UseLowercaseLogin)
            .Default(true);
    }

    TString Host;
    ui16 Port;
    bool Secure;

    TDuration RequestTimeout;
    TDuration AttemptTimeout;
    TDuration BackoffTimeout;
    bool UseLowercaseLogin;
};

DEFINE_REFCOUNTED_TYPE(TDefaultBlackboxServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TDefaultTvmServiceConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDefaultTvmServiceConfig()
    {
        RegisterParameter("port", Port);
        RegisterParameter("token", Token);
        RegisterParameter("request_timeout", RequestTimeout)
            .Default(TDuration::Seconds(3));
    }

    ui16 Port;
    TString Token;

    TDuration RequestTimeout;
};

DEFINE_REFCOUNTED_TYPE(TDefaultTvmServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingDefaultTvmServiceConfig
    : public TDefaultTvmServiceConfig
    , public TAsyncExpiringCacheConfig
{ };

DEFINE_REFCOUNTED_TYPE(TCachingDefaultTvmServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTokenAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TBlackboxTokenAuthenticatorConfig()
    {
        RegisterParameter("scope", Scope);
        RegisterParameter("enable_scope_check", EnableScopeCheck)
            .Default(true);
    }

    TString Scope;
    bool EnableScopeCheck;
};

DEFINE_REFCOUNTED_TYPE(TBlackboxTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTicketAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TString BlackboxServiceId;

    TBlackboxTicketAuthenticatorConfig()
    {
        RegisterParameter("blackbox_service_id", BlackboxServiceId)
            .Default("blackbox");
    }
};

DEFINE_REFCOUNTED_TYPE(TBlackboxTicketAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingBlackboxTokenAuthenticatorConfig
    : public TBlackboxTokenAuthenticatorConfig
    , public TAsyncExpiringCacheConfig
{ };

DEFINE_REFCOUNTED_TYPE(TCachingBlackboxTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCypressTokenAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TCypressTokenAuthenticatorConfig()
    {
        RegisterParameter("root_path", RootPath)
            .Default("//sys/tokens");
        RegisterParameter("realm", Realm)
            .Default("cypress");

        RegisterParameter("secure", Secure)
            .Default(false);
    }

    NYPath::TYPath RootPath;
    TString Realm;

    bool Secure;
};

DEFINE_REFCOUNTED_TYPE(TCypressTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingCypressTokenAuthenticatorConfig
    : public TCypressTokenAuthenticatorConfig
    , public TAsyncExpiringCacheConfig
{ };

DEFINE_REFCOUNTED_TYPE(TCachingCypressTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

static const auto DefaultCsrfTokenTtl = TDuration::Days(7);

class TBlackboxCookieAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TBlackboxCookieAuthenticatorConfig()
    {
        RegisterParameter("domain", Domain)
            .Default("yt.yandex-team.ru");

        RegisterParameter("csrf_secret", CsrfSecret)
            .Default();
        RegisterParameter("csrf_token_ttl", CsrfTokenTtl)
            .Default(DefaultCsrfTokenTtl);
    }

    TString Domain;

    TNullable<TString> CsrfSecret;
    TDuration CsrfTokenTtl;
};

DEFINE_REFCOUNTED_TYPE(TBlackboxCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingBlackboxCookieAuthenticatorConfig
    : public TBlackboxCookieAuthenticatorConfig
    , public TAsyncExpiringCacheConfig
{
public:
    TCachingBlackboxCookieAuthenticatorConfig()
    { }
};

DEFINE_REFCOUNTED_TYPE(TCachingBlackboxCookieAuthenticatorConfig)

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
    NAuth::TCachingDefaultTvmServiceConfigPtr TvmService;
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
            .Optional();
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

        return "";
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

} // namespace NAuth
} // namespace NYT
