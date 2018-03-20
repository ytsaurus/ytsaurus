#pragma once

#include <yt/core/misc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NBlackbox {

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
    }

    TString Host;
    ui16 Port;
    bool Secure;

    TDuration RequestTimeout;
    TDuration AttemptTimeout;
    TDuration BackoffTimeout;
};

DEFINE_REFCOUNTED_TYPE(TDefaultBlackboxServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TTokenAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TTokenAuthenticatorConfig()
    {
        RegisterParameter("scope", Scope);
        RegisterParameter("enable_scope_check", EnableScopeCheck)
            .Optional();
    }

    TString Scope;
    bool EnableScopeCheck = true;
};

DEFINE_REFCOUNTED_TYPE(TTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingTokenAuthenticatorConfig
    : public TTokenAuthenticatorConfig
    , public TExpiringCacheConfig
{ };

DEFINE_REFCOUNTED_TYPE(TCachingTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCookieAuthenticatorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TCookieAuthenticatorConfig()
    { }
};

DEFINE_REFCOUNTED_TYPE(TCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingCookieAuthenticatorConfig
    : public TCookieAuthenticatorConfig
    , public TExpiringCacheConfig
{
public:
    TCachingCookieAuthenticatorConfig()
    { }
};

DEFINE_REFCOUNTED_TYPE(TCachingCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT
