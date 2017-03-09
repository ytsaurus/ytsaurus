#pragma once

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NBlackbox {

////////////////////////////////////////////////////////////////////////////////

class TDefaultBlackboxServiceConfig
    : public NYTree::TYsonSerializable
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

    Stroka Host;
    ui16 Port;
    bool Secure;

    TDuration RequestTimeout;
    TDuration AttemptTimeout;
    TDuration BackoffTimeout;
};

DEFINE_REFCOUNTED_TYPE(TDefaultBlackboxServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TTokenAuthenticatorConfig
    : public NYTree::TYsonSerializable
{
public:
    TTokenAuthenticatorConfig()
    {
        RegisterParameter("scope", Scope);
        RegisterParameter("client_ids", ClientIds);

        RegisterParameter("enable_scope_check", EnableScopeCheck)
            .Optional();
        RegisterParameter("enable_client_ids_check", EnableClientIdsCheck)
            .Optional();
    }

    Stroka Scope;
    yhash_map<Stroka, Stroka> ClientIds;

    bool EnableScopeCheck = true;
    bool EnableClientIdsCheck = true;
};

DEFINE_REFCOUNTED_TYPE(TTokenAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TCookieAuthenticatorConfig
    : public NYTree::TYsonSerializable
{
public:
    TCookieAuthenticatorConfig()
    { }
};

DEFINE_REFCOUNTED_TYPE(TCookieAuthenticatorConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT
