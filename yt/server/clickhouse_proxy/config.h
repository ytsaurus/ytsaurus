#pragma once

#include "public.h"

#include <yt/server/misc/config.h>

#include <yt/ytlib/auth/config.h>

#include <yt/core/http/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NClickHouseProxy {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseProxyConfig
    : public NYTree::TYsonSerializable
{ };

DEFINE_REFCOUNTED_TYPE(TClickHouseProxyConfig)

////////////////////////////////////////////////////////////////////////////////

class TClickHouseProxyServerConfig
    : public TServerConfig
    , public NAuth::TAuthenticationManagerConfig
{
public:
    //! Proxy-to-master connection.
    int WorkerThreadPoolSize;

    NHttp::TServerConfigPtr ClickHouseProxyHttpServer;

    TClickHouseProxyConfigPtr ClickHouseProxy;

    TClickHouseProxyServerConfig()
    {
        RegisterParameter("worker_thread_pool_size", WorkerThreadPoolSize)
            .GreaterThan(0)
            .Default(8);

        RegisterParameter("clickhouse_proxy_http_server", ClickHouseProxyHttpServer)
            .DefaultNew();

        RegisterParameter("clickhouse_proxy", ClickHouseProxy)
            .DefaultNew();


        RegisterPreprocessor([&] {
            // Default for clickhouse.
            ClickHouseProxyHttpServer->Port = 8123;
        });

        RegisterPostprocessor([&] {
            if (BlackboxCookieAuthenticator) {
                THROW_ERROR_EXCEPTION("Blackbox cookie authenticator is not supported in clickhouse proxy");
            }
            if (CypressTokenAuthenticator) {
                THROW_ERROR_EXCEPTION("Cypress token authenticator is not supported in clickhouse proxy");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TClickHouseProxyServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellProxy
} // namespace NYT
