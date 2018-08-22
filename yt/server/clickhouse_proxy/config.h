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
{
public:
    TString DiscoveryPath;
    NHttp::TClientConfigPtr HttpClient;

    TClickHouseProxyConfig()
    {
        RegisterParameter("discovery_path", DiscoveryPath)
            .Default("//sys/clickhouse/cliques");
        RegisterParameter("http_client", HttpClient)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TClickHouseProxyConfig)

////////////////////////////////////////////////////////////////////////////////

class TClickHouseProxyServerConfig
    : public TServerConfig
{
public:
    //! Proxy-to-master connection.
    int WorkerThreadPoolSize;

    NHttp::TServerConfigPtr ClickHouseProxyHttpServer;

    TClickHouseProxyConfigPtr ClickHouseProxy;

    NAuth::TAuthenticationManagerConfigPtr AuthenticationManager;

    TClickHouseProxyServerConfig()
    {
        RegisterParameter("worker_thread_pool_size", WorkerThreadPoolSize)
            .GreaterThan(0)
            .Default(8);

        RegisterParameter("clickhouse_proxy_http_server", ClickHouseProxyHttpServer)
            .DefaultNew();

        RegisterParameter("clickhouse_proxy", ClickHouseProxy)
            .DefaultNew();

        RegisterParameter("authentication_manager", AuthenticationManager)
            .DefaultNew();

        RegisterPreprocessor([&] {
            // Default for clickhouse.
            ClickHouseProxyHttpServer->Port = 8123;
        });

        RegisterPostprocessor([&] {
            if (AuthenticationManager->BlackboxCookieAuthenticator) {
                THROW_ERROR_EXCEPTION("Blackbox cookie authenticator is not supported in clickhouse proxy");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TClickHouseProxyServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellProxy
} // namespace NYT
