#pragma once

#include "public.h"

#include <yt/server/misc/config.h>

#include <yt/ytlib/api/native/config.h>

#include <yt/core/concurrency/config.h>
#include <yt/core/ytree/fluent.h>

namespace NYT::NClickHouseServer::NNative {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TNativeClientCacheConfig
    : public TAsyncExpiringCacheConfig
{
public:
    TNativeClientCacheConfig() = default;
};

DEFINE_REFCOUNTED_TYPE(TNativeClientCacheConfig);

////////////////////////////////////////////////////////////////////////////////

class TEngineConfig
    : public TYsonSerializable
{
public:
    TEngineConfig()
    {
        RegisterParameter("clickhouse_users", ClickHouseUsers)
            .Default(BuildYsonNodeFluently()
                .BeginMap()
                    .Item("profiles").BeginMap()
                        .Item("default").BeginMap()
                            .Item("readonly").Value(2)
                        .EndMap()
                    .EndMap()
                    .Item("quotas").BeginMap()
                        .Item("default").BeginMap()
                            .Item("interval").BeginMap()
                                .Item("duration").Value(3600)
                                .Item("errors").Value(0)
                                .Item("execution_time").Value(0)
                                .Item("queries").Value(0)
                                .Item("read_rows").Value(0)
                                .Item("result_rows").Value(0)
                            .EndMap()
                        .EndMap()
                    .EndMap()
                    .Item("user_template").BeginMap()
                        .Item("networks").BeginMap()
                            .Item("ip").Value("::/0")
                        .EndMap()
                        .Item("password").Value("")
                        .Item("profile").Value("default")
                        .Item("quota").Value("default")
                    .EndMap()
                    .Item("users").BeginMap().EndMap()
                .EndMap()->AsMap());

        RegisterParameter("data_path", DataPath)
            .Default("data");

        RegisterParameter("log_level", LogLevel)
            .Default("trace");

        RegisterParameter("cypress_root_path", CypressRootPath)
            .Default("//sys/clickhouse");

        RegisterParameter("user_name", UserName)
            .Default("yt-clickhouse");

        RegisterParameter("listen_hosts", ListenHosts)
            .Default(std::vector<TString> {"::"});

        SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
    }

    //! A map setting CH security policy.
    IMapNodePtr ClickHouseUsers;

    //! Path in filesystem to the internal state.
    TString DataPath;

    //! Path in Cypress with coordination map node, external dictionaries etc.
    TString CypressRootPath;

    //! Log level for internal CH logging.
    TString LogLevel;

    //! User for communication with YT.
    TString UserName;

    //! Hosts to listen.
    std::vector<TString> ListenHosts;
};

DEFINE_REFCOUNTED_TYPE(TEngineConfig);

////////////////////////////////////////////////////////////////////////////////

class TConfig
    : public TServerConfig
{
public:
    NApi::NNative::TConnectionConfigPtr ClusterConnection;

    TNativeClientCacheConfigPtr ClientCache;

    //! Controls incoming bandwidth used by scan jobs.
    NConcurrency::TThroughputThrottlerConfigPtr ScanThrottler;

    bool ValidateOperationPermission;

    TEngineConfigPtr Engine;

    TConfig()
    {
        RegisterParameter("cluster_connection", ClusterConnection);

        RegisterParameter("client_cache", ClientCache)
            .DefaultNew();

        RegisterParameter("scan_throttler", ScanThrottler)
            .Default();

        RegisterParameter("validate_operation_permission", ValidateOperationPermission)
            .Default(true);

        RegisterParameter("engine", Engine)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NNative
