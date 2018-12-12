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

class TUserConfig
    : public TYsonSerializable
{
public:
    // This field is overriden by DefaultProfile in TEngineConfig.
    THashMap<TString, THashMap<TString, INodePtr>> Profiles;
    IMapNodePtr Quotas;
    IMapNodePtr UserTemplate;
    IMapNodePtr Users;

    TUserConfig()
    {
        RegisterParameter("profiles", Profiles)
            .Default();

        RegisterParameter("quotas", Quotas)
            .Default(BuildYsonNodeFluently()
                .BeginMap()
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
                .EndMap()->AsMap());

        RegisterParameter("user_template", UserTemplate)
            .Default(BuildYsonNodeFluently()
                .BeginMap()
                    .Item("networks").BeginMap()
                        .Item("ip").Value("::/0")
                    .EndMap()
                    .Item("password").Value("")
                    .Item("profile").Value("default")
                    .Item("quota").Value("default")
                .EndMap()->AsMap());

        RegisterParameter("users", Users)
            .Default(BuildYsonNodeFluently().BeginMap().EndMap()->AsMap());
    }
};

DEFINE_REFCOUNTED_TYPE(TUserConfig);

////////////////////////////////////////////////////////////////////////////////

class TEngineConfig
    : public TYsonSerializable
{
public:
    TEngineConfig()
    {
        RegisterParameter("users", Users)
            .DefaultNew();

        RegisterParameter("data_path", DataPath)
            .Default("data");

        RegisterParameter("log_level", LogLevel)
            .Default("trace");

        RegisterParameter("cypress_root_path", CypressRootPath)
            .Default("//sys/clickhouse");

        RegisterParameter("listen_hosts", ListenHosts)
            .Default(std::vector<TString> {"::"});

        RegisterParameter("settings", Settings)
            .Optional()
            .MergeBy(EMergeStrategy::Combine);

        RegisterPreprocessor([&] {
            Settings["readonly"] = ConvertToNode(2);
            Settings["max_memory_usage_for_all_queries"] = ConvertToNode(9_GB);
            Settings["max_threads"] = ConvertToNode(32);
            Settings["max_concurrent_queries_for_user"] = ConvertToNode(10);
        });

        RegisterPostprocessor([&] {
            auto& userDefaultProfile = Users->Profiles["default"];
            for (auto& [key, value] : Settings) {
                userDefaultProfile[key] = value;
            }

            Settings = userDefaultProfile;
        });

        SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
    }

    //! A map setting CH security policy.
    TUserConfigPtr Users;

    //! Path in filesystem to the internal state.
    TString DataPath;

    //! Path in Cypress with coordination map node, external dictionaries etc.
    TString CypressRootPath;

    //! Log level for internal CH logging.
    TString LogLevel;

    //! ClickHouse settings.
    //! Refer to https://clickhouse.yandex/docs/en/operations/settings/settings/ for a complete list.
    //! This map is merged into `users/profiles/default`.
    THashMap<TString, INodePtr> Settings;

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

    //! User for communication with YT.
    TString User;

    TConfig()
    {
        RegisterParameter("cluster_connection", ClusterConnection);

        RegisterParameter("client_cache", ClientCache)
            .DefaultNew();

        RegisterParameter("scan_throttler", ScanThrottler)
            .Default();

        RegisterParameter("validate_operation_permission", ValidateOperationPermission)
            .Default(true);

        RegisterParameter("user", User)
            .Default("yt-clickhouse");

        RegisterParameter("engine", Engine)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NNative
