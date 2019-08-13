#pragma once

#include "private.h"

#include <yt/server/lib/misc/config.h>

#include <yt/ytlib/api/native/config.h>

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/client/misc/config.h>

#include <yt/client/ypath/rich.h>

#include <yt/core/concurrency/config.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TUserConfig
    : public NYTree::TYsonSerializable
{
public:
    // This field is overriden by DefaultProfile in TEngineConfig.
    THashMap<TString, THashMap<TString, NYTree::INodePtr>> Profiles;
    NYTree::IMapNodePtr Quotas;
    NYTree::IMapNodePtr UserTemplate;
    NYTree::IMapNodePtr Users;

    TUserConfig()
    {
        RegisterParameter("profiles", Profiles)
            .Default();

        RegisterParameter("quotas", Quotas)
            .Default(NYTree::BuildYsonNodeFluently()
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
            .Default(NYTree::BuildYsonNodeFluently()
                .BeginMap()
                    .Item("networks").BeginMap()
                        .Item("ip").Value("::/0")
                    .EndMap()
                    .Item("password").Value("")
                    .Item("profile").Value("default")
                    .Item("quota").Value("default")
                .EndMap()->AsMap());

        RegisterParameter("users", Users)
            .Default(NYTree::BuildYsonNodeFluently().BeginMap().EndMap()->AsMap());
    }
};

DEFINE_REFCOUNTED_TYPE(TUserConfig);

////////////////////////////////////////////////////////////////////////////////

class TDictionarySourceYtConfig
    : public NYTree::TYsonSerializable
{
public:
    NYPath::TRichYPath Path;

    TDictionarySourceYtConfig()
    {
        RegisterParameter("path", Path);
    }
};

DEFINE_REFCOUNTED_TYPE(TDictionarySourceYtConfig);

////////////////////////////////////////////////////////////////////////////////

//! Source configuration.
//! Extra supported configuration type is "yt".
//! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_sources/
class TDictionarySourceConfig
    : public NYTree::TYsonSerializable
{
public:
    // TODO(max42): proper value omission.
    TDictionarySourceYtConfigPtr Yt;

    TDictionarySourceConfig()
    {
        RegisterParameter("yt", Yt)
            .Default(nullptr);
    }
};

DEFINE_REFCOUNTED_TYPE(TDictionarySourceConfig);

////////////////////////////////////////////////////////////////////////////////

//! External dictionary configuration.
//! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict/
class TDictionaryConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Name;

    //! Source configuration.
    TDictionarySourceConfigPtr Source;

    //! Layout configuration.
    //! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_layout/
    NYTree::IMapNodePtr Layout;

    //! Structure configuration.
    //! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_structure/
    NYTree::IMapNodePtr Structure;

    //! Lifetime configuration.
    //! See: https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict_lifetime/
    NYTree::INodePtr Lifetime;

    TDictionaryConfig()
    {
        RegisterParameter("name", Name);
        RegisterParameter("source", Source);
        RegisterParameter("layout", Layout);
        RegisterParameter("structure", Structure);
        RegisterParameter("lifetime", Lifetime);
    }
};

DEFINE_REFCOUNTED_TYPE(TDictionaryConfig)

////////////////////////////////////////////////////////////////////////////////

class TSubqueryConfig
    : public NYTree::TYsonSerializable
{
public:
    NChunkClient::TFetcherConfigPtr ChunkSliceFetcher;

    int MaxSlicedChunkCount;
    int MaxChunksPerFetch;
    int MaxChunksPerLocateRequest;

    TSubqueryConfig()
    {
        RegisterParameter("chunk_slice_fetcher", ChunkSliceFetcher)
            .DefaultNew();
        RegisterParameter("max_sliced_chunk_count", MaxSlicedChunkCount)
            .Default(100);
        RegisterParameter("max_chunks_per_fetch", MaxChunksPerFetch)
            .Default(100'000);
        RegisterParameter("max_chunks_per_locate_request", MaxChunksPerLocateRequest)
            .Default(10'000);
    }
};

DEFINE_REFCOUNTED_TYPE(TSubqueryConfig)

////////////////////////////////////////////////////////////////////////////////

class TEngineConfig
    : public NYTree::TYsonSerializable
{
public:
    //! A map setting CH security policy.
    TUserConfigPtr Users;

    //! Path in filesystem to the internal state.
    TString DataPath;

    //! Path in Cypress with coordination map node, external dictionaries etc.
    TString CypressRootPath;

    //! Log level for internal CH logging.
    TString LogLevel;

    //! External dictionaries.
    std::vector<TDictionaryConfigPtr> Dictionaries;

    //! ClickHouse settings.
    //! Refer to https://clickhouse.yandex/docs/en/operations/settings/settings/ for a complete list.
    //! This map is merged into `users/profiles/default`.
    THashMap<TString, NYTree::INodePtr> Settings;

    //! Hosts to listen.
    std::vector<TString> ListenHosts;

    //! Paths to geodata stuff.
    TString PathToRegionsHierarchyFile;
    TString PathToRegionsNameFiles;

    //! Subquery logic configuration.
    TSubqueryConfigPtr Subquery;

    NYTree::INodePtr CreateTableDefaultAttributes;

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
            .MergeBy(NYTree::EMergeStrategy::Combine);

        RegisterParameter("dictionaries", Dictionaries)
            .Default();

        RegisterParameter("path_to_regions_hierarchy_file", PathToRegionsHierarchyFile)
            .Default("./geodata/regions_hierarchy.txt");

        RegisterParameter("path_to_regions_name_files", PathToRegionsNameFiles)
            .Default("./geodata/");

        RegisterParameter("subquery", Subquery)
            .DefaultNew();

        RegisterParameter("create_table_default_attributes", CreateTableDefaultAttributes)
            .MergeBy(NYTree::EMergeStrategy::Combine)
            .Default(NYTree::BuildYsonNodeFluently()
                .BeginMap()
                    .Item("optimize_for").Value("scan")
                .EndMap());

        RegisterPreprocessor([&] {
            Settings["max_memory_usage_for_all_queries"] = NYTree::ConvertToNode(9_GB);
            Settings["max_threads"] = NYTree::ConvertToNode(32);
            Settings["max_concurrent_queries_for_user"] = NYTree::ConvertToNode(10);
        });

        RegisterPostprocessor([&] {
            auto& userDefaultProfile = Users->Profiles["default"];
            for (auto& [key, value] : Settings) {
                userDefaultProfile[key] = value;
            }

            Settings = userDefaultProfile;
        });

        SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
    }
};

DEFINE_REFCOUNTED_TYPE(TEngineConfig);

////////////////////////////////////////////////////////////////////////////////

class TMemoryWatchdogConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Memory limit for the job.
    size_t MemoryLimit;

    //! If remaining memory becomes less than `CodicilWatermark`, process dumps its query registry
    //! to simplify the investigation of its inevitable^W possible death.
    size_t CodicilWatermark;

    //! Check period.
    TDuration Period;

    TMemoryWatchdogConfig()
    {
        // Default is effective infinity.
        RegisterParameter("memory_limit", MemoryLimit)
            .Default(1_TB);
        RegisterParameter("codicil_watermark", CodicilWatermark)
            .Default(0);
        RegisterParameter("period", Period)
            .Default(TDuration::Seconds(1));
    }
};

DEFINE_REFCOUNTED_TYPE(TMemoryWatchdogConfig);

////////////////////////////////////////////////////////////////////////////////

class TClickHouseServerBootstrapConfig
    : public TServerConfig
{
public:
    NApi::NNative::TConnectionConfigPtr ClusterConnection;

    TSlruCacheConfigPtr ClientCache;

    //! Authorization settings.
    bool ValidateOperationAccess;
    TDuration OperationAclUpdatePeriod;

    TEngineConfigPtr Engine;

    //! User for communication with YT.
    TString User;

    TDuration ProfilingPeriod;

    NTableClient::TTableWriterConfigPtr TableWriterConfig;

    TMemoryWatchdogConfigPtr MemoryWatchdog;

    //! Note that CliqueId will be added to Directory automatically.
    TDiscoveryConfigPtr Discovery;

    TDuration GossipPeriod;

    //! To avoid reciving queries after shut down this value should be at least
    //! max(GossipPeriod, Proxy.ClickHouse.CliqueCache.AgeThreshold +
    //! Proxy.ClickHouse.CliqueCache.MasterCacheExpireTime)
    TDuration InterruptionGracefulTimeout;

    TClickHouseServerBootstrapConfig()
    {
        RegisterParameter("cluster_connection", ClusterConnection);

        RegisterParameter("client_cache", ClientCache)
            .DefaultNew();

        RegisterParameter("validate_operation_access", ValidateOperationAccess)
            .Default(true);
        RegisterParameter("operation_acl_update_period", OperationAclUpdatePeriod)
            .Default(TDuration::Minutes(1));

        RegisterParameter("user", User)
            .Default("yt-clickhouse");

        RegisterParameter("engine", Engine)
            .DefaultNew();

        RegisterParameter("profiling_period", ProfilingPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("memory_watchdog", MemoryWatchdog)
            .Default(New<TMemoryWatchdogConfig>());

        RegisterParameter("table_writer_config", TableWriterConfig)
            .DefaultNew();

        RegisterParameter("discovery", Discovery);

        RegisterParameter("gossip_period", GossipPeriod)
            .Default(TDuration::Seconds(1));
        
        RegisterParameter("interruption_graceful_timeout", InterruptionGracefulTimeout)
            .Default(TDuration::Seconds(30));
    }
};

DEFINE_REFCOUNTED_TYPE(TClickHouseServerBootstrapConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
