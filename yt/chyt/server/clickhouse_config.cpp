#include "clickhouse_config.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void TUserConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("profiles", &TThis::Profiles)
        .Default();

    registrar.Parameter("quotas", &TThis::Quotas)
        .DefaultCtor([] {
            return NYTree::BuildYsonNodeFluently()
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
                .EndMap()->AsMap();
        })
        .ResetOnLoad();

    registrar.Parameter("user_template", &TThis::UserTemplate)
        .DefaultCtor([] {
            return NYTree::BuildYsonNodeFluently()
                .BeginMap()
                    .Item("networks").BeginMap()
                        .Item("ip").Value("::/0")
                    .EndMap()
                    .Item("password").Value("")
                    .Item("profile").Value("default")
                    .Item("quota").Value("default")
                .EndMap()->AsMap();
        })
        .ResetOnLoad();

    registrar.Parameter("users", &TThis::Users)
        .DefaultCtor([] {
            return NYTree::BuildYsonNodeFluently().BeginMap().EndMap()->AsMap();
        });
}

////////////////////////////////////////////////////////////////////////////////

void TDictionarySourceYtConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
}

////////////////////////////////////////////////////////////////////////////////

void TDictionarySourceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("yt", &TThis::Yt)
        .DefaultCtor([] () { return nullptr; });
}

////////////////////////////////////////////////////////////////////////////////

void TDictionaryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("name", &TThis::Name);
    registrar.Parameter("source", &TThis::Source);
    registrar.Parameter("layout", &TThis::Layout);
    registrar.Parameter("structure", &TThis::Structure);
    registrar.Parameter("lifetime", &TThis::Lifetime);
}

////////////////////////////////////////////////////////////////////////////////

void TSystemLogConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("engine", &TThis::Engine)
        .Default("ENGINE = Buffer('system', 'query_log_older', 1, 1, 1800, 1000000000000, 1000000000000, 1000000000000, 1000000000000)");
    registrar.Parameter("flush_interval_milliseconds", &TThis::FlushIntervalMilliseconds)
        .Default(100);
}

////////////////////////////////////////////////////////////////////////////////

void TPocoInvalidCertificateHandlerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("name", &TThis::Name)
        .Default("RejectCertificateHandler");
}

////////////////////////////////////////////////////////////////////////////////

void TPocoOpenSslConfigEntry::Register(TRegistrar registrar)
{
    // NOTE(dakovalkov): options names are in camelCase because they correspond to Poco::Net::SSLManager's options.
    // https://docs.pocoproject.org/current/Poco.Net.SSLManager.html
    registrar.Parameter("invalidCertificateHandler", &TThis::InvalidCertificateHandler)
        .DefaultNew();
    registrar.Parameter("caConfig", &TThis::CAConfig)
        .Default("/etc/ssl/certs");
}

////////////////////////////////////////////////////////////////////////////////

void TPocoOpenSslConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("server", &TThis::Server)
        .DefaultNew();
    registrar.Parameter("client", &TThis::Client)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TClickHouseConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("users", &TThis::Users)
        .DefaultNew();

    registrar.Parameter("data_path", &TThis::DataPath)
        .Default("./data");

    registrar.Parameter("log_level", &TThis::LogLevel)
        .Default("trace");

    registrar.Parameter("dictionaries", &TThis::Dictionaries)
        .Default();

    registrar.Parameter("path_to_regions_hierarchy_file", &TThis::PathToRegionsHierarchyFile)
        .Default();

    registrar.Parameter("path_to_regions_name_files", &TThis::PathToRegionsNameFiles)
        .Default();

    registrar.Parameter("timezone", &TThis::Timezone)
        .Default("Europe/Moscow");

    registrar.Parameter("query_log", &TThis::QueryLog)
        .DefaultNew();

    registrar.Parameter("query_thread_log", &TThis::QueryLog)
        .DefaultNew();

    registrar.Parameter("part_log", &TThis::QueryLog)
        .DefaultNew();

    registrar.Parameter("max_concurrent_queries", &TThis::MaxConcurrentQueries)
        .Default(0);

    registrar.Parameter("max_connections", &TThis::MaxConnections)
        .Default(1024);

    registrar.Parameter("max_thread_pool_size", &TThis::MaxThreadPoolSize)
        .Default(10000);
    registrar.Parameter("max_thread_pool_free_size", &TThis::MaxThreadPoolFreeSize)
        .Default(1000);
    registrar.Parameter("thread_pool_queue_size", &TThis::ThreadPoolQueueSize)
        .Default(10000);

    registrar.Parameter("max_io_thread_pool_size", &TThis::MaxIOThreadPoolSize)
        .Default(100);
    registrar.Parameter("max_io_thread_pool_free_size", &TThis::MaxIOThreadPoolFreeSize)
        .Default(0);
    registrar.Parameter("io_thread_pool_queue_size", &TThis::IOThreadPoolQueueSize)
        .Default(10000);

    registrar.Parameter("keep_alive_timeout", &TThis::KeepAliveTimeout)
        .Default(10);

    registrar.Parameter("tcp_port", &TThis::TcpPort)
        .Default(0);
    registrar.Parameter("http_port", &TThis::HttpPort)
        .Default(0);

    registrar.Parameter("settings", &TThis::Settings)
        .DefaultCtor([] {
            THashMap<TString, NYTree::INodePtr> map;
            map["max_memory_usage_for_all_queries"] = NYTree::ConvertToNode(9_GB);
            map["max_threads"] = NYTree::ConvertToNode(32);
            map["max_concurrent_queries_for_user"] = NYTree::ConvertToNode(10);
            map["connect_timeout_with_failover_ms"] = NYTree::ConvertToNode(1000); // 1 sec.
            map["log_queries"] = NYTree::ConvertToNode(1);
            map["optimize_move_to_prewhere"] = NYTree::ConvertToNode(0);
            // CH hedged requests use their own poller implementation over epoll, which is kind of
            // broken around our 2.04 branch (it imposes busy loop in polling thread).
            map["use_hedged_requests"] = NYTree::ConvertToNode(0);
            return map;
        });

    registrar.Parameter("max_server_memory_usage", &TThis::MaxServerMemoryUsage)
        .Default();

    // NOTE(dakovalkov): this option name correspond to Poco::Net::SSLManager's config option.
    // https://docs.pocoproject.org/current/Poco.Net.SSLManager.html
    registrar.Parameter("openSSL", &TThis::OpenSsl)
        .DefaultNew();

    registrar.Parameter("query_masking_rules", &TThis::QueryMaskingRules)
        .DefaultCtor([] {
            return NYTree::BuildYsonNodeFluently()
                .BeginMap()
                    // NB: all items should start with "rule".
                    .Item("rule_0")
                        .BeginMap()
                            .Item("regexp").Value(R"regexp(remote\([\s\S]*?\))regexp")
                            .Item("replace").Value("remote(...)")
                        .EndMap()
                    .Item("rule_1")
                        .BeginMap()
                            .Item("regexp").Value(R"regexp(remoteSecure\([\s\S]*?\))regexp")
                            .Item("replace").Value("remoteSecure(...)")
                        .EndMap()
                    .Item("rule_2")
                        .BeginMap()
                            .Item("regexp").Value(R"regexp(url\([\s\S]*?\))regexp")
                            .Item("replace").Value("url(...)")
                        .EndMap()
                .EndMap()->AsMap();
        })
        .ResetOnLoad();

    registrar.Postprocessor([] (TThis* config) {
        auto& userDefaultProfile = config->Users->Profiles["default"];
        for (auto& [key, value] : config->Settings) {
            userDefaultProfile[key] = value;
        }

        config->Settings = userDefaultProfile;

        // See DB::Context::setPath.
        if (config->DataPath.empty() || config->DataPath.back() != '/') {
            config->DataPath.push_back('/');
        }
    });

    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
