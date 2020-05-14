#include "clickhouse_config.h"

#include <yt/core/ytree/fluent.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TSystemLogConfig::TSystemLogConfig()
{
    RegisterParameter("engine", Engine)
        .Default("ENGINE = Memory()");
    RegisterParameter("flush_interval_milliseconds", FlushIntervalMilliseconds)
        .Default(100);
}

////////////////////////////////////////////////////////////////////////////////

TUserConfig::TUserConfig()
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

////////////////////////////////////////////////////////////////////////////////

TDictionarySourceYtConfig::TDictionarySourceYtConfig()
{
    RegisterParameter("path", Path);
}

////////////////////////////////////////////////////////////////////////////////

TDictionarySourceConfig::TDictionarySourceConfig()
{
    RegisterParameter("yt", Yt)
        .Default(nullptr);
}

////////////////////////////////////////////////////////////////////////////////

TDictionaryConfig::TDictionaryConfig()
{
    RegisterParameter("name", Name);
    RegisterParameter("source", Source);
    RegisterParameter("layout", Layout);
    RegisterParameter("structure", Structure);
    RegisterParameter("lifetime", Lifetime);
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseConfig::TClickHouseConfig()
{
    RegisterParameter("users", Users)
        .DefaultNew();

    RegisterParameter("data_path", DataPath)
        .Default("./data");

    RegisterParameter("log_level", LogLevel)
        .Default("trace");

    RegisterParameter("dictionaries", Dictionaries)
        .Default();

    RegisterParameter("path_to_regions_hierarchy_file", PathToRegionsHierarchyFile)
        .Default();

    RegisterParameter("path_to_regions_name_files", PathToRegionsNameFiles)
        .Default();

    RegisterParameter("query_log", QueryLog)
        .DefaultNew();

    RegisterParameter("query_thread_log", QueryLog)
        .DefaultNew();

    RegisterParameter("part_log", QueryLog)
        .DefaultNew();

    RegisterParameter("max_concurrent_queries", MaxConcurrentQueries)
        .Default(0);

    RegisterParameter("max_connections", MaxConnections)
        .Default(1024);

    RegisterParameter("keep_alive_timeout", KeepAliveTimeout)
        .Default(10);

    RegisterParameter("tcp_port", TcpPort)
        .Default(0);
    RegisterParameter("http_port", HttpPort)
        .Default(0);

    RegisterParameter("settings", Settings)
        .Optional()
        .MergeBy(NYTree::EMergeStrategy::Combine);

    RegisterPreprocessor([&] {
        Settings["max_memory_usage_for_all_queries"] = NYTree::ConvertToNode(9_GB);
        Settings["max_threads"] = NYTree::ConvertToNode(32);
        Settings["max_concurrent_queries_for_user"] = NYTree::ConvertToNode(10);
        Settings["connect_timeout_with_failover_ms"] = NYTree::ConvertToNode(1000); // 1 sec.
    });

    RegisterPostprocessor([&] {
        auto& userDefaultProfile = Users->Profiles["default"];
        for (auto& [key, value] : Settings) {
            userDefaultProfile[key] = value;
        }

        Settings = userDefaultProfile;

        // See DB::Context::setPath.
        if (DataPath.empty() || DataPath.back() != '/') {
            DataPath.push_back('/');
        }
    });

    SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
