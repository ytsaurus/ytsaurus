#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/library/auth/auth.h>

#include <yt/yt/core/ytree/fluent.h>

#include <array>
#include <utility>

namespace NYT::NYqlAgent {

using namespace NSecurityClient;
using namespace NAuth;
using namespace NYTree;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr std::array<std::pair<TStringBuf, TStringBuf>, 56> DefaultGatewaySettings{{
    {"DefaultCalcMemoryLimit", "1G"},
    {"EvaluationTableSizeLimit", "1M"},
    {"DefaultMaxJobFails", "5"},
    {"DefaultMemoryLimit", "512m"},
    {"MapJoinLimit", "2048m"},
    {"ClientMapTimeout", "10s"},
    {"MapJoinShardCount", "4"},
    {"CommonJoinCoreLimit", "128m"},
    {"CombineCoreLimit", "128m"},
    {"SwitchLimit", "128m"},
    {"JoinMergeTablesLimit", "64"},
    {"DataSizePerJob", "1g"},
    {"MaxJobCount", "16384"},
    {"PublishedCompressionCodec", "zstd_5"},
    {"TemporaryCompressionCodec", "zstd_5"},
    {"PublishedErasureCodec", "none"},
    {"TemporaryErasureCodec", "none"},
    {"OptimizeFor", "scan"},
    {"PythonCpu", "4.0"},
    {"JavascriptCpu", "4.0"},
    {"ErasureCodecCpu", "5.0"},
    {"AutoMerge", "relaxed"},
    {"QueryCacheMode", "normal"},
    {"QueryCacheSalt", "YQL-10935"},
    {"UseSkiff", "1"},
    {"MaxInputTables", "1000"},
    {"MaxInputTablesForSortedMerge", "100"},
    {"MaxOutputTables", "50"},
    {"InferSchemaTableCountThreshold", "50"},
    {"MaxExtraJobMemoryToFuseOperations", "3g"},
    {"UseColumnarStatistics", "auto"},
    {"ParallelOperationsLimit", "16"},
    {"ReleaseTempData", "immediate"},
    {"LookupJoinLimit", "1M"},
    {"LookupJoinMaxRows", "900"},
    {"MaxReplicationFactorToFuseOperations", "20.0"},
    {"LLVMMemSize", "256M"},
    {"LLVMPerNodeMemSize", "10K"},
    {"JoinEnableStarJoin", "true"},
    {"FolderInlineItemsLimit", "200"},
    {"FolderInlineDataLimit", "500K"},
    {"WideFlowLimit", "101"},
    {"NativeYtTypeCompatibility", "complex,date,null,void,date,float,json,decimal"},
    {"MapJoinUseFlow", "1"},
    {"HybridDqDataSizeLimitForOrdered", "384M"},
    {"HybridDqDataSizeLimitForUnordered", "8G"},
    {"UseYqlRowSpecCompactForm", "false"},
    {"_UseKeyBoundApi", "false"},
    {"UseNewPredicateExtraction", "true"},
    {"PruneKeyFilterLambda", "true"},
    {"JoinCommonUseMapMultiOut", "true"},
    {"UseAggPhases", "true"},
    {"EnforceJobUtc", "true"},
    {"_ForceJobSizeAdjuster", "true"},
    {"_EnableWriteReorder", "true"},
    {"_EnableYtPartitioning", "true"},
}};

constexpr std::array<std::pair<TStringBuf, TStringBuf>, 2> DefaultClusterSettings{{
    {"QueryCacheChunkLimit", "100000"},
    {"_UseKeyBoundApi", "true"},
}};

////////////////////////////////////////////////////////////////////////////////

IListNodePtr MergeDefaultSettings(const IListNodePtr& settings, const auto& defaults)
{
    auto result = CloneNode(settings)->AsList();

    THashSet<TString> presentSettings;
    for (const auto& setting : settings->GetChildren()) {
        presentSettings.insert(setting->AsMap()->GetChildOrThrow("name")->GetValue<TString>());
    }

    for (const auto& [name, value] : defaults) {
        if (presentSettings.contains(name)) {
            continue;
        }
        auto setting = BuildYsonNodeFluently()
            .BeginMap()
                .Item("name").Value(name)
                .Item("value").Value(value)
            .EndMap();
        result->AddChild(std::move(setting));
    }

    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

IListNodePtr TYqlPluginConfig::MergeClusterDefaultSettings(const IListNodePtr& clusterConfigSettings)
{
    return MergeDefaultSettings(clusterConfigSettings, DefaultClusterSettings);
}

void TYqlPluginConfig::Register(TRegistrar registrar)
{
    auto defaultRemoteFilePatterns = BuildYsonNodeFluently()
        .BeginList()
            .Item().BeginMap()
                .Item("pattern").Value("yt://([a-zA-Z0-9\\-_]+)/([^&@?]+)$")
                .Item("cluster").Value("$1")
                .Item("path").Value("$2")
            .EndMap()
        .EndList();

    registrar.Parameter("gateway_config", &TThis::GatewayConfig)
        .Default(GetEphemeralNodeFactory()->CreateMap());
    registrar.Parameter("file_storage_config", &TThis::FileStorageConfig)
        .Default(GetEphemeralNodeFactory()->CreateMap());
    registrar.Parameter("operation_attributes", &TThis::OperationAttributes)
        .Default(GetEphemeralNodeFactory()->CreateMap());
    registrar.Parameter("yt_token_path", &TThis::YTTokenPath)
        .Default();
    registrar.Parameter("yql_plugin_shared_library", &TThis::YqlPluginSharedLibrary)
        .Default();

    registrar.Postprocessor([=] (TThis* config) {
        auto gatewayConfig = config->GatewayConfig->AsMap();
        gatewayConfig->AddChild("remote_file_patterns", defaultRemoteFilePatterns);
        gatewayConfig->AddChild("mr_job_bin", BuildYsonNodeFluently().Value("./mrjob"));
        gatewayConfig->AddChild("yt_log_level", BuildYsonNodeFluently().Value("YL_DEBUG"));
        gatewayConfig->AddChild("execute_udf_locally_if_possible", BuildYsonNodeFluently().Value(true));

        auto fileStorageConfig = config->FileStorageConfig->AsMap();
        fileStorageConfig->AddChild("max_files", BuildYsonNodeFluently().Value(1 << 13));
        fileStorageConfig->AddChild("max_size_mb", BuildYsonNodeFluently().Value(1 << 14));
        fileStorageConfig->AddChild("retry_count", BuildYsonNodeFluently().Value(3));

        auto gatewaySettings = gatewayConfig->FindChild("default_settings");
        if (!gatewaySettings) {
            gatewaySettings = GetEphemeralNodeFactory()->CreateList();
        } else {
            gatewayConfig->RemoveChild(gatewaySettings);
        }
        gatewaySettings = MergeDefaultSettings(gatewaySettings->AsList(), DefaultGatewaySettings);
        YT_VERIFY(gatewayConfig->AddChild("default_settings", std::move(gatewaySettings)));

        gatewayConfig->AddChild("cluster_mapping", GetEphemeralNodeFactory()->CreateList());
        for (const auto& cluster : gatewayConfig->GetChildOrThrow("cluster_mapping")->AsList()->GetChildren()) {
            auto clusterMap = cluster->AsMap();
            auto settings = clusterMap->FindChild("settings");
            if (settings) {
                clusterMap->RemoveChild(settings);
            } else {
                settings = GetEphemeralNodeFactory()->CreateList();
            }
            YT_VERIFY(clusterMap->AddChild("settings", MergeClusterDefaultSettings(settings->AsList())));
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("bus_client", &TThis::BusClient)
        .DefaultNew();
    registrar.Parameter("yql_thread_count", &TThis::YqlThreadCount)
        .Default(256);
}

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentDynamicConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("yql_agent", &TThis::YqlAgent)
        .DefaultNew();
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("user", &TThis::User)
        .Default(YqlAgentUserName);
    registrar.Parameter("cypress_annotations", &TThis::CypressAnnotations)
        .Default(BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());
    registrar.Parameter("root", &TThis::Root)
        .Default("//sys/yql_agent");
    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();
    registrar.Parameter("dynamic_config_path", &TThis::DynamicConfigPath)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (auto& dynamicConfigPath = config->DynamicConfigPath; dynamicConfigPath.empty()) {
            dynamicConfigPath = config->Root + "/config";
        }
    });
};

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentServerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("yql_agent", &TThis::YqlAgent)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
