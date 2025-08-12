#include "config.h"

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/ytree/fluent.h>

#include <util/string/vector.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto DefaultGatewaySettings = std::to_array<std::pair<TStringBuf, TStringBuf>>({
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
    {"_EnforceRegexpProbabilityFail", "0"},
    {"_ForceJobSizeAdjuster", "true"},
    {"_EnableWriteReorder", "true"},
    {"_EnableYtPartitioning", "true"},
    {"HybridDqExecution", "true"},
    {"DQRPCReaderInflight", "1"},
});

constexpr auto DefaultDQGatewaySettings = std::to_array<std::pair<TStringBuf, TStringBuf>>({
    {"EnableComputeActor", "1"},
    {"ComputeActorType", "async"},
    {"EnableStrip", "true"},
    {"EnableInsert", "true"},
    {"ChannelBufferSize", "1000000"},
    {"PullRequestTimeoutMs", "3000000"},
    {"PingTimeoutMs", "30000"},
    {"MaxTasksPerOperation", "100"},
    {"MaxTasksPerStage", "30"},
    {"AnalyzeQuery", "true"},
    {"EnableFullResultWrite", "true"},
    {"_FallbackOnRuntimeErrors", "DQ computation exceeds the memory limit,requirement data.GetRaw().size(),_Unwind_Resume,Cannot load time zone"},
    {"MemoryLimit", "3G"},
    {"_EnablePrecompute", "1"},
    {"UseAggPhases", "true"},
    {"UseWideChannels", "true"},
    {"HashJoinMode","off"},
    {"UseFastPickleTransport","true"},
    {"UseOOBTransport","true"},
    {"_MaxAttachmentsSize","3221225472"},
});

constexpr auto DefaultClusterSettings = std::to_array<std::pair<TStringBuf, TStringBuf>>({
    {"QueryCacheChunkLimit", "100000"},
    {"_UseKeyBoundApi", "true"},
});

constexpr auto DefaultYtflowGatewaySettings = std::to_array<std::pair<TStringBuf, TStringBuf>>({
    {"GatewayThreads", "16"},
    {"GracefulUpdate", "1"},
    {"ControllerCount", "1"},
    {"ControllerMemoryLimit", "1G"},
    {"ControllerRpcPort", "10080"},
    {"ControllerMonitoringPort", "10081"},
    {"WorkerCount", "10"},
    {"WorkerMemoryLimit", "1G"},
    {"WorkerRpcPort", "10082"},
    {"WorkerMonitoringPort", "10083"},
    {"YtPartitionCount", "10"}
});

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

using NSecurityClient::YqlAgentUserName;

void TVanillaJobFile::Register(TRegistrar registrar)
{
    registrar.Parameter("name", &TThis::Name)
        .NonEmpty();
    registrar.Parameter("local_path", &TThis::LocalPath)
        .NonEmpty();
}

void TDQYTBackend::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_name", &TThis::ClusterName)
        .Default();
    registrar.Parameter("jobs_per_operation", &TThis::JobsPerOperation)
        .Default(5);
    registrar.Parameter("max_jobs", &TThis::MaxJobs)
        .Default(150);
    registrar.Parameter("vanilla_job_lite", &TThis::VanillaJobLite)
        .Default();
    registrar.Parameter("vanilla_job_command", &TThis::VanillaJobCommand)
        .Default("./dq_vanilla_job");
    registrar.Parameter("vanilla_job_file", &TThis::VanillaJobFiles)
        .Default();
    registrar.Parameter("prefix", &TThis::Prefix)
        .Default("//sys/yql_agent/dq/data");
    registrar.Parameter("upload_replication_factor", &TThis::UploadReplicationFactor)
        .Default(7);
    registrar.Parameter("token_file", &TThis::TokenFile)
        .Default();
    registrar.Parameter("user", &TThis::User)
        .Default();
    registrar.Parameter("pool", &TThis::Pool)
        .Default();
    registrar.Parameter("pool_trees", &TThis::PoolTrees)
        .Default({});
    registrar.Parameter("owner", &TThis::Owner)
        // TODO(babenko): migrate to std::string
        .Default({TString(YqlAgentUserName)});
    registrar.Parameter("cpu_limit", &TThis::CpuLimit)
        .Default(6);
    registrar.Parameter("worker_capacity", &TThis::WorkerCapacity)
        .Default(24);
    registrar.Parameter("memory_limit", &TThis::MemoryLimit)
        .Default(64424509440);
    registrar.Parameter("cache_size", &TThis::CacheSize)
        .Default(6000000000);
    registrar.Parameter("use_tmp_fs", &TThis::UseTmpFs)
        .Default(true);
    registrar.Parameter("network_project", &TThis::NetworkProject)
        .Default("");
    registrar.Parameter("can_use_compute_actor", &TThis::CanUseComputeActor)
        .Default(true);
    registrar.Parameter("enforce_job_utc", &TThis::EnforceJobUtc)
        .Default(true);
    registrar.Parameter("use_local_l_d_library_path", &TThis::UseLocalLDLibraryPath)
        .Default(false);
    registrar.Parameter("scheduling_tag_filter", &TThis::SchedulingTagFilter)
        .Default({});
}

void TDQYTCoordinator::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_name", &TThis::ClusterName)
        .Default();
    registrar.Parameter("prefix", &TThis::Prefix)
        .Default("//sys/yql_agent/dq_coord");
    registrar.Parameter("token_file", &TThis::TokenFile)
        .Default();
    registrar.Parameter("user", &TThis::User)
        .Default();
    registrar.Parameter("debug_log_file", &TThis::DebugLogFile)
        .Default();
}

void TDQManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("interconnect_port", &TThis::InterconnectPort)
        .Default();
    registrar.Parameter("grpc_port", &TThis::GrpcPort)
        .Default();
    registrar.Parameter("actor_threads", &TThis::ActorThreads)
        .Default(4);
    registrar.Parameter("use_ipv4", &TThis::UseIPv4)
        .Default(false);
    registrar.Parameter("address_resolver", &TThis::AddressResolver)
        .Default();

    registrar.Parameter("yt_backends", &TThis::YTBackends)
        .Default();
    registrar.Parameter("yt_coordinator", &TThis::YTCoordinator)
        .DefaultNew();
    registrar.Parameter("interconnect_settings", &TThis::ICSettings)
        .Default(GetEphemeralNodeFactory()->CreateMap());
}

////////////////////////////////////////////////////////////////////////////////

IListNodePtr TYqlPluginConfig::MergeClusterDefaultSettings(const IListNodePtr& clusterConfigSettings)
{
    return MergeDefaultSettings(clusterConfigSettings, DefaultClusterSettings);
}

void TAdditionalSystemLib::Register(TRegistrar registrar)
{
    registrar.Parameter("file", &TThis::File)
        .Default();
}

void TYqlProcessPluginConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);

    registrar.Parameter("slots_count", &TThis::SlotsCount)
        .Default(32);

    registrar.Parameter("slots_root_path", &TThis::SlotsRootPath)
        .Default("/yt/plugin_slots");

    registrar.Parameter("check_process_active_delay", &TThis::CheckProcessActiveDelay)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("log_manager_template", &TThis::LogManagerTemplate)
        .DefaultNew();
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

    registrar.Parameter("gateway", &TThis::GatewayConfig)
        .Alias("gateway_config")
        .Default(GetEphemeralNodeFactory()->CreateMap())
        .ResetOnLoad();
    registrar.Parameter("dq_gateway", &TThis::DQGatewayConfig)
        .Alias("dq_gateway_config")
        .Default(GetEphemeralNodeFactory()->CreateMap())
        .ResetOnLoad();
    registrar.Parameter("ytflow_gateway", &TThis::YtflowGatewayConfig)
        .Alias("ytflow_gateway_config")
        .Default(GetEphemeralNodeFactory()->CreateMap())
        .ResetOnLoad();
    registrar.Parameter("file_storage", &TThis::FileStorageConfig)
        .Alias("file_storage_config")
        .Default(GetEphemeralNodeFactory()->CreateMap())
        .ResetOnLoad();
    registrar.Parameter("operation_attributes", &TThis::OperationAttributes)
        .Default(GetEphemeralNodeFactory()->CreateMap())
        .ResetOnLoad();
    registrar.Parameter("yt_token_path", &TThis::YTTokenPath)
        .Default();
    registrar.Parameter("ui_origin", &TThis::UIOrigin)
        .Default();
    registrar.Parameter("yql_plugin_shared_library", &TThis::YqlPluginSharedLibrary)
        .Default();
    registrar.Parameter("additional_system_libs", &TThis::AdditionalSystemLibs)
        .Default();
    registrar.Parameter("dq_manager", &TThis::DQManagerConfig)
        .Alias("dq_manager_config")
        .DefaultNew();
    registrar.Parameter("enable_dq", &TThis::EnableDQ)
        .Default(false);
    registrar.Parameter("libraries", &TThis::Libraries)
        .Default();

    registrar.Parameter("process_plugin_config", &TThis::ProcessPluginConfig)
        .DefaultNew();

    registrar.Postprocessor([=] (TThis* config) {
        auto gatewayConfig = config->GatewayConfig->AsMap();
        gatewayConfig->AddChild("remote_file_patterns", defaultRemoteFilePatterns);
        gatewayConfig->AddChild("mr_job_bin", BuildYsonNodeFluently().Value("./mrjob"));
        gatewayConfig->AddChild("yt_log_level", BuildYsonNodeFluently().Value("YL_DEBUG"));
        gatewayConfig->AddChild("execute_udf_locally_if_possible", BuildYsonNodeFluently().Value(false));

        auto fileStorageConfig = config->FileStorageConfig->AsMap();
        fileStorageConfig->AddChild("max_files", BuildYsonNodeFluently().Value(1 << 13));
        fileStorageConfig->AddChild("max_size_mb", BuildYsonNodeFluently().Value(1 << 14));
        fileStorageConfig->AddChild("retry_count", BuildYsonNodeFluently().Value(3));

        auto gatewaySettings = gatewayConfig->FindChild("default_settings");
        if (gatewaySettings) {
            gatewayConfig->RemoveChild(gatewaySettings);
        } else {
            gatewaySettings = GetEphemeralNodeFactory()->CreateList();
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

        if (!config->AdditionalSystemLibs.empty()) {
            auto mrJobSystemLibs = GetEphemeralNodeFactory()->CreateList();
            for (const auto& lib : config->AdditionalSystemLibs) {
                auto file = BuildYsonNodeFluently()
                    .BeginMap()
                        .Item("file").Value(lib->File)
                    .EndMap();
                mrJobSystemLibs->AddChild(std::move(file));
            }

            gatewayConfig->AddChild("mr_job_system_libs_with_md5", std::move(mrJobSystemLibs));

            for (auto& backend : config->DQManagerConfig->YTBackends) {
                for (const auto& lib : config->AdditionalSystemLibs) {
                    auto pathParts = SplitString(lib->File, "/");

                    TVanillaJobFilePtr vanillaJobFile = New<TVanillaJobFile>();
                    vanillaJobFile->Name = pathParts.back(),
                    vanillaJobFile->LocalPath = lib->File,

                    backend->VanillaJobFiles.emplace_back(std::move(vanillaJobFile));
                }
                backend->UseLocalLDLibraryPath = true;
            }
        }

        auto dqGatewayConfig = config->DQGatewayConfig->AsMap();
        auto dqGatewaySettings = dqGatewayConfig->FindChild("default_settings");
        if (dqGatewaySettings) {
            dqGatewayConfig->RemoveChild(dqGatewaySettings);
        } else {
            dqGatewaySettings = GetEphemeralNodeFactory()->CreateList();
        }
        dqGatewaySettings = MergeDefaultSettings(dqGatewaySettings->AsList(), DefaultDQGatewaySettings);
        YT_VERIFY(dqGatewayConfig->AddChild("default_settings", std::move(dqGatewaySettings)));

        dqGatewayConfig->AddChild("default_auto_percentage", BuildYsonNodeFluently().Value(100));

        auto ytflowGatewayConfig = config->YtflowGatewayConfig->AsMap();
        auto ytflowGatewaySettings = ytflowGatewayConfig->FindChild("default_settings");
        if (ytflowGatewaySettings) {
            ytflowGatewayConfig->RemoveChild(ytflowGatewaySettings);
        } else {
            ytflowGatewaySettings = GetEphemeralNodeFactory()->CreateList();
        }
        ytflowGatewaySettings = MergeDefaultSettings(ytflowGatewaySettings->AsList(), DefaultYtflowGatewaySettings);
        YT_VERIFY(ytflowGatewayConfig->AddChild("default_settings", std::move(ytflowGatewaySettings)));

        auto icSettingsConfig = config->DQManagerConfig->ICSettings->AsMap();
        auto closeOnIdleMs = icSettingsConfig->FindChild("close_on_idle_ms");
        if (!closeOnIdleMs) {
            icSettingsConfig->AddChild("close_on_idle_ms", BuildYsonNodeFluently().Value(0));
        }
    });
}

} // namespace NYT::NYqlPlugin