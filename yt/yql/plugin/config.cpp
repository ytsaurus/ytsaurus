#include "config.h"

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/ytree/fluent.h>

#include <util/generic/hash_set.h>
#include <util/string/vector.h>

namespace NYT::NYqlPlugin {

using namespace NYTree;

using NSecurityClient::YqlAgentUserName;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto DefaultGatewaySettings = std::to_array<std::pair<TStringBuf, TStringBuf>>({
    {"DefaultCalcMemoryLimit", "1G"},
    {"EvaluationTableSizeLimit", "1M"},
    {"DefaultMaxJobFails", "5"},
    {"DefaultMemoryLimit", "512m"},
    {"MapJoinLimit", "2048m"},
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
    {"QueryCacheSalt", "YQLOVERYT-41"},
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
    {"NativeYtTypeCompatibility", "complex,date,null,void,date,float,json,decimal,uuid"},
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
    {"UseNativeYtTypes", "true"},
    {"UseNativeDynamicTableRead", "true"},
    {"RuntimeClusterSelection", "auto"},
    {"_EnableRLSTablesSupport", "true"},
    {"_EnableDynamicTablesWrite", "true"},
});

constexpr auto DefaultDQGatewaySettings = std::to_array<std::pair<TStringBuf, TStringBuf>>({
    {"EnableComputeActor", "1"},
    {"ComputeActorType", "async"},
    {"EnableStrip", "false"},
    {"EnableInsert", "true"},
    {"ChannelBufferSize", "1000000"},
    {"PullRequestTimeoutMs", "3000000"},
    {"PingTimeoutMs", "30000"},
    {"MaxTasksPerOperation", "100"},
    {"MaxTasksPerStage", "30"},
    {"AnalyzeQuery", "true"},
    {"EnableFullResultWrite", "true"},
    {"_FallbackOnRuntimeErrors", "DQ computation exceeds the memory limit,requirement data.GetRaw().size(),_Unwind_Resume,Cannot load time zone,Memory limit exceeded in MKQL runtime"},
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
    {"_RpcTimeout", "10s"},
    {"_MasterLockTimeout", "2m"},
    {"_MasterLockPingPeriod", "30s"},
    {"_FiniteStreams", "0"},
    {"_YtUseSourceWatermark", "0"},
    {"EnableComputationPatternResources", "false"},
    {"GracefulUpdate", "1"},
    {"UpdateTimeout", "600s"},
    {"ControllerCount", "1"},
    {"ControllerCpuLimit", "1.0"},
    {"ControllerMemoryLimit", "1G"},
    {"ControllerRpcPort", "10080"},
    {"ControllerMonitoringPort", "10081"},
    {"_ControllerWriteLogsToFile", "false"},
    {"_ControllerEnableStderrLogging", "true"},
    {"_ControllerLogLevel", "info"},
    {"WorkerCount", "1"},
    {"WorkerCpuLimit", "1.0"},
    {"WorkerMemoryLimit", "1G"},
    {"WorkerRpcPort", "10080"},
    {"WorkerMonitoringPort", "10081"},
    {"_WorkerWriteLogsToFile", "false"},
    {"_WorkerEnableStderrLogging", "true"},
    {"_WorkerLogLevel", "info"},
    {"_LogsDirectory", "logs"},
    {"YtConsumerVital", "false"},
    {"YtPartitionCount", "1"},
    {"LookupJoinInflightRowLimit", "100"},
    {"LookupJoinInflightLookupLimit", "5"},
    {"LookupJoinLookupTimeout", "10s"},
    {"_SwitchComputationNodeBufferSizeBytes", "0"},
    {"_RunVanillaOperation", "true"},
});

constexpr auto DefaultPQGatewaySettings = std::array<std::pair<TStringBuf, TStringBuf>, 0>{};

constexpr auto DefaultSolomonGatewaySettings = std::array<std::pair<TStringBuf, TStringBuf>, 0>{};

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

void TVanillaJobFile::Register(TRegistrar registrar)
{
    registrar.Parameter("name", &TThis::Name)
        .NonEmpty();
    registrar.Parameter("local_path", &TThis::LocalPath)
        .NonEmpty();
}

////////////////////////////////////////////////////////////////////////////////

void TDQYTBackend::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_name", &TThis::ClusterName)
        .Default().DontSerializeDefault();
    registrar.Parameter("proxy_address", &TThis::ProxyAddress)
        .Default().DontSerializeDefault();
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

    registrar.Postprocessor([] (TThis* config) {
        if (config->ClusterName.empty()) {
            THROW_ERROR_EXCEPTION("DQ backend cluster_name must not be empty");
        }
        if (config->JobsPerOperation == 0) {
            THROW_ERROR_EXCEPTION("DQ backend %Qv: jobs_per_operation must be positive", config->ClusterName);
        }
        if (config->MaxJobs == 0) {
            THROW_ERROR_EXCEPTION("DQ backend %Qv: max_jobs must be positive", config->ClusterName);
        }
        if (config->MaxJobs < config->JobsPerOperation) {
            THROW_ERROR_EXCEPTION(
                "DQ backend %Qv: max_jobs (%v) must not be less than jobs_per_operation (%v)",
                config->ClusterName,
                config->MaxJobs,
                config->JobsPerOperation);
        }
        if (config->MaxJobs % config->JobsPerOperation != 0) {
            THROW_ERROR_EXCEPTION(
                "DQ backend %Qv: max_jobs (%v) must be divisible by jobs_per_operation (%v)",
                config->ClusterName,
                config->MaxJobs,
                config->JobsPerOperation);
        }
        if (config->VanillaJobLite.empty()) {
            THROW_ERROR_EXCEPTION("DQ backend %Qv: vanilla_job_lite must not be empty", config->ClusterName);
        }
        if (config->VanillaJobCommand.empty()) {
            THROW_ERROR_EXCEPTION("DQ backend %Qv: vanilla_job_command must not be empty", config->ClusterName);
        }
        if (config->Prefix.empty()) {
            THROW_ERROR_EXCEPTION("DQ backend %Qv: prefix must not be empty", config->ClusterName);
        }
        if (config->TokenFile.empty()) {
            THROW_ERROR_EXCEPTION("DQ backend %Qv: token_file must not be empty", config->ClusterName);
        }
        if (config->UploadReplicationFactor == 0) {
            THROW_ERROR_EXCEPTION("DQ backend %Qv: upload_replication_factor must be positive", config->ClusterName);
        }
        if (config->CpuLimit <= 0) {
            THROW_ERROR_EXCEPTION("DQ backend %Qv: cpu_limit must be positive", config->ClusterName);
        }
        if (config->MemoryLimit <= 0) {
            THROW_ERROR_EXCEPTION("DQ backend %Qv: memory_limit must be positive", config->ClusterName);
        }
        if (config->CacheSize < 0) {
            THROW_ERROR_EXCEPTION("DQ backend %Qv: cache_size must not be negative", config->ClusterName);
        }
        if (config->UseTmpFs && config->CacheSize > config->MemoryLimit) {
            THROW_ERROR_EXCEPTION(
                "DQ backend %Qv: cache_size (%v) must not exceed memory_limit (%v) when use_tmp_fs is enabled",
                config->ClusterName,
                config->CacheSize,
                config->MemoryLimit);
        }
        if (config->WorkerCapacity <= 0) {
            THROW_ERROR_EXCEPTION("DQ backend %Qv: worker_capacity must be positive", config->ClusterName);
        }

        THashSet<TString> fileNames;
        for (const auto& file : config->VanillaJobFiles) {
            if (!fileNames.insert(file->Name).second) {
                THROW_ERROR_EXCEPTION(
                    "DQ backend %Qv: duplicate vanilla_job_file name %Qv",
                    config->ClusterName,
                    file->Name);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDQYTCoordinator::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_name", &TThis::ClusterName)
        .Default().DontSerializeDefault();
    registrar.Parameter("proxy_address", &TThis::ProxyAddress)
        .Default().DontSerializeDefault();
    registrar.Parameter("prefix", &TThis::Prefix)
        .Default("//sys/yql_agent/dq_coord");
    registrar.Parameter("token_file", &TThis::TokenFile)
        .Default();
    registrar.Parameter("user", &TThis::User)
        .Default();
    registrar.Parameter("debug_log_file", &TThis::DebugLogFile)
        .Default();

}

////////////////////////////////////////////////////////////////////////////////

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

    registrar.Postprocessor([] (TThis* config) {
        // DQ manager config is always present in the top-level config, including
        // installations where DQ is disabled.
        if (config->YTBackends.empty()) {
            return;
        }

        if (config->InterconnectPort == 0) {
            THROW_ERROR_EXCEPTION("DQ manager interconnect_port must be positive");
        }
        if (config->GrpcPort == 0) {
            THROW_ERROR_EXCEPTION("DQ manager grpc_port must be positive");
        }
        if (config->InterconnectPort == config->GrpcPort) {
            THROW_ERROR_EXCEPTION("DQ manager interconnect_port and grpc_port must be different");
        }
        if (config->ActorThreads == 0) {
            THROW_ERROR_EXCEPTION("DQ manager actor_threads must be positive");
        }
        if (!config->YTCoordinator) {
            THROW_ERROR_EXCEPTION("DQ coordinator config must be specified");
        }
        if (config->YTCoordinator->ClusterName.empty() && config->YTCoordinator->ProxyAddress.empty()) {
            THROW_ERROR_EXCEPTION("DQ coordinator requires either cluster_name or proxy_address");
        }
        if (config->YTCoordinator->Prefix.empty()) {
            THROW_ERROR_EXCEPTION("DQ coordinator prefix must not be empty");
        }
        if (config->YTCoordinator->TokenFile.empty()) {
            THROW_ERROR_EXCEPTION("DQ coordinator token_file must not be empty");
        }

        THashSet<TString> clusterNames;
        TString vanillaJobLite;
        constexpr ui32 WorkerNodeIdCount = 8192 - 512;
        if (config->YTBackends.size() > WorkerNodeIdCount) {
            THROW_ERROR_EXCEPTION(
                "Too many DQ backends: %v backends cannot share %v worker node IDs",
                config->YTBackends.size(),
                WorkerNodeIdCount);
        }
        const ui32 nodesPerBackend = config->YTBackends.empty()
            ? 0
            : WorkerNodeIdCount / config->YTBackends.size();
        for (const auto& backend : config->YTBackends) {
            if (!clusterNames.insert(backend->ClusterName).second) {
                THROW_ERROR_EXCEPTION("Duplicate DQ backend cluster_name %Qv", backend->ClusterName);
            }
            if (backend->MaxJobs > nodesPerBackend) {
                THROW_ERROR_EXCEPTION(
                    "DQ backend %Qv: max_jobs (%v) exceeds its worker node ID range (%v)",
                    backend->ClusterName,
                    backend->MaxJobs,
                    nodesPerBackend);
            }
            if (vanillaJobLite.empty()) {
                vanillaJobLite = backend->VanillaJobLite;
            } else if (backend->VanillaJobLite != vanillaJobLite) {
                THROW_ERROR_EXCEPTION(
                    "All DQ backends must use the same vanilla_job_lite; backend %Qv has %Qv instead of %Qv",
                    backend->ClusterName,
                    backend->VanillaJobLite,
                    vanillaJobLite);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TAdditionalSystemLib::Register(TRegistrar registrar)
{
    registrar.Parameter("file", &TThis::File)
        .Default();
}

void TProcessYqlPluginConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);

    registrar.Parameter("slot_count", &TThis::SlotCount)
        .Default(32);

    registrar.Parameter("slots_root_path", &TThis::SlotsRootPath)
        .Default("/yt/plugin_slots");

    registrar.Parameter("check_process_active_delay", &TThis::CheckProcessActiveDelay)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("default_request_timeout", &TThis::DefaultRequestTimeout)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("run_request_timeout", &TThis::RunRequestTimeout)
        .Default(TDuration::Days(7));

    registrar.Parameter("log_manager_template", &TThis::LogManagerTemplate)
        .DefaultNew();
}

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
    registrar.Parameter("pq_gateway", &TThis::PQGatewayConfig)
        .Alias("pq_gateway_config")
        .Default(GetEphemeralNodeFactory()->CreateMap())
        .ResetOnLoad();
    registrar.Parameter("solomon_gateway", &TThis::SolomonGatewayConfig)
        .Alias("solomon_gateway_config")
        .Default(GetEphemeralNodeFactory()->CreateMap())
        .ResetOnLoad();
    registrar.Parameter("file_storage", &TThis::FileStorageConfig)
        .Alias("file_storage_config")
        .Default(GetEphemeralNodeFactory()->CreateMap())
        .ResetOnLoad();
    registrar.Parameter("tvm", &TThis::TvmConfig)
        .Alias("tvm_config")
        .Default(GetEphemeralNodeFactory()->CreateMap())
        .ResetOnLoad();
    registrar.Parameter("yt_access_provider", &TThis::YtAccessProviderConfig)
        .Alias("yt_access_provider_config")
        .Default(GetEphemeralNodeFactory()->CreateMap())
        .ResetOnLoad();
    registrar.Parameter("operation_attributes", &TThis::OperationAttributes)
        .Default(GetEphemeralNodeFactory()->CreateMap())
        .ResetOnLoad();
    registrar.Parameter("yt_token_path", &TThis::YTTokenPath)
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
        if (config->EnableDQ && config->DQManagerConfig->YTBackends.empty()) {
            THROW_ERROR_EXCEPTION("DQ is enabled but no YT backends are configured");
        }

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

        auto pqGatewayConfig = config->PQGatewayConfig->AsMap();
        auto pqGatewaySettings = pqGatewayConfig->FindChild("default_settings");
        if (pqGatewaySettings) {
            pqGatewayConfig->RemoveChild(pqGatewaySettings);
        } else {
            pqGatewaySettings = GetEphemeralNodeFactory()->CreateList();
        }
        pqGatewaySettings = MergeDefaultSettings(pqGatewaySettings->AsList(), DefaultPQGatewaySettings);
        YT_VERIFY(pqGatewayConfig->AddChild("default_settings", std::move(pqGatewaySettings)));

        auto solomonGatewayConfig = config->SolomonGatewayConfig->AsMap();
        auto solomonGatewaySettings = solomonGatewayConfig->FindChild("default_settings");
        if (solomonGatewaySettings) {
            solomonGatewayConfig->RemoveChild(solomonGatewaySettings);
        } else {
            solomonGatewaySettings = GetEphemeralNodeFactory()->CreateList();
        }
        solomonGatewaySettings = MergeDefaultSettings(solomonGatewaySettings->AsList(), DefaultSolomonGatewaySettings);
        YT_VERIFY(solomonGatewayConfig->AddChild("default_settings", std::move(solomonGatewaySettings)));

        auto icSettingsConfig = config->DQManagerConfig->ICSettings->AsMap();
        auto closeOnIdleMs = icSettingsConfig->FindChild("close_on_idle_ms");
        if (!closeOnIdleMs) {
            icSettingsConfig->AddChild("close_on_idle_ms", BuildYsonNodeFluently().Value(0));
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TYqlPluginDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("gateways_config", &TThis::GatewaysConfig)
        .Default();
    registrar.Parameter("max_supported_yql_version", &TThis::MaxSupportedYqlVersion)
        .Default();
    registrar.Parameter("proto_gateways_configs", &TThis::ProtoGatewaysConfigs)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
