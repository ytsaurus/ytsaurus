#include "vanilla_launcher.h"

#include "config.h"

#include <yt/yt/flow/library/cpp/vanilla/current_operation.h>
#include <yt/yt/flow/library/cpp/vanilla/files.h>
#include <yt/yt/flow/library/cpp/vanilla/spec.h>

#include <yt/yt/flow/library/cpp/client/pipeline.h>

#include <yt/yt/flow/lib/native_client/public.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/cache/rpc.h>

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/library/auth/auth.h>

#include <util/generic/size_literals.h>

#include <util/system/env.h>
#include <util/system/execpath.h>
#include <util/system/fstat.h>
#include <util/system/tempfile.h>

#include <sys/stat.h>

#include <util/stream/file.h>

#include <functional>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

// Fixed ports inside a vanilla job. Network-isolated jobs (own IP) don't collide, and
// the monitoring resolver can target a known port. Chosen outside the privileged (<1024)
// and ephemeral (>=32768) ranges. The user can still override them via the node_config patch.
constexpr int DefaultRpcPort = 10080;
constexpr int DefaultMonitoringPort = 10081;

// In-job file names for the binary and the node config; also the keys under vanilla/files.
constexpr TStringBuf BinaryFileName = "flow_server";
constexpr TStringBuf NodeConfigFileName = "node_config";

// Default task limits applied when the config leaves them unset (controllers and workers share these).
constexpr i64 DefaultMemoryLimit = 18_GB;
constexpr int DefaultCpuLimit = 6;

const NLogging::TLogger Logger("FlowVanillaLauncher");

void ShutdownPriorVanillaOperation(
    const NApi::IClientPtr& pipelineClient,
    const NYPath::TYPath& pipelinePath,
    TDuration waitTimeout)
{
    auto pipelineExists = WaitFor(pipelineClient->NodeExists(pipelinePath)).ValueOrThrow();
    if (!pipelineExists) {
        return;
    }
    auto prior = ReadVanillaOperationManifest(pipelineClient, pipelinePath);
    if (!prior) {
        return;
    }
    // The vanilla op may live on a different cluster than the pipeline node — query it there with
    // the recorded role. Spinning up a separate client for the recorded cluster is cheap.
    auto opClient = NClient::NCache::CreateClient(prior->Cluster, prior->ProxyRole);

    NScheduler::TOperationIdOrAlias opIdOrAlias{TString(prior->Alias)};
    NApi::TGetOperationOptions getOpts;
    getOpts.Attributes = THashSet<std::string>{"state", "id"};
    // A running operation's alias only resolves through scheduler runtime state.
    getOpts.IncludeRuntime = true;
    NApi::TOperation opInfo;
    try {
        opInfo = WaitFor(opClient->GetOperation(opIdOrAlias, getOpts)).ValueOrThrow();
    } catch (const std::exception& ex) {
        YT_LOG_INFO(ex, "Prior vanilla operation lookup failed (Alias: %v)", prior->Alias);
        return;
    }
    if (!opInfo.State || IsVanillaOperationStateTerminal(*opInfo.State)) {
        return;
    }
    bool graceful = IsGracefulUpdateFromEnv();
    YT_LOG_INFO("Shutting down prior vanilla operation (Alias: %v, OperationId: %v, State: %v, Graceful: %v)",
        prior->Alias,
        opInfo.Id,
        *opInfo.State,
        graceful);
    if (graceful) {
        WaitFor(pipelineClient->StopPipeline(pipelinePath)).ThrowOnError();
        WaitPipelineState(pipelineClient, pipelinePath, EPipelineState::Stopped, waitTimeout);
    } else {
        WaitFor(pipelineClient->PausePipeline(pipelinePath)).ThrowOnError();
        WaitPipelineState(pipelineClient, pipelinePath, EPipelineState::Paused, waitTimeout);
    }
    WaitFor(opClient->AbortOperation(opIdOrAlias)).ThrowOnError();
}

//! Logging config for a vanilla job: a rotated, zstd-compressed info-level log under the job
//! sandbox's `logs/` directory so a job-shell user sees the full controller/worker history
//! (the job stderr only keeps a short tail), plus a stderr writer for crash traces.
NLogging::TLogManagerConfigPtr BuildVanillaJobLoggingConfig()
{
    // clang-format off
    auto node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("writers").BeginMap()
                .Item("stderr").BeginMap()
                    .Item("type").Value("stderr")
                    .Item("format").Value("plain_text")
                .EndMap()
                .Item("file").BeginMap()
                    .Item("type").Value("file")
                    .Item("file_name").Value("logs/flow.log")
                    // Compress rotated segments: a few GB of zstd holds a long history.
                    .Item("enable_compression").Value(true)
                    .Item("compression_method").Value("zstd")
                    .Item("rotation_policy").BeginMap()
                        // Rotate at 256 MB (uncompressed); keep up to 4 GB of compressed segments.
                        .Item("max_segment_size").Value(256_MB)
                        .Item("max_total_size_to_keep").Value(4_GB)
                    .EndMap()
                .EndMap()
            .EndMap()
            .Item("rules").BeginList()
                .Item().BeginMap()
                    .Item("min_level").Value("info")
                    // Drop chatty infrastructure categories so the budget holds pipeline history.
                    .Item("exclude_categories").BeginList()
                        .Item().Value("BufferMetrics")
                        .Item().Value("Bus")
                        .Item().Value("Concurrency")
                        .Item().Value("Dns")
                        .Item().Value("Jaeger")
                        .Item().Value("Monitoring")
                        .Item().Value("Net")
                        .Item().Value("Profiling")
                        .Item().Value("QueryClient")
                        .Item().Value("RpcClient")
                        .Item().Value("RpcProxyClient")
                        .Item().Value("RpcServer")
                        .Item().Value("Solomon")
                    .EndList()
                    .Item("writers").BeginList().Item().Value("file").EndList()
                .EndMap()
                .Item().BeginMap()
                    .Item("min_level").Value("error")
                    .Item("writers").BeginList().Item().Value("stderr").EndList()
                .EndMap()
            .EndList()
        .EndMap();
    // clang-format on
    return ConvertTo<NLogging::TLogManagerConfigPtr>(std::move(node));
}

//! Whether the local file carries an executable bit; a job file delivered with one (the flow binary,
//! a python/java companion) must stay executable in the sandbox so the worker can run it.
bool IsLocalFileExecutable(const std::string& path)
{
    return (TFileStat(TString(path)).Mode & (S_IXUSR | S_IXGRP | S_IXOTH)) != 0;
}

//! The YSON-serialized YT_PROXY_URL_ALIASING_CONFIG to propagate into the job environment: the
//! explicitly configured rules if any, otherwise the launcher's own env (so a job that itself runs
//! under aliasing passes the same rules down to its child operation).
std::optional<std::string> BuildProxyUrlAliasingConfig(const THashMap<std::string, std::string>& rules)
{
    if (!rules.empty()) {
        auto yson = ConvertToYsonString(rules).ToString();
        return std::string(yson.data(), yson.size());
    }
    if (auto fromEnv = GetEnv("YT_PROXY_URL_ALIASING_CONFIG"); !fromEnv.empty()) {
        return std::string(fromEnv.data(), fromEnv.size());
    }
    return std::nullopt;
}

//! The forced binary checksum to propagate into the job environment. The launcher writes the
//! pipeline's flow core target from its own version, so jobs that resolved a different checksum
//! would never match that target: whoever forces the value must force the same one downstream.
std::optional<std::string> GetBinaryChecksumOverride()
{
    if (auto fromEnv = GetEnv("YT_FLOW_BINARY_CHECKSUM_OVERRIDE"); !fromEnv.empty()) {
        return std::string(fromEnv.data(), fromEnv.size());
    }
    return std::nullopt;
}

//! Builds one task of the operation spec; shared by the pipeline launcher and the external entry
//! point. `resolveFile` maps an uploadable local file (the binary, the node config, user local
//! files) to the Cypress path the running operation reads it from. When `localBinaryPath` is set
//! (local-mode cluster), the job runs the binary straight from that on-disk path and it is not
//! uploaded — the exec node shares this host's filesystem, so a multi-GB copy per job is pure waste.
TVanillaTaskSpec BuildTaskSpec(
    const std::string& name,
    const std::string& flowMode,
    const TFlowVanillaTask& task,
    const std::optional<std::string>& networkProject,
    const std::string& nodeConfigPath,
    const std::optional<std::string>& localBinaryPath,
    const std::function<NYPath::TYPath(const std::string& fileName, const std::string& localPath, bool executable)>& resolveFile)
{
    TVanillaTaskSpec taskSpec;
    taskSpec.Name = name;
    taskSpec.FlowMode = flowMode;
    taskSpec.JobCount = task.JobCount;
    taskSpec.MemoryLimit = task.MemoryLimit;
    taskSpec.CpuLimit = task.CpuLimit;
    taskSpec.PortCount = task.PortCount;
    taskSpec.Command = localBinaryPath
        ? Format("%v --config %v", *localBinaryPath, NodeConfigFileName)
        : Format("./%v --config %v", BinaryFileName, NodeConfigFileName);
    taskSpec.NetworkProject = networkProject;
    taskSpec.SystemLayerPath = task.SystemLayerPath;
    taskSpec.Environment = task.Environment;
    for (const auto& layer : task.Layers) {
        taskSpec.Layers.push_back(NYPath::TYPath(layer));
    }

    std::vector<std::pair<std::string, std::string>> uploads;
    if (!localBinaryPath) {
        uploads.push_back({std::string(BinaryFileName), GetExecPath()});
    }
    uploads.push_back({std::string(NodeConfigFileName), nodeConfigPath});
    for (const auto& [fileName, localPath] : task.LocalFiles) {
        uploads.emplace_back(fileName, localPath);
    }
    for (const auto& [fileName, localPath] : uploads) {
        bool executable = IsLocalFileExecutable(localPath);
        taskSpec.Files.push_back({fileName, resolveFile(fileName, localPath, executable), executable});
    }
    // External Cypress file references are passed through verbatim and never copied.
    for (const auto& [fileName, cypressPath] : task.CypressFiles) {
        taskSpec.Files.push_back({fileName, NYPath::TYPath(cypressPath), /*executable*/ false});
    }
    return taskSpec;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TVanillaTaskConfig::Register(TRegistrar registrar)
{
    // Required: count. Memory/cpu are optional; the launcher fills task-specific defaults.
    registrar.Parameter("count", &TThis::Count)
        .GreaterThan(0);
    registrar.Parameter("memory_limit", &TThis::MemoryLimit)
        .GreaterThan(NYTree::TSize(0))
        .Default();
    registrar.Parameter("cpu_limit", &TThis::CpuLimit)
        .GreaterThan(0)
        .Default();
    registrar.Parameter("port_count", &TThis::PortCount)
        .GreaterThan(0)
        .Default();
    registrar.Parameter("local_files", &TThis::LocalFiles)
        .Default();
    registrar.Parameter("cypress_files", &TThis::CypressFiles)
        .Default();
    registrar.Parameter("layers", &TThis::Layers)
        .Default();
    registrar.Parameter("system_layer_path", &TThis::SystemLayerPath)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TVanillaConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);

    // Required parameters — no default, fail YSON parsing if absent.
    registrar.Parameter("pool", &TThis::Pool)
        .NonEmpty();
    registrar.Parameter("worker", &TThis::Worker);

    registrar.Parameter("controller", &TThis::Controller)
        .DefaultCtor([] {
            auto config = New<TVanillaTaskConfig>();
            config->Count = 1;
            return config;
        });

    registrar.Parameter("runtime_cluster", &TThis::RuntimeCluster)
        .Default();
    registrar.Parameter("runtime_proxy_role", &TThis::RuntimeProxyRole)
        .Default();

    registrar.Parameter("cache_path", &TThis::CachePath)
        .NonEmpty()
        .Default(TString(VanillaFileCachePath));

    registrar.Parameter("max_failed_job_count", &TThis::MaxFailedJobCount)
        .Default(10000);
    registrar.Parameter("wait_timeout", &TThis::WaitTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("solomon_resolver_tag", &TThis::SolomonResolverTag)
        .Default("ytflow_vanilla_common");

    registrar.Parameter("alias", &TThis::Alias)
        .Default();
    registrar.Parameter("title", &TThis::Title)
        .Default();
    registrar.Parameter("network_project", &TThis::NetworkProject)
        .Default();

    registrar.Parameter("proxy_url_aliasing_rules", &TThis::ProxyUrlAliasingRules)
        .Default();

    registrar.Parameter("secret_env", &TThis::SecretEnv)
        .Default();

    registrar.Parameter("node_config", &TThis::NodeConfigPatch)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TFlowNodeConfigPtr BuildDefaultVanillaNodeConfig(
    const NYPath::TRichYPath& pipelinePath,
    std::optional<std::string> proxyRole)
{
    // clang-format off
    auto node = BuildYsonNodeFluently()
        .BeginMap()
            .Item("cluster_url").Value(pipelinePath.GetCluster().value())
            .Item("path").Value(pipelinePath.GetPath())
            .DoIf(proxyRole.has_value(), [&] (auto fluent) {
                fluent.Item("proxy_role").Value(*proxyRole);
            })
            .Item("rpc_port").Value(DefaultRpcPort)
            .Item("monitoring_port").Value(DefaultMonitoringPort)
            .Item("abort_on_unrecognized_options").Value(false)
        .EndMap();
    // clang-format on
    auto config = ConvertTo<TFlowNodeConfigPtr>(std::move(node));
    config->SetSingletonConfig(BuildVanillaJobLoggingConfig());
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void LaunchInVanillaJob(
    const NYPath::TRichYPath& pipelinePath,
    const std::optional<std::string>& proxyRole,
    const TVanillaConfigPtr& vanillaConfig)
{
    if (!vanillaConfig->Enable) {
        return;
    }

    auto nodeConfig = BuildDefaultVanillaNodeConfig(pipelinePath, proxyRole);
    if (vanillaConfig->NodeConfigPatch) {
        nodeConfig = ConvertTo<TFlowNodeConfigPtr>(
            PatchNode(ConvertToNode(nodeConfig), vanillaConfig->NodeConfigPatch));
    }

    auto pipelineCluster = pipelinePath.GetCluster().value();
    auto runtimeCluster = vanillaConfig->RuntimeCluster.value_or(pipelineCluster);
    bool sameCluster = runtimeCluster == pipelineCluster;
    auto runtimeProxyRole = sameCluster ? proxyRole : vanillaConfig->RuntimeProxyRole;
    auto alias = vanillaConfig->Alias.value_or(BuildVanillaOperationAlias(pipelinePath));

    // The node config is delivered to the job as the "node_config" file.
    auto nodeConfigFile = std::make_shared<TTempFile>(MakeTempName(/*wrkDir*/ nullptr, "config"));
    {
        TFileOutput output(nodeConfigFile->Name());
        output << ConvertToYsonString(nodeConfig).ToString();
    }

    auto pipelineClient = NClient::NCache::CreateClient(pipelineCluster, proxyRole);
    auto runtimeClient = sameCluster
        ? pipelineClient
        : NClient::NCache::CreateClient(runtimeCluster, runtimeProxyRole);

    auto filesDir = GetVanillaFilesDir(pipelinePath.GetPath());
    auto aliasingConfig = BuildProxyUrlAliasingConfig(vanillaConfig->ProxyUrlAliasingRules);
    auto binaryChecksumOverride = GetBinaryChecksumOverride();

    // The cache-backed files keyed by in-job name (binary + node config + user local files), so the
    // commit phase can stamp the durable vanilla/files copy from the shared cache. The md5 is
    // computed once per file here and reused for both the runtime and the pipeline cluster uploads.
    struct TCanonicalFile
    {
        std::string LocalPath;
        bool Executable = false;
        TString MD5;
    };

    THashMap<std::string, TCanonicalFile> canonicalFiles;

    auto makeTask = [&] (const TVanillaTaskConfigPtr& config) {
        TFlowVanillaTask task;
        task.JobCount = config->Count;
        task.MemoryLimit = config->MemoryLimit.value_or(NYTree::TSize(DefaultMemoryLimit));
        task.CpuLimit = config->CpuLimit.value_or(DefaultCpuLimit);
        task.PortCount = config->PortCount.value_or(0);
        task.LocalFiles = config->LocalFiles;
        task.CypressFiles = config->CypressFiles;
        task.Layers = config->Layers;
        task.SystemLayerPath = config->SystemLayerPath;
        if (aliasingConfig) {
            task.Environment["YT_PROXY_URL_ALIASING_CONFIG"] = *aliasingConfig;
        }
        if (binaryChecksumOverride) {
            task.Environment["YT_FLOW_BINARY_CHECKSUM_OVERRIDE"] = *binaryChecksumOverride;
        }
        return task;
    };

    // For the launch variant (`persist` = false) the file_paths reference the runtime cluster's
    // cache (uploading the canon there if missing); for the persisted variant they reference the
    // durable vanilla/files copies.
    auto buildTask = [&] (
        const std::string& name,
        const std::string& flowMode,
        const TVanillaTaskConfigPtr& config,
        bool persist) {
        return BuildTaskSpec(
            name,
            flowMode,
            makeTask(config),
            vanillaConfig->NetworkProject,
            nodeConfigFile->Name(),
            // The reanimate source must hold the durable binary, so the launcher always canonicalizes
            // it into vanilla/files rather than running an ephemeral on-disk copy.
            /*localBinaryPath*/ std::nullopt,
            [&, persist] (const std::string& fileName, const std::string& localPath, bool executable) {
                auto& canonical = canonicalFiles[fileName];
                if (canonical.MD5.empty()) {
                    canonical = {localPath, executable, ComputeLocalFileMD5(localPath)};
                }
                return persist
                    ? NYPath::TYPath(Format("%v/%v", filesDir, fileName))
                    : EnsureFileInCache(runtimeClient, localPath, canonical.MD5, vanillaConfig->CachePath);
            });
    };

    auto buildSpec = [&] (std::vector<TVanillaTaskSpec> tasks) {
        TVanillaSpec spec;
        spec.Pool = vanillaConfig->Pool;
        spec.Alias = alias;
        spec.Title = vanillaConfig->Title;
        spec.MaxFailedJobCount = static_cast<int>(vanillaConfig->MaxFailedJobCount);
        spec.SolomonResolverTag = vanillaConfig->SolomonResolverTag;
        spec.MonitoringPort = nodeConfig->MonitoringPort;
        // Layers require porto nodes.
        spec.UsePorto = !vanillaConfig->Controller->Layers.empty() || !vanillaConfig->Worker->Layers.empty();
        spec.SecretEnv = vanillaConfig->SecretEnv;
        spec.Tasks = std::move(tasks);
        return spec;
    };

    // Prepare: upload the canon into the runtime cluster's cache while a prior operation, if any,
    // keeps the pipeline running. The spec carries only the secret_env names; the secure vault is
    // injected from the environment right before the start (the same path reanimate takes).
    std::vector<TVanillaTaskSpec> runtimeTasks = {
        buildTask("controller", "Controller", vanillaConfig->Controller, /*persist*/ false),
        buildTask("worker", "Worker", vanillaConfig->Worker, /*persist*/ false),
    };
    auto launchSpec = BuildVanillaOperationSpec(buildSpec(std::move(runtimeTasks)));
    InjectSecureVaultFromEnv(launchSpec);

    // Switch (make-before-break): stop the prior operation, record the manifest, then start the
    // prepared one. The manifest goes first — the alias is known up front, and a write after the
    // start could fail, leaving a running operation the manifest does not point at.
    ShutdownPriorVanillaOperation(pipelineClient, pipelinePath.GetPath(), vanillaConfig->WaitTimeout);

    auto manifest = New<TVanillaOperationManifest>();
    manifest->Cluster = runtimeCluster;
    manifest->Alias = alias;
    manifest->ProxyRole = runtimeProxyRole;
    WriteVanillaOperationManifest(pipelineClient, pipelinePath.GetPath(), manifest);

    auto operationId = WaitFor(runtimeClient->StartOperation(
        NScheduler::EOperationType::Vanilla,
        ConvertToYsonString(launchSpec)))
        .ValueOrThrow();
    YT_LOG_INFO("Started vanilla operation (Alias: %v, Cluster: %v, OperationId: %v)",
        alias,
        runtimeCluster,
        operationId);

    // Commit: only now refresh the durable canon, persist the spec, and prune stale files. A failure
    // before this point leaves the pipeline reanimatable on the previous version, since the old
    // vanilla/files and spec are untouched.
    NApi::TCreateNodeOptions dirOptions;
    dirOptions.Recursive = true;
    dirOptions.IgnoreExisting = true;
    WaitFor(pipelineClient->CreateNode(filesDir, NObjectClient::EObjectType::MapNode, dirOptions)).ThrowOnError();

    THashSet<std::string> keepNames;
    for (const auto& [fileName, file] : canonicalFiles) {
        auto pipelineCachePath = EnsureFileInCache(pipelineClient, file.LocalPath, file.MD5, vanillaConfig->CachePath);
        NApi::TCopyNodeOptions copyOptions;
        copyOptions.Recursive = true;
        copyOptions.Force = true;
        WaitFor(pipelineClient->CopyNode(pipelineCachePath, Format("%v/%v", filesDir, fileName), copyOptions))
            .ThrowOnError();
        // A nested in-job name (e.g. "java_companion/companion.jar") lives under a top-level child of
        // vanilla/files; keep that child, since the prune lists only the directory's direct children.
        keepNames.insert(fileName.substr(0, fileName.find('/')));
    }

    std::vector<TVanillaTaskSpec> persistTasks = {
        buildTask("controller", "Controller", vanillaConfig->Controller, /*persist*/ true),
        buildTask("worker", "Worker", vanillaConfig->Worker, /*persist*/ true),
    };
    auto persistSpec = BuildVanillaOperationSpec(buildSpec(std::move(persistTasks)));
    WriteVanillaOperationSpec(pipelineClient, pipelinePath.GetPath(), persistSpec);

    PruneVanillaFiles(pipelineClient, filesDir, keepNames);
}

////////////////////////////////////////////////////////////////////////////////

std::string StartFlowVanillaOperation(const TFlowVanillaOptions& options)
{
    const auto& nodeConfig = options.NodeConfig;
    // Both the cluster and the proxy role default to the node config's connection settings.
    auto runtimeCluster = options.RuntimeCluster.value_or(nodeConfig->ClusterUrl);
    auto proxyRole = options.ProxyRole ? options.ProxyRole : nodeConfig->ProxyRole;
    auto client = NClient::NCache::CreateClient(runtimeCluster, proxyRole);
    auto cacheDir = options.CachePath.empty() ? NYPath::TYPath(VanillaFileCachePath) : options.CachePath;
    auto aliasingConfig = BuildProxyUrlAliasingConfig(options.ProxyUrlAliasingRules);
    auto binaryChecksumOverride = GetBinaryChecksumOverride();

    // On a local (test) cluster the exec node shares this host's filesystem, so each job runs the
    // launcher binary straight from its on-disk path instead of receiving an uploaded multi-GB copy.
    // Uploading it (cache plus one delivered copy per controller/worker job) is what exhausts the
    // small node disks under the heavy agent suite. Mirrors the mapreduce client's local-mode binary
    // handling; this external entry point never persists, so no reanimate source is needed.
    std::optional<std::string> localBinaryPath;
    if (IsLocalModeCluster(client)) {
        localBinaryPath = GetExecPath();
    }

    // The node config is delivered to the job as the "node_config" file.
    auto nodeConfigFile = std::make_shared<TTempFile>(MakeTempName(/*wrkDir*/ nullptr, "config"));
    {
        TFileOutput output(nodeConfigFile->Name());
        output << ConvertToYsonString(nodeConfig).ToString();
    }

    // Each local file is resolved to a Cypress path once and reused by the controller and worker tasks
    // (they share the binary), uploaded through the content-addressed cache so the md5 is computed once
    // and the blob deduplicated across operations. On a local cluster the binary, the one heavy file, is
    // not uploaded at all (it runs in place), so the cache only ever handles the small node config here.
    THashMap<std::string, NYPath::TYPath> resolvedFiles;
    auto resolveFile = [&] (const std::string& localPath) {
        auto [it, inserted] = resolvedFiles.try_emplace(localPath);
        if (inserted) {
            it->second = EnsureFileInCache(client, localPath, ComputeLocalFileMD5(localPath), cacheDir);
        }
        return it->second;
    };
    auto buildTask = [&] (const std::string& name, const std::string& flowMode, TFlowVanillaTask task) {
        if (aliasingConfig) {
            task.Environment.emplace("YT_PROXY_URL_ALIASING_CONFIG", *aliasingConfig);
        }
        if (binaryChecksumOverride) {
            task.Environment.emplace("YT_FLOW_BINARY_CHECKSUM_OVERRIDE", *binaryChecksumOverride);
        }
        return BuildTaskSpec(
            name,
            flowMode,
            task,
            options.NetworkProject,
            nodeConfigFile->Name(),
            localBinaryPath,
            [&] (const std::string& /*fileName*/, const std::string& localPath, bool /*executable*/) {
                return resolveFile(localPath);
            });
    };

    TVanillaSpec spec;
    spec.Pool = options.Pool;
    NYPath::TRichYPath pipelinePath(nodeConfig->Path);
    pipelinePath.SetCluster(nodeConfig->ClusterUrl);
    spec.Alias = options.Alias.value_or(BuildVanillaOperationAlias(pipelinePath));
    spec.Title = options.Title;
    spec.MaxFailedJobCount = options.MaxFailedJobCount;
    spec.SolomonResolverTag = options.SolomonResolverTag;
    spec.MonitoringPort = nodeConfig->MonitoringPort;
    spec.Description = options.Description;
    spec.UsePorto = !options.Controller.Layers.empty() || !options.Worker.Layers.empty();
    spec.Tasks = {
        buildTask("controller", "Controller", options.Controller),
        buildTask("worker", "Worker", options.Worker),
    };

    auto specNode = BuildVanillaOperationSpec(spec);
    // External launchers deliver a literal secure vault rather than secret_env names.
    specNode->RemoveChild("secret_env");
    auto secureVault = GetEphemeralNodeFactory()->CreateMap();
    for (const auto& [name, value] : options.SecureVault) {
        secureVault->AddChild(name, ConvertToNode(value));
    }
    if (!secureVault->FindChild("YT_TOKEN")) {
        secureVault->AddChild("YT_TOKEN", ConvertToNode(NAuth::LoadToken().value()));
    }
    specNode->AddChild("secure_vault", secureVault);

    auto operationId = WaitFor(client->StartOperation(
        NScheduler::EOperationType::Vanilla,
        ConvertToYsonString(specNode)))
        .ValueOrThrow();
    return Format("%v", operationId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
