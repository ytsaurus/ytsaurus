#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/guid.h>

#include <yt/cpp/mapreduce/interface/init.h>

#include <yt/yql/tools/ytflow_worker/config/ytflow_worker_config.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/cache/rpc.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/yson/yson_builder.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/flow/library/cpp/pipeline_helpers/pipeline.h>
#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/vanilla_launcher.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>

#include <util/generic/guid.h>
#include <util/generic/hash_set.h>
#include <util/stream/output.h>
#include <util/system/env.h>
#include <util/system/thread.h>


namespace {

void InitializeSecureVaultEnv() {
    auto secureVault = NYT::NodeFromYsonString(
        GetEnv("YT_SECURE_VAULT", "{}"));

    for (auto node : secureVault.AsMap()) {
        SetEnv(node.first, node.second.AsString());
    }
}

// TODO(ngc224): improve asap
void InitializeCpuToVCpuEnv() {
    auto clusterName = TryGetEnv("YT_CLUSTER_NAME");
    auto operationId = TryGetEnv("YT_OPERATION_ID");
    auto jobId = TryGetEnv("YT_JOB_ID");

    if (!clusterName || !operationId || !jobId) {
        return;
    }

    try {
        auto connectionConfig = NYT::New<NYT::NApi::NRpcProxy::TConnectionConfig>();
        NYT::NClient::NCache::SetClusterUrl(connectionConfig, *clusterName);

        auto client = NYT::NClient::NCache::CreateClient(
            std::move(connectionConfig),
            NYT::NApi::GetClientOptionsFromEnv());

        auto options = NYT::NApi::TGetJobOptions();
        options.Attributes = THashSet<std::string>{"address"};

        auto future = client->GetJob(
            NYT::NScheduler::TOperationIdOrAlias::FromString(*operationId),
            NYT::NJobTrackerClient::TJobId(NYT::TGuid::FromString(*jobId)),
            options)
            .Apply(BIND([client] (const NYT::NYson::TYsonString& attributesYson) {
                auto attributes = NYT::NYTree::ConvertTo<NYT::NYTree::IMapNodePtr>(
                    attributesYson);

                auto address = NYT::NYTree::ConvertTo<TString>(
                    attributes->GetChildOrThrow("address"));

                auto path = NYT::Format(
                    "//sys/exec_nodes/%v/@annotations/cpu_to_vcpu_factor", address);

                return client->GetNode(path);
            }));

        auto cpuToVCpuFactorYson = NYT::NConcurrency::WaitFor(future)
            .ValueOrThrow();

        auto cpuToVCpuFactor = NYT::NYTree::ConvertTo<double>(cpuToVCpuFactorYson);

        SetEnv("DEPLOY_CPU_TO_VCPU_FACTOR", NYT::Format("%v", cpuToVCpuFactor));
        SetEnv("YT_CPU_TO_VCPU_FACTOR", NYT::Format("%v", cpuToVCpuFactor));

        Cerr << "Set cpu_to_vcpu_factor env: " << cpuToVCpuFactor << Endl;
    } catch (const std::exception&) {
        Cerr
            << "Failed to set cpu_to_vcpu_factor env: "
            << CurrentExceptionMessage()
            << Endl;
    }
}

void SymlinkLogsDirectory() {
    auto jobId = TryGetEnv("YT_JOB_ID");
    auto logsDirectory = TryGetEnv("YQL_YTFLOW_LOGS_DIRECTORY");
    auto sandboxDirectory = TryGetEnv("HOME");

    if (!jobId || !logsDirectory || !sandboxDirectory) {
        return;
    }

    auto rootDirectory = TStringBuf(*sandboxDirectory).RSplitOff('/');
    auto homeDirectory = TString(rootDirectory) + "/home";

    auto targetPath = *sandboxDirectory + "/" + *logsDirectory;
    auto linkPath = homeDirectory + "/" + *logsDirectory;

    try {
        NYT::NFS::MakeSymbolicLink(targetPath, linkPath);

        Cerr
            << "Created logs symlink: "
            << linkPath << " -> " << targetPath << Endl;
    } catch (const std::exception&) {
        Cerr
            << "Failed to create symlink for logs: "
            << CurrentExceptionMessage()
            << Endl;
    }
}

void Initialize() {
    InitializeSecureVaultEnv();
    InitializeCpuToVCpuEnv();
    SymlinkLogsDirectory();
}

} // namespace


namespace NYql::NYtflow {

class TWorker
    : public virtual NYT::TProgram
    , public NYT::TProgramConfigMixin<TWorkerConfig>
{
public:
    TWorker()
        : TProgramConfigMixin(Opts_)
    { }

protected:
    void DoRun() override
    {
        auto config = ConvertTo<TWorkerConfigPtr>(GetConfigNode());

        if (config->RunVanilla) {
            NYT::NFlow::TFlowVanillaOptions vanillaOptions;
            vanillaOptions.NodeConfig = config;
            vanillaOptions.Controller = MakeControllerVanillaTask(config);
            vanillaOptions.Worker = MakeWorkerVanillaTask(config);
            vanillaOptions.Pool = config->Pool;
            vanillaOptions.RuntimeCluster = config->RuntimeCluster;
            vanillaOptions.Description = config->Description;
            vanillaOptions.Title = config->Title;
            vanillaOptions.NetworkProject = config->NetworkProject;
            vanillaOptions.SolomonResolverTag = config->SolomonResolverTag.value_or("");

            if (auto secureVaultEnv = GetEnv("YQL_YTFLOW_SECURE_VAULT")) {
                vanillaOptions.SecureVault = NYT::NYTree::ConvertTo<THashMap<std::string, std::string>>(
                    NYT::NYson::TYsonString(secureVaultEnv));
            }

            auto operationId = NYT::NFlow::StartFlowVanillaOperation(vanillaOptions);

            NYT::NodeToYsonStream(
                NYT::TNode::CreateMap()
                    ("operation_id", TString(operationId)),
                &Cout);

            return;
        }

        NYT::NFlow::RunPipeline(
            config->ClusterUrl,
            config->ProxyRole,
            config->Path,
            config->PipelineSpec,
            config->DynamicPipelineSpec,
            config->SetFlowCoreTarget,
            config->GracefulUpdate,
            NYT::NFlow::DefaultWaitPipelineTimeout,
            /*enablePipelineCreation*/ false);
    }

private:
    NYT::NFlow::TFlowVanillaTask MakeVanillaTask(
        uint64_t rpcPort,
        uint64_t monitoringPort,
        double cpuLimit,
        uint64_t memoryLimit,
        const NYT::NLogging::TLogManagerConfigPtr& logManagerConfig)
    {
        auto configNode = NYT::TNode::CreateMap()
            ("rpc_port", rpcPort)
            ("monitoring_port", monitoringPort)
            ("bus_server", NYT::TNode::CreateMap()
                ("port", rpcPort))
            ("logging", NYT::NodeFromYsonString(NYT::NYson::ConvertToYsonString(
                logManagerConfig).AsStringBuf()));

        NYT::NFlow::TFlowVanillaTask task;
        task.CpuLimit = cpuLimit;
        task.MemoryLimit = memoryLimit;
        task.Environment["YT_FLOW_CONFIG"] = NYT::NodeToYsonString(configNode);

        if (auto jobEnv = GetEnv("YQL_YTFLOW_JOB_ENVIRONMENT")) {
            auto jobEnvMap = NYT::NYTree::ConvertTo<THashMap<TString, TString>>(
                NYT::NYson::TYsonString(jobEnv));

            for (const auto& [key, value] : jobEnvMap) {
                task.Environment[std::string(key)] = std::string(value);
            }
        }

        return task;
    }

    NYT::NFlow::TFlowVanillaTask MakeControllerVanillaTask(const TWorkerConfigPtr& config)
    {
        auto task = MakeVanillaTask(
            config->ControllerRpcPort,
            config->ControllerMonitoringPort,
            config->ControllerCpuLimit,
            config->ControllerMemoryLimit,
            config->ControllerLogManagerConfig);
        task.JobCount = config->ControllerCount;
        return task;
    }

    NYT::NFlow::TFlowVanillaTask MakeWorkerVanillaTask(const TWorkerConfigPtr& config)
    {
        auto task = MakeVanillaTask(
            config->WorkerRpcPort,
            config->WorkerMonitoringPort,
            config->WorkerCpuLimit,
            config->WorkerMemoryLimit,
            config->WorkerLogManagerConfig);
        task.JobCount = config->WorkerCount;

        for (const auto& localFile : config->LocalFiles) {
            task.LocalFiles[localFile->Name] = localFile->Path;
        }

        return task;
    }
};

} // namespace NYql::NYtflow

int main(int argc, const char** argv)
{
    Initialize();
    // NYT::NFlow::Initialize bootstraps the mapreduce client itself.
    NYT::NFlow::Initialize(argc, argv);
    return NYql::NYtflow::TWorker().Run(argc, argv);
}
