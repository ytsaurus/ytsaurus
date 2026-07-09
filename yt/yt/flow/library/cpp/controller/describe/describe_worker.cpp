#include "describe_worker.h"
#include "intermediate_description.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/internal_urls.h>
#include <yt/yt/flow/library/cpp/companion/java_companion_manager.h>

namespace NYT::NFlow::NDescribe {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

constexpr std::string_view DefaultDeployAddress = NInternalUrls::DeployAddress;

void TExtendedWorkerDescription::Register(TRegistrar registrar)
{
    registrar.Parameter("messages", &TThis::Messages)
        .Default();

    registrar.Parameter("banned", &TThis::Banned)
        .Default(false);
    registrar.Parameter("coefficients", &TThis::Coefficients)
        .DefaultCtor([] {
            return NYTree::GetEphemeralNodeFactory()->CreateMap();
        });
    registrar.Parameter("flamegraph_address", &TThis::FlamegraphAddress)
        .Default();
    registrar.Parameter("deploy_address", &TThis::DeployAddress)
        .Default(std::string(DefaultDeployAddress));
    registrar.Parameter("backtraces_address", &TThis::BacktracesAddress)
        .Default("/api/v3/flow_backtraces_are_not_supported_yet");
    registrar.Parameter("jfr_download_commmand", &TThis::JfrDownloadCommand)
        .Default();

    registrar.Parameter("monitoring_tag", &TThis::MonitoringTag)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr std::string_view YtprofUrlPrefix = NInternalUrls::YtprofUrlPrefix;

bool HasJavaCompanionResource(const TFlowViewPtr& flowView)
{
    const auto& pipelineSpec = flowView->State->ExecutionSpec->PipelineSpec->GetValue();
    for (const auto& [resourceId, resourceSpec] : pipelineSpec->Resources) {
        if (resourceSpec->ResourceClassName == TypeName<NCompanion::TJavaCompanionManager>()) {
            return true;
        }
    }
    return false;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TExtendedWorkerDescription DescribeWorker(
    const TFlowViewPtr& flowView,
    const std::string& workerAddress)
{
    TExtendedWorkerDescription description;

    auto worker = GetOrDefault(flowView->State->Workers, workerAddress);
    if (!worker) {
        THROW_ERROR_EXCEPTION("Worker not found")
            << TErrorAttribute("worker_address", workerAddress);
    }

    auto workerPartitions = GetOrDefault(GetWorkersPartitionIntermediateDescriptions(flowView, workerAddress), workerAddress);

    FillWorkerDescription(flowView, worker, workerPartitions, description, description.Messages);

    do {
        TStringBuf address = description.MonitoringAddress;
        TStringBuf host;
        TStringBuf port;
        if (!address.TryRSplit(':', host, port)) {
            break;
        }
        description.MonitoringTag = host;
        if (!YtprofUrlPrefix.empty()) {
            description.FlamegraphAddress = Format("%v?host=%v&port=%v&path=ytprof/profile", YtprofUrlPrefix, host, port);
        }
        auto memoryFlamegraphAddress = YtprofUrlPrefix.empty()
            ? TString{}
            : Format("%v?host=%v&port=%v&path=ytprof/heap", YtprofUrlPrefix, host, port);

        std::string jfrDownloadCommand;
        if (HasJavaCompanionResource(flowView)) {
            auto jfrCommand = Format("ya run yt/yt/flow/tools/download_jfr --pipeline-path %v:%v --worker \"%v\"",
                flowView->EphemeralState->PipelinePath.GetCluster(),
                flowView->EphemeralState->PipelinePath.GetPath(),
                workerAddress);
            description.JfrDownloadCommand = jfrCommand;
            jfrDownloadCommand = Format("* Java Companion latest JFR download command: `%v`\n", jfrCommand);
        }

        std::string flamegraphLinks;
        if (!description.FlamegraphAddress.empty()) {
            flamegraphLinks =
                "Flamegraphs (click to link, wait, copy link from json result and follow this link):\n" +
                Format("* Cpu flamegraph: [%v](%v)\n", description.FlamegraphAddress, description.FlamegraphAddress) +
                Format("* Memory flamegraph: [%v](%v)\n", memoryFlamegraphAddress, memoryFlamegraphAddress);
        }

        auto& message = description.Messages.emplace_back();
        message.Level = ELogLevel::Info;
        message.Text = "Useful links";
        // clang-format off
        message.MarkdownText =
            Format("Worker name: `%v`\n\n", worker->Name) +
            Format("Worker rpc address: `%v`\n\n", worker->RpcAddress) +
            Format("Worker monitoring address: `%v`\n\n", worker->MonitoringAddress) +
            Format("Worker remote shell command: `%v`\n\n", worker->RemoteShellCommand) +
            Format("Worker incarnation id: `%v`\n\n", worker->IncarnationId) +
            Format("Worker vcpu factor: %v\n\n", worker->VcpuFactor) +
            Format("Worker vcpu limit: %v\n\n", worker->VcpuLimit) +
            Format("Worker build version: `%v`\n\n", worker->BuildVersion) +
            Format("Worker flow core version: `%v`\n\n", worker->FlowCoreVersion) +
            flamegraphLinks +
            jfrDownloadCommand +
            "\n";
        // clang-format on
    } while (false);

    return description;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
