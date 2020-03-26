#include "private.h"
#include "config.h"
#include "operation.h"
#include "operation_controller.h"
#include "operation_description.h"
#include "scheduler_strategy_host.h"
#include "shared_data.h"
#include "node_shard.h"
#include "control_thread.h"

#include <yt/server/scheduler/public.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/configure_singletons.h>

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/core/logging/public.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/action_queue.h>

#include <yt/core/http/server.h>

#include <yt/core/misc/shutdown.h>
#include <yt/core/misc/property.h>

#include <yt/library/phdr_cache/phdr_cache.h>

#include <yt/core/ytalloc/bindings.h>

namespace NYT {

using namespace NSchedulerSimulator;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NPhoenix;
using namespace NJobTrackerClient;
using namespace NScheduler;
using namespace NControllerAgent;
using namespace NConcurrency;
using namespace NLogging;

static const auto& Logger = SchedulerSimulatorLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

TJobResources GetNodeResourceLimit(const TNodeResourcesConfigPtr& config)
{
    TJobResources resourceLimits;
    resourceLimits.SetMemory(config->Memory);
    resourceLimits.SetCpu(TCpuResource(config->Cpu));
    resourceLimits.SetUserSlots(config->UserSlots);
    resourceLimits.SetNetwork(config->Network);
    return resourceLimits;
}

std::vector<TExecNodePtr> CreateExecNodes(const std::vector<TNodeGroupConfigPtr>& nodeGroups)
{
    std::vector<TExecNodePtr> execNodes;

    NNodeTrackerClient::NProto::TDiskResources diskResources;
    auto* locationResources = diskResources.add_disk_location_resources();
    locationResources->set_limit(100_GB);
    locationResources->set_usage(0);

    for (const auto& nodeGroupConfig : nodeGroups) {
        for (int i = 0; i < nodeGroupConfig->Count; ++i) {
            // NB: 0 is InvalidNodeId therefore we need +1.
            auto nodeId = execNodes.size() + 1;
            TNodeDescriptor descriptor("node" + ToString(nodeId));

            auto node = New<TExecNode>(nodeId, descriptor, NScheduler::ENodeState::Online);
            node->Tags() = nodeGroupConfig->Tags;
            node->SetResourceLimits(GetNodeResourceLimit(nodeGroupConfig->ResourceLimits));
            node->SetDiskResources(diskResources);
            node->SetMasterState(NNodeTrackerClient::ENodeState::Online);
            node->SetSchedulerState(NScheduler::ENodeState::Online);
            execNodes.push_back(node);
        }
    }

    return execNodes;
}

std::vector<TExecNodePtr> CreateExecNodesFromNode(const INodePtr& nodeGroupsNode)
{
    std::vector<TNodeGroupConfigPtr> nodeGroups;
    Deserialize(nodeGroups, nodeGroupsNode);
    return CreateExecNodes(nodeGroups);
}

std::vector<TExecNodePtr> CreateExecNodesFromFile(const TString& nodeGroupsFilename)
{
    try {
        TIFStream configStream(nodeGroupsFilename);
        return CreateExecNodesFromNode(ConvertToNode(&configStream));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading node groups") << ex;
    }
}

TSchedulerConfigPtr LoadSchedulerConfigFromNode(const INodePtr& schedulerConfigNode)
{
    TSchedulerConfigPtr schedulerConfig;
    Deserialize(schedulerConfig, schedulerConfigNode);
    return schedulerConfig;
}

TSchedulerConfigPtr LoadSchedulerConfigFromFile(const TString& schedulerConfigFilename)
{
    try {
        TIFStream configStream(schedulerConfigFilename);
        return LoadSchedulerConfigFromNode(ConvertToNode(&configStream));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading scheduler config") << ex;
    }
}

TInstant FindEarliestTime(const std::vector<TOperationDescription>& operations)
{
    auto earliestTime = TInstant::Max();
    for (const auto& operation : operations) {
        earliestTime = std::min(earliestTime, operation.StartTime);
    }
    return earliestTime;
}

std::vector<TOperationDescription> LoadOperations(bool shiftOperationsToStart)
{
    std::vector<TOperationDescription> operations;
    {
        TLoadContext context;
        context.SetInput(&Cin);
        Load(context, operations);
    }
    if (shiftOperationsToStart) {
        const auto earliestTime = FindEarliestTime(operations);
        for (auto& operation : operations) {
            operation.StartTime = earliestTime;
        }
    }
    return operations;
}

INodePtr LoadPoolTrees(const TString& poolTreesFilename)
{
    try {
        TIFStream configStream(poolTreesFilename);
        return ConvertToNode(&configStream);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading pool trees") << ex;
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void RunSimulation(const TSchedulerSimulatorConfigPtr& config)
{
    YT_LOG_INFO("Reading operations description");

    std::vector<TExecNodePtr> execNodes = CreateExecNodesFromFile(config->NodeGroupsFilename);

    YT_LOG_INFO("Discovered %v nodes", execNodes.size());

    YT_VERIFY(!execNodes.empty());

    const auto operations = LoadOperations(config->ShiftOperationsToStart);
    const TInstant earliestTime = FindEarliestTime(operations);

    TFixedBufferFileOutput eventLogOutputStream(config->EventLogFilename);

    auto schedulerConfig = LoadSchedulerConfigFromFile(config->SchedulerConfigFilename);
    auto poolTreesNode = LoadPoolTrees(config->PoolTreesFilename);

    TSharedOperationStatisticsOutput statisticsOutput(config->OperationsStatsFilename);

    auto simulatorControlThread = New<TSimulatorControlThread>(
        &execNodes,
        &eventLogOutputStream,
        &statisticsOutput,
        config,
        schedulerConfig,
        operations,
        earliestTime);

    simulatorControlThread->Initialize(poolTreesNode);
    WaitFor(simulatorControlThread->AsyncRun())
        .ThrowOnError();
}

class TSchedulerSimulatorProgram
    : public TProgram
    , public TProgramPdeathsigMixin
{
public:
    TSchedulerSimulatorProgram()
        : TProgramPdeathsigMixin(Opts_)
    {
        Opts_.AddLongOption("allow-debug-mode", "allow running simulator in debug mode")
            .NoArgument()
            .StoreTrue(&AllowDebugMode_);
        Opts_.SetFreeArgsNum(1);
        Opts_.SetFreeArgTitle(0, "SIMULATOR_CONFIG_FILENAME");
        SetCrashOnError();
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        // NB(eshcherbin): It usually doesn't make much sense running the simulator built in debug mode
        // but this occasionally still happens by mistake. Thus we now immediately crash unless debug
        // mode has been explicitly allowed.
#ifndef NDEBUG
        YT_LOG_FATAL_IF(
            !AllowDebugMode_,
            "Running the simulator in debug mode is forbidden by default. Use '--allow-debug-mode' to allow it explicitly.");
#endif

        // TODO(antonkikh): Which of these are actually needed?
        ConfigureUids();
        ConfigureSignals();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        EnablePhdrCache();
        NYTAlloc::EnableYTLogging();
        NYTAlloc::EnableYTProfiling();
        NYTAlloc::SetLibunwindBacktraceProvider();
        NYTAlloc::ConfigureFromEnv();
        NYTAlloc::EnableStockpile();

        if (HandlePdeathsigOptions()) {
            return;
        }

        auto config = LoadConfig<TSchedulerSimulatorConfig>(/* configFilename */ parseResult.GetFreeArgs()[0]);
        ConfigureSingletons(config);

        {
            config->MonitoringServer->Port = config->MonitoringPort;
            config->MonitoringServer->BindRetryCount = config->BusServer->BindRetryCount;
            config->MonitoringServer->BindRetryBackoff = config->BusServer->BindRetryBackoff;
            config->MonitoringServer->ServerName = "monitoring";
            auto httpServer = NHttp::CreateServer(config->MonitoringServer);

            NMonitoring::TMonitoringManagerPtr monitoringManager;
            NYTree::IMapNodePtr orchidRoot;
            NMonitoring::Initialize(httpServer, &monitoringManager, &orchidRoot);

            YT_LOG_INFO("Listening for HTTP requests on port %v", httpServer->GetAddress().GetPort());
            httpServer->Start();

            RunSimulation(config);

            httpServer->Stop();
        }

        NYT::Shutdown();
    }

private:
    bool AllowDebugMode_ = false;
};

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    return NYT::TSchedulerSimulatorProgram().Run(argc, argv);
}
