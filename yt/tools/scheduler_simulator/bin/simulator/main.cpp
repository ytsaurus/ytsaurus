#include "private.h"
#include "config.h"
#include "operation.h"
#include "operation_controller.h"
#include "operation_description.h"
#include "scheduler_strategy_host.h"
#include "shared_data.h"
#include "node_shard.h"

#include <yt/server/scheduler/public.h>

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/core/logging/public.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/misc/shutdown.h>
#include <yt/core/misc/property.h>


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

namespace {

TJobResources GetNodeResourceLimit(TNodeResourcesConfigPtr config)
{
    TJobResources resourceLimits;
    resourceLimits.SetMemory(config->Memory);
    resourceLimits.SetCpu(TCpuResource(config->Cpu));
    resourceLimits.SetUserSlots(config->UserSlots);
    resourceLimits.SetNetwork(config->Network);
    return resourceLimits;
}

TSchedulerSimulatorConfigPtr LoadConfig(const char* configFilename)
{
    INodePtr configNode;
    try {
        TIFStream configStream(configFilename);
        configNode = ConvertToNode(&configStream);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading scheduler simulator configuration") << ex;
    }

    auto config = New<TSchedulerSimulatorConfig>();
    config->Load(configNode);

    return config;
}

std::vector<TExecNodePtr> CreateExecNodes(const std::vector<TNodeGroupConfigPtr>& nodeGroups)
{
    std::vector<TExecNodePtr> execNodes;

    NNodeTrackerClient::NProto::TDiskResources diskResources;
    auto* diskReport = diskResources.add_disk_reports();
    diskReport->set_limit(100_GB);
    diskReport->set_usage(0);

    for (const auto& nodeGroupConfig : nodeGroups) {
        for (int i = 0; i < nodeGroupConfig->Count; ++i) {
            // NB: 0 is InvalidNodeId therefore we need +1.
            auto nodeId = execNodes.size() + 1;
            TNodeDescriptor descriptor("node" + ToString(nodeId));

            auto node = New<TExecNode>(nodeId, descriptor);
            node->Tags() = nodeGroupConfig->Tags;
            node->SetResourceLimits(GetNodeResourceLimit(nodeGroupConfig->ResourceLimits));
            node->SetDiskInfo(diskResources);
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

std::vector<TOperationDescription> LoadOperations()
{
    std::vector<TOperationDescription> operations;
    {
        TLoadContext context;
        context.SetInput(&Cin);
        Load(context, operations);
    }
    return operations;
}

TOperationDescriptions CreateOperationDescriptions(const std::vector<TOperationDescription>& operations)
{
    TOperationDescriptions operationDescriptions;
    for (const auto& operation : operations) {
        operationDescriptions[operation.Id] = operation;
    }
    return operationDescriptions;
}

TInstant FindEarliestTime(const std::vector<TOperationDescription>& operations)
{
    auto earliestTime = TInstant::Max();
    for (const auto& operation : operations) {
        earliestTime = std::min(earliestTime, operation.StartTime);
    }
    return earliestTime;
}

} // namespace

void Run(const char* configFilename)
{
    const auto config = LoadConfig(configFilename);

    TLogManager::Get()->Configure(config->Logging);

    LOG_INFO("Reading operations description");

    std::vector<TExecNodePtr> execNodes = CreateExecNodesFromFile(config->NodeGroupsFilename);

    LOG_INFO("Discovered %v nodes", execNodes.size());

    YCHECK(!execNodes.empty());

    const auto operations = LoadOperations();
    const auto operationDescriptions = CreateOperationDescriptions(operations);
    TSharedOperationStatistics operationStatistics(operationDescriptions);
    TSharedOperationStatisticsOutput operationStatisticsOutput(config->OperationsStatsFilename);
    TSharedRunningOperationsMap runningOperationsMap;

    const TInstant earliestTime = FindEarliestTime(operations);

    TSharedSchedulerEvents events(operations, config->HeartbeatPeriod, earliestTime, execNodes.size(), config->ThreadCount);
    TSharedJobAndOperationCounter jobAndOperationCounter(operations.size());

    TFixedBufferFileOutput eventLogFile(config->EventLogFilename);
    TSchedulerStrategyHost strategyHost(&execNodes, &eventLogFile);

    TThreadPoolPtr threadPool = New<TThreadPool>(config->ThreadCount, "Workers");
    auto invoker = threadPool->GetInvoker();

    auto schedulerConfig = LoadSchedulerConfigFromFile(config->SchedulerConfigFilename);

    TSharedSchedulingStrategy schedulingData(
        strategyHost,
        invoker,
        config,
        schedulerConfig,
        earliestTime,
        config->ThreadCount);

    LOG_INFO("Simulation started using %v threads", config->ThreadCount);

    operationStatisticsOutput.PrintHeader();

    std::vector<TFuture<void>> asyncWorkerResults;
    for (int workerId = 0; workerId < config->ThreadCount; ++workerId) {
        auto worker = New<NSchedulerSimulator::TNodeShard>(
            &execNodes,
            &events,
            &schedulingData,
            &operationStatistics,
            &operationStatisticsOutput,
            &runningOperationsMap,
            &jobAndOperationCounter,
            config,
            schedulerConfig,
            earliestTime,
            workerId);
        asyncWorkerResults.emplace_back(
            BIND(&NSchedulerSimulator::TNodeShard::Run, worker)
                .AsyncVia(invoker)
                .Run());
    }

    WaitFor(Combine(asyncWorkerResults))
        .ThrowOnError();

    LOG_INFO("Simulation finished");

    schedulingData.OnMasterDisconnected(invoker);
    threadPool->Shutdown();
}

} // namespace NYT


int main(int argc, char** argv)
{
    if (argc != 2) {
        printf("Usage: scheduler_simulator <config_filename>\n");
        return 1;
    }

    NYT::Run(argv[1]);
    NYT::Shutdown();

    return 0;
}
