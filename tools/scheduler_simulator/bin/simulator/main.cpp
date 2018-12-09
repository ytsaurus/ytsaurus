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

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/core/logging/public.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/action_queue.h>

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

TJobResources GetNodeResourceLimit(const TNodeResourcesConfigPtr& config)
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

void Run(const char* configFilename)
{
    const auto config = LoadConfig(configFilename);

    TLogManager::Get()->Configure(config->Logging);

    LOG_INFO("Reading operations description");

    std::vector<TExecNodePtr> execNodes = CreateExecNodesFromFile(config->NodeGroupsFilename);

    LOG_INFO("Discovered %v nodes", execNodes.size());

    YCHECK(!execNodes.empty());

    const auto operations = LoadOperations(config->ShiftOperationsToStart);
    const TInstant earliestTime = FindEarliestTime(operations);

    TFixedBufferFileOutput eventLogOutputStream(config->EventLogFilename);

    auto schedulerConfig = LoadSchedulerConfigFromFile(config->SchedulerConfigFilename);
    auto poolTreesNode = LoadPoolTrees(config->PoolTreesFilename);

    auto simulatorControlThread = New<TSimulatorControlThread>(
        &execNodes,
        &eventLogOutputStream,
        config,
        schedulerConfig,
        operations,
        earliestTime);

    simulatorControlThread->Initialize(poolTreesNode);
    WaitFor(simulatorControlThread->AsyncRun())
        .ThrowOnError();
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
