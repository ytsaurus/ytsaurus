#include <yt/core/test_framework/framework.h>

#include "control_thread.h"

#include <yt/core/yson/null_consumer.h>

#include <yt/core/ytree/node.h>

#include <util/stream/null.h>

#include <contrib/libs/gmock/gmock/gmock.h>
#include <contrib/libs/gmock/gmock/gmock-matchers.h>
#include <contrib/libs/gmock/gmock/gmock-actions.h>


namespace NYT::NSchedulerSimulator {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NScheduler;
using namespace NControllerAgent;
using namespace NConcurrency;
using namespace NNodeTrackerClient;

using NJobTrackerClient::EJobType;

////////////////////////////////////////////////////////////////////////////////

namespace {

TExecNodePtr CreateExecNode(TJobResources resourceLimits)
{
    auto node = New<TExecNode>(/* nodeId */ 1, TNodeDescriptor(ToString("node1")));
    node->Tags().insert("internal");
    node->SetResourceLimits(resourceLimits);

    NNodeTrackerClient::NProto::TDiskResources diskResources;
    auto* diskReport = diskResources.add_disk_reports();
    diskReport->set_limit(100_GB);
    diskReport->set_usage(0);
    node->SetDiskInfo(diskResources);

    return node;
}

TExecNodePtr CreateExecNode(int cpu, i64 memory)
{
    auto resourceLimits = ZeroJobResources();

    resourceLimits.SetCpu(TCpuResource(cpu));
    resourceLimits.SetMemory(memory);
    resourceLimits.SetUserSlots(100);
    resourceLimits.SetNetwork(100);

    return CreateExecNode(resourceLimits);
}

TJobResources CreateJobResources(int cpu, i64 memory, int network)
{
    auto resources = ZeroJobResources();

    resources.SetCpu(TCpuResource(cpu));
    resources.SetMemory(memory);
    resources.SetNetwork(network);
    resources.SetUserSlots(1);

    return resources;
}

TJobDescription CreateJobDescription(i64 durationInSeconds, TJobResources resourceLimits)
{
    TJobDescription job;

    job.Duration = TDuration::Seconds(durationInSeconds);
    job.ResourceLimits = resourceLimits;
    job.Id = TJobId::Create();
    job.Type = EJobType::Vanilla;
    job.State = "completed";

    return job;
}

std::vector<TJobDescription> CreateJobDescriptions(i64 durationInSeconds, TJobResources resourceLimits, int count)
{
    std::vector<TJobDescription> jobs;

    jobs.reserve(count);
    for (int i = 0; i < count; ++i) {
        jobs.push_back(CreateJobDescription(durationInSeconds, resourceLimits));
    }

    return jobs;
}

TOperationDescription CreateOperationDescription(
    std::vector<TJobDescription> jobDescriptions,
    TInstant startTime)
{
    TOperationDescription operation;

    operation.Id = TOperationId::Create();
    operation.JobDescriptions = std::move(jobDescriptions);
    operation.StartTime = startTime;
    operation.Duration = TDuration::Seconds(100);
    operation.AuthenticatedUser = "test";
    operation.Type = EOperationType::Vanilla;
    operation.State = "completed";
    operation.InTimeframe = true;

// usage of the spec:
//    treeParams->Weight = spec->Weight;
//    treeParams->Pool = GetTree(tree)->CreatePoolName(spec->Pool, user);
//    treeParams->ResourceLimits = spec->ResourceLimits;
    auto spec = New<TOperationSpecBase>();
    auto specNode = NYTree::ConvertToNode(spec)->AsMap();
    operation.Spec = specNode;

    return operation;
}

NYTree::INodePtr CreatePoolTreesConfig()
{
    auto configYson = NYTree::BuildYsonStringFluently(NYson::EYsonFormat::Binary)
        .BeginAttributes()
            .Item("default_tree").Value("physical")
        .EndAttributes()
        .BeginMap()
            .Item("physical")
            .BeginAttributes()
                .Item("default_parent_pool").Value("research")
                .Item("nodes_filter").Value("internal")
            .EndAttributes()
            .BeginMap()
                .Item("research").BeginMap().EndMap()
            .EndMap()
        .EndMap();

    return NYTree::ConvertToNode(configYson);
}

class TStatisticsOutputMock
    : public IOperationStatisticsOutput
{
public:
    THashMap<TOperationId, TOperationStatistics> StatsByOperationId;

    virtual void PrintEntry(TOperationId id, TOperationStatistics stats) override
    {
        StatsByOperationId[id] = std::move(stats);
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TControlThreadTest
    : public testing::Test
{
protected:

};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControlThreadTest, TestSimpleSingleOperation)
{
    auto earliestTime = TInstant::Now();
    auto simulatorConfig = New<TSchedulerSimulatorConfig>();
    auto schedulerConfig = New<TSchedulerConfig>();
    TNullOutput eventLogOutputStream;

    std::vector<TExecNodePtr> execNodes;
    execNodes.push_back(CreateExecNode(100, 100_GB));

    std::vector<TOperationDescription> operations = {
        CreateOperationDescription(
            CreateJobDescriptions(120, CreateJobResources(1, 2_GB, 0), /* count */ 10'000),
            earliestTime
        )
    };

    TStatisticsOutputMock statisticsOutput;

    auto simulatorControlThread = New<TSimulatorControlThread>(
        &execNodes,
        &eventLogOutputStream,
        &statisticsOutput,
        simulatorConfig,
        schedulerConfig,
        operations,
        earliestTime);

    simulatorControlThread->Initialize(CreatePoolTreesConfig());
    WaitFor(simulatorControlThread->AsyncRun())
        .ThrowOnError();

    auto& stats = statisticsOutput.StatsByOperationId[operations[0].Id];
    EXPECT_EQ(10'000, stats.JobCount);
    EXPECT_EQ(0, stats.PreemptedJobCount);
    EXPECT_EQ(TDuration::Seconds(120).Seconds(), stats.JobMaxDuration.Seconds());
    EXPECT_EQ((TDuration::Seconds(120) * 10'000).Seconds(), stats.JobsTotalDuration.Seconds());
    EXPECT_EQ(TDuration::Zero(), stats.PreemptedJobsTotalDuration);
    EXPECT_EQ(TDuration::Zero(), stats.StartTime);
}

} // namespace
} // namespace NYT::NSchedulerSimulator
