#include <yt/yt/core/test_framework/framework.h>

#include "control_thread.h"

#include <yt/yt/core/yson/null_consumer.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/stream/null.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NSchedulerSimulator {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NScheduler;
using namespace NControllerAgent;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NYson;

using NJobTrackerClient::EJobType;

////////////////////////////////////////////////////////////////////////////////

namespace {

TJobResources CreateJobResources(int cpu, i64 memory, int network)
{
    TJobResources resources;

    resources.SetCpu(cpu);
    resources.SetMemory(memory);
    resources.SetNetwork(network);
    resources.SetUserSlots(1);

    return resources;
}

TJobDescription CreateJobDescription(TDuration jobDuration, const TJobResources& resourceLimits)
{
    TJobDescription job;

    job.Duration = jobDuration;
    job.ResourceLimits = resourceLimits;
    job.Id = TJobId(TGuid::Create());
    job.Type = EJobType::Vanilla;
    job.State = "completed";

    return job;
}

std::vector<TJobDescription> CreateJobDescriptions(
    TDuration jobDuration,
    const TJobResources& resourceLimits,
    int count)
{
    std::vector<TJobDescription> jobs;

    jobs.reserve(count);
    for (int i = 0; i < count; ++i) {
        jobs.push_back(CreateJobDescription(jobDuration, resourceLimits));
    }

    return jobs;
}

TYsonString CreatePoolTreesConfig()
{
    auto physicalTreeConfig = New<TFairShareStrategyTreeConfig>();
    physicalTreeConfig->NodesFilter = NYTree::ConvertTo<TSchedulingTagFilter>("internal");
    physicalTreeConfig->DefaultParentPool = "research";

    physicalTreeConfig->FairShareStarvationTimeout = TDuration::Seconds(30);
    physicalTreeConfig->FairShareStarvationTolerance = 0.8;

    physicalTreeConfig->PreemptionSatisfactionThreshold = 1.0;
    physicalTreeConfig->AggressivePreemptionSatisfactionThreshold = 0.5;

    physicalTreeConfig->MaxUnpreemptibleRunningAllocationCount = 10;

    // Intentionally disables profiling since simulator is not ready for profiling.
    physicalTreeConfig->EnableScheduledAndPreemptedResourcesProfiling = false;

    return ConvertToYsonString(NYTree::BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("default_tree").Value("physical")
        .EndAttributes()
        .BeginMap()
            .Item("physical")
            .BeginAttributes()
                .Items(NYTree::ConvertToNode(physicalTreeConfig)->AsMap())
            .EndAttributes()
            .BeginMap()
                .Item("research")
                    .BeginAttributes()
                        .Item("id").Value(TGuid())
                    .EndAttributes()
                    .BeginMap()
                    .EndMap()
                .Item("test10")
                    .BeginAttributes()
                        .Item("id").Value(TGuid())
                    .EndAttributes()
                    .BeginMap()
                        .Item("test11")
                            .BeginAttributes()
                                .Item("id").Value(TGuid())
                            .EndAttributes()
                            .BeginMap()
                            .EndMap()
                        .Item("test12")
                            .BeginAttributes()
                                .Item("id").Value(TGuid())
                            .EndAttributes()
                            .BeginMap()
                            .EndMap()
                    .EndMap()
                .Item("test20")
                    .BeginAttributes()
                        .Item("id").Value(TGuid())
                    .EndAttributes()
                    .BeginMap()
                    .EndMap()
            .EndMap()
        .EndMap());
}

class TStatisticsOutputMock
    : public IOperationStatisticsOutput
{
public:
    const TInstant EarliestTime;

    explicit TStatisticsOutputMock(TInstant earliestTime)
        : EarliestTime(earliestTime)
    { }

    void PrintEntry(TOperationId id, TOperationStatistics stats) override
    {
        StatsByOperationId_.Insert(id, std::move(stats));
    }

    TOperationStatistics GetStatistics(TOperationId operationId)
    {
        return StatsByOperationId_.Get(operationId);
    }

    void ExpectCorrect(const TOperationDescription& operationDescription) const
    {
        auto& stats = StatsByOperationId_.Get(operationDescription.Id);
        EXPECT_EQ(std::ssize(operationDescription.JobDescriptions), stats.JobCount);

        TDuration expectedTotalDuration = TDuration::Zero();
        TDuration expectedMaxDuration = TDuration::Zero();
        for (auto jobDescription : operationDescription.JobDescriptions) {
            expectedTotalDuration += jobDescription.Duration;
            expectedMaxDuration = Max(expectedMaxDuration, jobDescription.Duration);
        }
        EXPECT_NEAR(
            stats.JobsTotalDuration.Seconds(),
            (expectedTotalDuration + stats.PreemptedJobsTotalDuration).Seconds(),
            1);
        EXPECT_NEAR(stats.JobMaxDuration.Seconds(), expectedMaxDuration.Seconds(), 1);
        EXPECT_NEAR(stats.StartTime.Seconds(), (operationDescription.StartTime - EarliestTime).Seconds(), 1);
    }

    void ExpectNoPreemption(const TOperationDescription& operationDescription) const
    {
        auto& stats = StatsByOperationId_.Get(operationDescription.Id);
        EXPECT_EQ(stats.PreemptedJobCount, 0);
        EXPECT_EQ(stats.PreemptedJobsTotalDuration.NanoSeconds(), 0u);
    }

private:
    TLockProtectedMap<TOperationId, TOperationStatistics> StatsByOperationId_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TControlThreadTest
    : public testing::Test
{
protected:
    TOperationDescription CreateOperationDescription(
        std::vector<TJobDescription> jobDescriptions,
        TInstant startTime,
        std::optional<TString> pool = std::nullopt)
    {
        TOperationDescription operation;

        operation.Id = TOperationId(TGuid::FromString(Format("123-12345678-12345678-1%07d", NextOperationId_)));
        operation.JobDescriptions = std::move(jobDescriptions);
        operation.StartTime = startTime;
        operation.Duration = TDuration::Seconds(100);
        operation.AuthenticatedUser = "test";
        operation.Type = EOperationType::Vanilla;
        operation.State = "completed";
        operation.InTimeframe = true;

        auto spec = New<TOperationSpecBase>();
        spec->Pool = std::move(pool);
        operation.Spec = ConvertToYsonString(spec);

        NextOperationId_ += 1;
        return operation;
    }

    TExecNodePtr CreateExecNode(const TJobResources& resourceLimits)
    {
        auto node = New<TExecNode>(
            /*nodeId*/ NextNodeId_,
            TNodeDescriptor(Format("node%02d", NextNodeId_)),
            NScheduler::ENodeState::Online);
        NextNodeId_ = TNodeId(NextNodeId_.Underlying() + 1);
        node->SetTags(TBooleanFormulaTags(THashSet<TString>{"internal"}));
        node->SetResourceLimits(resourceLimits);

        auto diskResources = TDiskResources{
            .DiskLocationResources = {
                TDiskResources::TDiskLocationResources{
                    .Usage = 0,
                    .Limit = 100_GB,
                },
            },
        };

        node->SetDiskResources(diskResources);

        return node;
    }

    TExecNodePtr CreateExecNode(int cpu, i64 memory, int userSlots = 100, int network = 100)
    {
        TJobResources resourceLimits;

        resourceLimits.SetCpu(cpu);
        resourceLimits.SetMemory(memory);
        resourceLimits.SetUserSlots(userSlots);
        resourceLimits.SetNetwork(network);

        return CreateExecNode(resourceLimits);
    }

private:
    int NextOperationId_ = 1;
    NNodeTrackerClient::TNodeId NextNodeId_ = NNodeTrackerClient::TNodeId(1);
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControlThreadTest, TestSingleOperation)
{
    const auto earliestTime = TInstant::ParseIso8601("2019-01-01T12:00:00+00:00");
    auto simulatorConfig = New<TSchedulerSimulatorConfig>();
    auto schedulerConfig = New<TSchedulerConfig>();
    TNullOutput eventLogOutputStream;

    std::vector<TExecNodePtr> execNodes;
    execNodes.push_back(CreateExecNode(/* cpu */ 100, /* memory */ 100_GB));

    std::vector<TOperationDescription> operations = {
        CreateOperationDescription(
            CreateJobDescriptions(TDuration::Seconds(120), CreateJobResources(/* cpu */ 1, 2_GB, 0), /* count */ 100),
            earliestTime
        )
    };

    TStatisticsOutputMock statisticsOutput(earliestTime);

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

    statisticsOutput.ExpectCorrect(operations[0]);
    statisticsOutput.ExpectNoPreemption(operations[0]);
}

TEST_F(TControlThreadTest, TestTwoComplementaryOperations)
{
    const auto earliestTime = TInstant::ParseIso8601("2019-01-01T12:00:00+00:00");
    auto simulatorConfig = New<TSchedulerSimulatorConfig>();
    auto schedulerConfig = New<TSchedulerConfig>();
    TNullOutput eventLogOutputStream;

    std::vector<TExecNodePtr> execNodes;
    execNodes.push_back(CreateExecNode(/* cpu */ 100, 100_GB));

    std::vector<TOperationDescription> operations = {
        CreateOperationDescription(
            CreateJobDescriptions(TDuration::Seconds(120), CreateJobResources(1, 2_GB, 0), /* count */ 100),
            earliestTime
        ),
        CreateOperationDescription(
            CreateJobDescriptions(TDuration::Seconds(120), CreateJobResources(2, 1_GB, 1), /* count */ 100),
            earliestTime
        )
    };

    TStatisticsOutputMock statisticsOutput(earliestTime);

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

    statisticsOutput.ExpectCorrect(operations[0]);
    statisticsOutput.ExpectCorrect(operations[1]);

    statisticsOutput.ExpectNoPreemption(operations[0]);
    statisticsOutput.ExpectNoPreemption(operations[1]);
}

TEST_F(TControlThreadTest, TestNormalPreemption)
{
    const auto earliestTime = TInstant::ParseIso8601("2019-01-01T12:00:00+00:00");
    auto simulatorConfig = New<TSchedulerSimulatorConfig>();
    auto schedulerConfig = New<TSchedulerConfig>();
    TNullOutput eventLogOutputStream;

    std::vector<TExecNodePtr> execNodes;
    // Current implementation doesn't schedule more than 1 job with preemption in 1 heartbeat.
    // Thus, we need many nodes to generate many heartbeats and preempt many jobs.
    for (int i = 0; i < 10; i++) {
        execNodes.push_back(CreateExecNode(10, 10_GB));
    }

    std::vector<TOperationDescription> operations = {
        CreateOperationDescription(
            CreateJobDescriptions(TDuration::Seconds(120), CreateJobResources(2, 2_GB, 0), /* count */ 100),
            /* startTime */ earliestTime,
            /* pool */ "test10"
        ),
        CreateOperationDescription(
            CreateJobDescriptions(TDuration::Seconds(120), CreateJobResources(2, 2_GB, 1), /* count */ 100),
            /* startTime */ earliestTime + TDuration::Seconds(20),
            /* pool */ "test20"
        )
    };

    TStatisticsOutputMock statisticsOutput(earliestTime);

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

    statisticsOutput.ExpectCorrect(operations[0]);
    statisticsOutput.ExpectCorrect(operations[1]);

    auto operation0Stats = statisticsOutput.GetStatistics(operations[0].Id);
    EXPECT_GE(operation0Stats.PreemptedJobCount, 20);
    EXPECT_LE(operation0Stats.PreemptedJobCount, 22);
    EXPECT_GE(static_cast<ssize_t>(operation0Stats.PreemptedJobsTotalDuration.Seconds()), 50 * 20 - 1);
    EXPECT_LE(static_cast<ssize_t>(operation0Stats.PreemptedJobsTotalDuration.Seconds()), 120 * 20 + 1);

    statisticsOutput.ExpectNoPreemption(operations[1]);
}

/////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSchedulerSimulator
