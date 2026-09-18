#include "simulation.h"

#include <yt/yt/core/test_framework/framework.h>

#include <limits>

namespace NYT::NFlow::NBalancer {
namespace {

using namespace NYT::NFlow::NBalancer::NTesting;

////////////////////////////////////////////////////////////////////////////////

TScenario BaseScenario()
{
    TScenario scenario;
    scenario.WorkerCapacity = 32'000.;
    scenario.QueueCap = 40'000.;
    scenario.PartitionsPerComputation = 100;
    scenario.DemandPerPartition = 300.;
    scenario.ModelGpuMemory = 10;
    scenario.WorkerGpuMemory = 48; // four models per worker
    scenario.PreloadDelaySteps = 15 * 60 / StepSeconds;
    scenario.TotalSteps = 2 * 60 * 60 / StepSeconds;
    scenario.CheckpointMinutes = {1, 5, 10, 30, 60, 120};
    scenario.PlanningHorizonSeconds = 600.;
    scenario.ZeroQueueLatencySeconds = 1.;
    scenario.RebalanceTargetDeviation = 0.1;
    scenario.ExpectLoadWithinCapacity = true;
    return scenario;
}

//! Start after a spec apply: five workers, four computations, total demand (120k) below
//! total capacity but above the capacity of any two workers, models preloaded on two
//! workers, the rest get them 15 minutes after the balancer asks.
TScenario SpecApplyTwoPreloaded()
{
    auto scenario = BaseScenario();
    scenario.Name = "SpecApplyTwoPreloaded";
    scenario.WorkerCount = 5;
    scenario.ComputationCount = 4;
    scenario.PreloadedWorkersAtStart = 2;
    return scenario;
}

//! Cold start with more models than fit into one worker: twenty workers, eight
//! computations (240k total demand against 640k capacity), no model preloaded anywhere,
//! at most four models per worker.
TScenario ManyModelsColdStart()
{
    auto scenario = BaseScenario();
    scenario.Name = "ManyModelsColdStart";
    scenario.WorkerCount = 20;
    scenario.ComputationCount = 8;
    scenario.PreloadedWorkersAtStart = 0;
    return scenario;
}

std::vector<TScenario> Scenarios()
{
    return {
        SpecApplyTwoPreloaded(),
        ManyModelsColdStart(),
    };
}

////////////////////////////////////////////////////////////////////////////////

class TResourceBalancerSimulationTest
    : public ::testing::TestWithParam<TScenario>
{ };

//! Runs the scenario, logs the trajectory and checks the invariants any correct
//! balancer keeps; the quality thresholds (max share, moves per hour) are logged only.
TEST_P(TResourceBalancerSimulationTest, Trajectory)
{
    const auto& scenario = GetParam();
    TSimulation simulation(scenario);
    simulation.Run();

    const auto& checkpoints = simulation.Checkpoints();
    ASSERT_EQ(std::ssize(checkpoints), std::ssize(scenario.CheckpointMinutes));

    EXPECT_EQ(simulation.StrayCount(), 0);
    for (const auto& cp : checkpoints) {
        int total = 0;
        for (const auto& placement : cp.Placement) {
            for (const auto& [_, count] : placement) {
                total += count;
            }
        }
        EXPECT_EQ(total + cp.Stray, scenario.TotalPartitions()) << "at minute " << cp.Minute;
    }

    const auto& violations = simulation.Violations();
    EXPECT_EQ(violations[EViolation::AddWithoutModel], 0);
    EXPECT_EQ(violations[EViolation::DelWorkerMismatch], 0);
    EXPECT_EQ(violations[EViolation::UnloadInUse], 0);
    EXPECT_EQ(violations[EViolation::AddWithoutDel], 0);

    if (scenario.ExpectLoadWithinCapacity) {
        for (const auto& [worker, load] : checkpoints.back().Load) {
            EXPECT_LE(load, scenario.WorkerCapacity) << worker << " at minute " << checkpoints.back().Minute;
        }
    }

    // The balancer's own goal: equal queues. At the end the queues are either all empty
    // by its definition (shorter than ZeroQueueLatency worth of capacity) or spread by
    // no more than RebalanceTargetDeviation of the longest one.
    {
        double maxQueue = 0.;
        double minQueue = std::numeric_limits<double>::max();
        for (const auto& [_, queue] : checkpoints.back().Queue) {
            maxQueue = std::max(maxQueue, queue);
            minQueue = std::min(minQueue, queue);
        }
        double emptyQueue = scenario.ZeroQueueLatencySeconds * scenario.WorkerCapacity;
        EXPECT_TRUE(maxQueue <= emptyQueue || (maxQueue - minQueue) <= scenario.RebalanceTargetDeviation * maxQueue)
            << "queues at minute " << checkpoints.back().Minute << ": min " << minQueue << ", max " << maxQueue;
    }
}

INSTANTIATE_TEST_SUITE_P(
    Scenarios,
    TResourceBalancerSimulationTest,
    ::testing::ValuesIn(Scenarios()),
    [] (const ::testing::TestParamInfo<TScenario>& info) {
        return info.param.Name;
    });

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NBalancer
