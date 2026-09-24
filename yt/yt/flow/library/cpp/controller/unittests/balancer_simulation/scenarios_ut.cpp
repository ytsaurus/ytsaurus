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

//! Partitions run on the two preloaded workers, demand halves on minutes 5..15.
//! Expected: the preloads ordered for the other workers survive the dip, and after it
//! the two workers are unloaded.
TScenario DemandDipAfterPlacement()
{
    auto scenario = SpecApplyTwoPreloaded();
    scenario.Name = "DemandDipAfterPlacement";
    scenario.InitialShares = {0.5, 0.5};
    scenario.DemandDipStartMinute = 5;
    scenario.DemandDipEndMinute = 15;
    scenario.DemandDipMultiplier = 0.5;
    scenario.ExpectNoPreloadCancellationWhileLoading = true;
    return scenario;
}

//! One worker holds 34% of every computation (40.8k against 32k), the other four are
//! below capacity. Expected: the worker is drained.
TScenario OneWorkerOverloaded()
{
    auto scenario = BaseScenario();
    scenario.Name = "OneWorkerOverloaded";
    scenario.WorkerCount = 5;
    scenario.ComputationCount = 4;
    scenario.PreloadedWorkersAtStart = 5;
    scenario.InitialShares = {0.34, 0.21, 0.17, 0.16, 0.12};
    return scenario;
}

//! One worker holds 60% of every computation (72k against 32k). Expected: the worker is drained.
TScenario OneWorkerHeavilyOverloaded()
{
    auto scenario = OneWorkerOverloaded();
    scenario.Name = "OneWorkerHeavilyOverloaded";
    scenario.InitialShares = {0.6, 0.1, 0.1, 0.1, 0.1};
    return scenario;
}

std::vector<TScenario> Scenarios()
{
    return {
        SpecApplyTwoPreloaded(),
        ManyModelsColdStart(),
        DemandDipAfterPlacement(),
        OneWorkerOverloaded(),
        OneWorkerHeavilyOverloaded(),
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

    if (scenario.ExpectNoPreloadCancellationWhileLoading) {
        EXPECT_EQ(simulation.PreloadCancelledWhileLoading(), 0);
    }

    for (const auto& [worker, load] : checkpoints.back().Load) {
        EXPECT_LE(load, scenario.WorkerCapacity) << worker << " at minute " << checkpoints.back().Minute;
    }

    // The balancer's own goal: equal queues. At the end the queues are either all empty
    // by its definition (shorter than ZeroQueueLatency worth of capacity) or spread by
    // no more than RebalanceTargetDeviation of the longest one.
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

INSTANTIATE_TEST_SUITE_P(
    Scenarios,
    TResourceBalancerSimulationTest,
    ::testing::ValuesIn(Scenarios()),
    [] (const ::testing::TestParamInfo<TScenario>& info) {
        return info.param.Name;
    });

////////////////////////////////////////////////////////////////////////////////

//! The action at |index| if it belongs to |round|.
std::string Describe(const std::vector<TMove>& moves, int index, int round)
{
    if (index >= std::ssize(moves) || moves[index].Step != round) {
        return "no more moves";
    }
    const auto& move = moves[index];
    return Format("partition %v from %Qv to %Qv", move.Partition, move.From, move.To);
}

std::string Describe(const std::vector<TPreloadEvent>& events, int index, int round)
{
    if (index >= std::ssize(events) || events[index].Step != round) {
        return "no more preload actions";
    }
    const auto& event = events[index];
    return Format("%v %v on %v", event.Add ? "Add" : "Del", event.Resource, event.Worker);
}

//! Compares two action logs ordered by round. Returns the first round where they differ and the
//! index of the first different action, or std::nullopt when they are equal.
template <class T, class TEqual>
std::optional<std::pair<int, int>> FirstMismatch(const std::vector<T>& expected, const std::vector<T>& actual, TEqual equal)
{
    int common = std::min(std::ssize(expected), std::ssize(actual));
    int index = 0;
    while (index < common && equal(expected[index], actual[index])) {
        ++index;
    }
    if (index == std::ssize(expected) && index == std::ssize(actual)) {
        return std::nullopt;
    }
    int round = std::numeric_limits<int>::max();
    if (index < std::ssize(expected)) {
        round = std::min(round, expected[index].Step);
    }
    if (index < std::ssize(actual)) {
        round = std::min(round, actual[index].Step);
    }
    return std::pair(round, index);
}

class TUnusedResourceStatusTest
    : public ::testing::TestWithParam<std::tuple<TScenario, EUnusedResourceStatus>>
{ };

//! Statistics of a resource no job on the worker uses must not change the balancer's decisions.
TEST_P(TUnusedResourceStatusTest, SameDecisions)
{
    const auto& [scenario, unusedResourceStatus] = GetParam();
    TSimulation reference(scenario);
    reference.Run();

    auto replaced = scenario;
    replaced.UnusedResourceStatus = unusedResourceStatus;
    TSimulation simulation(replaced);
    simulation.Run();

    auto move = FirstMismatch(reference.Moves(), simulation.Moves(), [] (const TMove& lhs, const TMove& rhs) {
        return std::tie(lhs.Step, lhs.Partition, lhs.From, lhs.To) == std::tie(rhs.Step, rhs.Partition, rhs.From, rhs.To);
    });
    if (move) {
        auto [round, index] = *move;
        ADD_FAILURE() << "moves differ in round " << round << ": "
                      << Describe(reference.Moves(), index, round) << " without replacement, "
                      << Describe(simulation.Moves(), index, round) << " with it";
    }

    auto preload = FirstMismatch(reference.PreloadEvents(), simulation.PreloadEvents(), [] (const TPreloadEvent& lhs, const TPreloadEvent& rhs) {
        return std::tie(lhs.Step, lhs.Worker, lhs.Resource, lhs.Add) == std::tie(rhs.Step, rhs.Worker, rhs.Resource, rhs.Add);
    });
    if (preload) {
        auto [round, index] = *preload;
        ADD_FAILURE() << "preload actions differ in round " << round << ": "
                      << Describe(reference.PreloadEvents(), index, round) << " without replacement, "
                      << Describe(simulation.PreloadEvents(), index, round) << " with it";
    }
}

INSTANTIATE_TEST_SUITE_P(
    Scenarios,
    TUnusedResourceStatusTest,
    ::testing::Combine(
        ::testing::ValuesIn(Scenarios()),
        ::testing::Values(EUnusedResourceStatus::Frozen, EUnusedResourceStatus::Huge)),
    [] (const ::testing::TestParamInfo<std::tuple<TScenario, EUnusedResourceStatus>>& info) {
        return Format("%v_%v", std::get<0>(info.param).Name, std::get<1>(info.param));
    });

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NBalancer
