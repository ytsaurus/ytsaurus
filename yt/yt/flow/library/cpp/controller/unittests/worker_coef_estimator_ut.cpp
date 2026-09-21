#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/controller/worker_coef_estimator.h>

#include <yt/yt/core/test_framework/framework.h>

#include <cmath>

namespace NYT::NFlow::NBalancer {
namespace {

////////////////////////////////////////////////////////////////////////////////

const TInstant T0 = TInstant::Seconds(1'000'000);

class TWorkerCoefEstimatorTest
    : public ::testing::Test
{
protected:
    TBalancerGroupStatePtr State = New<TBalancerGroupState>();
    TWorkerCoefEstimatorConfig Config;

    TWorkerCoefEstimator MakeEstimator()
    {
        return TWorkerCoefEstimator(State, Config);
    }

    double LogCoef(const std::string& worker)
    {
        return State->WorkerLogCoefs.at(worker);
    }
};

TEST_F(TWorkerCoefEstimatorTest, ObservationRecomputesEveryConnectedWorker)
{
    auto estimator = MakeEstimator();
    estimator.AddObservation("A", "C", 0.0, 1.0, T0);
    estimator.AddObservation("B", "D", 0.0, 1.0, T0);
    estimator.AddObservation("A", "B", 0.2, 1.0, T0);
    estimator.Solve();
    EXPECT_NEAR(LogCoef("A"), -0.095, 2e-3);
    EXPECT_NEAR(LogCoef("B"), 0.095, 2e-3);
    EXPECT_NEAR(LogCoef("C"), -0.091, 2e-3);
    EXPECT_NEAR(LogCoef("D"), 0.091, 2e-3);

    // The new edge touches only A and B, yet C follows A and D follows B.
    estimator.AddObservation("A", "B", 0.6, 1.0, T0);
    estimator.Solve();
    EXPECT_NEAR(LogCoef("A"), -0.195, 2e-3);
    EXPECT_NEAR(LogCoef("B"), 0.195, 2e-3);
    EXPECT_NEAR(LogCoef("C"), -0.186, 2e-3);
    EXPECT_NEAR(LogCoef("D"), 0.186, 2e-3);
    EXPECT_NEAR(estimator.GetCoef("B") / estimator.GetCoef("A"), std::exp(0.39), 2e-3);
}

TEST_F(TWorkerCoefEstimatorTest, ResultDoesNotDependOnOrder)
{
    std::vector<std::tuple<std::string, std::string, double>> observations = {
        {"A", "B", 0.3},
        {"B", "C", -0.1},
        {"C", "A", 0.2},
        {"A", "B", 0.5},
        {"D", "A", 0.7},
        {"C", "D", -0.4},
    };
    auto solve = [&] (const std::vector<int>& order) {
        State = New<TBalancerGroupState>();
        auto estimator = MakeEstimator();
        for (int index : order) {
            const auto& [from, to, obs] = observations[index];
            estimator.AddObservation(from, to, obs, 1.0, T0);
        }
        estimator.Solve();
        return State->WorkerLogCoefs;
    };
    auto first = solve({0, 1, 2, 3, 4, 5});
    auto second = solve({5, 3, 1, 4, 0, 2});
    ASSERT_EQ(first.size(), second.size());
    for (const auto& [worker, value] : first) {
        EXPECT_NEAR(value, second.at(worker), 1e-5) << worker;
    }
}

TEST_F(TWorkerCoefEstimatorTest, PairIsOneEdgeRegardlessOfDirection)
{
    auto estimator = MakeEstimator();
    estimator.AddObservation("B", "A", 0.3, 0.5, T0);
    ASSERT_EQ(estimator.GetEdgeCount(), 1);
    const auto& edge = State->WorkerCoefEdges.at("A").at("B");
    EXPECT_EQ(edge.From, "A");
    EXPECT_EQ(edge.To, "B");
    EXPECT_DOUBLE_EQ(edge.Obs, -0.3);

    // A swap: both partitions measure the same difference, the edge doubles its weight.
    estimator.AddObservation("A", "B", -0.1, 0.5, T0);
    ASSERT_EQ(estimator.GetEdgeCount(), 1);
    EXPECT_DOUBLE_EQ(edge.Weight, 1.0);
    EXPECT_DOUBLE_EQ(edge.Obs, -0.2);
}

TEST_F(TWorkerCoefEstimatorTest, ScaleIsPinnedByThePrior)
{
    auto estimator = MakeEstimator();
    estimator.AddObservation("A", "B", 0.6, 1.0, T0);
    estimator.AddObservation("C", "D", -1.0, 1.0, T0);
    estimator.Solve();
    EXPECT_NEAR(LogCoef("A") + LogCoef("B"), 0.0, 1e-6);
    EXPECT_NEAR(LogCoef("C") + LogCoef("D"), 0.0, 1e-6);
    EXPECT_DOUBLE_EQ(estimator.GetCoef("E"), 1.0);
    EXPECT_DOUBLE_EQ(estimator.GetCoef("unknown"), 1.0);
}

//! Workers without observations cost nothing in the persisted state: no coefficient, no last-seen time.
TEST_F(TWorkerCoefEstimatorTest, UnobservedWorkersLeaveNoState)
{
    auto estimator = MakeEstimator();
    estimator.Prune({"A", "B", "Z"}, T0);
    estimator.Solve();
    EXPECT_TRUE(State->WorkerLogCoefs.empty());
    EXPECT_TRUE(State->WorkerLastSeen.empty());

    estimator.AddObservation("A", "B", 0.6, 1.0, T0);
    estimator.Prune({"A", "B", "Z"}, T0);
    estimator.Solve();
    EXPECT_EQ(State->WorkerLogCoefs.size(), 2u);
    EXPECT_EQ(State->WorkerLastSeen.size(), 2u);
    EXPECT_FALSE(State->WorkerLastSeen.contains("Z"));
}

TEST_F(TWorkerCoefEstimatorTest, OnePartitionIsDampedManyAreNot)
{
    auto estimator = MakeEstimator();
    estimator.AddObservation("A", "B", 0.6, 0.04, T0);
    estimator.Solve();
    EXPECT_NEAR(LogCoef("B") - LogCoef("A"), 0.6 * 0.04 / (0.04 + 0.025), 1e-4);

    State = New<TBalancerGroupState>();
    auto strong = MakeEstimator();
    strong.AddObservation("A", "B", 0.6, 1.0, T0);
    strong.Solve();
    EXPECT_NEAR(LogCoef("B") - LogCoef("A"), 0.6 / 1.025, 1e-4);
}

TEST_F(TWorkerCoefEstimatorTest, ObservationComparesCpuPerMessage)
{
    auto estimator = MakeEstimator();
    EXPECT_NEAR(*estimator.MakeObservation(1.0, 100.0, 2.0, 100.0), std::log(2.0), 1e-9);
    // A backlog catch-up doubles both CPU and rate.
    EXPECT_NEAR(*estimator.MakeObservation(1.0, 100.0, 2.0, 200.0), 0.0, 1e-9);
    EXPECT_FALSE(estimator.MakeObservation(1.0, 100.0, 2.0, 2000.0));
    EXPECT_FALSE(estimator.MakeObservation(1.0, 100.0, 2.0, 5.0));
    EXPECT_FALSE(estimator.MakeObservation(0.001, 100.0, 2.0, 100.0));
    EXPECT_FALSE(estimator.MakeObservation(1.0, 0.0, 2.0, 100.0));
}

TEST_F(TWorkerCoefEstimatorTest, OldEvidenceFadesOnlyWhenNewArrives)
{
    auto estimator = MakeEstimator();
    estimator.AddObservation("A", "B", 1.0, 1.0, T0);
    estimator.Solve();
    auto frozen = State->WorkerLogCoefs;

    // Nothing arrives for a long time: nothing changes.
    estimator.Prune({"A", "B"}, T0 + Config.HalfLife * 10);
    estimator.Solve();
    EXPECT_NEAR(LogCoef("A"), frozen.at("A"), 1e-8);
    EXPECT_NEAR(LogCoef("B"), frozen.at("B"), 1e-8);

    // Three half-lives later the old edge weighs 1/8 against the new observation.
    estimator.AddObservation("A", "B", 0.0, 1.0, T0 + Config.HalfLife * 3);
    ASSERT_EQ(estimator.GetEdgeCount(), 1);
    EXPECT_NEAR(State->WorkerCoefEdges.at("A").at("B").Weight, 1.125, 1e-9);
    EXPECT_NEAR(State->WorkerCoefEdges.at("A").at("B").Obs, 0.125 / 1.125, 1e-9);
}

TEST_F(TWorkerCoefEstimatorTest, EdgeLimitDropsTheOldest)
{
    Config.MaxEdgesPerWorker = 2;
    auto estimator = MakeEstimator();
    estimator.AddObservation("A", "B", 0.1, 1.0, T0);
    estimator.AddObservation("A", "C", 0.1, 1.0, T0 + TDuration::Seconds(1));
    estimator.AddObservation("A", "D", 0.1, 1.0, T0 + TDuration::Seconds(2));
    ASSERT_EQ(estimator.GetEdgeCount(), 2);
    EXPECT_FALSE(State->WorkerCoefEdges.at("A").contains("B"));
}

TEST_F(TWorkerCoefEstimatorTest, AbsentWorkerIsRememberedUntilRetention)
{
    auto estimator = MakeEstimator();
    estimator.AddObservation("A", "B", 0.6, 1.0, T0);
    estimator.AddObservation("B", "C", 0.6, 1.0, T0);
    estimator.Prune({"A", "B", "C"}, T0);
    estimator.Solve();
    double coefC = estimator.GetCoef("C");
    EXPECT_GT(coefC, 1.0);

    // C leaves; its edges and coefficient survive until the retention runs out, so a returning C
    // and the history of its partitions still see it.
    estimator.Prune({"A", "B"}, T0 + Config.Retention / 2);
    estimator.Solve();
    EXPECT_NEAR(estimator.GetCoef("C"), coefC, 1e-8);
    EXPECT_EQ(estimator.GetEdgeCount(), 2);

    estimator.Prune({"A", "B"}, T0 + Config.Retention + TDuration::Seconds(1));
    estimator.Solve();
    EXPECT_EQ(estimator.GetEdgeCount(), 1);
    EXPECT_DOUBLE_EQ(estimator.GetCoef("C"), 1.0);
    EXPECT_FALSE(State->WorkerLogCoefs.contains("C"));
}

TEST_F(TWorkerCoefEstimatorTest, CoefIsClamped)
{
    auto estimator = MakeEstimator();
    estimator.AddObservation("A", "B", 5.0, 100.0, T0);
    estimator.Solve();
    EXPECT_DOUBLE_EQ(estimator.GetCoef("B"), Config.MaxRatio);
    EXPECT_DOUBLE_EQ(estimator.GetCoef("A"), 1.0 / Config.MaxRatio);
}

TEST_F(TWorkerCoefEstimatorTest, SolverConvergesOnALongChain)
{
    auto estimator = MakeEstimator();
    std::vector<std::string> workers;
    for (int index = 0; index < 50; ++index) {
        workers.push_back(Format("w%02d", index));
        if (index > 0) {
            estimator.AddObservation(workers[index - 1], workers[index], 0.05, 1.0, T0);
        }
    }
    estimator.Solve();
    for (int index = 1; index < 50; ++index) {
        EXPECT_LT(LogCoef(workers[index - 1]), LogCoef(workers[index]));
    }
    EXPECT_NEAR(LogCoef(workers[0]) + LogCoef(workers[49]), 0.0, 1e-4);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NBalancer
