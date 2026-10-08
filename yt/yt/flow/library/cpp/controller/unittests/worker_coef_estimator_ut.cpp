#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/controller/worker_coef_estimator.h>

#include <yt/yt/core/test_framework/framework.h>

#include <cmath>
#include <limits>
#include <tuple>

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

TEST_F(TWorkerCoefEstimatorTest, IdlePartitionsGiveNoObservation)
{
    auto estimator = MakeEstimator();
    // An idle partition has nothing to compare even when the ratio is finite.
    EXPECT_FALSE(estimator.MakeObservation(0.06, 0.001, 0.06, 0.001));
    EXPECT_FALSE(estimator.MakeObservation(0.1, 0.005, 0.05, 0.005));
    // A rate the EMA has decayed into the subnormal range, as seen in production.
    EXPECT_FALSE(estimator.MakeObservation(0.06, 1e-320, 0.06, 1e-320));
}

TEST_F(TWorkerCoefEstimatorTest, NonFiniteMeasurementsGiveNoObservation)
{
    const double inf = std::numeric_limits<double>::infinity();
    auto estimator = MakeEstimator();
    EXPECT_FALSE(estimator.MakeObservation(std::nan(""), 100.0, 2.0, 100.0));
    EXPECT_FALSE(estimator.MakeObservation(1.0, std::nan(""), 2.0, 100.0));
    EXPECT_FALSE(estimator.MakeObservation(1.0, 100.0, inf, 100.0));
    EXPECT_FALSE(estimator.MakeObservation(-inf, 100.0, 2.0, 100.0));
    EXPECT_FALSE(estimator.MakeObservation(1.0, -100.0, 2.0, 100.0));
    // The rate ratio of two infinities is NaN and passes the ratio gate.
    EXPECT_FALSE(estimator.MakeObservation(1.0, inf, 2.0, inf));
    // Passes every gate, yet CPU per message overflows on both sides.
    EXPECT_FALSE(estimator.MakeObservation(1e307, 0.01, 1e307, 0.01));
}

TEST_F(TWorkerCoefEstimatorTest, NonFiniteObservationIsIgnored)
{
    const double inf = std::numeric_limits<double>::infinity();
    auto estimator = MakeEstimator();
    estimator.AddObservation("A", "B", 0.2, 1.0, T0);
    estimator.AddObservation("A", "B", std::nan(""), 1.0, T0);
    estimator.AddObservation("A", "B", 0.2, inf, T0);
    estimator.AddObservation("A", "B", 0.2, 0.0, T0);
    estimator.AddObservation("A", "B", 0.2, -1.0, T0);
    estimator.AddObservation("B", "C", inf, 1.0, T0);
    estimator.AddObservation("C", "D", 0.1, std::nan(""), T0);
    EXPECT_EQ(estimator.GetEdgeCount(), 1);
    const auto& edge = State->WorkerCoefEdges.at("A").at("B");
    EXPECT_EQ(edge.Obs, 0.2);
    EXPECT_EQ(edge.Weight, 1.0);
}

//! A state written before non-finite observations were rejected: one such value discredits
//! every observation of the state.
TEST_F(TWorkerCoefEstimatorTest, PoisonedStateIsReset)
{
    const double inf = std::numeric_limits<double>::infinity();
    const double nan = std::nan("");
    auto putEdge = [&] (const std::string& from, const std::string& to, double obs, double weight) {
        auto& edge = State->WorkerCoefEdges[from][to];
        edge.From = from;
        edge.To = to;
        edge.Obs = obs;
        edge.Weight = weight;
        edge.UpdatedAt = T0;
    };

    struct TPoison
    {
        std::string Name;
        std::optional<std::tuple<std::string, std::string, double, double>> Edge;
        std::optional<std::pair<std::string, double>> Coef;
    };

    std::vector<TPoison> poisons = {
        {"nan obs", std::tuple{"A", "B", nan, 0.0625}, {}},
        {"inf obs", std::tuple{"G", "H", inf, 1.0}, {}},
        {"inf weight", std::tuple{"A", "D", 0.2, inf}, {}},
        {"zero weight", std::tuple{"E", "F", 0.1, 0.0}, {}},
        {"negative weight", std::tuple{"E", "F", 0.1, -1.0}, {}},
        {"nan coef", {}, std::pair{"A", nan}},
        {"inf coef", {}, std::pair{"C", inf}},
        {"-inf coef", {}, std::pair{"D", -inf}},
    };
    auto seed = [&] (const TPoison& poison) {
        State = New<TBalancerGroupState>();
        putEdge("A", "C", 0.2, 1.0);
        State->WorkerLogCoefs["A"] = -0.1;
        State->WorkerLogCoefs["C"] = 0.1;
        State->WorkerLastSeen["A"] = T0;
        if (poison.Edge) {
            std::apply(putEdge, *poison.Edge);
        }
        if (poison.Coef) {
            State->WorkerLogCoefs[poison.Coef->first] = poison.Coef->second;
        }
    };
    for (const auto& poison : poisons) {
        seed(poison);
        EXPECT_TRUE(MakeEstimator().ResetIfPoisoned()) << poison.Name;

        seed(poison);
        auto estimator = MakeEstimator();
        for (const auto& [worker, logCoef] : State->WorkerLogCoefs) {
            if (!std::isfinite(logCoef)) {
                EXPECT_EQ(estimator.GetCoef(worker), 1.0) << poison.Name << " " << worker;
            }
        }
        estimator.Solve();
        EXPECT_EQ(estimator.GetEdgeCount(), 0) << poison.Name;
        EXPECT_TRUE(State->WorkerLogCoefs.empty()) << poison.Name;
        EXPECT_TRUE(State->WorkerLastSeen.empty()) << poison.Name;
    }

    // A finite state is left alone.
    State = New<TBalancerGroupState>();
    putEdge("A", "C", 0.2, 1.0);
    auto estimator = MakeEstimator();
    EXPECT_FALSE(estimator.ResetIfPoisoned());
    estimator.Solve();
    EXPECT_EQ(estimator.GetEdgeCount(), 1);
    EXPECT_EQ(std::ssize(State->WorkerLogCoefs), 2);
    EXPECT_NEAR(LogCoef("C") - LogCoef("A"), 0.2, 0.01);
}

TEST_F(TWorkerCoefEstimatorTest, ObservationResetsPoisonedStateFirst)
{
    for (const auto& [from, to, obs] : {std::tuple("A", "B", std::nan("")), std::tuple("C", "D", 0.1)}) {
        auto& edge = State->WorkerCoefEdges[from][to];
        edge.From = from;
        edge.To = to;
        edge.Obs = obs;
        edge.Weight = 1.0;
        edge.UpdatedAt = T0;
    }

    // Neither the poisoned edge nor its finite neighbour survives; the observation does.
    auto estimator = MakeEstimator();
    estimator.AddObservation("A", "B", 0.3, 1.0, T0);
    EXPECT_EQ(estimator.GetEdgeCount(), 1);
    const auto& edge = State->WorkerCoefEdges.at("A").at("B");
    EXPECT_EQ(edge.Obs, 0.3);
    EXPECT_EQ(edge.Weight, 1.0);
    estimator.Solve();
    EXPECT_EQ(estimator.GetEdgeCount(), 1);
    EXPECT_NEAR(LogCoef("B") - LogCoef("A"), 0.3, 0.02);
}

//! Finite but extreme values can overflow the merge and the solver; neither may store the result.
TEST_F(TWorkerCoefEstimatorTest, OverflowLeavesNoNonFiniteValue)
{
    auto estimator = MakeEstimator();
    estimator.AddObservation("A", "B", 700.0, 1e306, T0);
    estimator.AddObservation("A", "B", 700.0, 1e306, T0);
    const auto& edge = State->WorkerCoefEdges.at("A").at("B");
    EXPECT_EQ(edge.Obs, 700.0);
    EXPECT_EQ(edge.Weight, 2e306);
    estimator.AddObservation("A", "B", 700.0, 1e308, T0);
    estimator.AddObservation("A", "B", 700.0, 1e308, T0);
    EXPECT_EQ(edge.Obs, 700.0);
    EXPECT_EQ(edge.Weight, 1e308);

    State = New<TBalancerGroupState>();
    auto& huge = State->WorkerCoefEdges["A"]["B"];
    huge.From = "A";
    huge.To = "B";
    huge.Obs = 1e308;
    huge.Weight = 2.0;
    huge.UpdatedAt = T0;
    auto solver = MakeEstimator();
    for (int round = 0; round < 2; ++round) {
        solver.Solve();
        EXPECT_EQ(solver.GetEdgeCount(), 1);
        EXPECT_TRUE(State->WorkerLogCoefs.empty());
        EXPECT_EQ(solver.GetCoef("A"), 1.0);
    }
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
