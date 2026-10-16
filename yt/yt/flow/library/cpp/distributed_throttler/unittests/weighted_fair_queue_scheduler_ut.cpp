#include <yt/yt/flow/library/cpp/distributed_throttler/weighted_fair_queue_scheduler.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow::NDistributedThrottler {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TWeightedFairQueueSchedulerTest
    : public ::testing::Test
{ };

TEST_F(TWeightedFairQueueSchedulerTest, SingleClassPreservesHeadPriority)
{
    TWeightedFairQueueScheduler scheduler({{"a", 1.0}, {"b", 1.0}});
    scheduler.Activate("a", 200);
    scheduler.Activate("b", 100);

    EXPECT_EQ(scheduler.SelectClass(), "b");
}

TEST_F(TWeightedFairQueueSchedulerTest, BackloggedClassesFollowWeights)
{
    for (const auto& weights : std::vector<THashMap<TQuotaClassId, double>>{
            {{"a", 3.0}, {"b", 1.0}},
            {{"a", 5.0}, {"b", 3.0}, {"c", 1.0}},
         }) {
        TWeightedFairQueueScheduler scheduler(weights);
        for (const auto& [classId, _] : weights) {
            scheduler.Activate(classId, 0);
        }

        THashMap<TQuotaClassId, int> counts;
        for (int index = 0; index < 9000; ++index) {
            auto classId = *scheduler.SelectClass();
            ++counts[classId];
            scheduler.Charge(classId, 1, scheduler.GetWeight(classId));
        }

        const auto referenceClass = weights.begin()->first;
        for (const auto& [classId, weight] : weights) {
            EXPECT_NEAR(
                static_cast<double>(counts[classId]) / counts[referenceClass],
                weight / weights.at(referenceClass),
                0.01);
        }
    }
}

TEST_F(TWeightedFairQueueSchedulerTest, InactiveClassIsExcluded)
{
    TWeightedFairQueueScheduler scheduler({{"a", 3.0}, {"b", 1.0}});
    scheduler.Activate("a", 0);

    for (int index = 0; index < 100; ++index) {
        EXPECT_EQ(scheduler.SelectClass(), "a");
        scheduler.Charge("a", 1, scheduler.GetWeight("a"));
    }
}

TEST_F(TWeightedFairQueueSchedulerTest, IdleShareIsRedistributed)
{
    TWeightedFairQueueScheduler scheduler({{"a", 5.0}, {"b", 3.0}, {"c", 1.0}});
    scheduler.Activate("b", 0);
    scheduler.Activate("c", 0);

    THashMap<TQuotaClassId, int> counts;
    for (int index = 0; index < 4000; ++index) {
        auto classId = *scheduler.SelectClass();
        ++counts[classId];
        scheduler.Charge(classId, 1, scheduler.GetWeight(classId));
    }

    EXPECT_EQ(counts["a"], 0);
    EXPECT_NEAR(static_cast<double>(counts["b"]) / counts["c"], 3.0, 0.02);
}

TEST_F(TWeightedFairQueueSchedulerTest, WakingClassDoesNotBankCredit)
{
    TWeightedFairQueueScheduler scheduler({{"a", 1.0}, {"b", 1.0}});
    scheduler.Activate("a", 0);
    for (int index = 0; index < 100; ++index) {
        EXPECT_EQ(scheduler.SelectClass(), "a");
        scheduler.Charge("a", 1, 1.0);
    }

    scheduler.Activate("b", 1000);
    EXPECT_EQ(scheduler.SelectClass(), "b");
    scheduler.Charge("b", 1, 1.0);
    EXPECT_EQ(scheduler.SelectClass(), "a");
}

TEST_F(TWeightedFairQueueSchedulerTest, WakingClassPreemptsSingleBacklog)
{
    TWeightedFairQueueScheduler scheduler({{"backlog", 1.0}, {"waking", 1.0}});
    scheduler.Activate("backlog", 0);
    EXPECT_EQ(scheduler.SelectClass(), "backlog");
    scheduler.Charge("backlog", 10, 1.0);

    scheduler.Activate("waking", 1000000);
    EXPECT_EQ(scheduler.SelectClass(), "waking");
}

TEST_F(TWeightedFairQueueSchedulerTest, WeightReconfigureChangesSubsequentShare)
{
    TWeightedFairQueueScheduler scheduler({{"a", 1.0}, {"b", 1.0}});
    scheduler.Activate("a", 0);
    scheduler.Activate("b", 0);
    for (int index = 0; index < 100; ++index) {
        auto classId = *scheduler.SelectClass();
        scheduler.Charge(classId, 1, scheduler.GetWeight(classId));
    }

    scheduler.Reconfigure({{"a", 4.0}, {"b", 1.0}});
    THashMap<TQuotaClassId, int> counts;
    for (int index = 0; index < 5000; ++index) {
        auto classId = *scheduler.SelectClass();
        ++counts[classId];
        scheduler.Charge(classId, 1, scheduler.GetWeight(classId));
    }
    EXPECT_NEAR(static_cast<double>(counts["a"]) / counts["b"], 4.0, 0.02);
}

TEST_F(TWeightedFairQueueSchedulerTest, ChargeRollbackSurvivesWeightChange)
{
    TWeightedFairQueueScheduler scheduler({{"a", 5.0}, {"b", 1.0}});
    scheduler.Activate("a", 0);
    scheduler.Activate("b", 0);

    // Charge |a| at its original weight, then reconfigure before rolling the
    // charge back the way a refund does.
    const auto chargeWeight = scheduler.GetWeight("a");
    const double chargedVirtualTime = 100.0 / chargeWeight;
    scheduler.Charge("a", 100, chargeWeight);
    scheduler.Reconfigure({{"a", 1.0}, {"b", 1.0}});
    scheduler.ChargeVirtualTime("a", -chargedVirtualTime);

    // The rollback must leave the two classes even, so they alternate rather
    // than one of them monopolizing selection to repay a phantom deficit.
    THashMap<TQuotaClassId, int> counts;
    for (int index = 0; index < 1000; ++index) {
        auto classId = *scheduler.SelectClass();
        ++counts[classId];
        scheduler.Charge(classId, 1, scheduler.GetWeight(classId));
    }
    EXPECT_NEAR(static_cast<double>(counts["a"]) / counts["b"], 1.0, 0.02);
}

TEST_F(TWeightedFairQueueSchedulerTest, RetiredClassDrainsButCannotBeReactivated)
{
    TWeightedFairQueueScheduler scheduler({{"a", 1.0}});
    scheduler.Activate("a", 0);
    scheduler.Reconfigure({});

    EXPECT_TRUE(scheduler.IsRetired("a"));
    EXPECT_FALSE(scheduler.IsAccepting("a"));
    EXPECT_EQ(scheduler.SelectClass(), "a");

    scheduler.Deactivate("a");
    scheduler.RemoveRetiredClass("a");
    EXPECT_FALSE(scheduler.Contains("a"));
}

TEST_F(TWeightedFairQueueSchedulerTest, RenormalizationPreservesSelectionOrder)
{
    TWeightedFairQueueScheduler reference({{"a", 1.0}, {"b", 1.0}}, 1e12);
    TWeightedFairQueueScheduler renormalizing({{"a", 1.0}, {"b", 1.0}}, 5.0);
    for (auto* scheduler : {&reference, &renormalizing}) {
        scheduler->Activate("a", 0);
        scheduler->Activate("b", 0);
    }

    for (int index = 0; index < 1000; ++index) {
        auto expected = *reference.SelectClass();
        auto actual = *renormalizing.SelectClass();
        EXPECT_EQ(actual, expected);
        reference.Charge(expected, 1, reference.GetWeight(expected));
        renormalizing.Charge(actual, 1, renormalizing.GetWeight(actual));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NDistributedThrottler
