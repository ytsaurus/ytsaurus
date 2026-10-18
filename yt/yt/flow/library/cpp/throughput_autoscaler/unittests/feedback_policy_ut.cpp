#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/flow/library/cpp/throughput_autoscaler/feedback_policy.h>

#include <array>
#include <limits>

namespace NYT::NFlow {
namespace {

const NLogging::TLogger Logger("Test");

TThroughputPartitioningObservation Observation()
{
    return {
        .PartitionCount = 4,
        .Capacity = 4,
        .Processed = 4,
        .BusyPartitionShare = 1,
        .Demand = 20,
        .NormalBacklog = 8,
        .ExcessBacklog = 28'792,
    };
}

class TThroughputFeedbackTest
    : public ::testing::Test
{
protected:
    TThroughputSpecPtr Config_ = New<TThroughputSpec>();
    TPartitioningSpecPtr ComputationSpec_ = New<TPartitioningSpec>();
    TThroughputFeedbackPolicy Policy_{{}, Config_, ComputationSpec_, Logger};
    const TThroughputFeedbackState& State_ = Policy_.GetState();
    TThroughputPartitioningObservation Observation_ = Observation();

    void SetUp() override
    {
        Config_->MinimumGrowthFactor = 1.5;
        Config_->MaximumSteadyGrowthFactor = 4;
        Config_->MaximumCatchUpGrowthFactor = 2;
        Config_->ScaleDownCapacityReserve = 2;
        Config_->WarmupPeriod = TDuration::Minutes(3);
        Config_->MaxRetryWarmupPeriod = TDuration::Minutes(24);
    }

    void Restore(TThroughputFeedbackState state = {})
    {
        Policy_ = TThroughputFeedbackPolicy(std::move(state), Config_, ComputationSpec_, Logger);
    }

    TThroughputAutoscalerDecision Suggest()
    {
        Policy_.Reconfigure(Config_, ComputationSpec_);
        return Policy_.SuggestPartitionCount(Observation_);
    }

    bool RequestReset(i64 pipelineGeneration)
    {
        Config_->ResetGeneration = pipelineGeneration;
        Policy_.Reconfigure(Config_, ComputationSpec_);
        return Policy_.ApplyResetGeneration();
    }

    TThroughputAutoscalerDecision Step(i64 count, double capacity = 4)
    {
        Observation_.PartitionCount = count;
        Observation_.Capacity = capacity;
        Observation_.Processed = capacity;
        Observation_.ProcessedCredit = capacity;
        return Suggest();
    }
};

TEST_F(TThroughputFeedbackTest, GrowthBoundsDistinguishSteadyFlowFromCatchUp)
{
    Config_->MinimumGrowthFactor = 1.25;
    Config_->MaximumSteadyGrowthFactor = 3;
    Config_->MaximumCatchUpGrowthFactor = 1.75;
    for (const auto [steady, demand, expected] : {
            std::array<double, 3>{0, 101, 13},
            {0, 150, 15},
            {0, 1000, 18},
            {250, 1000, 25},
            {800, 1000, 30},
         })
    {
        SCOPED_TRACE(steady);
        SCOPED_TRACE(demand);
        Restore();
        Observation_.SteadyDemand = steady;
        Observation_.Demand = demand;
        EXPECT_EQ(Step(10, 100).TargetPartitionCount, expected);
    }
}

TEST_F(TThroughputFeedbackTest, HistoricalReserveGuardsPlateauReleaseButNotDegradationRollback)
{
    TThroughputFeedbackState state;
    state.Anchor.emplace();
    state.Anchor->PartitionCount = 32;
    state.Anchor->Capacity = 512;
    state.Anchor->BusyPartitionShare = 1;
    state.Phase = EThroughputSearchPhase::Plateau;
    state.ObservedPartitionCount = 128;
    Observation_.PartitionCount = 128;
    Observation_.Capacity = 512;
    Observation_.Processed = 200;
    Observation_.Demand = 20;
    Observation_.PeakSteadyDemand = 100;
    Observation_.ExcessBacklog = 0;
    Restore(state);
    EXPECT_EQ(Suggest().TargetPartitionCount, 50);
    EXPECT_EQ(Suggest().TargetPartitionCount, 50);

    state.Anchor->Capacity = 1000;
    Restore(state);
    EXPECT_EQ(Suggest().TargetPartitionCount, 32);
    EXPECT_TRUE(State_.CapacityDegraded);
    EXPECT_EQ(Suggest().TargetPartitionCount, 32);
}

TEST_F(TThroughputFeedbackTest, HistoricalPeakCannotRequestOrAmplifyGrowth)
{
    for (double peak : {0.0, 1000.0}) {
        Restore();
        Observation_.PartitionCount = 32;
        Observation_.Capacity = 128;
        Observation_.Processed = 128;
        Observation_.SteadyDemand = 20;
        Observation_.PeakSteadyDemand = peak;
        Observation_.Demand = 100;
        Observation_.ExcessBacklog = 100;
        EXPECT_EQ(Suggest().TargetPartitionCount, 32);
        Observation_.Demand = 140;
        EXPECT_EQ(Suggest().TargetPartitionCount, 48);
    }
}

TEST_F(TThroughputFeedbackTest, ShrinkHysteresisAndStepLimitPreserveLoadReserve)
{
    Config_->ScaleDownTolerance = 0.25;
    Config_->MaximumScaleDownFactor = 4;
    Observation_.PartitionCount = 32;
    Observation_.Capacity = 64;
    Observation_.Demand = 2;
    Observation_.ExcessBacklog = 0;
    for (const auto [processed, expected] : {std::pair{26.0, 32}, {24.0, 24}, {2.0, 8}}) {
        SCOPED_TRACE(processed);
        Restore();
        Observation_.Processed = processed;
        EXPECT_EQ(Suggest().TargetPartitionCount, expected);
    }
}

TEST_F(TThroughputFeedbackTest, PlateauIgnoresDemandGrowthAndIncomparableCapacity)
{
    Config_->MinimumComparablePartitionShare = 0.5;
    Observation_.BusyPartitionShare = 0.5;
    EXPECT_EQ(Step(4).TargetPartitionCount, 8);
    EXPECT_EQ(Step(8).TargetPartitionCount, 16);
    EXPECT_EQ(Step(16).TargetPartitionCount, 16);
    for (int cycle = 0; cycle < 3; ++cycle) {
        Observation_.Demand *= 2;
        for (const auto [share, capacity] : {
                std::array<double, 2>{0.125, 2},
                {0.125, 8},
                {1, 4},
             })
        {
            Observation_.BusyPartitionShare = share;
            EXPECT_EQ(Step(16, capacity).TargetPartitionCount, 16);
            EXPECT_FALSE(State_.CapacityDegraded);
        }
    }
    EXPECT_EQ(Step(16, 8).TargetPartitionCount, 4);
    EXPECT_EQ(Step(4, 8).TargetPartitionCount, 8);
    EXPECT_EQ(State_.Anchor->Capacity, 8);
}

TEST_F(TThroughputFeedbackTest, SparseCurrentCapacityCannotTeachGainOrPlateau)
{
    for (double referenceShare : {0.125, 1.0}) {
        for (double capacity : {2.0, 4.0, 8.0}) {
            SCOPED_TRACE(referenceShare);
            SCOPED_TRACE(capacity);
            Restore();
            Observation_ = Observation();
            Observation_.MaxPartitionCount = 37;
            ComputationSpec_->ThroughputMaxExplorationFactor = 16;
            Observation_.BusyPartitionShare = referenceShare;
            EXPECT_EQ(Step(4).TargetPartitionCount, 8);
            Observation_.BusyPartitionShare = 0.125;
            i64 count = 8;
            while (count < 37) {
                const auto decision = Step(count, capacity);
                ASSERT_GT(decision.TargetPartitionCount, count);
                ASSERT_LE(decision.TargetPartitionCount, 37);
                count = decision.TargetPartitionCount;
                ASSERT_TRUE(State_.Anchor);
                EXPECT_EQ(State_.Anchor->PartitionCount, 4);
                EXPECT_EQ(State_.Anchor->Capacity, 4);
                EXPECT_EQ(State_.Anchor->BusyPartitionShare, referenceShare);
                EXPECT_EQ(State_.Phase, EThroughputSearchPhase::FirstProbe);
                EXPECT_FALSE(State_.CapacityDegraded);
            }
            EXPECT_EQ(Step(count, capacity).TargetPartitionCount, 37);
            EXPECT_NE(State_.Phase, EThroughputSearchPhase::Plateau);
        }
    }
}

TEST_F(TThroughputFeedbackTest, SparseExplorationStopsWithoutUsefulProgress)
{
    Observation_.BusyPartitionShare = 0.125;
    Observation_.Demand = 1000;
    Observation_.MaxPartitionCount = 512;
    for (int count : {8, 16, 32}) {
        EXPECT_EQ(Step(count, 16).TargetPartitionCount, 2 * count);
    }
    for (int cycle = 0; cycle < 3; ++cycle) {
        EXPECT_EQ(Step(64, 16).TargetPartitionCount, 64);
        EXPECT_FALSE(State_.CapacityDegraded);
    }
}

TEST_F(TThroughputFeedbackTest, ExplorationCreditRequiresLoadedCompletedProgress)
{
    Config_->MinimumExplorationLoadShare = 0.5;
    Observation_.BusyPartitionShare = 0.125;
    Observation_.Demand = 1000;
    EXPECT_EQ(Step(8, 16).TargetPartitionCount, 16);
    Policy_.AcceptPartitionCount(8, 64, std::nullopt);
    EXPECT_EQ(Step(64, 16).TargetPartitionCount, 64);

    Observation_.Capacity = 160;
    Observation_.Processed = 32;
    Observation_.ProcessedCredit = 32;
    EXPECT_EQ(Suggest().TargetPartitionCount, 64);
    EXPECT_EQ(State_.Exploration->BestProcessed, 16);

    Observation_.Capacity = 64;
    Observation_.Processed = 40;
    Observation_.ProcessedCredit.reset();
    EXPECT_EQ(Suggest().TargetPartitionCount, 64);
    EXPECT_EQ(State_.Exploration->BestProcessed, 16);
    Observation_.ProcessedCredit = 24;
    EXPECT_EQ(Suggest().TargetPartitionCount, 96);
    EXPECT_EQ(State_.Exploration->BestProcessed, 24);
    EXPECT_EQ(State_.Exploration->OriginProcessed, 16);
    EXPECT_EQ(State_.Exploration->OriginPartitionCount, 8);
    EXPECT_EQ(Step(96, 24).TargetPartitionCount, 96);
    EXPECT_EQ(Step(96, 16).TargetPartitionCount, 96);
    EXPECT_FALSE(State_.CapacityDegraded);
}

TEST_F(TThroughputFeedbackTest, ExplorationHistorySurvivesReleaseResetAndLegacyFloor)
{
    Observation_.BusyPartitionShare = 0.125;
    Observation_.Demand = 1000;
    EXPECT_EQ(Step(8, 16).TargetPartitionCount, 16);
    Policy_.AcceptPartitionCount(8, 64, std::nullopt);
    EXPECT_EQ(Step(64, 16).TargetPartitionCount, 64);

    Observation_.Demand = 8;
    Observation_.ExcessBacklog = 0;
    Observation_.Processed = 4;
    auto release = Suggest();
    EXPECT_EQ(release.TargetPartitionCount, 32);
    EXPECT_EQ(release.Action, EThroughputPartitioningAction::ReturnToWorkingPoint);
    Policy_.AcceptPartitionCount(64, 32, std::nullopt);
    EXPECT_EQ(Step(32, 16).TargetPartitionCount, 32);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::Idle);

    Restore(NYTree::ConvertTo<TThroughputFeedbackState>(NYson::ConvertToYsonString(State_)));
    Observation_.Demand = 1000;
    EXPECT_EQ(Step(32, 16).TargetPartitionCount, 64);
    EXPECT_EQ(Step(64, 16).TargetPartitionCount, 64);
    for (int generation = 1; generation <= 3; ++generation) {
        EXPECT_TRUE(RequestReset(generation));
        EXPECT_EQ(Step(64, 16).TargetPartitionCount, 32);
        EXPECT_EQ(Step(32, 16).TargetPartitionCount, 64);
        EXPECT_EQ(Step(64, 16).TargetPartitionCount, 64);
    }

    EXPECT_EQ(Step(96, 16).TargetPartitionCount, 96);
    EXPECT_EQ(State_.Exploration->OriginPartitionCount, 8);
    EXPECT_EQ(Policy_.ComputeExplorationPartitionCountLimit(), 64);
    EXPECT_EQ(Step(96, 32).TargetPartitionCount, 128);
    EXPECT_EQ(State_.Exploration->OriginPartitionCount, 8);
}

TEST_F(TThroughputFeedbackTest, ChangedExplorationFactorClipsAnUnappliedProbeWithoutRebasing)
{
    Observation_.BusyPartitionShare = 0.125;
    Observation_.Demand = 1000;
    ComputationSpec_->ThroughputMaxExplorationFactor = 16;
    EXPECT_EQ(Step(8, 16).TargetPartitionCount, 16);
    Policy_.AcceptPartitionCount(8, 96, std::nullopt);
    EXPECT_EQ(Step(96, 16).TargetPartitionCount, 128);

    ComputationSpec_->ThroughputMaxExplorationFactor = 8;
    EXPECT_EQ(Step(96, 16).TargetPartitionCount, 96);
    EXPECT_FALSE(State_.PendingPartitionCount);
    EXPECT_EQ(Step(96, 16).TargetPartitionCount, 96);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::RatePlateau);
    EXPECT_EQ(State_.Exploration->OriginPartitionCount, 8);

    ComputationSpec_->ThroughputMaxExplorationFactor = 16;
    EXPECT_EQ(Step(96, 16).TargetPartitionCount, 128);
    EXPECT_EQ(State_.Exploration->OriginPartitionCount, 8);
}

TEST_F(TThroughputFeedbackTest, ClippedProbeMustActuallyApplyBeforeLearningRatePlateau)
{
    Observation_.BusyPartitionShare = 0.125;
    Observation_.Demand = 1000;
    EXPECT_EQ(Step(8, 16).TargetPartitionCount, 16);
    Policy_.AcceptPartitionCount(8, 48, std::nullopt);
    EXPECT_EQ(Step(48, 16).TargetPartitionCount, 64);
    Policy_.AcceptPartitionCount(48, 48, std::nullopt);
    EXPECT_NE(State_.Phase, EThroughputSearchPhase::RatePlateau);
    EXPECT_FALSE(State_.PendingPartitionCount);
    EXPECT_EQ(Step(48, 16).TargetPartitionCount, 64);
    EXPECT_EQ(Step(48, 16).TargetPartitionCount, 64);
    EXPECT_NE(State_.Phase, EThroughputSearchPhase::RatePlateau);
    EXPECT_EQ(Step(64, 16).TargetPartitionCount, 64);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::RatePlateau);
}

TEST_F(TThroughputFeedbackTest, RecoveredExposurePreservesRetryAndDoesNotRepeatedlyReanchor)
{
    EXPECT_EQ(Step(4).TargetPartitionCount, 8);
    EXPECT_EQ(Step(8, 2).TargetPartitionCount, 4);
    ASSERT_EQ(State_.Phase, EThroughputSearchPhase::Recalibrating);
    ASSERT_TRUE(State_.CapacityDegraded);
    const auto retryPeriod = State_.RetryWarmupPeriod;

    Observation_.BusyPartitionShare = 0.125;
    EXPECT_EQ(Step(4, 2).TargetPartitionCount, 8);
    Observation_.BusyPartitionShare = 1;
    Observation_.AllowScaleUp = false;
    EXPECT_EQ(Step(8, 2).TargetPartitionCount, 8);
    ASSERT_TRUE(State_.Anchor);
    EXPECT_EQ(State_.Anchor->PartitionCount, 8);
    EXPECT_EQ(State_.Anchor->BusyPartitionShare, 1);
    EXPECT_TRUE(State_.CapacityDegraded);
    EXPECT_EQ(State_.RetryWarmupPeriod, retryPeriod);

    Observation_.AllowScaleUp = true;
    Observation_.BusyPartitionShare = 0.125;
    EXPECT_EQ(Step(8, 2).TargetPartitionCount, 16);
    Observation_.BusyPartitionShare = 1;
    EXPECT_EQ(Step(16, 2).TargetPartitionCount, 32);
    EXPECT_EQ(State_.Anchor->PartitionCount, 8);
    EXPECT_EQ(State_.Anchor->Capacity, 2);
    EXPECT_EQ(State_.Anchor->BusyPartitionShare, 1);
    EXPECT_EQ(Step(32, 2).TargetPartitionCount, 32);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::Plateau);
    EXPECT_EQ(State_.Anchor->PartitionCount, 8);
}

TEST_F(TThroughputFeedbackTest, SparseComparisonDoesNotOverrideGrowthEligibility)
{
    Observation_.BusyPartitionShare = 0.125;
    EXPECT_EQ(Step(4).TargetPartitionCount, 8);
    Observation_.PartitionCount = 8;
    Observation_.AllowScaleUp = false;
    EXPECT_EQ(Suggest().TargetPartitionCount, 8);
    Observation_.AllowScaleUp = true;
    Observation_.Processed = 1;
    EXPECT_EQ(Suggest().TargetPartitionCount, 8);
    Observation_.Processed = 4;
    Observation_.Demand = 4;
    EXPECT_EQ(Suggest().TargetPartitionCount, 8);
}

TEST_F(TThroughputFeedbackTest, LoadGateDoesNotBlockNormalizedLoadReturn)
{
    Observation_.Processed = 1;
    EXPECT_EQ(Suggest().TargetPartitionCount, 4);
    EXPECT_FALSE(State_.Anchor);
    Observation_.Processed = 1.01;
    EXPECT_EQ(Suggest().TargetPartitionCount, 8);
    EXPECT_EQ(Step(8).TargetPartitionCount, 16);
    EXPECT_EQ(Step(16).TargetPartitionCount, 16);
    Observation_.Processed = 0.5;
    Observation_.Demand = 0.5;
    Observation_.Capacity = 100;
    Observation_.ExcessBacklog = 0;
    auto result = Suggest();
    EXPECT_EQ(result.TargetPartitionCount, 4);
    EXPECT_EQ(result.Action, EThroughputPartitioningAction::ReturnToWorkingPoint);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::Plateau);
    Observation_.PartitionCount = 4;
    Observation_.Capacity = 12.5;
    EXPECT_EQ(Suggest().TargetPartitionCount, 2);
    EXPECT_FALSE(State_.Anchor);
}

TEST_F(TThroughputFeedbackTest, FallingCapacityReturnsAndKeepsRetryingAtTheWarmupCap)
{
    Config_->RetryWarmupFactor = 3;
    double capacity = 4;
    EXPECT_EQ(Step(4, capacity).TargetPartitionCount, 8);
    for (int iteration = 0; iteration < 7; ++iteration) {
        SCOPED_TRACE(iteration);
        if (iteration % 2) {
            EXPECT_EQ(Step(8, capacity).TargetPartitionCount, 16);
        }
        capacity /= 2;
        auto result = Step(iteration % 2 ? 16 : 8, capacity);
        EXPECT_EQ(result.TargetPartitionCount, 4);
        EXPECT_EQ(result.Action, EThroughputPartitioningAction::ReturnToWorkingPoint);
        EXPECT_TRUE(State_.CapacityDegraded);
        EXPECT_EQ(Policy_.GetWarmupPeriod(), TDuration::Minutes(iteration == 0 ? 9 : 24));
        EXPECT_EQ(Step(4, capacity).TargetPartitionCount, 8);
        EXPECT_TRUE(State_.CapacityDegraded);
        EXPECT_EQ(State_.Anchor->Capacity, capacity);
    }
    Config_->WarmupPeriod = TDuration::Minutes(60);
    EXPECT_EQ(Policy_.GetWarmupPeriod(), TDuration::Minutes(60));
}

TEST_F(TThroughputFeedbackTest, ResetRetainsTheLowerAnchorAndCoalescesDuringReturn)
{
    Step(4);
    Step(8);
    Step(16);
    EXPECT_TRUE(RequestReset(1));
    EXPECT_FALSE(RequestReset(1));
    EXPECT_EQ(Step(16).TargetPartitionCount, 4);
    EXPECT_FALSE(RequestReset(2));
    EXPECT_EQ(Step(4).TargetPartitionCount, 8);
    EXPECT_EQ(State_.Anchor->PartitionCount, 4);
}

TEST_F(TThroughputFeedbackTest, NormalizationCancelsUnappliedGrowth)
{
    EXPECT_EQ(Step(4).TargetPartitionCount, 8);
    Observation_.Demand = 2;
    Observation_.ExcessBacklog = 0;
    EXPECT_EQ(Step(4).TargetPartitionCount, 4);
    EXPECT_FALSE(State_.PendingPartitionCount);
    EXPECT_FALSE(State_.Anchor);
}

TEST_F(TThroughputFeedbackTest, ResetDuringPartialApplicationRetainsLowerAnchor)
{
    Step(4);
    RequestReset(1);
    EXPECT_EQ(Step(8).TargetPartitionCount, 4);
    EXPECT_EQ(State_.Anchor->PartitionCount, 4);
    EXPECT_EQ(Step(4).TargetPartitionCount, 8);
}

TEST_F(TThroughputFeedbackTest, CoveredDegradationSettlesAtArbitratedWorkingPoint)
{
    Observation_.Demand = 200;
    Step(8, 100);
    Policy_.AcceptPartitionCount(8, 32, std::nullopt);
    Observation_.PartitionCount = 32;
    Observation_.Capacity = 80;
    Observation_.Processed = Observation_.Demand = 70;
    Observation_.ExcessBacklog = 0;
    const auto decision = Suggest();
    EXPECT_EQ(decision.TargetPartitionCount, 28);
    EXPECT_EQ(decision.Action, EThroughputPartitioningAction::ReturnToWorkingPoint);
    EXPECT_TRUE(State_.CapacityDegraded);
    Policy_.AcceptPartitionCount(32, 32, std::nullopt);
    EXPECT_FALSE(State_.CapacityDegraded);
    EXPECT_FALSE(State_.PendingPartitionCount);
    EXPECT_FALSE(State_.Anchor);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::Idle);
    EXPECT_EQ(Policy_.GetWarmupPeriod(), Config_->WarmupPeriod);
    EXPECT_EQ(Step(32, 80).TargetPartitionCount, 32);

    Observation_.Demand = 160;
    EXPECT_EQ(Step(32, 80).TargetPartitionCount, 64);
    ASSERT_TRUE(State_.Anchor);
    EXPECT_EQ(State_.Anchor->PartitionCount, 32);
    EXPECT_EQ(State_.Anchor->Capacity, 80);
}

TEST_F(TThroughputFeedbackTest, CoveredDegradationNeedsNoReturnWithoutDemandHeadroom)
{
    for (const bool pendingRelease : {false, true}) {
        Restore();
        Observation_.Demand = 200;
        Step(8, 100);
        Policy_.AcceptPartitionCount(8, 32, std::nullopt);
        Observation_.PartitionCount = 32;
        Observation_.Capacity = 80;
        Observation_.ExcessBacklog = 0;
        if (pendingRelease) {
            Observation_.Processed = Observation_.Demand = 70;
            EXPECT_EQ(Suggest().TargetPartitionCount, 28);
        }
        Observation_.Processed = Observation_.Demand = 80;
        EXPECT_EQ(Suggest().TargetPartitionCount, 32);
        EXPECT_FALSE(State_.CapacityDegraded);
        EXPECT_FALSE(State_.PendingPartitionCount);
        EXPECT_FALSE(State_.Anchor);
        EXPECT_EQ(State_.Phase, EThroughputSearchPhase::Idle);
    }
}

TEST_F(TThroughputFeedbackTest, RestoredSearchContinuesWithoutManufacturingGrowth)
{
    Step(4);
    Step(8);
    Restore(NYTree::ConvertTo<TThroughputFeedbackState>(NYson::ConvertToYsonString(State_)));
    EXPECT_EQ(Step(16).TargetPartitionCount, 16);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::Plateau);
    EXPECT_EQ(Step(16, 2).TargetPartitionCount, 4);
    EXPECT_TRUE(State_.CapacityDegraded);
    EXPECT_EQ(Policy_.GetWarmupPeriod(), TDuration::Minutes(6));
    Restore(NYTree::ConvertTo<TThroughputFeedbackState>(NYson::ConvertToYsonString(State_)));
    EXPECT_EQ(Step(16, 2).TargetPartitionCount, 4);
    Policy_.AcceptPartitionCount(16, 4, std::nullopt);
    EXPECT_EQ(Step(4, 2).TargetPartitionCount, 8);
    EXPECT_TRUE(State_.CapacityDegraded);
    EXPECT_EQ(Policy_.GetWarmupPeriod(), TDuration::Minutes(6));
}

TEST_F(TThroughputFeedbackTest, BoundsDoNotInventASecondProbe)
{
    Observation_.MaxPartitionCount = 12;
    EXPECT_EQ(Step(4).TargetPartitionCount, 8);
    EXPECT_EQ(Step(8).TargetPartitionCount, 12);
    EXPECT_EQ(Step(12).TargetPartitionCount, 12);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::Plateau);
    Restore();
    Observation_.MaxPartitionCount = 8;
    EXPECT_EQ(Step(4).TargetPartitionCount, 8);
    EXPECT_EQ(Step(8).TargetPartitionCount, 8);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::FirstProbe);
    Restore();
    Observation_.MaxPartitionCount = std::numeric_limits<i64>::max();
    EXPECT_EQ(Step(std::numeric_limits<i64>::max() / 2).TargetPartitionCount, std::numeric_limits<i64>::max());
}

TEST_F(TThroughputFeedbackTest, RejectedGrowthAndFloorLimitedReturnUseActualCounts)
{
    EXPECT_EQ(Step(4).TargetPartitionCount, 8);
    Policy_.AcceptPartitionCount(4, 4, std::nullopt);
    EXPECT_EQ(Step(4).TargetPartitionCount, 8);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::FirstProbe);
    EXPECT_EQ(Step(8, 2).TargetPartitionCount, 4);
    Policy_.AcceptPartitionCount(8, 6, std::nullopt);
    EXPECT_EQ(Step(6, 2).TargetPartitionCount, 12);
    EXPECT_EQ(State_.Anchor->PartitionCount, 6);
}

TEST_F(TThroughputFeedbackTest, RefusedReturnKeepsAnchorUntilActuallyApplied)
{
    Step(4);
    Step(8);
    Step(16);
    RequestReset(1);
    for (int iteration = 0; iteration < 4; ++iteration) {
        EXPECT_EQ(Step(16).TargetPartitionCount, 4);
        Policy_.AcceptPartitionCount(16, 16, std::nullopt);
        EXPECT_EQ(State_.Anchor->PartitionCount, 4);
    }
    Observation_.Demand = 2;
    Observation_.ExcessBacklog = 0;
    EXPECT_EQ(Step(16, 8).TargetPartitionCount, 4);
    Policy_.AcceptPartitionCount(16, 12, std::nullopt);
    EXPECT_EQ(Step(12, 6).TargetPartitionCount, 12);
    EXPECT_FALSE(State_.Anchor);
}

TEST_F(TThroughputFeedbackTest, RejectedFirstProbeDoesNotRetainStaleCapacity)
{
    Step(4);
    Policy_.AcceptPartitionCount(4, 4, std::nullopt);
    EXPECT_EQ(Step(4, 2).TargetPartitionCount, 8);
    EXPECT_EQ(State_.Anchor->Capacity, 2);
    EXPECT_EQ(Step(8, 3).TargetPartitionCount, 16);
}

TEST_F(TThroughputFeedbackTest, ScaleDownRevalidationDoesNotAdvanceSearch)
{
    EXPECT_EQ(Step(4).TargetPartitionCount, 8);
    Observation_.PartitionCount = 8;
    Observation_.Capacity = 16;
    Observation_.Processed = 2;
    Observation_.ProcessedCredit = 2;
    Observation_.Demand = 2;
    Observation_.ExcessBacklog = 0;

    const auto proposal = Policy_.RevalidateScaleDown(Observation_);
    ASSERT_TRUE(proposal);
    EXPECT_EQ(proposal->TargetPartitionCount, 4);
    EXPECT_EQ(State_.PendingPartitionCount, 8);
    EXPECT_EQ(State_.ObservedPartitionCount, 4);
    ASSERT_TRUE(State_.Anchor);
    EXPECT_EQ(State_.Anchor->PartitionCount, 4);
    EXPECT_EQ(State_.Anchor->Capacity, 4);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::FirstProbe);

    Observation_.Demand = 20;
    EXPECT_FALSE(Policy_.RevalidateScaleDown(Observation_));
    EXPECT_EQ(State_.PendingPartitionCount, 8);
    EXPECT_EQ(State_.Anchor->Capacity, 4);
}

TEST_F(TThroughputFeedbackTest, PublishedShrinkTracksTheLayoutSelectedAfterCoverageRepair)
{
    Step(4);
    Step(8);
    auto restored = State_;
    restored.Phase = EThroughputSearchPhase::FirstProbe;
    restored.PendingPartitionCount.reset();
    Restore(std::move(restored));
    Observation_.PartitionCount = 8;
    Observation_.Processed = Observation_.Demand = 0.5;
    Observation_.ExcessBacklog = 0;
    const auto proposal = Suggest();
    ASSERT_EQ(proposal.TargetPartitionCount, 4);

    // The coordinator can clear invalid range coverage after observations were collected.
    auto unchanged = Policy_;
    unchanged.AcceptPartitionCount(0, 0, proposal);
    EXPECT_EQ(unchanged.GetState().Phase, EThroughputSearchPhase::Idle);
    EXPECT_FALSE(unchanged.GetState().PendingPartitionCount);

    Policy_.AcceptPartitionCount(0, 2, proposal);
    EXPECT_EQ(State_.PendingPartitionCount, 2);
    Policy_.AcknowledgeAppliedPartitionCount(2);
    EXPECT_FALSE(State_.Anchor);
    EXPECT_FALSE(State_.PendingPartitionCount);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::Idle);
}

TEST_F(TThroughputFeedbackTest, OrdinaryShrinkPreservesSearchUntilActuallyApplied)
{
    Step(4);
    Step(8);
    auto restored = State_;
    restored.Phase = EThroughputSearchPhase::FirstProbe;
    restored.PendingPartitionCount.reset();
    Restore(std::move(restored));
    Observation_.PartitionCount = 8;
    Observation_.Processed = Observation_.Demand = 0.5;
    Observation_.ExcessBacklog = 0;
    auto proposal = Suggest();
    EXPECT_EQ(proposal.TargetPartitionCount, 4);
    EXPECT_FALSE(State_.PendingPartitionCount);
    Policy_.AcceptPartitionCount(8, 8, proposal);
    ASSERT_TRUE(State_.Anchor);
    EXPECT_EQ(State_.Anchor->PartitionCount, 4);
    EXPECT_FALSE(State_.PendingPartitionCount);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::FirstProbe);
    proposal = Suggest();
    EXPECT_EQ(proposal.TargetPartitionCount, 4);
    Policy_.AcceptPartitionCount(8, 4, proposal);
    EXPECT_EQ(State_.PendingPartitionCount, 4);
    EXPECT_EQ(Step(4).TargetPartitionCount, 4);
    EXPECT_FALSE(State_.Anchor);
}

TEST_F(TThroughputFeedbackTest, PlateauReleaseRevalidatesDemandBeforeApplication)
{
    Step(4);
    Step(8);
    Step(16);
    Observation_.Demand = 2;
    Observation_.ExcessBacklog = 0;
    EXPECT_EQ(Step(16).TargetPartitionCount, 8);
    Policy_.AcceptPartitionCount(16, 16, std::nullopt);
    Observation_.Demand = 3;
    EXPECT_EQ(Step(16).TargetPartitionCount, 12);
    Policy_.AcceptPartitionCount(16, 16, std::nullopt);
    Observation_.Demand = 4;
    EXPECT_EQ(Step(16).TargetPartitionCount, 16);
    EXPECT_FALSE(State_.PendingPartitionCount);
    ASSERT_TRUE(State_.Anchor);
    EXPECT_EQ(State_.Anchor->PartitionCount, 4);
    Observation_.Demand = 5;
    EXPECT_EQ(Step(16).TargetPartitionCount, 16);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::Plateau);
    Observation_.Demand = 2;
    EXPECT_EQ(Step(16).TargetPartitionCount, 8);
    Policy_.AcceptPartitionCount(16, 8, std::nullopt);
    EXPECT_EQ(Step(8).TargetPartitionCount, 8);
    EXPECT_FALSE(State_.Anchor);
}

TEST_F(TThroughputFeedbackTest, OrdinaryShrinkCannotGoBelowDemandWhenProcessedIsLow)
{
    Observation_.PartitionCount = 32;
    Observation_.Capacity = 64;
    Observation_.Processed = 2;
    Observation_.Demand = 50;
    Observation_.ExcessBacklog = 0;
    EXPECT_EQ(Suggest().TargetPartitionCount, 25);
    Observation_.Demand = 70;
    EXPECT_EQ(Suggest().TargetPartitionCount, 32);
    EXPECT_FALSE(State_.PendingPartitionCount);
}

TEST_F(TThroughputFeedbackTest, UnappliedOrdinaryShrinkRechecksCurrentLoad)
{
    Observation_.PartitionCount = 32;
    Observation_.Capacity = 64;
    Observation_.Processed = Observation_.Demand = 2;
    Observation_.ExcessBacklog = 0;
    const auto proposal = Suggest();
    EXPECT_EQ(proposal.TargetPartitionCount, 16);
    Policy_.AcceptPartitionCount(32, 32, proposal);
    EXPECT_FALSE(State_.PendingPartitionCount);
    Observation_.Processed = 24;
    EXPECT_EQ(Suggest().TargetPartitionCount, 24);
}

TEST_F(TThroughputFeedbackTest, NumericRateNoiseDoesNotCauseGrowth)
{
    Observation_.Demand = 4 + 7.7e-10;
    EXPECT_EQ(Step(4).TargetPartitionCount, 4);
    EXPECT_FALSE(State_.Anchor);
}

TEST_F(TThroughputFeedbackTest, BacklogVetoesOrdinaryShrinkAndReserveNeverGrows)
{
    Observation_.PartitionCount = 32;
    Observation_.Capacity = 64;
    Observation_.Processed = 2;
    EXPECT_EQ(Suggest().TargetPartitionCount, 32);
    Observation_.ExcessBacklog = 0;
    Observation_.Processed = 40;
    EXPECT_EQ(Suggest().TargetPartitionCount, 32);
}

TEST_F(TThroughputFeedbackTest, PartialGrowthUsesActualAppliedCount)
{
    Observation_.Demand = 101;
    EXPECT_EQ(Step(5, 100).TargetPartitionCount, 8);
    Policy_.AcceptPartitionCount(5, 6, std::nullopt);
    EXPECT_EQ(Step(6, 100).TargetPartitionCount, 9);
    EXPECT_EQ(State_.Anchor->PartitionCount, 5);
    Policy_.AcceptPartitionCount(6, 6, std::nullopt);
    EXPECT_EQ(Step(6, 100).TargetPartitionCount, 9);
    EXPECT_EQ(Step(9, 100).TargetPartitionCount, 9);
    EXPECT_EQ(State_.Phase, EThroughputSearchPhase::Plateau);
}

TEST_F(TThroughputFeedbackTest, SteadyShortagePreservesAdmissionAndExplorationOrigin)
{
    Observation_.SteadyDemand = 800;
    Observation_.Demand = 1000;
    Observation_.AllowScaleUp = false;
    EXPECT_EQ(Step(10, 100).TargetPartitionCount, 10);
    Observation_.AllowScaleUp = true;
    Observation_.MaxPartitionCount = 25;
    EXPECT_EQ(Step(10, 100).TargetPartitionCount, 25);
    ASSERT_TRUE(State_.Exploration);
    EXPECT_EQ(State_.Exploration->OriginPartitionCount, 10);

    Restore();
    Observation_.MaxPartitionCount = 100;
    ComputationSpec_->ThroughputMaxExplorationFactor = 2.4;
    EXPECT_EQ(Step(10, 100).TargetPartitionCount, 24);
    EXPECT_EQ(Step(24, 100).TargetPartitionCount, 24);
    ASSERT_TRUE(State_.Exploration);
    EXPECT_EQ(State_.Exploration->OriginPartitionCount, 10);
    EXPECT_EQ(State_.Exploration->BestProcessed, 100);
}

TEST_F(TThroughputFeedbackTest, GainMustExceedNoiseAndExplainParallelismFromTheSameReference)
{
    Config_->CapacityChangeTolerance = 0.1;
    Config_->MinimumScalingEffectiveness = 0.5;
    Observation_.Demand = 1000;
    for (const auto [capacity, grows] : {std::pair{200.0, false}, {250.0, true}}) {
        SCOPED_TRACE(capacity);
        Restore();
        EXPECT_EQ(Step(10, 100).TargetPartitionCount, 20);
        EXPECT_EQ(Step(20, 120).TargetPartitionCount, 40);
        const auto decision = Step(40, capacity);
        EXPECT_EQ(decision.TargetPartitionCount > 40, grows);
        EXPECT_EQ(State_.Anchor->PartitionCount, grows ? 40 : 10);
    }
    Restore();
    Config_->MinimumScalingEffectiveness = 0.1;
    Observation_.Demand = 101;
    EXPECT_EQ(Step(10, 100).TargetPartitionCount, 15);
    Step(15, 110);
    EXPECT_EQ(State_.Anchor->PartitionCount, 10);
    Step(15, 111);
    EXPECT_EQ(State_.Anchor->PartitionCount, 15);
}

TEST_F(TThroughputFeedbackTest, SuccessfulGrowthSurvivesUnderfeedingBeforeCapacityLoss)
{
    Observation_.Demand = 40;
    Observation_.ExcessBacklog = 0;
    EXPECT_EQ(Step(2, 38).TargetPartitionCount, 3);

    Observation_.PartitionCount = 3;
    Observation_.Capacity = 55;
    Observation_.Processed = 43;
    EXPECT_EQ(Suggest().TargetPartitionCount, 3);

    ASSERT_TRUE(State_.Anchor);
    EXPECT_EQ(State_.Anchor->PartitionCount, 3);
    Observation_.Processed = 40;
    Observation_.BusyPartitionShare = 0;
    for (int window = 0; window < 3; ++window) {
        EXPECT_EQ(Suggest().TargetPartitionCount, 3);
    }

    Observation_.Capacity = 24;
    Observation_.Processed = 24;
    Observation_.BusyPartitionShare = 1;
    Observation_.Demand = 60;
    Observation_.ExcessBacklog = 100;
    const auto decision = Suggest();
    EXPECT_EQ(decision.TargetPartitionCount, 6);
    EXPECT_FALSE(State_.CapacityDegraded);
}

} // namespace
} // namespace NYT::NFlow
