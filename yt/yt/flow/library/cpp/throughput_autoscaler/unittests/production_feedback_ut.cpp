#include <yt/yt/flow/library/cpp/throughput_autoscaler/throughput_autoscaler.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TThroughputProductionFeedbackTest
    : public ::testing::Test
{
protected:
    const TInstant Start_ = TInstant::Seconds(1'800'000);
    const TComputationId Id_{"work"};
    const TStreamId Input_{"input"};
    TThroughputAutoscaler Autoscaler_;
    TPartitioningSpecPtr Partitioning_ = New<TPartitioningSpec>();
    TThroughputAutoscaler::TComputations Computations_;
    TThroughputSpecPtr Config_ = New<TThroughputSpec>();
    i64 Count_ = 4;
    std::optional<double> NewCount_ = 2;
    double ProcessedCount_ = 4;
    double CapacityCount_ = 4;
    double ReadyCount_ = 28'800;
    double InputWaiting_ = 0;
    double OutputWaiting_ = 0;
    double BusyPartitionShare_ = 1;
    bool HaveMeasurement_ = true;

    const TThroughputFeedbackState& Feedback() const
    {
        return Autoscaler_.GetFeedbackState(Id_);
    }

    void Restore(TThroughputFeedbackState state)
    {
        Autoscaler_ = TThroughputAutoscaler();
        Autoscaler_.InitializeComputation(Id_, std::move(state));
    }

    void SetUp() override
    {
        Restore({});
        auto spec = New<TComputationSpec>();
        spec->InputStreamIds.insert(Input_);
        Partitioning_->MinPartitionCount = 1;
        Partitioning_->MaxPartitionCount = 64;
        Computations_[Id_] = {
            .Spec = spec,
            .PartitioningSpec = Partitioning_,
        };
    }

    TThroughputPipelineObservation Sample(int seconds)
    {
        TThroughputComputationObservation computation;
        computation.PartitionCount = Count_;
        computation.ActualBatchDuration = TDuration::Seconds(1);
        computation.ReadyAt = Start_;
        computation.AllJobsHealthy = true;
        computation.Streams[Input_] = {
            .Ready = ReadyCount_,
            .Offered = NewCount_,
        };
        if (HaveMeasurement_) {
            auto& measurement = computation.Measurement.emplace();
            measurement.Begin = Start_ + TDuration::Seconds(seconds) - std::max(Config_->WarmupPeriod, Feedback().RetryWarmupPeriod);
            measurement.End = Start_ + TDuration::Seconds(seconds);
            measurement.Capacity = CapacityCount_;
            measurement.Processed = ProcessedCount_;
            measurement.BusyPartitionShare = BusyPartitionShare_;
            measurement.InputWaitingShare = InputWaiting_;
            measurement.OutputWaitingShare = OutputWaiting_;
        }
        return {
            .Now = Start_ + TDuration::Seconds(seconds),
            .Config = Config_,
            .Computations = {{Id_, std::move(computation)}},
        };
    }

    void Feed(int seconds)
    {
        auto sample = Sample(seconds);
        Autoscaler_.Reconfigure(Computations_, sample.Config);
        Autoscaler_.Evaluate(sample);
    }

    void Settle()
    {
        Feed(240);
        Feed(300);
        Feed(360);
        Feed(420);
    }

    std::optional<TThroughputAutoscalerDecision> Decision()
    {
        return Autoscaler_.FindDecision(Id_);
    }
};

TEST_F(TThroughputProductionFeedbackTest, AbortedGrowthDoesNotConsumeWarmupOrAcceptance)
{
    Feed(0);
    Autoscaler_.Commit();
    Feed(240);
    ASSERT_TRUE(Decision());
    const i64 target = Decision()->TargetPartitionCount;
    Autoscaler_.AcceptPartitionCount(Id_, Count_, target);
    ASSERT_EQ(Feedback().PendingPartitionCount, target);

    Autoscaler_.BeginIteration();
    EXPECT_FALSE(Feedback().PendingPartitionCount);
    EXPECT_FALSE(Decision());
    Feed(240);
    ASSERT_TRUE(Decision());
    EXPECT_EQ(Decision()->TargetPartitionCount, target);
}

TEST_F(TThroughputProductionFeedbackTest, ConfiguredWarmupAndDegradationBackoffGatePublicDecisions)
{
    Config_->WarmupPeriod = TDuration::Minutes(7);
    Feed(240);
    NewCount_ = 20;
    Feed(659);
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Feedback().Anchor);
    NewCount_ = 2;
    Feed(660);
    ASSERT_TRUE(Decision());
    EXPECT_EQ(Decision()->TargetPartitionCount, 8);
    Autoscaler_.AcceptPartitionCount(Id_, 4, 8);
    Count_ = 8;
    ProcessedCount_ = CapacityCount_ = 3;
    Feed(720);
    ProcessedCount_ = CapacityCount_ = 2;
    Feed(1080);
    Feed(1139);
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Autoscaler_.FindTelemetry(Id_)->CapacityDegraded);
    ASSERT_TRUE(Feedback().Anchor);
    EXPECT_EQ(Feedback().Anchor->Capacity, 4);
    Feed(1140);
    ASSERT_TRUE(Decision());
    EXPECT_EQ(Decision()->TargetPartitionCount, 4);
    EXPECT_TRUE(Autoscaler_.FindTelemetry(Id_)->CapacityDegraded);
    Autoscaler_.AcceptPartitionCount(Id_, 8, 4);
    Count_ = 4;
    Feed(1200);
    Feed(2039);
    EXPECT_FALSE(Decision());
    Feed(2040);
    ASSERT_TRUE(Decision());
    EXPECT_EQ(Decision()->TargetPartitionCount, 8);
    EXPECT_TRUE(Autoscaler_.FindTelemetry(Id_)->CapacityDegraded);
}

class TThroughputFallingCapacityTest
    : public TThroughputProductionFeedbackTest
    , public ::testing::WithParamInterface<bool>
{ };

TEST_P(TThroughputFallingCapacityTest, WaitsForCompleteWindowBeforeGrowthOrShrink)
{
    const bool shrink = GetParam();
    Config_->ScaleDownInterval = TDuration::Zero();
    Count_ = shrink ? 32 : 4;
    NewCount_ = ProcessedCount_ = shrink ? 0.5 : 2;
    ReadyCount_ = shrink ? 0 : 28'800;
    Feed(240);
    CapacityCount_ = 3;
    Feed(360);
    CapacityCount_ = 2.5;
    Feed(419);
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Feedback().Anchor);
    Feed(420);
    ASSERT_TRUE(Decision());
    EXPECT_EQ(Decision()->TargetPartitionCount, shrink ? 16 : 8);
    if (!shrink) {
        ASSERT_TRUE(Feedback().Anchor);
        EXPECT_EQ(Feedback().Anchor->Capacity, 2.5);
    }
}

INSTANTIATE_TEST_SUITE_P(GrowthAndShrink, TThroughputFallingCapacityTest, ::testing::Bool());

TEST_F(TThroughputProductionFeedbackTest, ShortageMustPersistAlongsideCapacityWarmup)
{
    ReadyCount_ = 0;
    NewCount_ = 4;
    Feed(240);
    Feed(400);
    NewCount_ = 4.01;
    Feed(410);
    Feed(420);
    EXPECT_FALSE(Decision());
    NewCount_ = 4;
    Feed(500);
    NewCount_ = 4.01;
    Feed(510);
    Feed(689);
    EXPECT_FALSE(Decision());
    Feed(690);
    ASSERT_TRUE(Decision());
    EXPECT_GT(Decision()->TargetPartitionCount, Count_);
}

TEST_F(TThroughputProductionFeedbackTest, RepeatedMeasurementDoesNotConfirmShortage)
{
    Feed(240);
    auto sample = Sample(600);
    GetOrCrash(sample.Computations, Id_).Measurement->End = Start_ + TDuration::Seconds(240);
    GetOrCrash(sample.Computations, Id_).Measurement->Begin = Start_;
    Autoscaler_.Reconfigure(Computations_, sample.Config);
    Autoscaler_.Evaluate(sample);
    EXPECT_FALSE(Decision());
    Feed(630);
    ASSERT_TRUE(Decision());
    EXPECT_EQ(Decision()->TargetPartitionCount, 8);
}

TEST_F(TThroughputProductionFeedbackTest, AppliedReleaseAcknowledgesHealthyLayoutWithoutMeasurement)
{
    Count_ = 16;
    Partitioning_->MaxPartitionCount = 128;
    CapacityCount_ = ProcessedCount_ = 268;
    NewCount_ = 6000;
    ReadyCount_ = 0;
    Feed(240);
    Feed(420);
    ASSERT_TRUE(Decision());
    Autoscaler_.AcceptPartitionCount(Id_, Count_, 128);
    Count_ = 128;
    CapacityCount_ = ProcessedCount_ = 113;
    NewCount_ = 60;
    Feed(425);
    Feed(605);
    ASSERT_TRUE(Decision());
    ASSERT_EQ(Decision()->TargetPartitionCount, 68);
    ASSERT_TRUE(Feedback().CapacityDegraded);
    Autoscaler_.AcceptPartitionCount(Id_, Count_, 68);
    Count_ = 68;

    HaveMeasurement_ = false;
    auto sample = Sample(606);
    GetOrCrash(sample.Computations, Id_).Repartitioning = true;
    Autoscaler_.Reconfigure(Computations_, sample.Config);
    Autoscaler_.Evaluate(sample);
    EXPECT_TRUE(Feedback().PendingPartitionCount);
    sample = Sample(607);
    GetOrCrash(sample.Computations, Id_).AllJobsHealthy = false;
    Autoscaler_.Reconfigure(Computations_, sample.Config);
    Autoscaler_.Evaluate(sample);
    EXPECT_TRUE(Feedback().PendingPartitionCount);
    Feed(608);
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Feedback().PendingPartitionCount);
    EXPECT_EQ(Feedback().ObservedPartitionCount, Count_);
    EXPECT_EQ(Feedback().Phase, EThroughputSearchPhase::Idle);
    EXPECT_FALSE(Feedback().CapacityDegraded);
    EXPECT_EQ(std::max(Config_->WarmupPeriod, Feedback().RetryWarmupPeriod), Config_->WarmupPeriod);
}

TEST_F(TThroughputProductionFeedbackTest, CoveredDemandPausesSearchWithoutRestartingAtLargerCount)
{
    Count_ = 32;
    Partitioning_->MaxPartitionCount = 256;
    ReadyCount_ = 0;
    NewCount_ = 4;
    TThroughputFeedbackState initial;
    initial.Anchor.emplace();
    initial.Anchor->PartitionCount = 8;
    initial.Anchor->Capacity = 4;
    initial.Anchor->BusyPartitionShare = 1;
    initial.ObservedPartitionCount = 8;
    initial.PendingPartitionCount = 32;
    initial.Phase = EThroughputSearchPhase::FirstProbe;
    Restore(std::move(initial));
    const auto& feedback = Feedback();
    Feed(240);
    Feed(420);
    EXPECT_FALSE(Decision());
    ASSERT_TRUE(feedback.Anchor);
    EXPECT_EQ(feedback.Anchor->PartitionCount, 8);
    ProcessedCount_ = CapacityCount_ = 3.9936988;
    Feed(450);
    Feed(630);
    ASSERT_TRUE(Decision());
    EXPECT_EQ(Decision()->TargetPartitionCount, 48);
    Autoscaler_.AcceptPartitionCount(Id_, 32, 48);
    Count_ = 48;
    ProcessedCount_ = CapacityCount_ = 4;
    Feed(660);
    Feed(840);
    EXPECT_FALSE(Decision());
    EXPECT_EQ(feedback.Phase, EThroughputSearchPhase::Plateau);
    ProcessedCount_ = CapacityCount_ = 3.99;
    Feed(900);
    Feed(1080);
    EXPECT_FALSE(Decision());
    EXPECT_EQ(feedback.Anchor->PartitionCount, 8);
}

TEST_F(TThroughputProductionFeedbackTest, HourlyBatchIncludesEarlierWarmNodesAndRevalidatesEachRecommendation)
{
    Count_ = 32;
    NewCount_ = ProcessedCount_ = 1;
    CapacityCount_ = 32;
    ReadyCount_ = 0;
    for (const auto* name : {"later", "loaded", "moved", "unhealthy", "falling", "backlogged"}) {
        auto entry = GetOrCrash(Computations_, Id_);
        Computations_[TComputationId(name)] = entry;
    }
    auto feed = [&] (int seconds) {
        auto sample = Sample(seconds);
        for (const auto& [id, _] : Computations_) {
            auto node = GetOrCrash(sample.Computations, Id_);
            if (id == TComputationId("later")) {
                node.ReadyAt = Start_ + TDuration::Seconds(500);
                node.Repartitioning = seconds < 500;
            }
            if (id == TComputationId("loaded") && seconds >= 3839) {
                GetOrCrash(node.Streams, Input_).Offered = 40;
            }
            if (id == TComputationId("backlogged") && seconds >= 3839 && seconds < 4020) {
                GetOrCrash(node.Streams, Input_).Ready = 1000;
            }
            if (id == TComputationId("moved") && seconds >= 3839) {
                node.ReadyAt = Start_ + TDuration::Seconds(3839);
            }
            if (id == TComputationId("unhealthy") && seconds >= 3839) {
                node.AllJobsHealthy = false;
            }
            if (id == TComputationId("falling") && seconds >= 3839) {
                node.Measurement->Capacity = 25;
            }
            if (id == Id_ && seconds == 3000) {
                node.Measurement->Processed = 12;
            }
            sample.Computations[id] = std::move(node);
        }
        Autoscaler_.Reconfigure(Computations_, sample.Config);
        Autoscaler_.Evaluate(sample);
    };
    for (int seconds : {240, 420, 500, 680, 3000, 3839}) {
        feed(seconds);
        for (const auto& [id, _] : Computations_) {
            EXPECT_FALSE(Autoscaler_.FindDecision(id));
        }
    }
    feed(3840);
    EXPECT_FALSE(Autoscaler_.FindTelemetry(Id_)->WarmupReady);
    EXPECT_FALSE(Autoscaler_.FindTelemetry(TComputationId("later"))->WarmupReady);
    ASSERT_TRUE(Decision());
    EXPECT_EQ(Decision()->TargetPartitionCount, 24);
    ASSERT_TRUE(Autoscaler_.FindDecision(TComputationId("later")));
    EXPECT_EQ(Autoscaler_.FindDecision(TComputationId("later"))->TargetPartitionCount, 16);
    const TComputationId backloggedId("backlogged");
    const auto telemetry = Autoscaler_.FindTelemetry(backloggedId);
    ASSERT_TRUE(telemetry && telemetry->Demand && telemetry->Capacity);
    EXPECT_LT(*telemetry->Demand, *telemetry->Capacity);
    for (const auto* name : {"loaded", "moved", "unhealthy", "falling", "backlogged"}) {
        EXPECT_FALSE(Autoscaler_.FindDecision(TComputationId(name))) << name;
    }
    feed(4020);
    feed(4200);
    const auto drainedDecision = Autoscaler_.FindDecision(backloggedId);
    ASSERT_TRUE(drainedDecision);
    EXPECT_LT(drainedDecision->TargetPartitionCount, Count_);
}

TEST_F(TThroughputProductionFeedbackTest, OrdinaryShrinksShareHourlyBatchesAndOnlyAppliedShrinkClosesWindow)
{
    Config_->WarmupPeriod = TDuration::Seconds(1);
    Count_ = 32;
    NewCount_ = ProcessedCount_ = 1;
    CapacityCount_ = 32;
    ReadyCount_ = 0;
    const TComputationId otherId("other");
    auto other = GetOrCrash(Computations_, Id_);
    Computations_[otherId] = other;
    auto feed = [&] (int seconds, i64 otherCount) {
        auto sample = Sample(seconds);
        sample.Computations[otherId] = GetOrCrash(sample.Computations, Id_);
        GetOrCrash(sample.Computations, otherId).PartitionCount = otherCount;
        Autoscaler_.Reconfigure(Computations_, sample.Config);
        Autoscaler_.Evaluate(sample);
    };
    feed(240, 32);
    feed(3839, 32);
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Autoscaler_.FindDecision(otherId));
    feed(3840, 32);
    ASSERT_TRUE(Decision());
    ASSERT_TRUE(Autoscaler_.FindDecision(otherId));
    EXPECT_EQ(Decision()->TargetPartitionCount, 16);
    EXPECT_EQ(Autoscaler_.FindDecision(otherId)->TargetPartitionCount, 16);
    Autoscaler_.AcceptPartitionCount(Id_, 32, 32);
    Autoscaler_.AcceptPartitionCount(otherId, 32, 32);
    feed(3841, 32);
    feed(3842, 32);
    ASSERT_TRUE(Decision());
    ASSERT_TRUE(Autoscaler_.FindDecision(otherId));
    Autoscaler_.AcceptPartitionCount(Id_, 32, 24);
    Autoscaler_.AcceptPartitionCount(otherId, 32, 32);
    Count_ = 24;
    feed(3843, 32);
    feed(3844, 32);
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Autoscaler_.FindDecision(otherId));
    feed(7441, 32);
    EXPECT_FALSE(Decision());
    feed(7442, 32);
    ASSERT_TRUE(Decision());
    ASSERT_TRUE(Autoscaler_.FindDecision(otherId));
    EXPECT_EQ(Decision()->TargetPartitionCount, 12);
    Autoscaler_.AcceptPartitionCount(Id_, 24, 12);
    EXPECT_EQ(Autoscaler_.FindDecision(otherId)->TargetPartitionCount, 16);
    Autoscaler_.AcceptPartitionCount(otherId, 32, 16);
    Count_ = 12;
    feed(7443, 16);
    feed(7444, 16);
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Autoscaler_.FindDecision(otherId));
}

TEST_F(TThroughputProductionFeedbackTest, PlateauReturnDoesNotConsumeOrdinaryScaleDownWindow)
{
    Config_->WarmupPeriod = TDuration::Seconds(1);
    Count_ = 32;
    NewCount_ = ProcessedCount_ = 0.5;
    ReadyCount_ = 0;
    TThroughputFeedbackState initial;
    initial.Anchor.emplace();
    initial.Anchor->PartitionCount = 4;
    initial.Anchor->Capacity = 4;
    initial.Anchor->BusyPartitionShare = 1;
    initial.ObservedPartitionCount = Count_;
    initial.Phase = EThroughputSearchPhase::Plateau;
    Restore(std::move(initial));
    Feed(240);
    Feed(241);
    ASSERT_TRUE(Decision());
    EXPECT_LT(Decision()->TargetPartitionCount, Count_);
    EXPECT_EQ(Decision()->Action, EThroughputPartitioningAction::ReturnToWorkingPoint);
    Autoscaler_.AcceptPartitionCount(Id_, Count_, Decision()->TargetPartitionCount);
    Count_ = Decision()->TargetPartitionCount;
    ReadyCount_ = 0;
    NewCount_ = ProcessedCount_ = 0.5;
    Feed(242);
    Feed(3839);
    EXPECT_FALSE(Decision());
    Feed(3840);
    ASSERT_TRUE(Decision());
    EXPECT_LT(Decision()->TargetPartitionCount, Count_);
    EXPECT_EQ(Decision()->Action, EThroughputPartitioningAction::Regular);
}

TEST_F(TThroughputProductionFeedbackTest, EmptyComputationDoesNotInventShrinkDecision)
{
    Count_ = 32;
    NewCount_ = 0;
    ProcessedCount_ = CapacityCount_ = 0;
    ReadyCount_ = 0;
    Settle();
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Feedback().Anchor);
}

TEST_F(TThroughputProductionFeedbackTest, SparseDegradedProbeUsesMeasuredExposureAndKeepsOutputWaitGate)
{
    Settle();
    ASSERT_TRUE(Decision());
    Autoscaler_.AcceptPartitionCount(Id_, 4, 8);
    Count_ = 8;
    BusyPartitionShare_ = 0.125;
    ProcessedCount_ = CapacityCount_ = 2;
    Config_->MaximumOutputWaitingShare = 0.1;
    OutputWaiting_ = 0.15;
    Feed(600);
    Feed(780);
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Feedback().CapacityDegraded);
    OutputWaiting_ = 0.05;
    Feed(800);
    EXPECT_FALSE(Decision());
    Feed(979);
    EXPECT_FALSE(Decision());
    Feed(980);
    ASSERT_TRUE(Decision());
    EXPECT_EQ(Decision()->TargetPartitionCount, 16);
    EXPECT_FALSE(Feedback().CapacityDegraded);
    ASSERT_TRUE(Feedback().Anchor);
    EXPECT_EQ(Feedback().Anchor->PartitionCount, 4);
    EXPECT_EQ(Feedback().Anchor->BusyPartitionShare, 1);
}

TEST_F(TThroughputProductionFeedbackTest, MissingMeasurementWaitsForFreshWarmup)
{
    HaveMeasurement_ = false;
    Settle();
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Feedback().Anchor);
    HaveMeasurement_ = true;
    Feed(600);
    EXPECT_FALSE(Decision());
    Feed(780);
    ASSERT_TRUE(Decision());
    EXPECT_EQ(Decision()->TargetPartitionCount, 8);
}

class TThroughputMeasurementGapTest
    : public TThroughputProductionFeedbackTest
    , public ::testing::WithParamInterface<double>
{ };

TEST_P(TThroughputMeasurementGapTest, CompleteWindowAfterGapPreservesWarmupReference)
{
    Feed(240);
    HaveMeasurement_ = false;
    Feed(420);
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Autoscaler_.FindTelemetry(Id_)->WarmupReady);
    HaveMeasurement_ = true;
    CapacityCount_ = GetParam();
    Feed(450);
    ASSERT_TRUE(Decision());
    EXPECT_EQ(Decision()->TargetPartitionCount, 8);
}

INSTANTIATE_TEST_SUITE_P(UnchangedAndChangedCapacity, TThroughputMeasurementGapTest, ::testing::Values(4.0, 4.8));

TEST_F(TThroughputProductionFeedbackTest, ObservationCannotCrossActualCohortBoundary)
{
    Feed(240);
    auto sample = Sample(420);
    auto& computation = GetOrCrash(sample.Computations, Id_);
    computation.ReadyAt = Start_ + TDuration::Seconds(350);
    Autoscaler_.Reconfigure(Computations_, sample.Config);
    Autoscaler_.Evaluate(sample);
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Feedback().Anchor);
}

TEST_F(TThroughputProductionFeedbackTest, RemovedComputationCannotRetainDecision)
{
    Settle();
    ASSERT_TRUE(Decision());
    const auto registered = GetOrCrash(Computations_, Id_);
    ASSERT_TRUE(Feedback().Exploration);
    Computations_.clear();
    auto emptySample = Sample(600);
    auto renewedSample = Sample(660);
    Autoscaler_.Reconfigure(Computations_, emptySample.Config);
    Autoscaler_.Evaluate(emptySample);
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Autoscaler_.FindTelemetry(Id_));
    Computations_[Id_] = registered;
    Autoscaler_.Reconfigure(Computations_, renewedSample.Config);
    Autoscaler_.Evaluate(renewedSample);
    EXPECT_FALSE(Decision());
    EXPECT_FALSE(Feedback().Exploration);
    Feed(840);
    ASSERT_TRUE(Decision());
    EXPECT_EQ(Decision()->TargetPartitionCount, 8);
}

TEST_F(TThroughputProductionFeedbackTest, NewPartitioningLimitAppliesToNextRound)
{
    Settle();
    ASSERT_TRUE(Decision());
    Partitioning_->MaxPartitionCount = 6;
    Feed(600);
    Feed(780);
    ASSERT_TRUE(Decision());
    EXPECT_GT(Decision()->TargetPartitionCount, Count_);
    Partitioning_->DesiredPartitionCount = Count_;
    Feed(960);
    EXPECT_FALSE(Decision());
}

TEST_F(TThroughputProductionFeedbackTest, OnlyTheAffectedComputationWaitsForItsObservation)
{
    const TComputationId missing("missing");
    const TComputationId moving("moving");
    const TComputationId retrying("retrying");
    const TComputationId delayed("delayed");
    for (const auto& id : {missing, moving, retrying, delayed}) {
        Computations_[id] = GetOrCrash(Computations_, Id_);
    }
    TThroughputFeedbackState retry;
    retry.RetryWarmupPeriod = TDuration::Minutes(12);
    Autoscaler_.InitializeComputation(retrying, std::move(retry));
    for (int seconds : {240, 300, 360, 420, 480}) {
        CapacityCount_ = ProcessedCount_ = seconds == 360 ? 3 : 4;
        auto sample = Sample(seconds);
        for (const auto& id : {missing, moving, retrying, delayed}) {
            sample.Computations[id] = GetOrCrash(sample.Computations, Id_);
        }
        GetOrCrash(sample.Computations, missing).Measurement.reset();
        GetOrCrash(sample.Computations, moving).Repartitioning = true;
        if (seconds == 420) {
            GetOrCrash(sample.Computations, delayed).Measurement->End = Start_ + TDuration::Seconds(360);
        }
        Autoscaler_.Reconfigure(Computations_, sample.Config);
        Autoscaler_.Evaluate(sample);
        for (const auto& id : {missing, moving, retrying}) {
            EXPECT_FALSE(Autoscaler_.FindDecision(id));
        }
        if (seconds == 420) {
            ASSERT_TRUE(Decision());
            EXPECT_GT(Decision()->TargetPartitionCount, Count_);
            EXPECT_FALSE(Autoscaler_.FindDecision(delayed));
        }
    }
    ASSERT_TRUE(Autoscaler_.FindDecision(delayed));
    EXPECT_GT(Autoscaler_.FindDecision(delayed)->TargetPartitionCount, Count_);
}

TEST_F(TThroughputProductionFeedbackTest, RepartitioningRestartsOwnObservation)
{
    Feed(240);
    auto sample = Sample(420);
    GetOrCrash(sample.Computations, Id_).Repartitioning = true;
    Autoscaler_.Reconfigure(Computations_, sample.Config);
    Autoscaler_.Evaluate(sample);
    EXPECT_FALSE(Decision());
    Feed(600);
    EXPECT_FALSE(Decision());
    Feed(780);
    EXPECT_TRUE(Decision());
}

TEST_F(TThroughputProductionFeedbackTest, UnhealthyCohortCannotSupplyCapacity)
{
    for (int seconds : {240, 420, 600}) {
        auto sample = Sample(seconds);
        GetOrCrash(sample.Computations, Id_).AllJobsHealthy = false;
        Autoscaler_.Reconfigure(Computations_, sample.Config);
        Autoscaler_.Evaluate(sample);
        EXPECT_FALSE(Decision());
    }
    EXPECT_FALSE(Feedback().Anchor);
}

TEST_F(TThroughputProductionFeedbackTest, StarvationDoesNotEraseBaseline)
{
    Settle();
    InputWaiting_ = 0.8;
    ReadyCount_ = 0;
    ProcessedCount_ = 0.1;
    CapacityCount_ = 0.5;
    Feed(600);
    Feed(780);
    EXPECT_FALSE(Decision());
    ASSERT_TRUE(Feedback().Anchor);
    EXPECT_EQ(Feedback().Anchor->Capacity, 4);
}

} // namespace
} // namespace NYT::NFlow
