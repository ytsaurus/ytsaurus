#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/flow/library/cpp/throughput_autoscaler/feedback_state.h>
#include <yt/yt/flow/library/cpp/throughput_autoscaler/processing_meter.h>
#include <yt/yt/flow/library/cpp/throughput_autoscaler/throughput_autoscaler.h>

#include <yt/yt/flow/library/cpp/common/persisted_state_control.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERejection,
    (OriginalLayout)
    (SparseWork)
    (OutputWaiting)
    (PendingReturn)
    (ChangedCohort)
    (NewShortage)
    (UnhealthyJobs)
);

const TInstant StartTime = TInstant::Seconds(10'000);

TThroughputStreamObservation MakeStream(double offered, double ready = 0)
{
    return {.Ready = ready, .Offered = offered};
}

TLineageRatio MakeLineageRatio(double count, std::optional<double> inputCount = 100)
{
    TLineageRatio result;
    if (inputCount && *inputCount > 0) {
        result.Count.emplace();
        result.Count->Ratio = count / *inputCount;
        result.Count->Weight = *inputCount;
    }
    return result;
}

TThroughputComputationObservation MakeComputation(
    i64 partitionCount,
    THashMap<TStreamId, TThroughputStreamObservation> streams,
    TLineageRatios lineageRatios = {})
{
    return {
        .PartitionCount = partitionCount,
        .AllJobsHealthy = true,
        .ActualBatchDuration = TDuration::Seconds(1),
        .Streams = std::move(streams),
        .LineageRatios = std::move(lineageRatios),
    };
}

class TThroughputAutoscalerTest
    : public ::testing::Test
{
protected:
    TThroughputAutoscaler Autoscaler_;
    TThroughputAutoscaler::TComputations Computations_;

    void AddComputation(
        const TComputationId& computationId,
        THashSet<TStreamId> inputStreams,
        THashSet<TStreamId> outputStreams = {},
        THashMap<TStreamId, THashSet<TStreamId>> dependencies = {},
        TPartitioningSpecPtr partitioningSpec = New<TPartitioningSpec>())
    {
        auto spec = New<TComputationSpec>();
        spec->InputStreamIds = std::move(inputStreams);
        spec->OutputStreamIds = std::move(outputStreams);
        spec->StreamsDependency = std::move(dependencies);
        AddComputation(computationId, std::move(spec), std::move(partitioningSpec));
    }

    void AddComputation(
        const TComputationId& computationId,
        TComputationSpecPtr spec,
        TPartitioningSpecPtr partitioningSpec = New<TPartitioningSpec>())
    {
        Computations_[computationId] = {
            .Spec = std::move(spec),
            .PartitioningSpec = std::move(partitioningSpec),
        };
    }

    void AddSourceComputation(
        const TComputationId& computationId,
        const TStreamId& sourceStream,
        THashSet<TStreamId> outputStreams,
        THashMap<TStreamId, THashSet<TStreamId>> dependencies)
    {
        auto spec = New<TComputationSpec>();
        spec->SourceStreams[sourceStream] = New<TSourceSpec>();
        spec->OutputStreamIds = std::move(outputStreams);
        spec->StreamsDependency = std::move(dependencies);
        AddComputation(computationId, std::move(spec), /*partitioningSpec*/ nullptr);
    }

    TThroughputPipelineObservation MakePipeline(
        TInstant now,
        THashMap<TComputationId, TThroughputComputationObservation> computations) const
    {
        auto config = New<TThroughputSpec>();
        config->CatchUpPeriod = TDuration::Minutes(30);
        return {
            .Now = now,
            .Config = std::move(config),
            .Computations = std::move(computations),
        };
    }

    void Evaluate(TThroughputPipelineObservation sample)
    {
        Autoscaler_.Reconfigure(Computations_, sample.Config);
        Autoscaler_.Evaluate(std::move(sample));
    }

    void MeasureThroughWarmup(
        TThroughputPipelineObservation sample,
        const THashMap<TComputationId, double>& capacities)
    {
        for (auto now : {sample.Now, sample.Now + sample.Config->WarmupPeriod}) {
            sample.Now = now;
            for (const auto& [id, capacity] : capacities) {
                auto& measurement = GetOrCrash(sample.Computations, id).Measurement.emplace();
                measurement.Begin = now - sample.Config->WarmupPeriod;
                measurement.End = now;
                measurement.Processed = capacity;
                measurement.Capacity = capacity;
                measurement.BusyPartitionShare = 1;
            }
            Evaluate(sample);
        }
    }

    std::optional<TThroughputAutoscalerDecision> Decision(
        const TComputationId& computationId) const
    {
        return Autoscaler_.FindDecision(computationId);
    }
};

TEST_F(TThroughputAutoscalerTest, SteadyPeakProtectsWeekendShrinkAndExpires)
{
    const TComputationId id("consumer");
    const TStreamId input("input");
    AddComputation(id, {input});
    i64 count = 100;
    auto observe = [&] (TInstant now, double offered) {
        auto computation = MakeComputation(count, {{input, MakeStream(offered)}});
        computation.Measurement = TThroughputProcessingMeasurement{
            .Begin = now - TDuration::Minutes(3),
            .End = now,
            .Processed = offered,
            .Capacity = 4.0 * count,
        };
        auto sample = MakePipeline(now, {{id, std::move(computation)}});
        sample.Config->ScaleDownInterval = TDuration::Zero();
        Autoscaler_.BeginIteration();
        Evaluate(std::move(sample));
        if (const auto decision = Decision(id)) {
            Autoscaler_.AcceptPartitionCount(id, count, decision->TargetPartitionCount);
            count = decision->TargetPartitionCount;
        }
        Autoscaler_.Commit();
    };
    observe(StartTime, 150);
    observe(StartTime + TDuration::Minutes(3), 150);
    ASSERT_EQ(count, 75);
    observe(StartTime + TDuration::Days(8), 20);
    observe(StartTime + TDuration::Days(8) + TDuration::Minutes(3), 20);
    EXPECT_EQ(count, 75);
    observe(StartTime + TDuration::Days(10), 20);
    EXPECT_EQ(count, 38);
}

TEST_F(TThroughputAutoscalerTest, SteadyPeakSurvivesDecisionAbortResetAndRestartWithoutCatchUpInflation)
{
    const TComputationId id("consumer");
    const TStreamId input("input");
    AddComputation(id, {input});
    auto sample = MakePipeline(StartTime, {{id, MakeComputation(100, {{input, MakeStream(100, 1'000'000)}})}});
    sample.Config->PeakDemandWindow = TDuration::Days(2);
    sample.Config->ScaleDownInterval = TDuration::Zero();
    auto observe = [&] (TInstant now) {
        sample.Now = now;
        GetOrCrash(sample.Computations, id).Measurement = TThroughputProcessingMeasurement{
            .Begin = now - TDuration::Minutes(3),
            .End = now,
            .Processed = 350,
            .Capacity = 400,
        };
        Autoscaler_.BeginIteration();
        Evaluate(sample);
    };
    observe(StartTime);
    Autoscaler_.Commit();
    observe(StartTime + TDuration::Minutes(3));
    EXPECT_EQ(Autoscaler_.FindTelemetry(id)->PeakSteadyDemand, 100);
    // Abort the decision, retaining observations of already committed work.
    Autoscaler_.BeginIteration();
    auto saved = Autoscaler_.SaveDemandHistory(id);
    auto state = Autoscaler_.GetFeedbackState(id);
    Autoscaler_ = TThroughputAutoscaler{};
    Autoscaler_.InitializeComputation(id, std::move(state), saved);
    sample.Config->ResetGeneration = 7;
    GetOrCrash(sample.Computations, id).Streams[input] = MakeStream(10);
    observe(StartTime + TDuration::Days(1));
    EXPECT_EQ(Autoscaler_.FindTelemetry(id)->PeakSteadyDemand, 100);
    observe(StartTime + TDuration::Days(3));
    EXPECT_EQ(Autoscaler_.FindTelemetry(id)->PeakSteadyDemand, 10);
}

TEST_F(TThroughputAutoscalerTest, CatchUpConvertsEachQueueOnceAndKeepsBaselineSeparate)
{
    const TComputationId source("source");
    const TComputationId filter("filter");
    const TComputationId consumer("consumer");
    const TStreamId external("external");
    const TStreamId first("first");
    const TStreamId second("second");
    AddSourceComputation(source, external, {first}, {{first, {external}}});
    AddComputation(filter, {first}, {second}, {{second, {first}}});
    AddComputation(consumer, {second});

    Evaluate(MakePipeline(
        StartTime,
        {
            {
                source,
                MakeComputation(1, {
                        {external, MakeStream(100, 1'440'200)},
                        {first, MakeStream(2000, 0)},
                                   },
                    {{first, {{external, MakeLineageRatio(200, 200)}}}}),
            },
            {
                filter,
                MakeComputation(4, {
                        {first, MakeStream(200, 180'200)},
                        {second, MakeStream(20, 0)},
                                   },
                    {{second, {{first, MakeLineageRatio(20, 200)}}}}),
            },
            {
                consumer,
                MakeComputation(4, {
                        {second, MakeStream(20, 18'020)},
                                   }),
            },
        }));

    EXPECT_EQ(Autoscaler_.FindTelemetry(source)->Demand, 900);
    EXPECT_EQ(Autoscaler_.FindTelemetry(source)->SteadyDemand, 100);
    auto filterDemand = Autoscaler_.FindTelemetry(filter)->Demand;
    EXPECT_EQ(filterDemand, 1'000);
    auto demand = Autoscaler_.FindTelemetry(consumer)->Demand;
    EXPECT_EQ(demand, 110);
    auto baseline = Autoscaler_.FindTelemetry(consumer)->SteadyDemand;
    EXPECT_EQ(baseline, 10);
}

TEST_F(TThroughputAutoscalerTest, FanOutScalesTargetAndBaselineIndependentlyOfLocalOffered)
{
    const TComputationId source("source");
    const TComputationId left("left");
    const TComputationId right("right");
    const TStreamId external("external");
    const TStreamId first("first");
    const TStreamId second("second");
    AddSourceComputation(source, external, {first, second}, {{first, {external}}, {second, {external}}});
    AddComputation(left, {first});
    AddComputation(right, {second});
    auto sample = MakePipeline(
        StartTime,
        {
            {
                source,
                MakeComputation(1, {{external, MakeStream(2, 176'404)}}, {
                        {first, {{external, MakeLineageRatio(5, 10)}}},
                        {second, {{external, MakeLineageRatio(2.5, 10)}}},
                                                                         }),
            },
            {
                left,
                MakeComputation(4, {{first, MakeStream(0, 2)}}),
            },
            {
                right,
                MakeComputation(4, {{second, MakeStream(0, 1)}}),
            },
        });
    Evaluate(sample);
    EXPECT_EQ(Autoscaler_.FindTelemetry(left)->Demand, 50);
    EXPECT_EQ(Autoscaler_.FindTelemetry(right)->Demand, 25);
    EXPECT_EQ(Autoscaler_.FindTelemetry(left)->SteadyDemand, 1);
    EXPECT_EQ(Autoscaler_.FindTelemetry(right)->SteadyDemand, 0.5);
    MeasureThroughWarmup(sample, {{left, 60}, {right, 20}});
    EXPECT_EQ(Autoscaler_.FindTelemetry(left)->Demand, 50);
    EXPECT_FALSE(Decision(left));
    ASSERT_TRUE(Decision(right));
    EXPECT_GT(Decision(right)->TargetPartitionCount, 4);
}

TEST_F(TThroughputAutoscalerTest, CatchUpMultiParentOutputDoesNotDuplicateTheBaseline)
{
    const TComputationId producer("producer");
    const TComputationId consumer("consumer");
    const TStreamId left("left");
    const TStreamId right("right");
    const TStreamId output("output");
    AddComputation(producer, {left, right}, {output}, {{output, {left, right}}});
    AddComputation(consumer, {output});
    auto sample = MakePipeline(
        StartTime,
        {
            {
                producer,
                MakeComputation(2, {
                        {left, MakeStream(100, 180'200)},
                        {right, MakeStream(50, 90'100)},
                        {output, MakeStream(70)},
                                   },
                    {
                        {
                            output,
                            {
                                {left, MakeLineageRatio(20, 100)},
                                {right, MakeLineageRatio(50, 50)},
                            },
                        },
                    }),
            },
            {
                consumer,
                MakeComputation(2, {{output, MakeStream(70)}}),
            },
        });
    Evaluate(sample);

    EXPECT_EQ(Autoscaler_.FindTelemetry(producer)->Demand, 300);
    EXPECT_EQ(Autoscaler_.FindTelemetry(consumer)->Demand, 140);
    EXPECT_EQ(Autoscaler_.FindTelemetry(consumer)->SteadyDemand, 70);
    MeasureThroughWarmup(sample, {{producer, 200}, {consumer, 100}});
    ASSERT_TRUE(Decision(producer));
    ASSERT_TRUE(Decision(consumer));
    EXPECT_GT(Decision(producer)->TargetPartitionCount, 2);
    EXPECT_GT(Decision(consumer)->TargetPartitionCount, 2);
}

TEST_F(TThroughputAutoscalerTest, CatchUpIncludesTimerAndVisitWork)
{
    const TComputationId id("computation");
    const TStreamId timer("timer");
    const TStreamId visit("visit");
    auto spec = New<TComputationSpec>();
    spec->TimerStreams[timer] = New<TTimerSpec>();
    spec->KeyVisitorStreams[visit] = New<TKeyVisitorStreamSpec>();
    AddComputation(id, spec);
    auto sample = MakePipeline(
        StartTime,
        {
            {
                id,
                MakeComputation(2, {
                        {timer, MakeStream(100, 180'200)},
                        {visit, MakeStream(50, 90'100)},
                                   }),
            },
        });
    Evaluate(sample);
    EXPECT_EQ(Autoscaler_.FindTelemetry(id)->Demand, 300);
    EXPECT_EQ(Autoscaler_.FindTelemetry(id)->SteadyDemand, 150);
    MeasureThroughWarmup(sample, {{id, 180}});
    ASSERT_TRUE(Decision(id));
    EXPECT_GT(Decision(id)->TargetPartitionCount, 2);
}

TEST_F(TThroughputAutoscalerTest, MissingLineageHoldsOnlyDependentDemand)
{
    const TComputationId sourceId("source");
    const TComputationId dependentId("dependent");
    const TComputationId independentId("independent");
    const TStreamId external("external");
    const TStreamId output("output");
    const TStreamId independentInput("independent_input");

    AddSourceComputation(sourceId, external, {output}, {{output, {external}}});
    AddComputation(dependentId, {output});
    AddComputation(independentId, {independentInput});

    auto sample = MakePipeline(
        StartTime + TDuration::Minutes(20),
        {
            {
                sourceId,
                MakeComputation(1, {
                        {external, MakeStream(100)},
                        {output, MakeStream(10)},
                                   }),
            },
            {
                dependentId,
                MakeComputation(10, {{output, MakeStream(10)}}),
            },
            {
                independentId,
                MakeComputation(10, {{independentInput, MakeStream(100)}}),
            },
        });
    Evaluate(sample);

    EXPECT_EQ(Autoscaler_.FindTelemetry(sourceId)->Demand, 100);
    EXPECT_FALSE(Autoscaler_.FindTelemetry(dependentId)->Demand);
    EXPECT_EQ(Autoscaler_.FindTelemetry(independentId)->Demand, 100);
    GetOrCrash(sample.Computations, sourceId).LineageRatios[output][external] = MakeLineageRatio(50, 100);
    sample.Now += TDuration::Seconds(1);
    Evaluate(sample);
    EXPECT_EQ(Autoscaler_.FindTelemetry(dependentId)->Demand, 50);
    EXPECT_FALSE(Autoscaler_.FindTelemetry(dependentId)->WarmupReady);
    EXPECT_FALSE(Decision(dependentId));
}

TEST_F(TThroughputAutoscalerTest, PrefilterDemandAndFilteredLineageUseTheirOwnPopulations)
{
    const TComputationId filterId("filter");
    const TComputationId consumerId("consumer");
    const TStreamId input("input");
    const TStreamId output("output");
    AddComputation(filterId, {input}, {output}, {{output, {input}}});
    AddComputation(consumerId, {output});

    for (const double keptShare : {0.1, 0.0}) {
        auto filter = MakeComputation(
            1,
            {
                {input, MakeStream(10'000, 0)},
                {output, MakeStream(10'000 * keptShare, 0)},
            },
            {{output, {{input, MakeLineageRatio(10'000 * keptShare, 10'000)}}}});
        auto consumer = MakeComputation(1, {{output, MakeStream(10'000 * keptShare, 0)}});

        Evaluate(MakePipeline(StartTime + (keptShare == 0 ? TDuration::Minutes(20) : TDuration::Zero()), {
                {filterId, filter},
                {consumerId, consumer},
                                                                                                         }));

        auto filterDemand = Autoscaler_.FindTelemetry(filterId)->Demand;
        auto consumerDemand = Autoscaler_.FindTelemetry(consumerId)->Demand;
        EXPECT_EQ(filterDemand, 10'000);
        EXPECT_EQ(consumerDemand, 10'000 * keptShare);
    }
}

TEST_F(TThroughputAutoscalerTest, ClippedProcessingPeakCannotExpandExplorationCredit)
{
    const TComputationId id("consumer");
    const TStreamId input("input");
    AddComputation(id, {input});
    TThroughputFeedbackState initial;
    initial.Exploration.emplace();
    initial.Exploration->OriginPartitionCount = 1;
    initial.Exploration->OriginProcessed = 40;
    initial.Exploration->BestProcessed = 40;
    initial.Anchor.emplace();
    initial.Anchor->PartitionCount = 1;
    initial.Anchor->Capacity = 40;
    initial.Anchor->BusyPartitionShare = 0.125;
    initial.ObservedPartitionCount = 8;
    initial.Phase = EThroughputSearchPhase::FirstProbe;
    Autoscaler_.InitializeComputation(id, std::move(initial));
    const auto& feedback = Autoscaler_.GetFeedbackState(id);

    std::vector<TProcessingObservationWindow> windows;
    for (int index = 0; index < 8; ++index) {
        const auto jobId = TJobId(TGuid::Create());
        auto& window = windows.emplace_back(jobId);
        for (int sample = 0; sample < 2; ++sample) {
            auto status = New<TJobStatus>();
            status->JobId = jobId;
            status->InitedTime = StartTime;
            const int seconds = sample * (index == 1 ? 360 : 180);
            status->UpdateTime = StartTime + TDuration::Seconds(seconds);
            auto& observation = status->ProcessingObservation;
            observation = New<TProcessingObservation>();
            observation->Sequence = sample + 1;
            observation->CapturedAt = status->UpdateTime;
            observation->ProcessedCount = index < 2 ? sample * 7200 : 0;
            observation->ProcessingTime = TDuration::Seconds(index < 2 ? sample * 180 : 0);
            observation->InputWaitingTime = TDuration::Seconds(seconds) - observation->ProcessingTime;
            ASSERT_EQ(window.Add(*status, status->UpdateTime), EProcessingObservationAddResult::Accepted);
        }
    }
    std::vector<const TProcessingObservationWindow*> cohort;
    for (const auto& window : windows) {
        cohort.push_back(&window);
    }
    auto measured = MeasureThroughputProcessing(New<TThroughputSpec>(), cohort, StartTime + TDuration::Seconds(360), TDuration::Minutes(3));
    ASSERT_TRUE(measured);
    EXPECT_EQ(measured->Processed, 60);
    EXPECT_EQ(measured->Capacity, 60);

    for (bool complete : {false, true}) {
        auto computation = MakeComputation(8, {{input, MakeStream(1000)}});
        computation.ReadyAt = StartTime;
        computation.Measurement = measured;
        if (!complete) {
            computation.Measurement->Begin = StartTime;
            computation.Measurement->End = StartTime;
        }
        Evaluate(MakePipeline(StartTime + TDuration::Seconds(complete ? 360 : 0), {{id, std::move(computation)}}));
    }
    EXPECT_TRUE(Autoscaler_.FindTelemetry(id)->WarmupReady);
    EXPECT_FALSE(Decision(id));
    EXPECT_EQ(feedback.Exploration->BestProcessed, 40);
    EXPECT_EQ(Autoscaler_.FindTelemetry(id)->ExplorationPartitionCountLimit, 8);
}

TEST_F(TThroughputAutoscalerTest, ExplicitMinimumOverridesDefaultMaximum)
{
    const TComputationId id("consumer");
    const TStreamId input("input");
    auto partitioning = New<TPartitioningSpec>();
    partitioning->MinPartitionCount = 25'000;
    AddComputation(id, {input}, {}, {}, partitioning);
    for (int seconds : {0, 180}) {
        auto computation = MakeComputation(21'000, {{input, MakeStream(200)}});
        auto& measurement = computation.Measurement.emplace();
        measurement.Begin = StartTime;
        measurement.End = StartTime + TDuration::Seconds(seconds);
        measurement.Processed = 100;
        measurement.Capacity = 100;
        const auto now = measurement.End;
        Evaluate(MakePipeline(now, {{id, std::move(computation)}}));
    }
    ASSERT_TRUE(Decision(id));
    EXPECT_EQ(Decision(id)->TargetPartitionCount, 25'000);
}

TEST_F(TThroughputAutoscalerTest, RejectedRecoveryPreservesTheOrdinaryDecisionWindow)
{
    const TComputationId id("consumer");
    const TStreamId input("input");
    AddComputation(id, {input});
    TThroughputFeedbackState initial;
    initial.Anchor.emplace();
    initial.Anchor->PartitionCount = 1;
    initial.Anchor->Capacity = 100;
    initial.Anchor->BusyPartitionShare = 1;
    initial.ObservedPartitionCount = 2;
    initial.Phase = EThroughputSearchPhase::Recalibrating;
    initial.CapacityDegraded = true;
    initial.RetryWarmupPeriod = TDuration::Minutes(6);
    Autoscaler_.InitializeComputation(id, std::move(initial));
    const auto& feedback = Autoscaler_.GetFeedbackState(id);
    for (int minute = 1; minute <= 7; ++minute) {
        auto now = StartTime + TDuration::Minutes(minute);
        auto computation = MakeComputation(2, {{input, MakeStream(200)}});
        computation.ReadyAt = StartTime;
        auto& measurement = computation.Measurement.emplace();
        measurement.Begin = minute <= 6 ? StartTime : now - TDuration::Minutes(6);
        measurement.End = now;
        measurement.Processed = 120;
        measurement.Capacity = 120;
        measurement.BusyPartitionShare = 1;
        computation.RecoveryMeasurement = measurement;
        if (minute > 3) {
            auto& recovery = *computation.RecoveryMeasurement;
            recovery.Begin = now - TDuration::Minutes(3);
            recovery.Processed = 105;
            recovery.Capacity = 105;
        }
        Evaluate(MakePipeline(now, {{id, std::move(computation)}}));
        EXPECT_TRUE(feedback.CapacityDegraded);
        EXPECT_EQ(feedback.RetryWarmupPeriod, TDuration::Minutes(6));
        if (minute < 7) {
            EXPECT_FALSE(Decision(id));
            EXPECT_EQ(feedback.Anchor->PartitionCount, 1);
        }
    }
    ASSERT_TRUE(Decision(id));
    EXPECT_EQ(Decision(id)->TargetPartitionCount, 4);
    EXPECT_EQ(feedback.Anchor->PartitionCount, 2);
    EXPECT_DOUBLE_EQ(feedback.Anchor->Capacity, 120);
    EXPECT_EQ(Autoscaler_.FindTelemetry(id)->Capacity, 120);
}

TEST_F(TThroughputAutoscalerTest, RepeatedRecoveryDoesNotSuppressLaterCapacityLoss)
{
    const TComputationId id("consumer");
    const TStreamId input("input");
    auto partitioning = New<TPartitioningSpec>();
    partitioning->MinPartitionCount = 1;
    AddComputation(id, {input}, {}, {}, partitioning);
    TThroughputFeedbackState initial;
    initial.Anchor.emplace();
    initial.Anchor->PartitionCount = 1;
    initial.Anchor->Capacity = 100;
    initial.Anchor->BusyPartitionShare = 1;
    initial.Exploration.emplace();
    initial.Exploration->OriginPartitionCount = 1;
    initial.Exploration->OriginProcessed = 100;
    initial.Exploration->BestProcessed = 100;
    initial.ObservedPartitionCount = 2;
    initial.Phase = EThroughputSearchPhase::Recalibrating;
    initial.CapacityDegraded = true;
    initial.RetryWarmupPeriod = TDuration::Minutes(6);
    Autoscaler_.InitializeComputation(id, std::move(initial));
    const auto& feedback = Autoscaler_.GetFeedbackState(id);

    struct TStep
    {
        i64 Count = 0;
        double Capacity = 0;
        i64 Target = 0;
        i64 Selected = 0;
        int Minutes = 0;
        i64 AnchorCount = 0;
        double AnchorCapacity = 0;
        int RetryMinutes = 0;
    };

    const TStep steps[] = {
        {2, 120, 8, 8, 4, 2, 120, 0},
        {8, 80, 2, 3, 4, 2, 120, 6},
        {3, 150, 8, 8, 4, 3, 150, 0},
        {8, 90, 3, 3, 4, 3, 150, 6},
        {3, 150, 8, 8, 7, 3, 150, 6},
        {8, 90, 3, 3, 7, 3, 150, 12},
    };
    auto readyAt = StartTime;
    for (const auto& step : steps) {
        SCOPED_TRACE(Format("Count: %v, Capacity: %v, ReadyAt: %v", step.Count, step.Capacity, readyAt));
        TThroughputPipelineObservation sample;
        for (int minute = 1; minute <= step.Minutes; ++minute) {
            auto now = readyAt + TDuration::Minutes(minute);
            auto computation = MakeComputation(step.Count, {{input, MakeStream(1'000)}});
            computation.ReadyAt = readyAt;
            auto& measurement = computation.Measurement.emplace();
            measurement.Begin = std::max(readyAt, now - std::max(TDuration::Minutes(3), feedback.RetryWarmupPeriod));
            measurement.End = now;
            measurement.Processed = step.Capacity;
            measurement.Capacity = step.Capacity;
            measurement.BusyPartitionShare = 1;
            computation.RecoveryMeasurement = measurement;
            computation.RecoveryMeasurement->Begin = std::max(readyAt, now - TDuration::Minutes(3));
            if (step.Count == 2) {
                measurement.Capacity = measurement.Processed = 80;
            }
            sample = MakePipeline(now, {{id, std::move(computation)}});
            Evaluate(sample);
            if (minute < step.Minutes) {
                EXPECT_FALSE(Decision(id));
            }
        }
        ASSERT_TRUE(Decision(id));
        ASSERT_TRUE(Autoscaler_.FindTelemetry(id));
        EXPECT_EQ(Autoscaler_.FindTelemetry(id)->Capacity, step.Capacity);
        EXPECT_EQ(Autoscaler_.FindTelemetry(id)->Processed, step.Capacity);
        EXPECT_EQ(Decision(id)->TargetPartitionCount, step.Target);
        ASSERT_TRUE(feedback.Anchor);
        EXPECT_EQ(feedback.Anchor->PartitionCount, step.AnchorCount);
        EXPECT_DOUBLE_EQ(feedback.Anchor->Capacity, step.AnchorCapacity);
        EXPECT_EQ(feedback.RetryWarmupPeriod, TDuration::Minutes(step.RetryMinutes));
        EXPECT_EQ(feedback.CapacityDegraded, step.RetryMinutes > 0);
        ASSERT_TRUE(feedback.Exploration);
        EXPECT_EQ(feedback.Exploration->OriginPartitionCount, 1);
        EXPECT_DOUBLE_EQ(feedback.Exploration->OriginProcessed, 100);
        Autoscaler_.AcceptPartitionCount(id, step.Count, step.Selected);
        EXPECT_EQ(feedback.PendingPartitionCount, step.Selected);
        sample.Now += TDuration::Seconds(30);
        Evaluate(sample);
        EXPECT_FALSE(Decision(id));
        readyAt = sample.Now;
    }
}

TEST_F(TThroughputAutoscalerTest, RecoveryCannotBypassObservationAndSearchAdmission)
{
    for (auto rejection : {ERejection::OriginalLayout, ERejection::SparseWork, ERejection::OutputWaiting, ERejection::PendingReturn, ERejection::ChangedCohort, ERejection::NewShortage, ERejection::UnhealthyJobs})
    {
        SCOPED_TRACE(static_cast<int>(rejection));
        Autoscaler_ = TThroughputAutoscaler();
        Computations_.clear();
        const TComputationId id("consumer");
        const TStreamId input("input");
        AddComputation(id, {input});
        TThroughputFeedbackState initial;
        initial.Anchor.emplace();
        initial.Anchor->PartitionCount = rejection == ERejection::OriginalLayout ? 2 : 1;
        initial.Anchor->Capacity = 100;
        initial.Anchor->BusyPartitionShare = 1;
        initial.ObservedPartitionCount = 2;
        initial.Phase = EThroughputSearchPhase::Recalibrating;
        initial.CapacityDegraded = true;
        initial.RetryWarmupPeriod = TDuration::Minutes(6);
        if (rejection == ERejection::PendingReturn) {
            initial.PendingPartitionCount = 1;
        }
        Autoscaler_.InitializeComputation(id, std::move(initial));
        const auto& feedback = Autoscaler_.GetFeedbackState(id);
        for (int minute = 1; minute <= 4; ++minute) {
            auto now = StartTime + TDuration::Minutes(minute);
            auto computation = MakeComputation(2, {{input, MakeStream(rejection == ERejection::NewShortage && minute < 4 ? 50 : 200)}});
            computation.ReadyAt = rejection == ERejection::ChangedCohort && minute >= 3
                ? StartTime + TDuration::Minutes(3)
                : StartTime;
            computation.AllJobsHealthy = rejection != ERejection::UnhealthyJobs;
            auto& measurement = computation.Measurement.emplace();
            measurement.Begin = StartTime;
            measurement.End = now;
            measurement.Processed = 100;
            measurement.Capacity = 100;
            measurement.BusyPartitionShare = 1;
            computation.RecoveryMeasurement = measurement;
            if (minute > 3) {
                auto& recovery = *computation.RecoveryMeasurement;
                recovery.Begin = now - TDuration::Minutes(3);
                recovery.Processed = 120;
                recovery.Capacity = 120;
                recovery.BusyPartitionShare = rejection == ERejection::SparseWork ? 0.5 : 1;
                recovery.OutputWaitingShare = rejection == ERejection::OutputWaiting ? 0.3 : 0;
            }
            Evaluate(MakePipeline(now, {{id, std::move(computation)}}));
            EXPECT_FALSE(Decision(id));
            EXPECT_TRUE(feedback.CapacityDegraded);
            EXPECT_EQ(feedback.RetryWarmupPeriod, TDuration::Minutes(6));
            EXPECT_DOUBLE_EQ(feedback.Anchor->Capacity, 100);
        }
    }
}

class TThroughputCommittedObservationTest
    : public ::testing::Test
{
protected:
    const TComputationId ComputationId_{"consumer"};
    const TStreamId Input_{"input"};
    TThroughputAutoscaler Autoscaler_;
    TThroughputAutoscaler::TComputations Computations_;
    TProcessingMeter Meter_;
    TFlowViewPtr FlowView_ = New<TFlowView>();
    std::vector<TPartitionPtr> Partitions_;
    std::vector<TJobStatusPtr> Jobs_;
    TInstant Now_ = StartTime;

    virtual TThroughputFeedbackState MakeInitialState() const
    {
        return {};
    }

    void SetUp() override
    {
        Autoscaler_.InitializeComputation(ComputationId_, MakeInitialState());
        auto spec = New<TComputationSpec>();
        spec->InputStreamIds.insert(Input_);
        auto partitioning = New<TPartitioningSpec>();
        partitioning->PartitionCountDoubleDelay = TDuration::Seconds(1);
        Computations_.emplace(
            ComputationId_,
            TThroughputAutoscaler::TComputation{
                .Spec = spec,
                .PartitioningSpec = partitioning,
            });
        auto control = New<TPersistedStateControl<std::string>>(New<TNullStorageHandler<std::string>>());
        FlowView_->State->AttachToControl(control);
        control->Recover();
        FlowView_->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Working, TestVersionProvider());
        for (int index = 0; index < 2; ++index) {
            auto partition = New<TPartition>();
            partition->PartitionId = TPartitionId(TGuid::Create());
            partition->ComputationId = ComputationId_;
            partition->State = EPartitionState::Executing;
            partition->CurrentJobId = TJobId(TGuid::Create());
            FlowView_->State->StartMutation();
            FlowView_->State->ExecutionSpec->Layout->Partitions.emplace(partition->PartitionId, partition);
            FlowView_->State->CommitMutation();
            auto status = New<TPartitionJobStatus>();
            status->CurrentJobId = partition->CurrentJobId;
            auto job = New<TJobStatus>();
            job->JobId = *partition->CurrentJobId;
            job->InitedTime = StartTime;
            job->FromPartitionTraverseData = New<TFromPartitionTraverseData>();
            auto node = job->FromPartitionTraverseData->Node;
            node->IterationCycle = 1;
            auto stream = New<TStreamTraverseData>();
            stream->InflightMetrics = New<TInflightMetrics>();
            stream->InflightMetrics->OfferedCountPerSec = 10;
            stream->InflightMetrics->ProcessedCountPerSec = 10;
            stream->InflightMetrics->ReadyCount = 0;
            node->Streams[Input_] = stream;
            status->CurrentJobStatus = job;
            FlowView_->Feedback->PartitionJobStatuses[partition->PartitionId] = status;
            Jobs_.push_back(job);
            Partitions_.push_back(partition);
        }
        for (int index = 0; index < 2; ++index) {
            Publish(index, 100 + 200 * index);
        }
        Poll();
    }

    void Publish(int index, double capacity, double busyShare = 0.5)
    {
        auto& job = Jobs_[index];
        auto next = New<TProcessingObservation>();
        const auto& previous = job->ProcessingObservation;
        next->Sequence = previous ? previous->Sequence + 1 : 1;
        next->CapturedAt = Now_;
        next->SpecGeneration = FlowView_->State->ExecutionSpec->GetEpoch();
        auto elapsed = previous ? Now_ - previous->CapturedAt : TDuration::Seconds(1);
        i64 count = static_cast<i64>(capacity * busyShare * elapsed.SecondsFloat());
        next->ProcessedCount = (previous ? previous->ProcessedCount : 0) + count;
        next->ProcessedByteSize = next->ProcessedCount * 10;
        auto busy = TDuration::MicroSeconds(elapsed.MicroSeconds() * busyShare);
        next->ProcessingTime = (previous ? previous->ProcessingTime : TDuration::Zero()) + busy;
        next->InputWaitingTime = (previous ? previous->InputWaitingTime : TDuration::Zero()) + elapsed - busy;
        job->ProcessingObservation = std::move(next);
    }

    void Poll()
    {
        FlowView_->State->CurrentTimestamp = TSystemTimestamp(Now_.Seconds());
        FlowView_->Feedback->UpdateTime = Now_;
        for (auto& [_, status] : FlowView_->Feedback->PartitionJobStatuses) {
            status->CurrentJobStatusUpdateTime = Now_;
            status->CurrentJobStatus->UpdateTime = Now_;
            status->CurrentJobStatus->Epoch = FlowView_->State->ExecutionSpec->GetEpoch();
        }
        const auto config = FlowView_->State->ExecutionSpec->DynamicPipelineSpec->GetValue()->JobManager->Partitioning->Throughput;
        Autoscaler_.Reconfigure(Computations_, config);
        Autoscaler_.Evaluate({
            .Now = Now_,
            .Config = config,
            .Computations = Meter_.Collect(FlowView_->State, FlowView_, config, Autoscaler_.ComputeMeasurementRequests()),
        });
    }

    void WarmUp(double firstCapacity = 100, double secondCapacity = 300)
    {
        for (int minute = 0; minute < 6; ++minute) {
            Now_ += TDuration::Minutes(1);
            Publish(0, firstCapacity);
            Publish(1, secondCapacity);
            Poll();
        }
    }
};

TEST_F(TThroughputCommittedObservationTest, DelayedFirstPublicationStillCompletesWarmup)
{
    Autoscaler_ = TThroughputAutoscaler{};
    Now_ += TDuration::Seconds(1);
    Poll();
    bool settled = false;
    for (int minute = 0; minute < 6; ++minute) {
        Now_ += TDuration::Minutes(1);
        Publish(0, 100);
        Publish(1, 300);
        Poll();
        settled |= Autoscaler_.FindTelemetry(ComputationId_)->WarmupReady;
    }
    EXPECT_EQ(Autoscaler_.FindTelemetry(ComputationId_)->Capacity, 400);
    EXPECT_TRUE(settled);
}

TEST_F(TThroughputCommittedObservationTest, MeterUsesCommittedLayoutDespiteCandidateJobRemoval)
{
    TProcessingMeter meter;
    const TProcessingMeter::TComputations computations{
        {
            ComputationId_,
            {
                .Spec = GetOrCrash(Computations_, ComputationId_).Spec,
                .Period = TDuration::Minutes(3),
            },
        },
    };
    auto config = New<TThroughputSpec>();
    config->MinimumBusyTimeShare = 0.4;
    auto collect = [&] (TProcessingMeter* current) {
        auto observations = current->Collect(FlowView_->State, FlowView_, config, computations);
        return GetOrCrash(observations, ComputationId_);
    };
    EXPECT_FALSE(collect(&meter).Measurement);
    Now_ += TDuration::Minutes(3);
    Publish(0, 100);
    Publish(1, 300);
    Poll();
    const auto observed = collect(&meter);
    ASSERT_TRUE(observed.Measurement);
    EXPECT_EQ(observed.Measurement->Processed, 200);
    EXPECT_EQ(observed.Measurement->Capacity, 400);
    EXPECT_EQ(observed.Measurement->BusyPartitionShare, 1);

    auto candidate = FlowView_->CopyPtr();
    candidate->State = candidate->State->Clone();
    candidate->State->StartMutation();
    candidate->State->ExecutionSpec->Layout->Partitions.erase(Partitions_[0]->PartitionId);
    const auto speculative = meter.Collect(FlowView_->State, candidate, config, computations);
    ASSERT_TRUE(GetOrCrash(speculative, ComputationId_).Measurement);
    EXPECT_EQ(GetOrCrash(speculative, ComputationId_).Measurement->Capacity, 400);
    const auto retried = collect(&meter);
    ASSERT_TRUE(retried.Measurement);
    EXPECT_EQ(retried.Measurement->Begin, StartTime);
    EXPECT_EQ(retried.Measurement->End, Now_);
    EXPECT_EQ(retried.Measurement->Capacity, 400);
}

TEST_F(TThroughputCommittedObservationTest, CommonMeasurementUsesEveryCurrentJob)
{
    EXPECT_FALSE(Autoscaler_.FindTelemetry(ComputationId_)->Processed);
    WarmUp();
    EXPECT_EQ(Autoscaler_.FindTelemetry(ComputationId_)->Processed, 200);
    Jobs_[1]->ProcessingObservation.Reset();
    Poll();
    EXPECT_FALSE(Autoscaler_.FindTelemetry(ComputationId_)->Processed);
}

TEST_F(TThroughputCommittedObservationTest, FailureBlocksDecisionsButPreservesCommittedHistory)
{
    WarmUp();
    Jobs_[1]->RetryableErrors.emplace("source", TError("Temporary input failure"));
    Poll();
    EXPECT_FALSE(Autoscaler_.FindTelemetry(ComputationId_)->Capacity);
    Jobs_[1]->RetryableErrors.clear();
    Poll();
    EXPECT_EQ(Autoscaler_.FindTelemetry(ComputationId_)->Capacity, 400);
    EXPECT_FALSE(Autoscaler_.FindTelemetry(ComputationId_)->WarmupReady);
    Now_ += TDuration::Minutes(4);
    Poll();
    ASSERT_TRUE(Autoscaler_.FindTelemetry(ComputationId_)->Capacity);
    EXPECT_FALSE(Autoscaler_.FindTelemetry(ComputationId_)->WarmupReady);
    EXPECT_FALSE(Autoscaler_.FindDecision(ComputationId_));
    Publish(0, 100);
    Publish(1, 300);
    Poll();
    EXPECT_TRUE(Autoscaler_.FindTelemetry(ComputationId_)->WarmupReady);
}

TEST_F(TThroughputCommittedObservationTest, CompletedPartitionsAreNotMissingMeasurementMembers)
{
    WarmUp();
    Partitions_[0]->State = EPartitionState::Completed;
    Partitions_[0]->CurrentJobId.reset();
    Poll();
    const auto telemetry = Autoscaler_.FindTelemetry(ComputationId_);
    EXPECT_EQ(telemetry->PartitionCount, 1);
    EXPECT_EQ(telemetry->Processed, 150);
}

TEST_F(TThroughputCommittedObservationTest, BatchingAllowanceUsesComputationWideNonemptyMean)
{
    Autoscaler_ = TThroughputAutoscaler{};
    for (auto& job : Jobs_) {
        ++job->ProcessingObservation->Sequence;
        job->ProcessingObservation->NonemptyEpochCount = 0;
        job->ProcessingObservation->NonemptyEpochDuration = TDuration::Zero();
        job->FromPartitionTraverseData->Node->Streams[Input_]->InflightMetrics->ReadyCount = 5'000;
    }
    Poll();
    Now_ += TDuration::Minutes(10);
    for (int index = 0; index < 2; ++index) {
        Publish(index, 100);
        auto& observation = Jobs_[index]->ProcessingObservation;
        observation->NonemptyEpochCount = index == 0 ? 4 : 1;
        observation->NonemptyEpochDuration = TDuration::Seconds(index == 0 ? 20 : 540);
        observation->ObservationDuration = TDuration::Seconds(1);
    }
    Poll();
    ASSERT_TRUE(Autoscaler_.FindTelemetry(ComputationId_)->Demand);
    EXPECT_NEAR(*Autoscaler_.FindTelemetry(ComputationId_)->Demand, 20 + 5520.0 / 900, 1e-9);
}

TEST_F(TThroughputCommittedObservationTest, MeasurementUpdatesOnlyAfterEveryJobCommits)
{
    WarmUp();
    Now_ += TDuration::Seconds(1);
    for (auto& job : Jobs_) {
        job->FromPartitionTraverseData->Node->IterationCycle = 100;
    }
    Poll();
    EXPECT_EQ(Autoscaler_.FindTelemetry(ComputationId_)->Capacity, 400);
    Publish(0, 400);
    Poll();
    EXPECT_EQ(Autoscaler_.FindTelemetry(ComputationId_)->Capacity, 400);
    Now_ += TDuration::Seconds(1);
    Publish(1, 600);
    Poll();
    const auto capacity = Autoscaler_.FindTelemetry(ComputationId_)->Capacity;
    ASSERT_TRUE(capacity);
    EXPECT_GT(*capacity, 400);
    EXPECT_LT(*capacity, 1000);
}

TEST_F(TThroughputCommittedObservationTest, CaptureValidationUsesUnroundedFeedbackTime)
{
    WarmUp();
    Now_ += TDuration::MilliSeconds(1500);
    for (int index = 0; index < 2; ++index) {
        Publish(index, 1000);
        Jobs_[index]->FromPartitionTraverseData->Node->IterationCycle.reset();
    }
    Poll();
    const auto capacity = Autoscaler_.FindTelemetry(ComputationId_)->Capacity;
    ASSERT_TRUE(capacity);
    EXPECT_GT(*capacity, 400);
    EXPECT_LT(*capacity, 2000);
}

TEST_F(TThroughputCommittedObservationTest, PublicationsWithoutJobReplacementPreserveWarmup)
{
    for (const auto& job : Jobs_) {
        job->FromPartitionTraverseData->Node->Streams[Input_]->InflightMetrics->OfferedCountPerSec = 1000;
    }
    for (int minute = 1; minute <= 4; ++minute) {
        Now_ += TDuration::Minutes(1);
        Publish(0, 100);
        Publish(1, 300);
        if (minute % 2) {
            auto other = New<TPartition>();
            other->PartitionId = TPartitionId(TGuid::Create());
            other->ComputationId = TComputationId("other");
            other->State = EPartitionState::Executing;
            FlowView_->State->StartMutation();
            FlowView_->State->ExecutionSpec->Layout->Partitions.emplace(other->PartitionId, other);
            FlowView_->State->CommitMutation();
        } else {
            auto spec = New<TDynamicPipelineSpec>();
            auto computation = New<TDynamicComputationSpec>();
            computation->EmptyBatchBackoff = TDuration::Seconds(minute);
            spec->Computations[ComputationId_] = computation;
            FlowView_->State->ExecutionSpec->DynamicPipelineSpec->TrySetValue(spec, TestVersionProvider());
        }
        Poll();
        EXPECT_EQ(Autoscaler_.FindTelemetry(ComputationId_)->Capacity, 400);
        EXPECT_EQ(Autoscaler_.FindDecision(ComputationId_).has_value(), minute == 4);
    }
}

class TThroughputConfiguredExposureTest
    : public TThroughputCommittedObservationTest
    , public ::testing::WithParamInterface<double>
{
protected:
    TThroughputFeedbackState MakeInitialState() const override
    {
        TThroughputFeedbackState feedback;
        feedback.Anchor.emplace();
        feedback.Anchor->PartitionCount = 1;
        feedback.Anchor->Capacity = 100;
        feedback.Anchor->BusyPartitionShare = 1;
        feedback.Exploration.emplace();
        feedback.Exploration->OriginPartitionCount = 1;
        feedback.Exploration->OriginProcessed = 60;
        feedback.Exploration->BestProcessed = 100;
        feedback.Phase = EThroughputSearchPhase::Recalibrating;
        feedback.ObservedPartitionCount = 2;
        feedback.PendingPartitionCount = 2;
        feedback.CapacityDegraded = true;
        feedback.RetryWarmupPeriod = TDuration::Minutes(6);
        return feedback;
    }
};

TEST_P(TThroughputConfiguredExposureTest, LegacyClampedRecoveryUsesConfiguredExposure)
{
    auto& config = FlowView_->State->ExecutionSpec->DynamicPipelineSpec->GetValue()->JobManager->Partitioning->Throughput;
    config->MinimumBusyTimeShare = 0.5;
    const auto& feedback = Autoscaler_.GetFeedbackState(ComputationId_);
    for (const auto& job : Jobs_) {
        job->FromPartitionTraverseData->Node->Streams[Input_]->InflightMetrics->OfferedCountPerSec = 100;
    }
    for (int minute = 1; minute <= 4; ++minute) {
        Now_ += TDuration::Minutes(1);
        Publish(0, 60, GetParam());
        Publish(1, 60, GetParam());
        Poll();
        if (minute < 4) {
            EXPECT_FALSE(Autoscaler_.FindDecision(ComputationId_));
            EXPECT_EQ(feedback.RetryWarmupPeriod, TDuration::Minutes(6));
        }
    }
    ASSERT_TRUE(Autoscaler_.FindDecision(ComputationId_));
    EXPECT_EQ(Autoscaler_.FindDecision(ComputationId_)->TargetPartitionCount, 4);
    EXPECT_FALSE(feedback.CapacityDegraded);
    EXPECT_EQ(feedback.RetryWarmupPeriod, TDuration::Zero());
    ASSERT_TRUE(feedback.Anchor);
    EXPECT_EQ(feedback.Anchor->PartitionCount, 2);
    EXPECT_DOUBLE_EQ(feedback.Anchor->Capacity, 120);
    ASSERT_TRUE(feedback.Exploration);
    EXPECT_EQ(feedback.Exploration->OriginPartitionCount, 1);
    EXPECT_DOUBLE_EQ(feedback.Exploration->OriginProcessed, 60);
    Now_ += TDuration::Minutes(1);
    Poll();
    EXPECT_FALSE(Autoscaler_.FindDecision(ComputationId_));
    EXPECT_EQ(feedback.PendingPartitionCount, 4);
}

INSTANTIATE_TEST_SUITE_P(FullAndPartialBusyTime, TThroughputConfiguredExposureTest, ::testing::Values(1.0, 0.6));

class TThroughputLongRetryTest
    : public TThroughputCommittedObservationTest
{
protected:
    TThroughputFeedbackState MakeInitialState() const override
    {
        TThroughputFeedbackState state;
        state.RetryWarmupPeriod = TDuration::Minutes(24);
        return state;
    }
};

TEST_F(TThroughputLongRetryTest, LowerRetryCapStillRetainsTheActiveWindow)
{
    auto config = FlowView_->State->ExecutionSpec->DynamicPipelineSpec->GetValue()->JobManager->Partitioning->Throughput;
    config->MaxRetryWarmupPeriod = TDuration::Minutes(6);
    for (const auto& job : Jobs_) {
        job->FromPartitionTraverseData->Node->Streams[Input_]->InflightMetrics->OfferedCountPerSec = 1'000;
    }
    for (int minute = 1; minute <= 24; ++minute) {
        Now_ += TDuration::Minutes(1);
        Publish(0, 100);
        Publish(1, 300);
        Poll();
        EXPECT_FALSE(Autoscaler_.FindDecision(ComputationId_));
    }
    Now_ += TDuration::Minutes(1);
    Publish(0, 100);
    Publish(1, 300);
    Poll();
    ASSERT_TRUE(Autoscaler_.FindDecision(ComputationId_));
    EXPECT_EQ(Autoscaler_.FindDecision(ComputationId_)->TargetPartitionCount, 8);
}

TEST_F(TThroughputCommittedObservationTest, LayoutPublicationKeepsLongEpochHistory)
{
    for (int epoch = 0; epoch < 2; ++epoch) {
        Now_ += TDuration::Minutes(5);
        for (int index = 0; index < 2; ++index) {
            Publish(index, 100 + 200 * index);
            Jobs_[index]->ProcessingObservation->ObservationDuration = TDuration::Minutes(5);
        }
        Poll();
    }
    auto other = New<TPartition>();
    other->PartitionId = TPartitionId(TGuid::Create());
    other->ComputationId = TComputationId("other");
    other->State = EPartitionState::Executing;
    FlowView_->State->StartMutation();
    FlowView_->State->ExecutionSpec->Layout->Partitions.emplace(other->PartitionId, other);
    FlowView_->State->CommitMutation();
    for (int minute = 1; minute < 5; ++minute) {
        Now_ += TDuration::Minutes(1);
        Poll();
        EXPECT_EQ(Autoscaler_.FindTelemetry(ComputationId_)->Capacity, 400);
        EXPECT_FALSE(Autoscaler_.FindTelemetry(ComputationId_)->WarmupReady);
    }
    Now_ += TDuration::Minutes(1);
    Publish(0, 100);
    Publish(1, 300);
    Poll();
    EXPECT_EQ(Autoscaler_.FindTelemetry(ComputationId_)->Capacity, 400);
    EXPECT_TRUE(Autoscaler_.FindTelemetry(ComputationId_)->WarmupReady);
}

TEST_F(TThroughputCommittedObservationTest, PausedPipelineRequiresPostResumeCoverageWithoutForgettingCounters)
{
    WarmUp();
    auto pipelineState = FlowView_->State->ExecutionSpec->PipelineState;
    Now_ += TDuration::Seconds(1);
    pipelineState->TrySetValue(EPipelineState::Paused, TestVersionProvider());
    Poll();
    EXPECT_FALSE(Autoscaler_.FindTelemetry(ComputationId_)->Capacity);
    pipelineState->TrySetValue(EPipelineState::Working, TestVersionProvider());
    Poll();
    EXPECT_EQ(Autoscaler_.FindTelemetry(ComputationId_)->Capacity, 400);
    EXPECT_FALSE(Autoscaler_.FindTelemetry(ComputationId_)->WarmupReady);
    Now_ += TDuration::Minutes(3);
    Publish(0, 100);
    Publish(1, 300);
    Poll();
    EXPECT_TRUE(Autoscaler_.FindTelemetry(ComputationId_)->WarmupReady);
}

TEST_F(TThroughputCommittedObservationTest, ComputationExplorationFactorClipsThePublicGrowthDecision)
{
    GetOrCrash(Computations_, ComputationId_).PartitioningSpec->MinPartitionCount = 1;
    GetOrCrash(Computations_, ComputationId_).PartitioningSpec->ThroughputMaxExplorationFactor = 1.5;
    for (const auto& job : Jobs_) {
        job->FromPartitionTraverseData->Node->Streams[Input_]->InflightMetrics->OfferedCountPerSec = 1000;
    }
    for (int minute = 1; minute <= 4; ++minute) {
        Now_ += TDuration::Minutes(1);
        Publish(0, 100);
        Publish(1, 300);
        Poll();
    }
    ASSERT_TRUE(Autoscaler_.FindDecision(ComputationId_));
    EXPECT_EQ(Autoscaler_.FindDecision(ComputationId_)->TargetPartitionCount, 3);
    ASSERT_TRUE(Autoscaler_.GetFeedbackState(ComputationId_).Exploration);
    EXPECT_EQ(Autoscaler_.GetFeedbackState(ComputationId_).Exploration->OriginPartitionCount, 2);
    EXPECT_EQ(Autoscaler_.FindTelemetry(ComputationId_)->ExplorationPartitionCountLimit, 3);
}

TEST_F(TThroughputCommittedObservationTest, LocalMeasurementPeriodDelaysGrowthUntilItsWindowIsCovered)
{
    GetOrCrash(Computations_, ComputationId_).PartitioningSpec->ThroughputWarmupPeriod = TDuration::Minutes(6);
    for (const auto& job : Jobs_) {
        job->FromPartitionTraverseData->Node->Streams[Input_]->InflightMetrics->OfferedCountPerSec = 1000;
    }
    for (int minute = 1; minute <= 7; ++minute) {
        Now_ += TDuration::Minutes(1);
        Publish(0, 100);
        Publish(1, 300);
        Poll();
        EXPECT_EQ(Autoscaler_.FindDecision(ComputationId_).has_value(), minute == 7);
    }
}

TEST_F(TThroughputCommittedObservationTest, HourlyScaleDownRequiresNewCompleteObservation)
{
    GetOrCrash(Computations_, ComputationId_).PartitioningSpec->MinPartitionCount = 1;
    for (int minute = 0; minute < 6; ++minute) {
        Now_ += TDuration::Minutes(1);
        Publish(0, 100, 0.1);
        Publish(1, 300, 0.1);
        Poll();
    }
    EXPECT_FALSE(Autoscaler_.FindDecision(ComputationId_));
    Now_ += TDuration::Hours(1);
    Poll();
    EXPECT_FALSE(Autoscaler_.FindDecision(ComputationId_));
    Publish(0, 100, 0.1);
    Poll();
    EXPECT_FALSE(Autoscaler_.FindDecision(ComputationId_));
    Publish(1, 300, 0.1);
    Poll();
    ASSERT_TRUE(Autoscaler_.FindDecision(ComputationId_));
    EXPECT_EQ(Autoscaler_.FindDecision(ComputationId_)->TargetPartitionCount, 1);
}

TEST_F(TThroughputCommittedObservationTest, ReplacementJobNeedsItsOwnWarmObservationCohort)
{
    WarmUp();
    Now_ += TDuration::Seconds(1);
    Partitions_[0]->CurrentJobId = TJobId(TGuid::Create());
    Publish(0, 1'000);
    Publish(1, 1'000);
    Poll();
    EXPECT_FALSE(Autoscaler_.FindTelemetry(ComputationId_)->Capacity);
    auto& firstStatus = FlowView_->Feedback->PartitionJobStatuses[Partitions_[0]->PartitionId];
    firstStatus->CurrentJobId = Partitions_[0]->CurrentJobId;
    Jobs_[0]->JobId = *Partitions_[0]->CurrentJobId;
    Jobs_[0]->ProcessingObservation.Reset();
    Now_ += TDuration::Seconds(1);
    Publish(0, 1'000);
    Poll();
    EXPECT_FALSE(Autoscaler_.FindTelemetry(ComputationId_)->Capacity);
    WarmUp(1'000, 1'000);
    ASSERT_TRUE(Autoscaler_.FindTelemetry(ComputationId_)->Capacity);
    EXPECT_EQ(Autoscaler_.FindTelemetry(ComputationId_)->Capacity, 2000);
}

TEST(TThroughputSampleTest, CompletedSourcePartitionsDoNotInflateDemand)
{
    const TComputationId sourceId("source");
    const TComputationId consumerId("consumer");
    const TStreamId external("external");
    const TStreamId input("input");
    auto sourceSpec = New<TComputationSpec>();
    sourceSpec->SourceStreams[external] = New<TSourceSpec>();
    sourceSpec->OutputStreamIds.insert(input);
    sourceSpec->StreamsDependency[input].insert(external);
    auto consumerSpec = New<TComputationSpec>();
    consumerSpec->InputStreamIds.insert(input);
    TThroughputAutoscaler autoscaler;
    TProcessingMeter meter;
    TThroughputAutoscaler::TComputations computations{
        {sourceId, {.Spec = sourceSpec}},
        {consumerId, {.Spec = consumerSpec, .PartitioningSpec = New<TPartitioningSpec>()}},
    };
    auto flowView = New<TFlowView>();
    auto control = New<TPersistedStateControl<std::string>>(New<TNullStorageHandler<std::string>>());
    flowView->State->AttachToControl(control);
    control->Recover();
    flowView->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Working, TestVersionProvider());
    auto addPartition = [&] (const TComputationId& id, const TStreamId& streamId, EPartitionState state) {
        auto partition = New<TPartition>();
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->ComputationId = id;
        partition->State = state;
        flowView->State->StartMutation();
        flowView->State->ExecutionSpec->Layout->Partitions.emplace(partition->PartitionId, partition);
        flowView->State->CommitMutation();
        if (state == EPartitionState::Completed) {
            return partition;
        }
        partition->CurrentJobId = TJobId(TGuid::Create());
        auto status = New<TPartitionJobStatus>();
        status->CurrentJobId = partition->CurrentJobId;
        auto job = New<TJobStatus>();
        job->JobId = *partition->CurrentJobId;
        job->InitedTime = StartTime;
        job->FromPartitionTraverseData = New<TFromPartitionTraverseData>();
        auto node = job->FromPartitionTraverseData->Node;
        node->IterationCycle = 1;
        auto stream = New<TStreamTraverseData>();
        stream->InflightMetrics = New<TInflightMetrics>();
        stream->InflightMetrics->OfferedCountPerSec = 100;
        stream->InflightMetrics->ProcessedCountPerSec = 100;
        stream->InflightMetrics->ReadyCount = 0;
        node->Streams[streamId] = stream;
        status->CurrentJobStatus = job;
        flowView->Feedback->PartitionJobStatuses[partition->PartitionId] = status;
        return partition;
    };
    auto activeSource = addPartition(sourceId, external, EPartitionState::Executing);
    addPartition(sourceId, external, EPartitionState::Completed);
    addPartition(consumerId, input, EPartitionState::Executing);
    flowView->EphemeralState->LineageRatios[input][MakeGlobalStreamId(sourceId, external, sourceSpec)] =
        MakeLineageRatio(100, 100);
    auto evaluate = [&] (int minutes) {
        const auto now = StartTime + TDuration::Minutes(minutes);
        flowView->State->CurrentTimestamp = TSystemTimestamp(now.Seconds());
        flowView->Feedback->UpdateTime = now;
        for (auto& [_, status] : flowView->Feedback->PartitionJobStatuses) {
            status->CurrentJobStatusUpdateTime = now;
            auto& job = status->CurrentJobStatus;
            job->Epoch = flowView->State->ExecutionSpec->GetEpoch();
            job->UpdateTime = now;
            auto previous = job->ProcessingObservation;
            job->ProcessingObservation = New<TProcessingObservation>();
            job->ProcessingObservation->Sequence = previous ? previous->Sequence + 1 : 1;
            job->ProcessingObservation->CapturedAt = now;
            job->ProcessingObservation->SpecGeneration = job->Epoch;
        }
        const auto config = New<TThroughputSpec>();
        autoscaler.Reconfigure(computations, config);
        autoscaler.Evaluate({
            .Now = now,
            .Config = config,
            .Computations = meter.Collect(flowView->State, flowView, config, autoscaler.ComputeMeasurementRequests()),
        });
    };
    evaluate(0);
    evaluate(20);
    EXPECT_FALSE(autoscaler.FindDecision(sourceId));
    EXPECT_FALSE(autoscaler.FindDecision(consumerId));
    EXPECT_EQ(autoscaler.FindTelemetry(consumerId)->Demand, 100);

    // Stale terminal-job feedback must not be sampled after the last source key completes.
    activeSource->State = EPartitionState::Completed;
    activeSource->CurrentJobId.reset();
    evaluate(40);
    EXPECT_FALSE(autoscaler.FindDecision(sourceId));
    EXPECT_FALSE(autoscaler.FindDecision(consumerId));
    EXPECT_EQ(autoscaler.FindTelemetry(consumerId)->Demand, 0);
    EXPECT_EQ(autoscaler.FindTelemetry(consumerId)->SteadyDemand, 0);
    EXPECT_EQ(autoscaler.FindTelemetry(sourceId)->PartitionCount, 0);
    EXPECT_EQ(autoscaler.FindTelemetry(sourceId)->SteadyDemand, 0);

    auto newSource = addPartition(sourceId, external, EPartitionState::Executing);
    newSource->CurrentJobId.reset();
    evaluate(41);
    EXPECT_FALSE(autoscaler.FindTelemetry(consumerId)->Demand);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
