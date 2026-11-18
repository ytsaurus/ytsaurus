#include <yt/yt/flow/library/cpp/common/stream_buffer_duration.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <limits>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TStreamBufferDurationTest
    : public ::testing::Test
{
protected:
    TPipelineSpecPtr Spec_ = New<TPipelineSpec>();
    TDynamicPipelineSpecPtr Dynamic_ = New<TDynamicPipelineSpec>();

    void Add(const std::string& computationId, const std::string& input, const std::string& output, double seconds, bool swift = true)
    {
        auto computation = New<TComputationSpec>();
        computation->ComputationClassName = swift ? "swift" : "transform";
        computation->InputStreamIds.insert(TStreamId(input));

        if (!output.empty()) {
            computation->OutputStreamIds.insert(TStreamId(output));
            computation->StreamsDependency[TStreamId(output)].insert(TStreamId(input));
        }

        Spec_->Computations[TComputationId(computationId)] = computation;
        auto dynamic = New<TDynamicComputationSpec>();
        dynamic->BatchDuration = TDuration::Seconds(seconds);
        Dynamic_->Computations[TComputationId(computationId)] = dynamic;
    }

    TStreamBufferDurationSnapshotPtr Build()
    {
        return BuildStreamBufferDurationSnapshot(Spec_, Dynamic_, [] (const std::string& name) {
            return name == "swift"    ? EInputAcknowledgementPolicy::AfterDependentOutputs
                : name == "transform" ? EInputAcknowledgementPolicy::PersistAfterEpoch
                                      : EInputAcknowledgementPolicy::Unknown;
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TStreamBufferDurationTest, InclusiveMaterializationBoundaryAndSideFormula)
{
    Add("producer", "source_input", "a", 2, /*swift*/ false);
    Add("swift", "a", "b", 3);
    Add("materializer", "b", "c", 5, /*swift*/ false);
    Add("writer", "c", "d", 100, /*swift*/ false);
    auto snapshot = Build();
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("a")).ChainSeconds, 10);
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("b")).ChainSeconds, 8);
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("source_input")).ChainSeconds, 2);
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("c")).ChainSeconds, 105);
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("d")).ChainSeconds, 100);
    EXPECT_EQ(GetOrCrash(snapshot->InputDurations, TComputationId("producer")).ChainSeconds, 2);
    EXPECT_EQ(GetOrCrash(snapshot->InputDurations, TComputationId("swift")).ChainSeconds, 3);
    EXPECT_EQ(GetOrCrash(snapshot->InputDurations, TComputationId("materializer")).ChainSeconds, 5);
    EXPECT_EQ(GetOrCrash(snapshot->InputDurations, TComputationId("writer")).ChainSeconds, 100);
    auto side = New<TDynamicBufferStateManagerSpec::TOneSideBufferSpec>();
    side->MaxDuration = TDuration::Minutes(15);
    const auto* estimate = snapshot->Streams.FindPtr(TStreamId("a"));
    EXPECT_EQ(ComputeBufferDurationSeconds(side, estimate), 900);
    EXPECT_FALSE(IsAutomaticBufferDurationEnabled(side));
    side->MaxDuration = TDuration::Minutes(10);
    EXPECT_EQ(ComputeBufferDurationSeconds(side, estimate), 600);
    EXPECT_FALSE(side->MinDuration);
    side->MinDuration = side->MaxDuration;
    EXPECT_FALSE(IsAutomaticBufferDurationEnabled(side));
    EXPECT_EQ(ComputeBufferDurationSeconds(side, estimate), 600);
    side->MinDuration = TDuration::Minutes(20);
    EXPECT_FALSE(IsAutomaticBufferDurationEnabled(side));
    EXPECT_EQ(ComputeBufferDurationSeconds(side, estimate), 600);
    side->MaxDuration = TDuration::Minutes(15);
    side->MinDuration = TDuration::Seconds(60);
    EXPECT_TRUE(IsAutomaticBufferDurationEnabled(side));
    EXPECT_EQ(ComputeBufferDurationSeconds(side, estimate), 60);
    side->MinDuration = TDuration::Seconds(1);
    side->DurationMultiplier = 0.5;
    EXPECT_EQ(ComputeBufferDurationSeconds(side, estimate), 5);
    side->DurationMultiplier = std::numeric_limits<double>::max();
    EXPECT_EQ(ComputeBufferDurationSeconds(side, estimate), 900);
    side->MinDuration = TDuration::Hours(1);
    EXPECT_EQ(ComputeBufferDurationSeconds(side, estimate), 900);
    EXPECT_EQ(ComputeBufferDurationSeconds(side, nullptr), 900);
}

TEST_F(TStreamBufferDurationTest, BranchesUseMaximumOfSerialSums)
{
    Add("left", "a", "b", 2);
    Add("right", "a", "c", 4);
    Add("left_sink", "b", "d", 7, /*swift*/ false);
    Add("right_sink", "c", "e", 3, /*swift*/ false);
    auto snapshot = Build();
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("a")).ChainSeconds, 9);
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("b")).ChainSeconds, 9);
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("c")).ChainSeconds, 7);
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("d")).ChainSeconds, 7);
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("e")).ChainSeconds, 3);
    Dynamic_->Computations.erase(TComputationId("left_sink"));
    auto defaultSnapshot = Build();
    EXPECT_EQ(GetOrCrash(defaultSnapshot->Streams, TStreamId("b")).ChainSeconds, 3);
    EXPECT_EQ(GetOrCrash(defaultSnapshot->InputDurations, TComputationId("left_sink")).ChainSeconds, 1);
    GetOrCrash(Dynamic_->Computations, TComputationId("right_sink"))->BatchDuration = TDuration::Zero();
    auto updatedSnapshot = Build();
    EXPECT_EQ(GetOrCrash(updatedSnapshot->Streams, TStreamId("c")).ChainSeconds, 4);
    EXPECT_EQ(GetOrCrash(updatedSnapshot->InputDurations, TComputationId("right_sink")).ChainSeconds, 0);
}

TEST_F(TStreamBufferDurationTest, OnlyDeclaredDependenciesMatterInComputationCycles)
{
    Add("one", "a", "b", 2);
    Add("two", "b", "c", 3);
    auto one = GetOrCrash(Spec_->Computations, TComputationId("one"));
    one->InputStreamIds.insert(TStreamId("c"));
    one->OutputStreamIds.insert(TStreamId("d"));
    one->StreamsDependency[TStreamId("d")].insert(TStreamId("c"));
    one->TimerStreams[TStreamId("timer")] = New<TTimerSpec>();
    one->StreamsDependency[TStreamId("timer")].insert(TStreamId("a"));
    one->StreamsDependency[TStreamId("d")].insert(TStreamId("timer"));
    one->SourceStreams[TStreamId("source")] = New<TSourceSpec>();
    one->StreamsDependency[TStreamId("d")].insert(TStreamId("source"));
    auto snapshot = Build();
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("a")).ChainSeconds, 7);
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("b")).ChainSeconds, 7);
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("c")).ChainSeconds, 5);
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("d")).ChainSeconds, 2);
    EXPECT_FALSE(snapshot->Streams.contains(TStreamId("one/timer")));
    EXPECT_FALSE(snapshot->Streams.contains(TStreamId("one/source")));
}

TEST_F(TStreamBufferDurationTest, ExtremeChainsSaturateBeforeApplyingSideCap)
{
    Add("first", "a", "b", 1);
    Add("second", "b", "c", 1, /*swift*/ false);
    GetOrCrash(Dynamic_->Computations, TComputationId("first"))->BatchDuration = TDuration::Max();
    GetOrCrash(Dynamic_->Computations, TComputationId("second"))->BatchDuration = TDuration::Max();
    auto snapshot = Build();
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("a")).ChainSeconds, TDuration::Max().SecondsFloat());
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("b")).ChainSeconds, TDuration::Max().SecondsFloat());
    EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("c")).ChainSeconds, TDuration::Max().SecondsFloat());
    auto side = New<TDynamicBufferStateManagerSpec::TOneSideBufferSpec>();
    side->MinDuration = TDuration::Seconds(1);
    side->DurationMultiplier = std::numeric_limits<double>::max();
    EXPECT_EQ(ComputeBufferDurationSeconds(side, snapshot->Streams.FindPtr(TStreamId("a"))), 60);
}

TEST_F(TStreamBufferDurationTest, SharedDiamondsAndSparseDependencies)
{
    constexpr int Count = 2000;
    for (int index = 0; index < Count; ++index) {
        Add("left" + std::to_string(index), "s" + std::to_string(index), "l" + std::to_string(index), 1);
        Add("right" + std::to_string(index), "s" + std::to_string(index), "r" + std::to_string(index), 1);
        auto nextStreamId = TStreamId("s" + std::to_string(index + 1));
        auto rightStreamId = TStreamId("r" + std::to_string(index));
        Add("join" + std::to_string(index), "l" + std::to_string(index), std::string(nextStreamId.Underlying()), 1);
        auto join = GetOrCrash(Spec_->Computations, TComputationId("join" + std::to_string(index)));
        join->InputStreamIds.insert(rightStreamId);
        join->StreamsDependency[nextStreamId].insert(rightStreamId);
    }

    EXPECT_EQ(GetOrCrash(Build()->Streams, TStreamId("s0")).ChainSeconds, Count * 2);
    Spec_->Computations.clear();
    Add("sparse", "a", "b", 1);
    auto sparse = GetOrCrash(Spec_->Computations, TComputationId("sparse"));
    for (int index = 0; index < Count; ++index) {
        auto input = TStreamId("i" + std::to_string(index));
        auto output = TStreamId("o" + std::to_string(index));
        sparse->InputStreamIds.insert(input);
        sparse->OutputStreamIds.insert(output);
        sparse->StreamsDependency[output].insert(input);
        Add("sink" + std::to_string(index), std::string(output.Underlying()), "", index, /*swift*/ false);
    }

    auto snapshot = Build();
    for (int index = 0; index < Count; ++index) {
        EXPECT_EQ(GetOrCrash(snapshot->Streams, TStreamId("i" + std::to_string(index))).ChainSeconds, index + 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
