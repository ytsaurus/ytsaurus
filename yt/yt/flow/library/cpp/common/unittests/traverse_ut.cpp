#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TTraverseTest, MergeStreamTraverseData)
{
    std::vector<TStreamTraverseDataPtr> streams = {
        ConvertTo<TStreamTraverseDataPtr>(TYsonString(TStringBuf(R"""(
            {
                epoch = 1;
                state = drained;
                system_watermark = 1712182928;
                event_watermark = 1712182911;
            }
        )"""))),
        ConvertTo<TStreamTraverseDataPtr>(TYsonString(TStringBuf(R"""(
            {
                epoch = 2;
                state = completed;
                system_watermark = 1712182900;
                event_watermark = 1712182903;
            }
        )"""))),
    };

    auto merged = MergeStreamTraverseData(streams, EInflightMerge::None);
    ASSERT_EQ(merged->Epoch, 1);
    ASSERT_EQ(merged->State, EStreamState::Drained);
    ASSERT_EQ(merged->SystemWatermark, TSystemTimestamp(1712182900));
    ASSERT_EQ(merged->EventWatermark, TSystemTimestamp(1712182903));
}

TEST(TTraverseTest, MergeInflightTraverseData)
{
    std::vector<TInflightStreamTraverseDataPtr> inflight = {
        ConvertTo<TInflightStreamTraverseDataPtr>(TYsonString(TStringBuf(R"""(
            {
                min_system_timestamp = 1712182928;
                min_event_timestamp = 1712182911;
            }
        )"""))),
        ConvertTo<TInflightStreamTraverseDataPtr>(TYsonString(TStringBuf(R"""(
            {
                min_event_timestamp = 1712182910;
            }
        )"""))),
        ConvertTo<TInflightStreamTraverseDataPtr>(TYsonString(TStringBuf(R"""(
            {
                min_system_timestamp = 1712182929;
            }
        )"""))),
    };
    auto merged = MergeInflightTraverseData(inflight);
    ASSERT_EQ(merged->MinSystemTimestamp, TSystemTimestamp(1712182928));
    ASSERT_EQ(merged->MinEventTimestamp, TSystemTimestamp(1712182910));
}

TEST(TTraverseTest, MergeInflightLifecycleMetrics)
{
    auto first = New<TInflightStreamTraverseData>();
    first->InflightMetrics->ReadyCount = 2;
    first->InflightMetrics->ReadyByteSize = 20;
    first->InflightMetrics->OfferedCountPerSec = 3;
    first->InflightMetrics->OfferedBytesPerSec = 30;
    auto second = New<TInflightStreamTraverseData>();
    second->InflightMetrics->ReadyCount = 5;
    second->InflightMetrics->ReadyByteSize = 50;
    second->InflightMetrics->OfferedCountPerSec = 7;
    second->InflightMetrics->OfferedBytesPerSec = 70;

    const auto merged = MergeInflightTraverseData({first, second});
    EXPECT_EQ(merged->InflightMetrics->ReadyCount, 7);
    EXPECT_EQ(merged->InflightMetrics->ReadyByteSize, 70);
    EXPECT_EQ(merged->InflightMetrics->OfferedCountPerSec, 10);
    EXPECT_EQ(merged->InflightMetrics->OfferedBytesPerSec, 100);
}

TEST(TTraverseTest, MergeInflightFlagsAreOrderIndependent)
{
    auto emptySuspended = New<TInflightStreamTraverseData>();
    emptySuspended->Empty = true;
    emptySuspended->Suspended = true;

    auto active = New<TInflightStreamTraverseData>();
    active->Empty = false;
    active->Suspended = false;

    for (const auto& inflights : {
            std::vector{emptySuspended, active},
            std::vector{active, emptySuspended},
         }) {
        const auto merged = MergeInflightTraverseData(inflights);
        EXPECT_FALSE(merged->Empty);
        EXPECT_FALSE(merged->Suspended);
    }
}

TEST(TTraverseTest, NoneMergeClearsLocalLifecycleMetrics)
{
    auto stream = New<TStreamTraverseData>();
    stream->InflightMetrics->ReadyCount = 1;
    stream->InflightMetrics->OfferedCountPerSec = 2;

    const auto merged = MergeStreamTraverseData({stream}, EInflightMerge::None);
    EXPECT_FALSE(merged->InflightMetrics->ReadyCount);
    EXPECT_FALSE(merged->InflightMetrics->OfferedCountPerSec);
}

TEST(TTraverseTest, ConsumerViewUsesCanonicalProducerAndLocalProgress)
{
    auto producer = New<TStreamTraverseData>();
    producer->InflightMetrics->Count = 20;
    producer->InflightMetrics->ByteSize = 200;
    producer->InflightMetrics->NewCountPerSec = 10;
    producer->InflightMetrics->NewBytesPerSec = 100;
    producer->InflightMetrics->OfferedCountPerSec = 80;
    producer->InflightMetrics->ReadyCount = 120;
    producer->InflightMetrics->ProcessedCountPerSec = 40;

    auto firstConsumer = New<TStreamTraverseData>();
    firstConsumer->Epoch = 3;
    firstConsumer->InflightMetrics->Count = 2;
    firstConsumer->InflightMetrics->ByteSize = 20;
    firstConsumer->InflightMetrics->NewCountPerSec = 1;
    firstConsumer->InflightMetrics->NewBytesPerSec = 10;
    firstConsumer->InflightMetrics->OfferedCountPerSec = 8;
    firstConsumer->InflightMetrics->ReadyCount = 0;
    firstConsumer->InflightMetrics->ReadyByteSize = 0;
    firstConsumer->InflightMetrics->ProcessedCountPerSec = 8;
    firstConsumer->InflightMetrics->ProcessedBytesPerSec = 80;

    auto secondConsumer = NYTree::CloneYsonStruct(firstConsumer);
    secondConsumer->InflightMetrics->ReadyCount = 6;
    secondConsumer->InflightMetrics->ReadyByteSize = 60;
    secondConsumer->InflightMetrics->ProcessedCountPerSec = 2;
    secondConsumer->InflightMetrics->ProcessedBytesPerSec = 20;

    const auto firstView = BuildConsumerStreamTraverseData(firstConsumer, producer);
    const auto secondView = BuildConsumerStreamTraverseData(secondConsumer, producer);

    EXPECT_EQ(firstView->Epoch, 3);
    EXPECT_EQ(firstView->InflightMetrics->Count, 20);
    EXPECT_EQ(firstView->InflightMetrics->ByteSize, 200);
    EXPECT_EQ(firstView->InflightMetrics->NewCountPerSec, 10);
    EXPECT_EQ(firstView->InflightMetrics->NewBytesPerSec, 100);
    EXPECT_EQ(firstView->InflightMetrics->OfferedCountPerSec, 8);
    EXPECT_EQ(firstView->InflightMetrics->ReadyCount, 0);
    EXPECT_EQ(firstView->InflightMetrics->ProcessedCountPerSec, 8);

    EXPECT_EQ(secondView->InflightMetrics->Count, 20);
    EXPECT_EQ(secondView->InflightMetrics->NewCountPerSec, 10);
    EXPECT_EQ(secondView->InflightMetrics->OfferedCountPerSec, 8);
    EXPECT_EQ(secondView->InflightMetrics->ReadyCount, 6);
    EXPECT_EQ(secondView->InflightMetrics->ProcessedCountPerSec, 2);
}

TEST(TTraverseTest, ApplyInflightPreservesProducerSystemWatermark)
{
    auto stream = New<TStreamTraverseData>();
    stream->SystemWatermark = TSystemTimestamp(100);
    stream->EventWatermark = TSystemTimestamp(200);

    auto inflight = New<TInflightStreamTraverseData>();
    inflight->InflightMetrics->Count = 1;

    auto applied = ApplyInflightTraverseData(stream, inflight);
    EXPECT_EQ(applied->SystemWatermark, TSystemTimestamp(100));

    inflight->MinSystemTimestamp = TSystemTimestamp(80);
    applied = ApplyInflightTraverseData(stream, inflight);
    EXPECT_EQ(applied->SystemWatermark, TSystemTimestamp(80));
}

TEST(TTraverseTest, MergeNodeKeepsMatureRatesWhenAnotherPartitionIsYoung)
{
    const TStreamId streamId("stream");
    auto mature = New<TNodeTraverseData>();
    mature->IterationCycle = 10;
    mature->ProcessingRates = New<TComputationProcessingRates>();
    mature->ProcessingRates->Rate1m.emplace();
    mature->ProcessingRates->Rate10m.emplace();
    mature->ProcessingRates->Rate1m.value().Processed.ProcessedMessagesPerSecond = 500;
    mature->Streams[streamId] = New<TStreamTraverseData>();
    mature->Streams[streamId]->InflightMetrics->ProcessedCountPerSec = 100;

    auto young = New<TNodeTraverseData>();
    young->IterationCycle = 1;
    young->Streams[streamId] = New<TStreamTraverseData>();

    const auto merged = MergeNodeTraverseData({mature, young});
    EXPECT_EQ(merged->Streams.at(streamId)->InflightMetrics->ProcessedCountPerSec, 100);
    EXPECT_FALSE(merged->IterationCycle);
    EXPECT_FALSE(merged->ProcessingRates);
}

TEST(TTraverseTest, CompletedPartitionReplacesLastRatesWithKnownZeros)
{
    auto spec = New<TExtendedComputationSpec>();
    const TStreamId streamId("source");
    spec->AllStreamIds.insert(streamId);
    auto completed = MakeCompletedPartitionTraverseData(7, TSystemTimestamp(300), spec)->Node;
    EXPECT_EQ(completed->ReportTime, TSystemTimestamp(300));
    EXPECT_FALSE(completed->IterationCycle);
    const auto& stream = completed->Streams.at(streamId);
    EXPECT_EQ(stream->State, EStreamState::Completed);
    EXPECT_EQ(stream->Epoch, 7);
    EXPECT_EQ(stream->SystemWatermark, TSystemTimestamp(300));
    EXPECT_EQ(stream->EventWatermark, TSystemTimestamp(300));
    EXPECT_EQ(stream->InflightMetrics->Count, 0);

    auto previous = CloneYsonStruct(completed);
    ASSERT_TRUE(previous->ProcessingRates);
    for (auto window : {&TComputationProcessingRates::Rate1m, &TComputationProcessingRates::Rate10m}) {
        auto& rate = previous->ProcessingRates.Get()->*window;
        ASSERT_TRUE(rate);
        rate->Processed.ProcessedMessagesPerSecond = 10;
        rate->Processed.ProcessedBytesPerSecond = 100;
        ASSERT_TRUE(rate->Capacity);
        rate->Capacity->ProcessedMessagesPerSecond = 20;
        rate->Capacity->ProcessedBytesPerSecond = 200;
    }
    auto advanced = AdvanceNodeTraverseData(previous, completed);
    auto merged = MergeNodeTraverseData({completed, advanced});
    ASSERT_TRUE(merged->ProcessingRates);
    for (auto window : {&TComputationProcessingRates::Rate1m, &TComputationProcessingRates::Rate10m}) {
        const auto& rate = merged->ProcessingRates.Get()->*window;
        ASSERT_TRUE(rate);
        EXPECT_DOUBLE_EQ(rate->Processed.ProcessedMessagesPerSecond, 0);
        EXPECT_DOUBLE_EQ(rate->Processed.ProcessedBytesPerSecond, 0);
        ASSERT_TRUE(rate->Capacity);
        EXPECT_DOUBLE_EQ(rate->Capacity->ProcessedMessagesPerSecond, 0);
        EXPECT_DOUBLE_EQ(rate->Capacity->ProcessedBytesPerSecond, 0);
    }

    auto unobserved = New<TNodeTraverseData>();
    unobserved->Streams[streamId] = New<TStreamTraverseData>();
    auto incomplete = MergeNodeTraverseData({completed, unobserved});
    EXPECT_FALSE(incomplete->ProcessingRates);
    EXPECT_EQ(incomplete->Streams.at(streamId)->State, EStreamState::Active);
}

TEST(TTraverseTest, MergeNodeSumsLocallyNormalizedCapacity)
{
    auto first = New<TNodeTraverseData>();
    first->ProcessingRates = New<TComputationProcessingRates>();
    first->ProcessingRates->Rate1m.emplace();
    first->ProcessingRates->Rate10m.emplace();
    auto& a = first->ProcessingRates->Rate1m.value();
    a.Processed.ProcessedMessagesPerSecond = 100;
    a.Processed.ProcessedBytesPerSecond = 800;
    a.Capacity = a.Processed;

    auto second = NYTree::CloneYsonStruct(first);
    auto& b = second->ProcessingRates->Rate1m.value();
    b.Capacity->ProcessedMessagesPerSecond = 1000;
    b.Capacity->ProcessedBytesPerSecond = 8000;

    const auto merged = MergeNodeTraverseData({first, second});
    ASSERT_TRUE(merged->ProcessingRates);
    const auto& rate = merged->ProcessingRates->Rate1m.value();
    EXPECT_DOUBLE_EQ(rate.Processed.ProcessedMessagesPerSecond, 200);
    EXPECT_DOUBLE_EQ(rate.Processed.ProcessedBytesPerSecond, 1600);
    ASSERT_TRUE(rate.Capacity);
    EXPECT_DOUBLE_EQ(rate.Capacity->ProcessedMessagesPerSecond, 1100);
    EXPECT_DOUBLE_EQ(rate.Capacity->ProcessedBytesPerSecond, 8800);
    EXPECT_DOUBLE_EQ(a.Capacity->ProcessedMessagesPerSecond, 100);

    b.Capacity.reset();
    const auto partial = MergeNodeTraverseData({first, second});
    EXPECT_DOUBLE_EQ(partial->ProcessingRates->Rate1m.value().Processed.ProcessedMessagesPerSecond, 200);
    EXPECT_FALSE(partial->ProcessingRates->Rate1m.value().Capacity);

    second->ProcessingRates->Rate1m.reset();
    EXPECT_FALSE(MergeNodeTraverseData({first, second})->ProcessingRates->Rate1m);
}

TEST(TTraverseTest, ServiceWindowsMergeIndependently)
{
    auto first = New<TNodeTraverseData>();
    first->ProcessingRates = New<TComputationProcessingRates>();
    first->ProcessingRates->Rate1m.emplace();
    first->ProcessingRates->Rate10m.emplace();
    first->ProcessingRates->Rate1m.value().Processed.ProcessedMessagesPerSecond = 100;
    first->ProcessingRates->Rate10m.value().Processed.ProcessedMessagesPerSecond = 40;
    auto second = CloneYsonStruct(first);
    second->ProcessingRates->Rate1m.value().Processed.ProcessedMessagesPerSecond = 200;
    second->ProcessingRates->Rate10m.value().Processed.ProcessedMessagesPerSecond = 60;
    const auto merged = MergeNodeTraverseData({first, second});
    EXPECT_DOUBLE_EQ(merged->ProcessingRates->Rate1m.value().Processed.ProcessedMessagesPerSecond, 300);
    EXPECT_DOUBLE_EQ(merged->ProcessingRates->Rate10m.value().Processed.ProcessedMessagesPerSecond, 100);
    second->ProcessingRates->Rate10m.reset();
    const auto partial = MergeNodeTraverseData({first, second});
    EXPECT_DOUBLE_EQ(partial->ProcessingRates->Rate1m.value().Processed.ProcessedMessagesPerSecond, 300);
    EXPECT_FALSE(partial->ProcessingRates->Rate10m);
}

TEST(TTraverseTest, AdvanceNodeKeepsProcessingRatesWithItsObservation)
{
    auto previous = New<TNodeTraverseData>();
    previous->ReportTime = TSystemTimestamp(10);
    previous->IterationCycle = 7;
    previous->ProcessingRates = New<TComputationProcessingRates>();
    previous->ProcessingRates->Rate1m.emplace();
    previous->ProcessingRates->Rate10m.emplace();
    previous->ProcessingRates->Rate1m.value().Processed.ProcessedMessagesPerSecond = 100;
    previous->ProcessingRates->Rate1m.value().Processed.ProcessedBytesPerSecond = 800;

    auto current = New<TNodeTraverseData>();
    current->ReportTime = TSystemTimestamp(20);
    current->IterationCycle = 8;
    current->ProcessingRates = New<TComputationProcessingRates>();
    current->ProcessingRates->Rate1m.emplace();
    current->ProcessingRates->Rate10m.emplace();
    current->ProcessingRates->Rate1m.value().Processed.ProcessedMessagesPerSecond = 200;
    current->ProcessingRates->Rate1m.value().Processed.ProcessedBytesPerSecond = 3200;

    auto advanced = AdvanceNodeTraverseData(previous, current);
    ASSERT_TRUE(advanced->ProcessingRates);
    EXPECT_EQ(advanced->IterationCycle, 8);
    EXPECT_EQ(advanced->ProcessingRates->Rate1m.value().Processed.ProcessedMessagesPerSecond, 200);
    EXPECT_EQ(advanced->ProcessingRates->Rate1m.value().Processed.ProcessedBytesPerSecond, 3200);
    EXPECT_EQ(previous->ProcessingRates->Rate1m.value().Processed.ProcessedMessagesPerSecond, 100);
    EXPECT_EQ(previous->IterationCycle, 7);

    current->IterationCycle = 9;
    current->ProcessingRates.Reset();
    advanced = AdvanceNodeTraverseData(advanced, current);
    EXPECT_EQ(advanced->IterationCycle, 9);
    EXPECT_FALSE(advanced->ProcessingRates);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
