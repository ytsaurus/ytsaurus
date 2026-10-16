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

    for (bool allowPartial : {false, true}) {
        const auto merged = MergeInflightTraverseData({first, second}, allowPartial);
        EXPECT_EQ(merged->InflightMetrics->ReadyCount, 7);
        EXPECT_EQ(merged->InflightMetrics->ReadyByteSize, 70);
        EXPECT_EQ(merged->InflightMetrics->OfferedCountPerSec, 10);
        EXPECT_EQ(merged->InflightMetrics->OfferedBytesPerSec, 100);
        EXPECT_FALSE(merged->InflightMetrics->NewCountPerSec);
        EXPECT_FALSE(merged->InflightMetrics->NewBytesPerSec);
        EXPECT_FALSE(merged->InflightMetrics->ProcessedCountPerSec);
        EXPECT_FALSE(merged->InflightMetrics->ProcessedBytesPerSec);
    }
}

TEST(TTraverseTest, PartialInflightMergePreservesRatesWithSinkSnapshot)
{
    const auto rateFields = {
        &TInflightMetrics::NewCountPerSec,
        &TInflightMetrics::NewBytesPerSec,
        &TInflightMetrics::OfferedCountPerSec,
        &TInflightMetrics::OfferedBytesPerSec,
        &TInflightMetrics::ProcessedCountPerSec,
        &TInflightMetrics::ProcessedBytesPerSec,
    };
    for (int sinkCount : {0, 2}) {
        auto output = New<TInflightStreamTraverseData>();
        output->Empty = false;
        output->Suspended = true;
        output->MinSystemTimestamp = TSystemTimestamp(80);
        output->MinEventTimestamp = TSystemTimestamp(100);
        output->InflightMetrics->Count = 3;
        output->InflightMetrics->ByteSize = 30;
        output->InflightMetrics->ReadyCount = 1;
        output->InflightMetrics->ReadyByteSize = 10;
        for (auto field : rateFields) {
            output->InflightMetrics.Get()->*field = 100;
        }

        auto sink = New<TInflightStreamTraverseData>();
        sink->Empty = sinkCount == 0;
        sink->Suspended = false;
        sink->InflightMetrics->Count = sinkCount;
        sink->InflightMetrics->ByteSize = 10 * sinkCount;
        sink->InflightMetrics->ReadyCount = 0;
        sink->InflightMetrics->ReadyByteSize = 0;
        if (sinkCount > 0) {
            sink->MinSystemTimestamp = TSystemTimestamp(60);
            sink->MinEventTimestamp = TSystemTimestamp(120);
        }

        for (const auto& inflights : {std::vector{output, sink}, std::vector{sink, output}}) {
            const auto merged = MergeInflightTraverseData(inflights, /*allowPartial*/ true);
            const auto strict = MergeInflightTraverseData(inflights);
            for (auto field : rateFields) {
                EXPECT_EQ(merged->InflightMetrics.Get()->*field, 100);
                EXPECT_FALSE(strict->InflightMetrics.Get()->*field);
                EXPECT_EQ(output->InflightMetrics.Get()->*field, 100);
                EXPECT_FALSE(sink->InflightMetrics.Get()->*field);
            }
            EXPECT_EQ(merged->InflightMetrics->Count, 3 + sinkCount);
            EXPECT_EQ(merged->InflightMetrics->ByteSize, 30 + 10 * sinkCount);
            EXPECT_EQ(merged->InflightMetrics->ReadyCount, 1);
            EXPECT_EQ(merged->InflightMetrics->ReadyByteSize, 10);
            EXPECT_EQ(merged->MinSystemTimestamp, TSystemTimestamp(sinkCount ? 60 : 80));
            EXPECT_EQ(merged->MinEventTimestamp, TSystemTimestamp(100));
            EXPECT_FALSE(merged->Empty);
            EXPECT_FALSE(merged->Suspended);
            EXPECT_EQ(output->InflightMetrics->Count, 3);
            EXPECT_EQ(sink->InflightMetrics->Count, sinkCount);
            EXPECT_EQ(output->MinSystemTimestamp, TSystemTimestamp(80));
        }
    }
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

TEST(TTraverseTest, MergeNodeDoesNotCombineLocalIterationCycles)
{
    const TStreamId streamId("stream");
    auto first = New<TNodeTraverseData>();
    first->IterationCycle = 10;
    first->Streams[streamId] = New<TStreamTraverseData>();
    first->Streams[streamId]->InflightMetrics->ProcessedCountPerSec = 100;
    auto second = New<TNodeTraverseData>();
    second->IterationCycle = 1;
    second->Streams[streamId] = New<TStreamTraverseData>();

    const auto merged = MergeNodeTraverseData({first, second});
    EXPECT_EQ(merged->Streams.at(streamId)->InflightMetrics->ProcessedCountPerSec, 100);
    EXPECT_FALSE(merged->IterationCycle);
}

TEST(TTraverseTest, CompletedPartitionHasNoLocalIteration)
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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
