#include <yt/yt/flow/library/cpp/computation/read_delay.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TReadDelayBatch MakeBatch(const char* id, ui64 timestamp, i64 count, i64 bytes)
{
    return {
        .FirstMessageId = TMessageId(id),
        .InputStatistics = {.Count = count, .ByteSize = bytes},
        .Timestamp = TSystemTimestamp(timestamp),
    };
}

TEST(TReadDelayTest, ThresholdIsMinimumWithSaturatingSubtraction)
{
    auto alignment = New<TWatermarkAlignmentSpec>();
    auto watermark = [] (const TStreamId& stream) {
        return TSystemTimestamp(stream == TStreamId("a") ? 100 : 50);
    };
    EXPECT_EQ(GetReadDelayThreshold(alignment, BIND(watermark)), InfinitySystemTimestamp);
    alignment->ReadDelays = {{TStreamId("a"), TDuration::Seconds(20)}, {TStreamId("b"), TDuration::Seconds(10)}};
    EXPECT_EQ(GetReadDelayThreshold(alignment, BIND(watermark)), TSystemTimestamp(40));
    alignment->ReadDelays->at(TStreamId("b")) = TDuration::Seconds(60);
    EXPECT_EQ(GetReadDelayThreshold(alignment, BIND(watermark)), ZeroSystemTimestamp);
}

TEST(TReadDelayTest, PublishesOnlyContiguousPrefixAndCountsHeldOriginalInput)
{
    TReadDelayQueue queue;
    auto memory = New<TOrderedMemory<TMessageId, TSystemTimestamp>>();
    queue.Push(MakeBatch("a", 100, 2, 100));
    queue.Push(MakeBatch("b", 200, 3, 600));
    queue.Push(MakeBatch("c", 50, 4, 1200));
    EXPECT_EQ(queue.GetPublishablePrefix(TSystemTimestamp(100), false, memory).BatchCount, 1u);
    EXPECT_EQ(queue.Pop().FirstMessageId, TMessageId("a"));
    auto status = queue.BuildStatus(TSystemTimestamp(100), false);
    EXPECT_EQ(status->HeldCount, 7);
    EXPECT_EQ(status->HeldByteSize, 1800);
    EXPECT_EQ(status->HeldBatchCount, 2);
    EXPECT_EQ(status->FirstBlockingMessageId, TMessageId("b"));
    EXPECT_EQ(status->RequiredEventThreshold, TSystemTimestamp(200));
    EXPECT_EQ(queue.GetPublishablePrefix(TSystemTimestamp(199), false, memory).BatchCount, 0u);
    EXPECT_EQ(queue.GetPublishablePrefix(TSystemTimestamp(200), false, memory).BatchCount, 2u);
    queue.Pop();
    queue.Pop();
    status = queue.BuildStatus(TSystemTimestamp(200), false);
    EXPECT_EQ(status->HeldCount, 0);
    EXPECT_EQ(status->HeldByteSize, 0);
    EXPECT_FALSE(status->FirstBlockingMessageId);
}

TEST(TReadDelayTest, DrainUsesOrderedMemoryFrontierInsteadOfEventGate)
{
    TReadDelayQueue queue;
    auto memory = New<TOrderedMemory<TMessageId, TSystemTimestamp>>();
    memory->Register(TMessageId("b"), TSystemTimestamp(10));
    queue.Push(MakeBatch("a", 200, 1, 10));
    queue.Push(MakeBatch("b", 300, 1, 20));
    queue.Push(MakeBatch("c", 0, 1, 30));
    EXPECT_EQ(queue.GetPublishablePrefix(ZeroSystemTimestamp, true, memory).BatchCount, 2u);
    queue.Pop();
    queue.Pop();
    EXPECT_EQ(queue.GetPublishablePrefix(InfinitySystemTimestamp, true, memory).BatchCount, 0u);
    const auto status = queue.BuildStatus(InfinitySystemTimestamp, true);
    EXPECT_TRUE(status->Draining);
    EXPECT_FALSE(status->RequiredEventThreshold);
    EXPECT_EQ(status->FirstBlockingMessageId, TMessageId("c"));
}

TEST(TReadDelayTest, BatchGateIncludesEveryCandidateAndEmptyBatchHasNoDelay)
{
    std::vector<TMessage> messages(3);
    messages[0].EventTimestamp = TSystemTimestamp(100);
    messages[1].EventTimestamp = TSystemTimestamp(300);
    messages[2].EventTimestamp = TSystemTimestamp(200);
    EXPECT_EQ(GetReadDelayBatchTimestamp(messages), TSystemTimestamp(300));
    EXPECT_EQ(GetReadDelayBatchTimestamp({}), ZeroSystemTimestamp);
}

TEST(TReadDelayTest, EstimatedInflightIsIndependentAndPreservesOtherMetrics)
{
    auto input = New<TInflightStreamTraverseData>();
    input->Suspended = true;
    input->MinSystemTimestamp = TSystemTimestamp(123);
    auto& metrics = input->InflightMetrics;
    metrics->Count = 300;
    metrics->ReadyCount = 100;
    metrics->ReadyByteSize = 1000;
    metrics->NewCountPerSec = 10;
    metrics->ProcessedCountPerSec = 5;
    metrics->OfferedCountPerSec = 10;
    auto result = BuildReadDelayInflight(input, {.Offered = TSourceWork{7, 70}, .Ready = TSourceWork{50, 500}});
    EXPECT_EQ(result->InflightMetrics->ReadyCount, 50);
    EXPECT_EQ(result->InflightMetrics->ReadyByteSize, 500);
    EXPECT_EQ(result->InflightMetrics->OfferedCountPerSec, 7);
    EXPECT_EQ(result->InflightMetrics->Count, 300);
    EXPECT_EQ(result->InflightMetrics->NewCountPerSec, 10);
    EXPECT_EQ(result->InflightMetrics->ProcessedCountPerSec, 5);
    EXPECT_TRUE(result->Suspended);
    EXPECT_EQ(result->MinSystemTimestamp, TSystemTimestamp(123));
    EXPECT_EQ(input->InflightMetrics->ReadyCount, 100);
    EXPECT_EQ(input->InflightMetrics->OfferedCountPerSec, 10);
    result = BuildReadDelayInflight(input, {});
    EXPECT_FALSE(result->InflightMetrics->ReadyCount);
    EXPECT_FALSE(result->InflightMetrics->OfferedCountPerSec);
    EXPECT_EQ(input->InflightMetrics->ReadyCount, 100);
}

TEST(TReadDelayTest, QualityWarningIsPublishedAndClearedThroughRetryableStatus)
{
    auto profiler = CreateSyncStatusProfiler();
    auto error = profiler->ErrorState("read_delay_estimate");
    error->SetError(GetReadDelayEstimateWarning({.CountDensityMismatch = true}));
    ASSERT_EQ(profiler->GetStatus().Errors.size(), 1u);
    error->SetError(GetReadDelayEstimateWarning({}));
    EXPECT_TRUE(profiler->GetStatus().Errors.empty());
}

TEST(TReadDelayTest, OriginalSamplesSurviveFilteringAndKeepRawLastId)
{
    auto schema = New<NTableClient::TTableSchema>();
    const auto makeMessage = [&] (const char* id) {
        TMessageBuilder builder(TStreamId("source"), schema);
        builder.SetMessageId(TMessageId(id));
        builder.SetSystemTimestamp(TSystemTimestamp(100));
        builder.SetAlignmentTimestamp(TSystemTimestamp(100));
        builder.SetEventTimestamp(TSystemTimestamp(90));
        return New<TInputMessage>(builder.Finish(), TKey());
    };
    std::vector<ISource::TMessageBatch> batches(1);
    batches[0].Messages = {makeMessage("first"), makeMessage("last")};
    const auto actualBytes = batches[0].Messages[0]->ByteSize + batches[0].Messages[1]->ByteSize;
    auto fallback = PrepareReadDelayInputs(batches);
    EXPECT_EQ(fallback[0].Statistics.Count, 2);
    EXPECT_EQ(fallback[0].Statistics.ByteSize, actualBytes);
    batches[0].OriginalStatistics = TBatchStatistics{.Count = 2, .ByteSize = actualBytes};
    auto samples = PrepareReadDelayInputs(batches);
    batches[0].Messages.clear();
    EXPECT_EQ(samples[0].LastMessageId, TMessageId("last"));
    EXPECT_EQ(samples[0].SystemTime, TInstant::Seconds(100));
    EXPECT_EQ(samples[0].Statistics.Count, 2);
    EXPECT_EQ(samples[0].Statistics.ByteSize, actualBytes);
    EXPECT_FALSE(samples[0].EventTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
