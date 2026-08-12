#include <yt/yt/flow/library/cpp/worker/input_buffer_detail.h>

#include <yt/yt/flow/library/cpp/buffers/epoch_cycle_tracker.h>
#include <yt/yt/flow/library/cpp/buffers/offered_rate_estimator.h>

#include <yt/yt/core/test_framework/framework.h>

#include <util/random/fast.h>

namespace NYT::NFlow::NWorker {
namespace {

using TStreamState = TInputBuffer::TStreamState;
using TConnectionState = TInputBuffer::TConnectionState;

NFlow::TStreamLimitUsageStatePtr MakeLimitState(i64 inflatedLimitBytes)
{
    auto state = New<NFlow::TStreamLimitUsageState>();
    state->SetLimitBytes(inflatedLimitBytes);
    return state;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TInputBufferConnectionStateTest, Acquire)
{
    auto connectionState = TConnectionState{
        .UpdateEpoch = 0,
        .Offer = {{TSystemTimestamp(10), 800}, {TSystemTimestamp(9), 400}, {TSystemTimestamp(6), 200}, {TSystemTimestamp(5), 100}},
        .FreshOffer = true,
        .InflatedByteLimit = 900,
    };

    connectionState.Acquire(130);
    ASSERT_EQ(770, connectionState.InflatedByteLimit);
    ASSERT_FALSE(connectionState.FreshOffer);
    ASSERT_EQ(170, connectionState.Offer.back().second);

    connectionState.Acquire(10000);
    ASSERT_TRUE(connectionState.Offer.empty());
    ASSERT_LE(connectionState.InflatedByteLimit, 0);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TInputBufferRecalculateStreamLimitsTest, Empty)
{
    TStreamState streamState = {
        .ConnectionStates = {},
        .RecalculateCounter = 0,
        .Epoch = 1,
        .LimitUsageState = MakeLimitState(1000),
    };
    TInputBuffer::RecalculateStreamLimits(streamState);
    EXPECT_EQ(2, streamState.Epoch);
    EXPECT_EQ(0, streamState.RecalculateCounter);
}

TEST(TInputBufferRecalculateStreamLimitsTest, Trivial)
{
    auto connectionId = TGuid::Create();
    TStreamState streamState = {
        .ConnectionStates = {
            {
                connectionId,
                TConnectionState{
                    .UpdateEpoch = 0,
                    .Offer = {},
                    .FreshOffer = true,
                    .InflatedByteLimit = 0,
                },
            },
        },
        .RecalculateCounter = 0,
        .Epoch = 1,
        .LimitUsageState = MakeLimitState(1000),
    };
    TInputBuffer::RecalculateStreamLimits(streamState);
    EXPECT_EQ(900, streamState.ConnectionStates.at(connectionId).InflatedByteLimit);
    EXPECT_EQ(2, streamState.Epoch);
    EXPECT_EQ(1, streamState.RecalculateCounter);
}

TEST(TInputBufferRecalculateStreamLimitsTest, Fresh)
{
    for (auto [freshOffer, expectedByteLimit] : std::vector<std::pair<bool, i64>>{{true, 200}, {false, 90}}) {
        auto connectionId = TGuid::Create();
        TStreamState streamState = {
            .ConnectionStates = {
                {
                    connectionId,
                    TConnectionState{
                        .UpdateEpoch = 0,
                        .Offer = {{TSystemTimestamp(6), 200}},
                        .FreshOffer = freshOffer,
                    },
                },
            },
            .RecalculateCounter = 0,
            .LimitUsageState = MakeLimitState(100),
        };
        TInputBuffer::RecalculateStreamLimits(streamState);
        EXPECT_EQ(expectedByteLimit, streamState.ConnectionStates.at(connectionId).InflatedByteLimit);
    }
}

TEST(TInputBufferRecalculateStreamLimitsTest, InflatedByteLimit)
{
    auto connectionId1 = TGuid::Create();
    auto connectionId2 = TGuid::Create();
    TStreamState streamState = {
        .ConnectionStates = {
            {
                connectionId1,
                TConnectionState{
                    .UpdateEpoch = 0,
                    .Offer = {{TSystemTimestamp(10), 800}, {TSystemTimestamp(9), 400}, {TSystemTimestamp(6), 200}, {TSystemTimestamp(5), 100}},
                    .FreshOffer = true,
                },
            },
            {
                connectionId2,
                TConnectionState{
                    .UpdateEpoch = 0,
                    .Offer = {{TSystemTimestamp(12), 100}, {TSystemTimestamp(5), 200}},
                    .FreshOffer = true,
                },
            },
        },
        .RecalculateCounter = 0,
        .LimitUsageState = MakeLimitState(1000),
    };
    TInputBuffer::RecalculateStreamLimits(streamState);
    EXPECT_EQ(700, streamState.ConnectionStates.at(connectionId1).InflatedByteLimit);
    EXPECT_EQ(200, streamState.ConnectionStates.at(connectionId2).InflatedByteLimit);
}

TEST(TInputBufferRecalculateStreamLimitsTest, Cleanup)
{
    auto connectionId1 = TGuid::Create();
    auto connectionId2 = TGuid::Create();
    TStreamState streamState = {
        .ConnectionStates = {
            {
                connectionId1,
                TConnectionState{
                    .UpdateEpoch = 100,
                    .Offer = {{TSystemTimestamp(10), 800}, {TSystemTimestamp(9), 400}, {TSystemTimestamp(6), 200}, {TSystemTimestamp(5), 100}},
                    .FreshOffer = true,
                    .InflatedByteLimit = 100500,
                },
            },
            {
                connectionId2,
                TConnectionState{
                    .UpdateEpoch = 0,
                    .Offer = {{TSystemTimestamp(12), 100}, {TSystemTimestamp(5), 200}},
                    .FreshOffer = true,
                    .InflatedByteLimit = 100500,
                },
            },
        },
        .RecalculateCounter = 0,
        .Epoch = 101,
        .LimitUsageState = MakeLimitState(0),
    };
    TInputBuffer::RecalculateStreamLimits(streamState);
    EXPECT_FALSE(streamState.ConnectionStates.contains(connectionId2));
    ASSERT_TRUE(streamState.ConnectionStates.contains(connectionId1));
    EXPECT_EQ(0, streamState.ConnectionStates.at(connectionId1).InflatedByteLimit);
}

TEST(TInputBufferRecalculateStreamLimitsTest, RegrantKeepsStaleConnections)
{
    auto staleConnectionId = TGuid::Create();
    auto freshConnectionId = TGuid::Create();
    TStreamState streamState = {
        .ConnectionStates = {
            {
                staleConnectionId,
                TConnectionState{
                    .UpdateEpoch = 0,
                    .Offer = {{TSystemTimestamp(12), 100}, {TSystemTimestamp(5), 200}},
                    .InflatedByteLimit = 100500,
                },
            },
            {
                freshConnectionId,
                TConnectionState{
                    .UpdateEpoch = 100,
                    .Offer = {{TSystemTimestamp(10), 800}, {TSystemTimestamp(9), 400}},
                },
            },
        },
        .RecalculateCounter = 42,
        .Epoch = 101,
        .LimitUsageState = MakeLimitState(1000),
    };
    TInputBuffer::RecalculateStreamLimits(streamState, /*collectStaleConnections*/ false);
    ASSERT_TRUE(streamState.ConnectionStates.contains(staleConnectionId));
    EXPECT_EQ(200, streamState.ConnectionStates.at(staleConnectionId).InflatedByteLimit);
    EXPECT_EQ(400, streamState.ConnectionStates.at(freshConnectionId).InflatedByteLimit);
    EXPECT_EQ(101, streamState.Epoch);
    EXPECT_EQ(42, streamState.RecalculateCounter);
}

TEST(TInputBufferGetPendingSizeTest, Simple)
{
    auto connectionId1 = TGuid::Create();
    auto connectionId2 = TGuid::Create();
    const TStreamState streamState = {
        .ConnectionStates = {
            {
                connectionId1,
                TConnectionState{
                    .Offer = {{TSystemTimestamp(10), 800}, {TSystemTimestamp(9), 400}},
                },
            },
            {
                connectionId2,
                TConnectionState{
                    .Offer = {{TSystemTimestamp(12), 100}, {TSystemTimestamp(5), 200}},
                },
            },
        },
    };

    EXPECT_EQ(TInputBuffer::GetPendingSize(streamState), 1500);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TInputBufferMessagesPriorityQueueTest, ExtractionOrder)
{
    TInputBuffer::TMessagesPriorityQueue queue;
    std::vector<std::pair<ui64, ui64>> keys = {{2, 4}, {1, 1}, {1, 5}, {3, 0}, {1, 3}, {2, 2}};
    for (auto [timestamp, seqNo] : keys) {
        queue.push({.AlignmentTimestamp = TSystemTimestamp(timestamp), .SeqNo = seqNo});
    }
    EXPECT_EQ(queue.size(), keys.size());

    std::sort(keys.begin(), keys.end());
    for (auto [timestamp, seqNo] : keys) {
        EXPECT_EQ(queue.front().SeqNo, seqNo);
        auto extracted = queue.extract_front();
        EXPECT_EQ(extracted.AlignmentTimestamp, TSystemTimestamp(timestamp));
        EXPECT_EQ(extracted.SeqNo, seqNo);
    }
    EXPECT_TRUE(queue.empty());
}

TEST(TInputBufferMessagesPriorityQueueTest, LargeRandomRoundTrip)
{
    TFastRng64 rng(42);
    TInputBuffer::TMessagesPriorityQueue queue;
    std::vector<std::pair<ui64, ui64>> keys;
    keys.reserve(5000);
    for (ui64 seqNo = 0; seqNo < 5000; ++seqNo) {
        keys.emplace_back(rng.GenRand() % 16, seqNo);
        queue.push({.AlignmentTimestamp = TSystemTimestamp(keys.back().first), .SeqNo = keys.back().second});
    }

    std::sort(keys.begin(), keys.end());
    for (auto [timestamp, seqNo] : keys) {
        auto extracted = queue.extract_front();
        ASSERT_EQ(extracted.AlignmentTimestamp, TSystemTimestamp(timestamp));
        ASSERT_EQ(extracted.SeqNo, seqNo);
    }
    EXPECT_TRUE(queue.empty());
}

TEST(TInputBufferOfferedRateTest, EstimatesProducerRateFromBacklogSpan)
{
    TStreamState streamState;
    streamState.LimitUsageState = New<TStreamLimitUsageState>(/*inflation*/ 0);

    // Two connections announce backlog: 30 MB produced over the event-time span
    // between the oldest (990) and newest (998) order timestamps = 8 seconds.
    auto& first = streamState.ConnectionStates[TGuid::Create()];
    first.Offer = {{TSystemTimestamp(995), 10'000'000}, {TSystemTimestamp(990), 10'000'000}};
    auto& second = streamState.ConnectionStates[TGuid::Create()];
    second.Offer = {{TSystemTimestamp(998), 10'000'000}};

    TInputBuffer::RecalculateStreamLimits(streamState, /*collectStaleConnections*/ true);
    EXPECT_DOUBLE_EQ(streamState.LimitUsageState->GetOfferedInflatedBytesPerSecond(), 30'000'000.0 / 8);

    // Empty backlog: no estimate.
    for (auto& [connectionId, connectionState] : streamState.ConnectionStates) {
        connectionState.Offer.clear();
    }
    TInputBuffer::RecalculateStreamLimits(streamState, /*collectStaleConnections*/ true);
    EXPECT_DOUBLE_EQ(streamState.LimitUsageState->GetOfferedInflatedBytesPerSecond(), 0.0);
}

TEST(TInputBufferOfferedRateTest, SingleTimestampBacklogIsCappedByAcceptedHistory)
{
    TStreamState streamState;
    streamState.LimitUsageState = New<TStreamLimitUsageState>(/*inflation*/ 0);

    // A 500 MB backlog announced within a single timestamp: with no history the
    // cold-start estimate claims the whole backlog per second.
    auto& connection = streamState.ConnectionStates[TGuid::Create()];
    connection.Offer = {{TSystemTimestamp(1000), 500'000'000}};
    TInputBuffer::RecalculateStreamLimits(streamState, /*collectStaleConnections*/ true);
    EXPECT_DOUBLE_EQ(streamState.LimitUsageState->GetOfferedInflatedBytesPerSecond(), 500'000'000.0);

    // With a steady 10 MB/s accepted history the same announcement is capped by
    // a multiple of the historical rate instead.
    for (i64 ts = 700; ts <= 1000; ++ts) {
        streamState.OfferedRateEstimator->RecordAccepted(10'000'000, ts);
    }
    TInputBuffer::RecalculateStreamLimits(streamState, /*collectStaleConnections*/ true);
    const double capped = streamState.LimitUsageState->GetOfferedInflatedBytesPerSecond();
    EXPECT_GE(capped, 10'000'000.0);
    EXPECT_LE(capped, 100'000'000.0);
}

// Drives the REAL drain-cycle sampling path (add → extract → inter-extraction
// interval) with an injected clock, instead of feeding RecordCycle by hand.
TEST(TInputBufferEpochCycleTest, SamplesInterExtractionIntervals)
{
    const TStreamId streamId("input");
    auto computationSpec = New<TComputationSpec>();
    computationSpec->InputStreamIds.insert(streamId);
    auto dynamicSpec = New<TDynamicComputationSpec>();
    dynamicSpec->BatchDuration = TDuration::Zero();
    auto tracker = New<TEpochCycleTracker>();
    auto now = std::make_shared<TInstant>(TInstant::Seconds(1000));

    auto buffer = New<TInputBuffer>(
        TJobId(TGuid::Create()),
        NFlow::TStreamLimitUsageStateMap{{streamId, New<TStreamLimitUsageState>(/*inflationPerMessage*/ 0)}},
        tracker,
        THashMap<TStreamId, NFlow::TOfferedRateEstimatorPtr>{},
        computationSpec,
        TComputationId("computation"),
        dynamicSpec,
        GetSyncInvoker(),
        NProfiling::TProfiler{},
        [now] {
            return *now;
        });
    buffer->UpdateMessageTransferingInfo(New<TMessageTransferingInfo>());

    auto connectionId = TGuid::Create();
    auto schema = New<NTableClient::TTableSchema>();
    int sequence = 0;
    auto extractOne = [&] {
        ++sequence;
        TMessageBuilder builder(streamId, schema);
        builder.SetMessageId(TMessageId(Format("msg-%v", sequence)));
        builder.SetSystemTimestamp(TSystemTimestamp(100 + sequence));
        builder.SetAlignmentTimestamp(TSystemTimestamp(100 + sequence));
        builder.SetEventTimestamp(TSystemTimestamp(100 + sequence));
        auto message = New<TInputMessage>(builder.Finish(), MakeKey(ToString(sequence)));
        // The sync invoker completes both futures before the calls return.
        Y_UNUSED(buffer->AddMessages(connectionId, {std::move(message)}, /*onProcessed*/ {}));
        auto batchOrError = buffer->GetInputBatch({streamId}).TryGet();
        ASSERT_TRUE(batchOrError.has_value());
        EXPECT_EQ(std::ssize(batchOrError->ValueOrThrow()), 1);
    };

    extractOne();
    for (int i = 0; i < 4; ++i) {
        *now += TDuration::Seconds(5);
        extractOne();
    }

    auto medianCycle = tracker->GetMedianCycle();
    ASSERT_TRUE(medianCycle.has_value());
    EXPECT_EQ(*medianCycle, TDuration::Seconds(5));
}

TEST(TInputBufferInflightMetricsTest, ExcludesExtractedAndDeduplicatedMessages)
{
    const TStreamId streamId("input");
    auto computationSpec = New<TComputationSpec>();
    computationSpec->InputStreamIds.insert(streamId);
    auto dynamicSpec = New<TDynamicComputationSpec>();
    dynamicSpec->BatchDuration = TDuration::Zero();
    auto now = std::make_shared<TInstant>(TInstant::Seconds(1000));

    auto buffer = New<TInputBuffer>(
        TJobId(TGuid::Create()),
        NFlow::TStreamLimitUsageStateMap{{streamId, New<TStreamLimitUsageState>(/*inflationPerMessage*/ 0)}},
        /*epochCycleTracker*/ nullptr,
        THashMap<TStreamId, NFlow::TOfferedRateEstimatorPtr>{},
        computationSpec,
        TComputationId("computation"),
        dynamicSpec,
        GetSyncInvoker(),
        NProfiling::TProfiler{},
        [now] {
            return *now;
        });
    buffer->UpdateMessageTransferingInfo(New<TMessageTransferingInfo>());

    const auto connectionId = TGuid::Create();
    buffer->AddConnectionOffer(connectionId, {{streamId, {
                    {TSystemTimestamp(1020), 1'000},
                    {TSystemTimestamp(1010), 1'000},
                                                         }}});

    auto schema = New<NTableClient::TTableSchema>();
    auto acknowledged = std::make_shared<THashSet<TMessageId>>();
    auto addMessage = [&] (int index) {
        TMessageBuilder builder(streamId, schema);
        auto messageId = TMessageId(Format("msg-%v", index));
        builder.SetMessageId(messageId);
        builder.SetSystemTimestamp(TSystemTimestamp(100 + index));
        builder.SetAlignmentTimestamp(TSystemTimestamp(100 + index));
        builder.SetEventTimestamp(TSystemTimestamp(100 + index));
        auto message = New<TInputMessage>(builder.Finish(), MakeKey(ToString(index)));
        auto onProcessed = BIND([acknowledged] (TJobId, std::vector<TMessageId> messageIds) {
            acknowledged->insert(messageIds.begin(), messageIds.end());
        });
        NConcurrency::WaitFor(buffer->AddMessages(connectionId, {std::move(message)}, std::move(onProcessed)))
            .ThrowOnError();
        return messageId;
    };

    auto firstId = addMessage(1);
    auto metrics = NConcurrency::WaitFor(buffer->GetInflightMetrics()).ValueOrThrow().at(streamId);
    EXPECT_EQ(metrics->Count, 1);
    ASSERT_TRUE(metrics->ByteSize);
    EXPECT_GT(*metrics->ByteSize, 0);
    ASSERT_EQ(metrics->ReadyCount, 1);
    EXPECT_FALSE(metrics->OfferedCountPerSec);
    EXPECT_FALSE(metrics->ProcessedCountPerSec);

    NConcurrency::WaitFor(buffer->GetInputBatch({streamId})).ThrowOnError();
    metrics = NConcurrency::WaitFor(buffer->GetInflightMetrics()).ValueOrThrow().at(streamId);
    EXPECT_EQ(metrics->Count, 1);
    ASSERT_EQ(metrics->ReadyCount, 0);

    buffer->MarkDeduplicated({firstId});

    metrics = NConcurrency::WaitFor(buffer->GetInflightMetrics()).ValueOrThrow().at(streamId);
    EXPECT_EQ(metrics->Count, 0);
    EXPECT_EQ(metrics->ByteSize, 0);
    ASSERT_EQ(metrics->ReadyCount, 0);
    EXPECT_FALSE(metrics->ProcessedCountPerSec);
    EXPECT_TRUE(acknowledged->contains(firstId));

    auto secondId = addMessage(2);
    NConcurrency::WaitFor(buffer->GetInputBatch({streamId})).ThrowOnError();
    *now += TDuration::Minutes(5);
    buffer->MarkPersisted({secondId});

    metrics = NConcurrency::WaitFor(buffer->GetInflightMetrics()).ValueOrThrow().at(streamId);
    EXPECT_EQ(metrics->Count, 0);
    EXPECT_EQ(metrics->ByteSize, 0);
    ASSERT_EQ(metrics->ReadyCount, 0);
    ASSERT_TRUE(metrics->ProcessedCountPerSec);
    EXPECT_GT(*metrics->ProcessedCountPerSec, 0);
    EXPECT_TRUE(acknowledged->contains(secondId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NWorker
