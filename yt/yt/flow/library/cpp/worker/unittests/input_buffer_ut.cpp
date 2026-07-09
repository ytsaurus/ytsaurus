#include <yt/yt/flow/library/cpp/worker/input_buffer_detail.h>

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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NWorker
