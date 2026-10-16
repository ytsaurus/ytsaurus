#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/kafka/read_session.h>

#include <limits>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 UnlimitedRows = 1'000;
constexpr i64 UnlimitedBytes = 10ll * 1024 * 1024;

TKafkaMessage MakeMessage(i64 offset, std::string value = "payload", std::optional<std::string> key = std::nullopt)
{
    TKafkaMessage message;
    message.Offset = offset;
    message.TimestampSeconds = 1'700'000'000;
    message.WriteTimestamp = TSystemTimestamp(message.TimestampSeconds);
    message.CreateTimestamp = TSystemTimestamp(message.TimestampSeconds);
    message.Key = std::move(key);
    message.Value = std::move(value);
    return message;
}

std::vector<TKafkaMessage> MakeRange(i64 beginOffset, i64 endOffset)
{
    std::vector<TKafkaMessage> messages;
    for (i64 offset = beginOffset; offset < endOffset; ++offset) {
        messages.push_back(MakeMessage(offset));
    }
    return messages;
}

std::vector<i64> OffsetsOf(const std::vector<TKafkaMessage>& messages)
{
    std::vector<i64> offsets;
    offsets.reserve(messages.size());
    for (const auto& message : messages) {
        offsets.push_back(message.Offset);
    }
    return offsets;
}

//! Drives the buffer through the poll loop's first iteration: apply the initial assign, then buffer.
void PollInto(TKafkaReadBuffer& buffer, std::vector<TKafkaMessage> messages)
{
    if (auto reassign = buffer.PeekReassign()) {
        buffer.ConfirmReassign(*reassign);
    }
    buffer.Push(std::move(messages));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKafkaReadBufferTest, AssignsToTheInitialOffsetBeforeBuffering)
{
    TKafkaReadBuffer buffer;
    buffer.Start(42);

    // The poll loop must be told to assign before it may buffer anything, and the seek stays pending
    // until the assign is confirmed.
    auto reassign = buffer.PeekReassign();
    ASSERT_TRUE(reassign.has_value());
    EXPECT_EQ(*reassign, 42);
    ASSERT_TRUE(buffer.PeekReassign().has_value());

    buffer.ConfirmReassign(42);
    EXPECT_FALSE(buffer.PeekReassign().has_value());
}

TEST(TKafkaReadBufferTest, AsksForNoAssignUntilStarted)
{
    // The consumer group must be consulted before anything is fetched.
    TKafkaReadBuffer buffer;

    EXPECT_FALSE(buffer.IsStarted());
    EXPECT_FALSE(buffer.PeekReassign().has_value());
    EXPECT_TRUE(buffer.GetBatch(0, std::nullopt, UnlimitedRows, UnlimitedBytes).empty());
    buffer.Push(MakeRange(0, 5));
    EXPECT_EQ(buffer.GetBufferBytes(), 0);

    // An offset asked for while unstarted must not become the seek target.
    buffer.Start(500);

    EXPECT_TRUE(buffer.IsStarted());
    auto reassign = buffer.PeekReassign();
    ASSERT_TRUE(reassign.has_value());
    EXPECT_EQ(*reassign, 500);
}

TEST(TKafkaReadBufferTest, ServesFromTheResolvedStartOffset)
{
    TKafkaReadBuffer buffer;
    buffer.Start(500);
    PollInto(buffer, MakeRange(500, 503));

    EXPECT_EQ(
        OffsetsOf(buffer.GetBatch(500, std::nullopt, UnlimitedRows, UnlimitedBytes)),
        (std::vector<i64>{500, 501, 502}));
}

TEST(TKafkaReadBufferTest, DropsMessagesPolledBeforeTheInitialAssign)
{
    TKafkaReadBuffer buffer;
    buffer.Start(100);

    buffer.Push(MakeRange(100, 110));

    EXPECT_EQ(buffer.GetBufferBytes(), 0);
    EXPECT_TRUE(buffer.GetBatch(100, std::nullopt, UnlimitedRows, UnlimitedBytes).empty());
}

TEST(TKafkaReadBufferTest, ServesBufferedMessagesInOrder)
{
    TKafkaReadBuffer buffer;
    buffer.Start(100);
    PollInto(buffer, MakeRange(100, 105));

    auto batch = buffer.GetBatch(100, std::nullopt, UnlimitedRows, UnlimitedBytes);

    EXPECT_EQ(OffsetsOf(batch), (std::vector<i64>{100, 101, 102, 103, 104}));
    EXPECT_EQ(buffer.GetBufferBytes(), 0);
}

TEST(TKafkaReadBufferTest, ContinuesFromTheLastServedOffset)
{
    TKafkaReadBuffer buffer;
    buffer.Start(100);
    PollInto(buffer, MakeRange(100, 105));

    EXPECT_EQ(std::ssize(buffer.GetBatch(100, std::nullopt, /*maxRows*/ 2, UnlimitedBytes)), 2);

    // 102 is the continuation, so it is served rather than treated as a seek.
    auto batch = buffer.GetBatch(102, std::nullopt, UnlimitedRows, UnlimitedBytes);
    EXPECT_EQ(OffsetsOf(batch), (std::vector<i64>{102, 103, 104}));
}

TEST(TKafkaReadBufferTest, HonorsRowAndByteLimits)
{
    TKafkaReadBuffer buffer;
    buffer.Start(0);
    PollInto(buffer, MakeRange(0, 10));

    EXPECT_EQ(std::ssize(buffer.GetBatch(0, std::nullopt, /*maxRows*/ 3, UnlimitedBytes)), 3);

    // The byte limit is checked before each message, so a batch stops at the first message that
    // crosses it and always carries at least one.
    auto batch = buffer.GetBatch(3, std::nullopt, UnlimitedRows, /*maxBytes*/ 1);
    EXPECT_EQ(std::ssize(batch), 1);
    EXPECT_EQ(batch[0].Offset, 3);
}

TEST(TKafkaReadBufferTest, StopsAtTheExclusiveOffsetLimit)
{
    TKafkaReadBuffer buffer;
    buffer.Start(100);
    PollInto(buffer, MakeRange(100, 110));

    auto batch = buffer.GetBatch(100, /*offsetLimitExclusive*/ 103, UnlimitedRows, UnlimitedBytes);

    EXPECT_EQ(OffsetsOf(batch), (std::vector<i64>{100, 101, 102}));
    // The withheld messages stay buffered for the next call.
    EXPECT_GT(buffer.GetBufferBytes(), 0);
    EXPECT_EQ(OffsetsOf(buffer.GetBatch(103, std::nullopt, UnlimitedRows, UnlimitedBytes)).front(), 103);
}

TEST(TKafkaReadBufferTest, ServesSparseOffsets)
{
    TKafkaReadBuffer buffer;
    buffer.Start(100);
    // A compacted topic delivers gaps; they must not be mistaken for a seek.
    PollInto(buffer, std::vector{MakeMessage(100), MakeMessage(105), MakeMessage(110)});

    auto batch = buffer.GetBatch(100, std::nullopt, UnlimitedRows, UnlimitedBytes);

    EXPECT_EQ(OffsetsOf(batch), (std::vector<i64>{100, 105, 110}));
    EXPECT_TRUE(buffer.GetBatch(111, std::nullopt, UnlimitedRows, UnlimitedBytes).empty());
    EXPECT_FALSE(buffer.PeekReassign().has_value());
}

TEST(TKafkaReadBufferTest, DropsMessagesBelowTheFetchCursor)
{
    TKafkaReadBuffer buffer;
    buffer.Start(100);
    PollInto(buffer, MakeRange(100, 105));

    // A redelivery of what is already buffered must not duplicate rows.
    buffer.Push(MakeRange(100, 105));

    auto batch = buffer.GetBatch(100, std::nullopt, UnlimitedRows, UnlimitedBytes);
    EXPECT_EQ(OffsetsOf(batch), (std::vector<i64>{100, 101, 102, 103, 104}));
}

TEST(TKafkaReadBufferTest, FetchCursorFollowsPushesAndSeeks)
{
    TKafkaReadBuffer buffer;
    buffer.Start(5);
    EXPECT_EQ(buffer.GetFetchCursor(), 5);

    buffer.ConfirmReassign(5);
    buffer.Push(MakeRange(5, 8));
    EXPECT_EQ(buffer.GetFetchCursor(), 8);

    // Serving does not move the fetch cursor; a seek does, and it stays put until the reassign lands.
    (void)buffer.GetBatch(5, std::nullopt, UnlimitedRows, UnlimitedBytes);
    EXPECT_EQ(buffer.GetFetchCursor(), 8);
    (void)buffer.GetBatch(20, std::nullopt, UnlimitedRows, UnlimitedBytes);
    EXPECT_EQ(buffer.GetFetchCursor(), 20);
    buffer.Push(MakeRange(8, 12));
    EXPECT_EQ(buffer.GetFetchCursor(), 20);
}

TEST(TKafkaReadBufferTest, RequestsASeekOnAnUnexpectedOffset)
{
    TKafkaReadBuffer buffer;
    buffer.Start(100);
    PollInto(buffer, MakeRange(100, 110));

    EXPECT_TRUE(buffer.GetBatch(50, std::nullopt, UnlimitedRows, UnlimitedBytes).empty());
    EXPECT_EQ(buffer.GetBufferBytes(), 0);
    auto reassign = buffer.PeekReassign();
    ASSERT_TRUE(reassign.has_value());
    EXPECT_EQ(*reassign, 50);
    buffer.ConfirmReassign(50);

    buffer.Push(MakeRange(50, 55));
    EXPECT_EQ(OffsetsOf(buffer.GetBatch(50, std::nullopt, UnlimitedRows, UnlimitedBytes)),
        (std::vector<i64>{50, 51, 52, 53, 54}));
}

TEST(TKafkaReadBufferTest, DiscardsMessagesPolledBeforeAPendingSeek)
{
    TKafkaReadBuffer buffer;
    buffer.Start(100);
    PollInto(buffer, MakeRange(100, 110));
    EXPECT_EQ(std::ssize(buffer.GetBatch(100, std::nullopt, UnlimitedRows, UnlimitedBytes)), 10);

    // The source rewinds to 50 while the poll thread is blocked in poll_batch at the old position.
    EXPECT_TRUE(buffer.GetBatch(50, std::nullopt, UnlimitedRows, UnlimitedBytes).empty());

    // That in-flight poll now lands. Its messages come from before the seek and must not be buffered:
    // serving them would jump the base from the offset 50 it asked for straight to 110, silently
    // skipping 50..109.
    buffer.Push(MakeRange(110, 120));

    EXPECT_TRUE(buffer.GetBatch(50, std::nullopt, UnlimitedRows, UnlimitedBytes).empty());
    EXPECT_EQ(buffer.GetBufferBytes(), 0);

    // Only after the seek is applied does the buffer accept data again.
    auto reassign = buffer.PeekReassign();
    ASSERT_TRUE(reassign.has_value());
    EXPECT_EQ(*reassign, 50);
    buffer.ConfirmReassign(50);
    buffer.Push(MakeRange(50, 53));
    EXPECT_EQ(OffsetsOf(buffer.GetBatch(50, std::nullopt, UnlimitedRows, UnlimitedBytes)),
        (std::vector<i64>{50, 51, 52}));
}

TEST(TKafkaReadBufferTest, SeeksForwardPastBufferedMessages)
{
    TKafkaReadBuffer buffer;
    buffer.Start(100);
    PollInto(buffer, MakeRange(100, 110));

    // Retention trimmed the partition and the base now asks for a higher offset.
    EXPECT_TRUE(buffer.GetBatch(200, std::nullopt, UnlimitedRows, UnlimitedBytes).empty());
    auto reassign = buffer.PeekReassign();
    ASSERT_TRUE(reassign.has_value());
    EXPECT_EQ(*reassign, 200);
    buffer.ConfirmReassign(200);

    buffer.Push(MakeRange(200, 202));
    EXPECT_EQ(OffsetsOf(buffer.GetBatch(200, std::nullopt, UnlimitedRows, UnlimitedBytes)),
        (std::vector<i64>{200, 201}));
}

TEST(TKafkaReadBufferTest, TracksBufferedBytes)
{
    constexpr i64 overhead = static_cast<i64>(sizeof(TKafkaMessage));

    TKafkaReadBuffer buffer;
    buffer.Start(0);
    buffer.ConfirmReassign(0);

    // Key bytes count too — a compacted topic full of key-heavy tombstones must still trip the
    // byte-based limits.
    buffer.Push(std::vector{MakeMessage(0, "aaaa", "kk"), MakeMessage(1, "bb")});
    EXPECT_EQ(buffer.GetBufferBytes(), 8 + 2 * overhead);

    EXPECT_EQ(std::ssize(buffer.GetBatch(0, std::nullopt, /*maxRows*/ 1, UnlimitedBytes)), 1);
    EXPECT_EQ(buffer.GetBufferBytes(), 2 + overhead);

    EXPECT_EQ(std::ssize(buffer.GetBatch(1, std::nullopt, UnlimitedRows, UnlimitedBytes)), 1);
    EXPECT_EQ(buffer.GetBufferBytes(), 0);
}

TEST(TKafkaReadBufferTest, KeyBytesBindTheBatchLimit)
{
    TKafkaReadBuffer buffer;
    buffer.Start(0);
    buffer.ConfirmReassign(0);

    // Tombstones: empty values, non-trivial keys.
    buffer.Push(std::vector{MakeMessage(0, "", "key-0"), MakeMessage(1, "", "key-1")});

    // Were keys not counted, both messages would weigh zero and the byte cap could never bind.
    auto batch = buffer.GetBatch(0, std::nullopt, UnlimitedRows, /*maxBytes*/ 1);
    EXPECT_EQ(std::ssize(batch), 1);
}

TEST(TKafkaReadBufferTest, EmptyRecordsStillFillTheBuffer)
{
    TKafkaReadBuffer buffer;
    buffer.Start(0);
    buffer.ConfirmReassign(0);

    // A valid record can carry no payload and no key at all; the fixed per-record overhead must keep
    // the byte-based backpressure meaningful, or a stream of such records grows the buffer unbounded.
    std::vector<TKafkaMessage> empty;
    for (i64 offset = 0; offset < 10; ++offset) {
        auto message = MakeMessage(offset, "");
        message.Value = std::nullopt;
        empty.push_back(std::move(message));
    }
    buffer.Push(std::move(empty));

    EXPECT_EQ(buffer.GetBufferBytes(), 10 * static_cast<i64>(sizeof(TKafkaMessage)));
}

TEST(TKafkaReadBufferTest, RetriesTheSeekWhenAssignFails)
{
    TKafkaReadBuffer buffer;
    buffer.Start(100);
    PollInto(buffer, MakeRange(100, 105));
    EXPECT_EQ(std::ssize(buffer.GetBatch(100, std::nullopt, UnlimitedRows, UnlimitedBytes)), 5);

    // Rewind to 50. The poll thread peeks the seek, but consumer->assign throws — no confirm.
    EXPECT_TRUE(buffer.GetBatch(50, std::nullopt, UnlimitedRows, UnlimitedBytes).empty());
    auto reassign = buffer.PeekReassign();
    ASSERT_TRUE(reassign.has_value());
    EXPECT_EQ(*reassign, 50);

    // The consumer is still fetching from the old position; nothing from there may be buffered
    // (offsets 105.. are >= the new fetch cursor and would be served as a bogus continuation).
    buffer.Push(MakeRange(105, 110));
    EXPECT_TRUE(buffer.GetBatch(50, std::nullopt, UnlimitedRows, UnlimitedBytes).empty());
    EXPECT_EQ(buffer.GetBufferBytes(), 0);

    // The seek stayed pending, so the next iteration retries the assign; then reading resumes.
    reassign = buffer.PeekReassign();
    ASSERT_TRUE(reassign.has_value());
    EXPECT_EQ(*reassign, 50);
    buffer.ConfirmReassign(50);
    buffer.Push(MakeRange(50, 53));
    EXPECT_EQ(OffsetsOf(buffer.GetBatch(50, std::nullopt, UnlimitedRows, UnlimitedBytes)),
        (std::vector<i64>{50, 51, 52}));
}

TEST(TKafkaReadBufferTest, PrefersANewerSeekOverAStaleConfirm)
{
    TKafkaReadBuffer buffer;
    buffer.Start(100);
    PollInto(buffer, MakeRange(100, 105));
    EXPECT_EQ(std::ssize(buffer.GetBatch(100, std::nullopt, UnlimitedRows, UnlimitedBytes)), 5);

    // Seek to 50; while assign(50) is in flight the source seeks again, to 70.
    EXPECT_TRUE(buffer.GetBatch(50, std::nullopt, UnlimitedRows, UnlimitedBytes).empty());
    auto stale = buffer.PeekReassign();
    ASSERT_TRUE(stale.has_value());
    EXPECT_TRUE(buffer.GetBatch(70, std::nullopt, UnlimitedRows, UnlimitedBytes).empty());

    // The stale confirm must not mark the newer seek applied.
    buffer.ConfirmReassign(*stale);
    auto reassign = buffer.PeekReassign();
    ASSERT_TRUE(reassign.has_value());
    EXPECT_EQ(*reassign, 70);

    // Data fetched at the stale position is dropped until the newer seek is applied.
    buffer.Push(MakeRange(50, 55));
    EXPECT_TRUE(buffer.GetBatch(70, std::nullopt, UnlimitedRows, UnlimitedBytes).empty());

    buffer.ConfirmReassign(70);
    buffer.Push(MakeRange(70, 72));
    EXPECT_EQ(OffsetsOf(buffer.GetBatch(70, std::nullopt, UnlimitedRows, UnlimitedBytes)),
        (std::vector<i64>{70, 71}));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKafkaBackpressureTest, PausesAtTheCapAndResumesAtHalf)
{
    EXPECT_FALSE(ShouldPauseKafkaFetch(63, 64));
    EXPECT_TRUE(ShouldPauseKafkaFetch(64, 64));
    EXPECT_TRUE(ShouldPauseKafkaFetch(100, 64));

    EXPECT_TRUE(ShouldResumeKafkaFetch(0, 64));
    EXPECT_TRUE(ShouldResumeKafkaFetch(32, 64));
    EXPECT_FALSE(ShouldResumeKafkaFetch(33, 64));
}

TEST(TKafkaBackpressureTest, DerivedQueueCapStaysInLibrdkafkaRange)
{
    // librdkafka accepts queued.max.messages.kbytes up to INT_MAX/1024 = 2097151; a larger derived
    // value makes configuration fail and leaves the partition without a consumer at all.
    EXPECT_EQ(DeriveKafkaQueuedMaxKbytes(64ll * 1024 * 1024), 32ll * 1024);
    EXPECT_EQ(DeriveKafkaQueuedMaxKbytes(1), 1);
    EXPECT_EQ(DeriveKafkaQueuedMaxKbytes(4ll * 1024 * 1024 * 1024), 2'097'151);
    EXPECT_EQ(DeriveKafkaQueuedMaxKbytes(std::numeric_limits<i64>::max()), 2'097'151);
}

TEST(TKafkaBackpressureTest, TheMinimalCapCanStillResume)
{
    // max_buffer_bytes = 1: every record pauses (per-record overhead exceeds the cap), and half the
    // cap rounds to zero — an exclusive comparison could then never resume the drained partition.
    EXPECT_TRUE(ShouldPauseKafkaFetch(static_cast<i64>(sizeof(TKafkaMessage)), 1));
    EXPECT_TRUE(ShouldResumeKafkaFetch(0, 1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
