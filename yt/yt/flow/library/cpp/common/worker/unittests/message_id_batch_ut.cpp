#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/worker/message_id_batch.h>

namespace NYT::NFlow::NWorker {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TMessageIdBatchTest, RoundTrip)
{
    std::deque<TMessageId> messageIds{
        TMessageId("a"),
        TMessageId("longer-message-id-2"),
        TMessageId(""),
        TMessageId("last"),
    };
    std::deque<const TMessageId*> messageIdPtrs;
    for (const auto& messageId : messageIds) {
        messageIdPtrs.push_back(&messageId);
    }

    auto buffer = SerializeMessageIdBatch(messageIdPtrs);
    auto parsed = ParseMessageIdBatch(buffer);

    EXPECT_EQ(parsed, messageIds);
}

TEST(TMessageIdBatchTest, Empty)
{
    EXPECT_EQ(SerializeMessageIdBatch(std::deque<const TMessageId*>{}).Size(), 0u);
    EXPECT_TRUE(ParseMessageIdBatch(TRef()).empty());
}

TEST(TMessageIdBatchTest, UnsupportedVersionThrows)
{
    const char buffer[] = {0x7f}; // A single-byte varint version unknown to the parser.
    EXPECT_THROW(ParseMessageIdBatch(TRef(buffer, sizeof(buffer))), std::exception);
}

TEST(TMessageIdBatchTest, TruncatedThrows)
{
    std::deque<TMessageId> messageIds{TMessageId("longer-message-id")};
    std::deque<const TMessageId*> messageIdPtrs{&messageIds[0]};
    auto buffer = SerializeMessageIdBatch(messageIdPtrs);

    auto truncated = buffer.Slice(buffer.Begin(), buffer.Begin() + 2);
    EXPECT_THROW(ParseMessageIdBatch(truncated), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NWorker
