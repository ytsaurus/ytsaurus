#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/message_batcher.h>

namespace NYT::NFlow {
namespace {


////////////////////////////////////////////////////////////////////////////////

using TOrderedMessage = std::pair<TSystemTimestamp, TMessage>;

struct TOrderedMessageComparator
{
    bool operator()(const TOrderedMessage& left, const TOrderedMessage& right) const
    {
        return std::pair{left.first, left.second.MessageId} < std::pair{right.first, right.second.MessageId};
    }
};

using TQueue = std::set<TOrderedMessage, TOrderedMessageComparator>;

using TQueuePriority = std::pair<TSystemTimestamp, const TMessageId&>;

std::vector<std::pair<TQueue*, std::function<TQueuePriority()>>> MakeQueuePairs(std::vector<TQueue*> queues)
{
    std::vector<std::pair<TQueue*, std::function<TQueuePriority()>>> result;
    result.reserve(queues.size());
    for (auto queue : queues) {
        result.emplace_back(queue, [queue] () -> TQueuePriority {
            YT_ASSERT(!queue->empty());
            return {queue->begin()->first, queue->begin()->second.MessageId};
        });
    }
    return result;
}

TEST(TMessageBatcherTest, MergingExtractBatch)
{
    auto makeOrderedMessage = [] (ui64 timestamp, const std::string& id) {
        TMessage message;
        message.MessageId = TMessageId(id);
        return TOrderedMessage{TSystemTimestamp(timestamp), message};
    };

    TMessageBatchLimiter limiter(9, 1e5);
    auto messages1 = TQueue{
        makeOrderedMessage(5, "5"),
        makeOrderedMessage(7, "7.1"),
        makeOrderedMessage(7, "7.3"),
        makeOrderedMessage(8, "8"),
        makeOrderedMessage(10, "10"),
        makeOrderedMessage(12, "12"),
    };
    auto messages2 = TQueue{
        makeOrderedMessage(6, "6"),
        makeOrderedMessage(7, "7.2"),
        makeOrderedMessage(7, "7.4"),
        makeOrderedMessage(11, "11"),
        makeOrderedMessage(15, "15"),
    };
    auto messages3 = TQueue();

    std::vector<TMessage> result;
    MergingExtractBatch(
        MakeQueuePairs({&messages1, &messages2, &messages3}),
        [] (TOrderedMessage& message) -> TMessage& {
            return message.second;
        },
        limiter,
        [&] (TOrderedMessage& orderedMessage) {
            result.push_back(std::move(orderedMessage.second));
        });

    EXPECT_EQ(messages1.size(), 1u);
    EXPECT_EQ(messages2.size(), 1u);

    EXPECT_EQ(result.size(), 9u);

    std::vector<std::string> messageIds;
    for (const auto& message : result) {
        messageIds.emplace_back(message.MessageId.Underlying());
    }
    EXPECT_THAT(messageIds, testing::ElementsAre("5", "6", "7.1", "7.2", "7.3", "7.4", "8", "10", "11"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
