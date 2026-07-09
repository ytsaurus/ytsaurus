#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TGenerateMessageIdTest, Basic)
{
    TUniqueSeqNo uniqueSeqNo(1862469525896877703);
    auto messageId = GenerateOrderedMessageId(uniqueSeqNo, "hit", LexicographicallySerialize(123l));
    auto expectedMessageId = TMessageId("19d8d22a8012d687-hit:17b");
    ASSERT_EQ(messageId.Underlying(), expectedMessageId.Underlying());
}

TEST(TGenerateMessageIdTest, WithSecondaryOffset)
{
    TUniqueSeqNo uniqueSeqNo(1862469525896877703);
    auto messageId = GenerateOrderedMessageId(
        uniqueSeqNo,
        "hit",
        LexicographicallySerialize(123l),
        LexicographicallySerialize(0l));
    auto expectedMessageId = TMessageId("19d8d22a8012d687-hit:17b:00");
    ASSERT_EQ(messageId.Underlying(), expectedMessageId.Underlying());
}

TEST(TGenerateMessageIdTest, FromSource)
{
    auto sourceMessageId = TMessageId("19d8d22a8012d687-event:02");
    auto messageId = GenerateInheritedMessageId(sourceMessageId, "hit", LexicographicallySerialize(123l));
    auto expectedMessageId = TMessageId("19d8d22a8012d687-event:02-hit:17b");
    ASSERT_EQ(messageId.Underlying(), expectedMessageId.Underlying());
}

TEST(TGenerateMessageIdTest, ZeroSeqNo)
{
    auto messageId = GenerateOrderedMessageId(TUniqueSeqNo(0), "s", "x");
    auto expectedMessageId = TMessageId("0000000000000000-s:x");
    ASSERT_EQ(messageId.Underlying(), expectedMessageId.Underlying());
}

TEST(TGenerateMessageIdTest, MaxSeqNo)
{
    auto messageId = GenerateOrderedMessageId(TUniqueSeqNo(std::numeric_limits<ui64>::max()), "s", "x");
    auto expectedMessageId = TMessageId("ffffffffffffffff-s:x");
    ASSERT_EQ(messageId.Underlying(), expectedMessageId.Underlying());
}

// Exercises the heap-fallback path: the assembled id is larger than the stack buffer threshold
// used by the implementation (~256 bytes), so the builder must allocate a std::string.
TEST(TGenerateMessageIdTest, LongStreamIdSpillsToHeap)
{
    const std::string longStreamId(400, 'x');
    auto messageId = GenerateOrderedMessageId(TUniqueSeqNo(1), TStreamId(longStreamId), "y");
    const auto expected = std::string("0000000000000001-") + longStreamId + ":y";
    ASSERT_EQ(messageId.Underlying(), expected);
}

TEST(TGenerateMessageIdTest, EmptyOffset)
{
    auto messageId = GenerateOrderedMessageId(TUniqueSeqNo(1), "s", TStringBuf());
    auto expectedMessageId = TMessageId("0000000000000001-s:");
    ASSERT_EQ(messageId.Underlying(), expectedMessageId.Underlying());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
