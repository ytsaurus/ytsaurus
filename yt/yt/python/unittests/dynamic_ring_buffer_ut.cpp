#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/python/common/dynamic_ring_buffer.h>

#include <library/cpp/yt/string/string.h>

#include <util/stream/mem.h>
#include <util/generic/vector.h>

namespace NYT::NPython {
namespace {

////////////////////////////////////////////////////////////////////////////////

TString AsString(TBuffer buf)
{
    TString result;
    buf.AsString(result);
    return result;
}

TString Extract(TDynamicRingBuffer& ringBuffer, size_t count)
{
    TBuffer buf(count);
    ringBuffer.Pop(&buf, count);
    return AsString(std::move(buf));
}

TEST(TDynamicRingBuffer, Simple)
{
    TDynamicRingBuffer buffer;

    buffer.Push("hello");
    EXPECT_EQ(buffer.Size(), 5u);

    EXPECT_EQ(Extract(buffer, 4), "hell");
    EXPECT_EQ(buffer.Size(), 1u);

    buffer.Push(", world");
    EXPECT_EQ(buffer.Size(), 8u);

    EXPECT_EQ(Extract(buffer, 4), "o, w");
    EXPECT_EQ(Extract(buffer, 4), "orld");
    EXPECT_TRUE(buffer.Empty());

    buffer.Push("HELLO, ");
    EXPECT_EQ(Extract(buffer, 5), "HELLO");
    buffer.Push("WORLD");
    EXPECT_EQ(Extract(buffer, 5), ", WOR");
    EXPECT_EQ(Extract(buffer, 2), "LD");

    EXPECT_TRUE(buffer.Empty());
}

TEST(TDynamicRingBuffer, Ring)
{
    TDynamicRingBuffer buffer;

    TVector<TString> alphabet;
    for (char c = 'a'; c <= 'z'; ++c) {
        alphabet.push_back(TString(1, c));
    }

    auto sub = Accumulate(alphabet.begin(), alphabet.begin() + 16, TString());
    buffer.Push(sub);

    size_t old_capacity = buffer.Capacity();

    for (int i = 0; i < 62; ++i) {
        EXPECT_EQ(Extract(buffer, 1), alphabet[i % alphabet.size()]);
        buffer.Push(alphabet[(i + 16) % alphabet.size()]);
    }
    EXPECT_EQ(buffer.Size(), 16u);
    EXPECT_EQ(buffer.Capacity(), old_capacity);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NPython
