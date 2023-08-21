#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/python/common/tee_input_stream.h>

#include <util/stream/mem.h>

namespace NYT::NPython {
namespace {

////////////////////////////////////////////////////////////////////////////////

TString AsString(TBuffer buf)
{
    TString result;
    buf.AsString(result);
    return result;
}

TString Extract(TTeeInputStream& stream, size_t count)
{
    TBuffer buf(count);
    stream.ExtractFromBuffer(&buf, count);
    return AsString(std::move(buf));
}

TEST(TTeeInputStreamTest, Simple)
{
    TMemoryInput input("Hello world!");
    TTeeInputStream stream(&input);

    char c;
    EXPECT_TRUE(stream.ReadChar(c));
    EXPECT_EQ(c, 'H');
    EXPECT_EQ(stream.Size(), 1u);

    TBuffer buf(10);
    EXPECT_EQ(stream.Read(buf.Data(), 4), 4u);
    EXPECT_EQ(stream.Size(), 5u);

    TStringBuf read_str(buf.Data(), 4);
    EXPECT_EQ(read_str, "ello");

    EXPECT_EQ(Extract(stream, 4), "Hell");

    TString rest = stream.ReadAll();
    EXPECT_EQ(rest, " world!");

    EXPECT_EQ(Extract(stream, stream.Size()), "o world!");
}

TEST(TTeeInputStreamTest, Realloc)
{
    TMemoryInput input("Hello world!");
    TTeeInputStream stream(&input);

    TBuffer buf(10);
    EXPECT_EQ(stream.Read(buf.Data(), 5), 5u);
    EXPECT_EQ(TStringBuf(buf.Data(), 5), "Hello");
    EXPECT_EQ(stream.Size(), 5u);

    Extract(stream, 3);

    EXPECT_EQ(stream.Read(buf.Data(), 6), 6u);
    EXPECT_EQ(TStringBuf(buf.Data(), 6), " world");
    EXPECT_EQ(stream.Size(), 8u);
    EXPECT_EQ(Extract(stream, 4), "lo w");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NPython
