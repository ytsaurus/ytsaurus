#include "stdafx.h"
#include "framework.h"

#include <ytlib/chunk_client/schema.h>

namespace NYT {
namespace NChunkClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSchemaTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemaTest, RangeContains)
{
    {
        TColumnRange range(""); // Infinite range
        EXPECT_TRUE(range.Contains(""));
        EXPECT_TRUE(range.Contains(Stroka('\0')));
        EXPECT_TRUE(range.Contains(TColumnRange("")));
        EXPECT_TRUE(range.Contains("anything"));
    }

    {
        TColumnRange range("", Stroka('\0'));
        EXPECT_TRUE(range.Contains(""));
        EXPECT_FALSE(range.Contains(Stroka('\0')));
        EXPECT_FALSE(range.Contains(TColumnRange("")));
        EXPECT_FALSE(range.Contains("anything"));
    }

    {
        TColumnRange range("abc", "abe");
        EXPECT_FALSE(range.Contains(""));
        EXPECT_TRUE(range.Contains("abcjkdhfsdhf"));
        EXPECT_TRUE(range.Contains("abd"));

        EXPECT_FALSE(range.Contains(TColumnRange("")));
        EXPECT_TRUE(range.Contains(TColumnRange("abc", "abd")));
        EXPECT_TRUE(range.Contains(TColumnRange("abc", "abe")));
    }
}

TEST_F(TSchemaTest, RangeOverlaps)
{
    {
        TColumnRange range("a", "b");
        EXPECT_FALSE(range.Overlaps(TColumnRange("b", "c")));
        EXPECT_TRUE(range.Overlaps(TColumnRange("anything", "c")));
    }

    {
        TColumnRange range("");
        EXPECT_TRUE(range.Overlaps(TColumnRange("")));
        EXPECT_TRUE(range.Overlaps(TColumnRange("", Stroka('\0'))));
        EXPECT_TRUE(range.Overlaps(TColumnRange("anything", "c")));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemaTest, ChannelContains)
{
    auto ch1 = TChannel::Empty();
    ch1.AddColumn("anything");
    EXPECT_TRUE(ch1.Contains("anything"));
    EXPECT_FALSE(ch1.Contains(TColumnRange("anything")));

    {
        auto ch2 = TChannel::Empty();
        ch2.AddColumn("anything");
        EXPECT_TRUE(ch1.Contains(ch2));
        EXPECT_TRUE(ch2.Contains(ch1));
    }

    ch1.AddRange(TColumnRange("m", "p"));

    {
        auto ch2 = TChannel::Empty();
        ch2.AddColumn("anything");
        EXPECT_TRUE(ch1.Contains(ch2));
        EXPECT_FALSE(ch2.Contains(ch1));

        ch2.AddRange(TColumnRange("m"));
        EXPECT_FALSE(ch1.Contains(ch2));
        EXPECT_TRUE(ch2.Contains(ch1));
    }
}

TEST_F(TSchemaTest, ChannelOverlaps)
{
    auto ch1 = TChannel::Empty();
    ch1.AddRange(TColumnRange("a", "c"));

    {
        auto ch2 = TChannel::Empty();
        ch2.AddColumn("anything");
        EXPECT_TRUE(ch1.Overlaps(ch2));
        EXPECT_TRUE(ch2.Overlaps(ch1));
    }

    {
        EXPECT_TRUE(TColumnRange("a", "c").Overlaps(TColumnRange("b", "d")));
        auto ch2 = TChannel::Empty();
        ch2.AddRange(TColumnRange("b", "d"));
        EXPECT_TRUE(ch1.Overlaps(ch2));
        EXPECT_TRUE(ch2.Overlaps(ch1));
    }

    {
        auto ch2 = TChannel::Empty();
        ch2.AddRange(TColumnRange(""));
        EXPECT_TRUE(ch1.Overlaps(ch2));
        EXPECT_TRUE(ch2.Overlaps(ch1));
    }

    {
        auto ch2 = TChannel::Empty();
        ch2.AddRange(TColumnRange("c", "d"));
        EXPECT_FALSE(ch1.Overlaps(ch2));
        EXPECT_FALSE(ch2.Overlaps(ch1));
    }

    ch1.AddColumn("Hello!");

    {
        auto ch2 = TChannel::Empty();
        ch2.AddRange(TColumnRange("c", "d"));
        ch2.AddColumn("Hello!");
        EXPECT_TRUE(ch1.Overlaps(ch2));
        EXPECT_TRUE(ch2.Overlaps(ch1));
    }
}

TEST_F(TSchemaTest, ChannelSubtract)
{
    {
        TChannel
            ch1 = TChannel::Empty(),
            ch2 = TChannel::Empty(),
            res = TChannel::Empty();

        ch1.AddRange(TColumnRange("a", "c"));
        ch1.AddColumn("something");

        ch2.AddColumn("something");
        ch1 -= ch2;

        EXPECT_FALSE(ch1.Contains(ch2));

        res.AddRange(TColumnRange("a", "c"));
        EXPECT_TRUE(ch1.Contains(res));
        EXPECT_TRUE(res.Contains(ch1));
    }

    {
        TChannel
            ch1 = TChannel::Empty(),
            ch2 = TChannel::Empty(),
            res = TChannel::Empty();

        ch1.AddRange(TColumnRange("a", "c"));
        ch1.AddColumn("something");

        ch2.AddRange(TColumnRange("a", "c"));
        ch1 -= ch2;

        EXPECT_FALSE(ch1.Contains(ch2));

        res.AddColumn("something");
        EXPECT_TRUE(ch1.Contains(res));
        EXPECT_TRUE(res.Contains(ch1));
    }

    {
        TChannel
            ch1 = TChannel::Empty(),
            ch2 = TChannel::Empty(),
            res = TChannel::Empty();

        ch1.AddRange(TColumnRange("a", "c"));
        ch1.AddColumn("something");

        ch2.AddRange(TColumnRange("b", "c"));
        ch1 -= ch2;

        EXPECT_FALSE(ch1.Contains(ch2));

        res.AddColumn("something");
        res.AddRange(TColumnRange("a", "b"));
        EXPECT_TRUE(ch1.Contains(res));
        EXPECT_TRUE(res.Contains(ch1));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NChunkClient
} // namespace NYT
