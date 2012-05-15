#include "stdafx.h"

#include <ytlib/table_client/schema.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NUnitTest {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NTableClient;

class TSchemaTest : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemaTest, RangeContains)
{
    {
        TRange range(""); // Infinite range
        EXPECT_IS_TRUE(range.Contains(""));
        EXPECT_IS_TRUE(range.Contains(Stroka('\0')));
        EXPECT_IS_TRUE(range.Contains(TRange("")));
        EXPECT_IS_TRUE(range.Contains("anything"));
    }

    {
        TRange range("", Stroka('\0'));
        EXPECT_IS_TRUE(range.Contains(""));
        EXPECT_IS_FALSE(range.Contains(Stroka('\0')));
        EXPECT_IS_FALSE(range.Contains(TRange("")));
        EXPECT_IS_FALSE(range.Contains("anything"));
    } 

    {
        TRange range("abc", "abe");
        EXPECT_IS_FALSE(range.Contains(""));
        EXPECT_IS_TRUE(range.Contains("abcjkdhfsdhf"));
        EXPECT_IS_TRUE(range.Contains("abd"));

        EXPECT_IS_FALSE(range.Contains(TRange("")));
        EXPECT_IS_TRUE(range.Contains(TRange("abc", "abd")));
        EXPECT_IS_TRUE(range.Contains(TRange("abc", "abe")));
    } 
}

TEST_F(TSchemaTest, RangeOverlaps)
{
    {
        TRange range("a", "b");
        EXPECT_IS_FALSE(range.Overlaps(TRange("b", "c")));
        EXPECT_IS_TRUE(range.Overlaps(TRange("anything", "c")));
    }

    {
        TRange range("");
        EXPECT_IS_TRUE(range.Overlaps(TRange("")));
        EXPECT_IS_TRUE(range.Overlaps(TRange("", Stroka('\0'))));
        EXPECT_IS_TRUE(range.Overlaps(TRange("anything", "c")));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemaTest, ChannelContains)
{
    TChannel ch1 = TChannel::CreateEmpty();
    ch1.AddColumn("anything");
    EXPECT_IS_TRUE(ch1.Contains("anything"));
    EXPECT_IS_FALSE(ch1.Contains(TRange("anything")));

    {
        TChannel ch2 = TChannel::CreateEmpty();
        ch2.AddColumn("anything");
        EXPECT_IS_TRUE(ch1.Contains(ch2));
        EXPECT_IS_TRUE(ch2.Contains(ch1));
    }

    ch1.AddRange(TRange("m", "p"));

    {
        TChannel ch2 = TChannel::CreateEmpty();
        ch2.AddColumn("anything");
        EXPECT_IS_TRUE(ch1.Contains(ch2));
        EXPECT_IS_FALSE(ch2.Contains(ch1));

        ch2.AddRange(TRange("m"));
        EXPECT_IS_FALSE(ch1.Contains(ch2));
        EXPECT_IS_TRUE(ch2.Contains(ch1));
    }
}

TEST_F(TSchemaTest, ChannelOverlaps)
{
    TChannel ch1 = TChannel::CreateEmpty();
    ch1.AddRange(TRange("a", "c"));

    {
        TChannel ch2 = TChannel::CreateEmpty();
        ch2.AddColumn("anything");
        EXPECT_IS_TRUE(ch1.Overlaps(ch2));
        EXPECT_IS_TRUE(ch2.Overlaps(ch1));
    }

    {
        EXPECT_IS_TRUE(TRange("a", "c").Overlaps(TRange("b", "d")));
        TChannel ch2 = TChannel::CreateEmpty();
        ch2.AddRange(TRange("b", "d"));
        EXPECT_IS_TRUE(ch1.Overlaps(ch2));
        EXPECT_IS_TRUE(ch2.Overlaps(ch1));
    }

    {
        TChannel ch2 = TChannel::CreateEmpty();
        ch2.AddRange(TRange(""));
        EXPECT_IS_TRUE(ch1.Overlaps(ch2));
        EXPECT_IS_TRUE(ch2.Overlaps(ch1));
    }

    {
        TChannel ch2 = TChannel::CreateEmpty();
        ch2.AddRange(TRange("c", "d"));
        EXPECT_IS_FALSE(ch1.Overlaps(ch2));
        EXPECT_IS_FALSE(ch2.Overlaps(ch1));
    }

    ch1.AddColumn("Hello!");

    {
        TChannel ch2 = TChannel::CreateEmpty();
        ch2.AddRange(TRange("c", "d"));
        ch2.AddColumn("Hello!");
        EXPECT_IS_TRUE(ch1.Overlaps(ch2));
        EXPECT_IS_TRUE(ch2.Overlaps(ch1));
    }
}

TEST_F(TSchemaTest, ChannelSubtract)
{
    {
        TChannel 
            ch1 = TChannel::CreateEmpty(), 
            ch2 = TChannel::CreateEmpty(), 
            res = TChannel::CreateEmpty();

        ch1.AddRange(TRange("a", "c"));
        ch1.AddColumn("something");

        ch2.AddColumn("something");
        ch1 -= ch2;

        EXPECT_IS_FALSE(ch1.Contains(ch2));

        res.AddRange(TRange("a", "c"));
        EXPECT_IS_TRUE(ch1.Contains(res));
        EXPECT_IS_TRUE(res.Contains(ch1));
    }

    {
        TChannel 
            ch1 = TChannel::CreateEmpty(), 
            ch2 = TChannel::CreateEmpty(), 
            res = TChannel::CreateEmpty();

        ch1.AddRange(TRange("a", "c"));
        ch1.AddColumn("something");

        ch2.AddRange(TRange("a", "c"));
        ch1 -= ch2;

        EXPECT_IS_FALSE(ch1.Contains(ch2));

        res.AddColumn("something");
        EXPECT_IS_TRUE(ch1.Contains(res));
        EXPECT_IS_TRUE(res.Contains(ch1));
    }

    {
        TChannel 
            ch1 = TChannel::CreateEmpty(), 
            ch2 = TChannel::CreateEmpty(), 
            res = TChannel::CreateEmpty();

        ch1.AddRange(TRange("a", "c"));
        ch1.AddColumn("something");

        ch2.AddRange(TRange("b", "c"));
        ch1 -= ch2;

        EXPECT_IS_FALSE(ch1.Contains(ch2));

        res.AddColumn("something");
        res.AddRange(TRange("a", "b"));
        EXPECT_IS_TRUE(ch1.Contains(res));
        EXPECT_IS_TRUE(res.Contains(ch1));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NUnitTest
} // namespace NYT

