#include <yt/yt/flow/examples/cpp/log_parser/lib/log_line_parser.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow::NExample {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TParseLogLineTest, DropsMalformed)
{
    EXPECT_TRUE(ParseLogLine("no separator").empty());
    EXPECT_TRUE(ParseLogLine("info:").empty());
    EXPECT_TRUE(ParseLogLine("").empty());
}

TEST(TParseLogLineTest, DropsUnknownLevel)
{
    EXPECT_TRUE(ParseLogLine("debug:noisy").empty());
}

TEST(TParseLogLineTest, ParsesSingleRecord)
{
    auto records = ParseLogLine("info:started");
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0], (TLogRecord{.Level = "info", .Text = "started"}));
}

TEST(TParseLogLineTest, SplitsMultiRecordLine)
{
    auto records = ParseLogLine("info:a;debug:skip;error:b");
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[0], (TLogRecord{.Level = "info", .Text = "a"}));
    EXPECT_EQ(records[1], (TLogRecord{.Level = "error", .Text = "b"}));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSeverityTest, RanksKnownLevels)
{
    EXPECT_EQ(SeverityRank("info"), 0);
    EXPECT_EQ(SeverityRank("warning"), 1);
    EXPECT_EQ(SeverityRank("error"), 2);
    EXPECT_EQ(SeverityRank("debug"), -1);
}

TEST(TSeverityTest, NamesInvertRanks)
{
    EXPECT_EQ(SeverityName(0), "info");
    EXPECT_EQ(SeverityName(1), "warning");
    EXPECT_EQ(SeverityName(2), "error");
    EXPECT_EQ(SeverityName(-1), "");
    EXPECT_EQ(SeverityName(3), "");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NExample
