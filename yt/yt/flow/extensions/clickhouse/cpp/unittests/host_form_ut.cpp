#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/describe_traits.h>
#include <yt/yt/flow/extensions/clickhouse/cpp/sink.h>
#include <yt/yt/flow/extensions/clickhouse/cpp/spec.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NFlow {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TCommonClickHouseSinkParametersPtr ParseParameters(TStringBuf yson)
{
    return ConvertTo<TCommonClickHouseSinkParametersPtr>(TYsonStringBuf(yson));
}

std::string MakeSinkLoggerTags(TStringBuf parametersYson)
{
    auto logger = MakeSinkLogger(
        NLogging::TLogger("ClickHouseHostFormTest"),
        *ParseParameters(parametersYson));
    return logger.GetTags().GetPayload().Underlying();
}

std::optional<std::string> MakeDescribeTarget(TStringBuf parametersYson)
{
    auto parameters = ConvertTo<IMapNodePtr>(TYsonStringBuf(parametersYson));
    New<TClickHouseDescribeTraits>(TDescribeTraitsContext{})->MakeLinks(parameters);
    auto target = parameters->FindChild("target");
    if (!target) {
        return std::nullopt;
    }
    return target->AsString()->GetValue();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseHostFormTest, SingleHostFormIsLogged)
{
    auto tags = MakeSinkLoggerTags(R"({host="ch-1";port=9000;table="t"})");
    EXPECT_THAT(tags, ::testing::HasSubstr("Host"));
    EXPECT_THAT(tags, ::testing::HasSubstr("ch-1"));
}

TEST(TClickHouseHostFormTest, FlatHostsFormIsLogged)
{
    auto tags = MakeSinkLoggerTags(R"({hosts=["ch-1";"ch-2"];port=9000;table="t"})");
    EXPECT_THAT(tags, ::testing::HasSubstr("Hosts"));
    EXPECT_THAT(tags, ::testing::HasSubstr("ch-1"));
    EXPECT_THAT(tags, ::testing::HasSubstr("ch-2"));
}

TEST(TClickHouseHostFormTest, ShardedFormLogsNoHostTag)
{
    auto tags = MakeSinkLoggerTags(R"({shard_hosts={a=["ch-a"];b=["ch-b"]};port=9000;table="t"})");
    EXPECT_THAT(tags, ::testing::Not(::testing::HasSubstr("Host")));
    EXPECT_THAT(tags, ::testing::HasSubstr("t"));
}

TEST(TClickHouseHostFormTest, SingleHostFormHasDescribeTarget)
{
    EXPECT_EQ(
        MakeDescribeTarget(R"({host="ch-1";port=9000;database="db";table="t"})"),
        std::optional<std::string>("ch-1:9000/db.t"));
}

TEST(TClickHouseHostFormTest, FlatHostsFormTargetsFirstHost)
{
    EXPECT_EQ(
        MakeDescribeTarget(R"({hosts=["ch-1";"ch-2"];port=9000;database="db";table="t"})"),
        std::optional<std::string>("ch-1:9000/db.t"));
}

TEST(TClickHouseHostFormTest, ShardedFormHasNoDescribeTarget)
{
    EXPECT_EQ(
        MakeDescribeTarget(R"({shard_hosts={a=["ch-a"]};port=9000;database="db";table="t"})"),
        std::nullopt);
}

TEST(TClickHouseHostFormTest, HostSelectionPolicyParsesAndDefaults)
{
    EXPECT_EQ(
        ParseParameters(R"({host=h;table=t})")->HostSelectionPolicy,
        EClickHouseHostSelectionPolicy::OrderedRoundRobin);
    EXPECT_EQ(
        ParseParameters(R"({host=h;table=t;host_selection_policy=ordered_round_robin})")->HostSelectionPolicy,
        EClickHouseHostSelectionPolicy::OrderedRoundRobin);
    EXPECT_EQ(
        ParseParameters(R"({host=h;table=t;host_selection_policy=random_start})")->HostSelectionPolicy,
        EClickHouseHostSelectionPolicy::RandomStart);
    EXPECT_THROW(
        ParseParameters(R"({host=h;table=t;host_selection_policy=unknown})"),
        TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
