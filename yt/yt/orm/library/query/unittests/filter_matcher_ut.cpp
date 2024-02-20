#include <yt/yt/orm/library/query/filter_matcher.h>

#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/yson/string.h>

#include <util/random/random.h>

namespace NYT::NOrm::NQuery::NTests {
namespace {

using namespace NYT;
using namespace NYT::NYTree;
using namespace NYT::NConcurrency;
using NYT::NYson::TYsonStringBuf;

////////////////////////////////////////////////////////////////////////////////

TEST(TFilterMatcherTest, OneEmptyAttributePath)
{
    auto matcher = CreateFilterMatcher("[/some/attribute/abc] = 456");
    EXPECT_THROW(matcher->Match(TYsonStringBuf("{some={attribute={abc=\"xyz\"}}}")).ValueOrThrow(), TErrorException);
    EXPECT_FALSE(matcher->Match(TYsonStringBuf("{some={attribute={abc=123}}}")).ValueOrThrow());
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("{some={attribute={abc=456}}}")).ValueOrThrow());
    EXPECT_FALSE(matcher->Match(TYsonStringBuf("1")).ValueOrThrow());
    EXPECT_FALSE(matcher->Match(TYsonStringBuf("\"123\"")).ValueOrThrow());
}

TEST(TFilterMatcherTest, InvalidAttributePath)
{
    EXPECT_THROW(CreateFilterMatcher("%true", {"/labels/"}), TErrorException);
    EXPECT_THROW(CreateFilterMatcher("%true", {"//"}), TErrorException);
    EXPECT_THROW(CreateFilterMatcher("%true", {"a"}), TErrorException);

    auto matcher = CreateFilterMatcher("%true", {"/labels"});
    matcher = CreateFilterMatcher("%true", {"/labels/key1"});
    Y_UNUSED(matcher);
}

TEST(TFilterMatcherTest, InvalidQuery)
{
    EXPECT_THROW(CreateFilterMatcher("a/b"), TErrorException);
    EXPECT_THROW(CreateFilterMatcher("[/a] = AND"), TErrorException);
    EXPECT_THROW(CreateFilterMatcher("[/a] = 123 AND"), TErrorException);
}

TEST(TFilterMatcherTest, BrokenAttributes)
{
    auto matcher = CreateFilterMatcher("[/labels/b] = 1", {"/labels"});
    EXPECT_THROW(matcher->Match(TYsonStringBuf("{;")).ValueOrThrow(), TErrorException);
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("{b=1}")).ValueOrThrow());
}

TEST(TFilterMatcherTest, IncompatiblyTypes)
{
    auto matcher = CreateFilterMatcher("[/labels/b] = 1", {"/labels"});
    EXPECT_THROW(matcher->Match(TYsonStringBuf("{b=\"abca\"}")).ValueOrThrow(), TErrorException);
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("{b=1}")).ValueOrThrow());
}

TEST(TFilterMatcherTest, SuccessfulMatch)
{
    auto matcher = CreateFilterMatcher("[/labels/b/c] = 1", {"/labels"});
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("{b={c=1}}")).ValueOrThrow());
    EXPECT_FALSE(matcher->Match(TYsonStringBuf("{}")).ValueOrThrow());
    EXPECT_FALSE(matcher->Match(TYsonStringBuf("{b={}}")).ValueOrThrow());
    EXPECT_FALSE(matcher->Match(TYsonStringBuf("{b=0}")).ValueOrThrow());
    EXPECT_FALSE(matcher->Match(TYsonStringBuf("{b={c=5}}")).ValueOrThrow());
}

TEST(TFilterMatcherTest, SeveralAttributes)
{
    auto matcher = CreateFilterMatcher("[/meta/id] = 15 AND [/labels/a] = 1", {"/labels", "/meta"});

    EXPECT_TRUE(matcher->Match({TYsonStringBuf("{a=1}"), TYsonStringBuf("{id=15}")}).ValueOrThrow());
    EXPECT_FALSE(matcher->Match({TYsonStringBuf("{}"), TYsonStringBuf("{id=15}")}).ValueOrThrow());
    EXPECT_FALSE(matcher->Match({TYsonStringBuf("{a=1}"), TYsonStringBuf("{}")}).ValueOrThrow());
    EXPECT_FALSE(matcher->Match({TYsonStringBuf("{}"), TYsonStringBuf("{}")}).ValueOrThrow());
    EXPECT_FALSE(matcher->Match({TYsonStringBuf("{b=1}"), TYsonStringBuf("{id=15}")}).ValueOrThrow());
    EXPECT_FALSE(matcher->Match({TYsonStringBuf("{a=1}"), TYsonStringBuf("{id=16}")}).ValueOrThrow());
}

TEST(TFilterMatcherTest, BinaryYson)
{
    auto matcher = CreateFilterMatcher("[/labels/b] = 1", {"/labels"});
    EXPECT_TRUE(matcher->Match(BuildYsonStringFluently(NYson::EYsonFormat::Binary)
        .BeginMap()
            .Item("b")
            .Value(1)
        .EndMap()).ValueOrThrow());
    EXPECT_FALSE(matcher->Match(BuildYsonStringFluently(NYson::EYsonFormat::Binary)
        .BeginMap()
            .Item("b")
            .Value(0)
        .EndMap()).ValueOrThrow());
}

TEST(TFilterMatcherTest, ScalarAttributes)
{
    auto matcher = CreateFilterMatcher("[/labels/a] = 1 and [/meta/id] > \"a\" and [/meta/creation_time] > 10", {"/meta/id", "/meta/creation_time", "/labels"});
    EXPECT_TRUE(matcher->Match({
        BuildYsonStringFluently().Value("aba"),
        BuildYsonStringFluently().Value(15),
        BuildYsonStringFluently().BeginMap().Item("a").Value(1).EndMap()
    }).ValueOrThrow());
    EXPECT_FALSE(matcher->Match({
        BuildYsonStringFluently().Value("a"),
        BuildYsonStringFluently().Value(15),
        BuildYsonStringFluently().BeginMap().Item("a").Value(1).EndMap()
    }).ValueOrThrow());
    EXPECT_FALSE(matcher->Match({
        BuildYsonStringFluently().Value("aba"),
        BuildYsonStringFluently().Value(10),
        BuildYsonStringFluently().BeginMap().Item("a").Value(1).EndMap()
    }).ValueOrThrow());
    EXPECT_FALSE(matcher->Match({
        BuildYsonStringFluently().Value("aba"),
        BuildYsonStringFluently().Value(15),
        BuildYsonStringFluently().BeginMap().Item("a").Value(2).EndMap()
    }).ValueOrThrow());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TConstantFilterMatcherTest, Simple)
{
    auto matcher = CreateConstantFilterMatcher(true);
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("")).ValueOrThrow());
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("{}")).ValueOrThrow());
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("{;]")).ValueOrThrow());
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("{a=1}")).ValueOrThrow());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFilterMatcherTest, ExtraNumberOfAttributes)
{
    auto matcher = CreateFilterMatcher("[/meta/pod_set_id] = \"123\"", {"/meta/id", "/meta/pod_set_id", "/labels"});
    EXPECT_TRUE(matcher->Match({
        BuildYsonStringFluently().Value("aba"),
        BuildYsonStringFluently().Value("123"),
        BuildYsonStringFluently().BeginMap().Item("a").Value(1).EndMap()
    }).ValueOrThrow());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFilterMatcherTest, HashInFilter)
{
    auto matcher = CreateFilterMatcher("[/labels/a] != #", {"/labels"});
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("{a=1}")).ValueOrThrow());
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("{a=\"aba\"}")).ValueOrThrow());
    EXPECT_FALSE(matcher->Match(TYsonStringBuf("{b=1}")).ValueOrThrow());

    auto matcherHash = CreateFilterMatcher("[/labels/a] = #", {"/labels"});
    EXPECT_FALSE(matcherHash->Match(TYsonStringBuf("{a=1}")).ValueOrThrow());
    EXPECT_FALSE(matcherHash->Match(TYsonStringBuf("{a=\"aba\"}")).ValueOrThrow());
    EXPECT_TRUE(matcherHash->Match(TYsonStringBuf("{b=1}")).ValueOrThrow());
    EXPECT_TRUE(matcherHash->Match(TYsonStringBuf("#")).ValueOrThrow());
    EXPECT_TRUE(matcherHash->Match(BuildYsonStringFluently().Entity()).ValueOrThrow());

    auto matcherLabelHash = CreateFilterMatcher("[/labels/a] = #", {"/labels/a"});
    EXPECT_FALSE(matcherLabelHash->Match(TYsonStringBuf("1")).ValueOrThrow());
    EXPECT_FALSE(matcherLabelHash->Match(TYsonStringBuf("\"aba\"")).ValueOrThrow());

//    Probably EXPECT_TRUE here. Waiting (YP-3472)
//    EXPECT_FALSE(matcherLabelHash->Match(BuildYsonStringFluently().Entity()).ValueOrThrow());
//    EXPECT_FALSE(matcherLabelHash->Match(TYsonStringBuf("#")).ValueOrThrow());

//    Uncomment this code after (YT-15191)
//    auto matcherWithIn = CreateFilterMatcher("[/labels/a] IN (#)", {"/labels"});
//    EXPECT_FALSE(matcherWithIn->Match(TYsonStringBuf("{a=1}")).ValueOrThrow());
//    EXPECT_TRUE(matcherWithIn->Match(TYsonStringBuf("{b=1}")).ValueOrThrow());

    auto matcherSpecHash = CreateFilterMatcher("[/spec/records] = #", {"/spec"});
    EXPECT_TRUE(matcherSpecHash->Match(TYsonStringBuf("{}")).ValueOrThrow());
    EXPECT_FALSE(matcherSpecHash->Match(
        BuildYsonStringFluently().BeginMap().
            Item("records").BeginList()
                .Item().BeginMap()
                    .Item("data").Value("my_data")
                    .Item("type").Value("my_type")
                    .Item("class").Value("my_class")
                    .Item("ttl").Value(1)
                .EndMap()
            .EndList()
        .EndMap()
    ).ValueOrThrow());

    auto matcherSpecRecordsHash = CreateFilterMatcher("[/spec/records] = #", {"/spec/records"});
    EXPECT_FALSE(matcherSpecRecordsHash->Match(
        BuildYsonStringFluently().BeginList()
            .Item().BeginMap()
                .Item("data").Value("my_data")
                .Item("type").Value("my_type")
                .Item("class").Value("my_class")
                .Item("ttl").Value(1)
            .EndMap()
        .EndList()
    ).ValueOrThrow());

//    Probably EXPECT_TRUE here. Waiting (YP-3472)
//    EXPECT_FALSE(matcherSpecRecordsHash->Match(TYsonStringBuf("{}")).ValueOrThrow());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFilterMatcherTest, FilterWithIn)
{
    auto matcher = CreateFilterMatcher("try_get_int64([/meta/id], \"\") IN (1, 2)", {"/meta/id"});
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("1")).ValueOrThrow());

    auto matcherSameTypes = CreateFilterMatcher("try_get_int64([/labels/a], \"\") IN (1, 2)", {"/labels"});
    EXPECT_TRUE(matcherSameTypes->Match(TYsonStringBuf("{a=1}")).ValueOrThrow());
    EXPECT_FALSE(matcherSameTypes->Match(TYsonStringBuf("{b=1}")).ValueOrThrow());
}

TEST(TFilterMatcherTest, ExceptionSafety)
{
    auto matcher = CreateFilterMatcher("[/labels/deploy_engine] = 'YP_LITE'", {"/labels"});
    EXPECT_FALSE(matcher->Match(TYsonStringBuf("{deploy_engine=\"QYP\"}")).ValueOrThrow());
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("{deploy_engine=\"YP_LITE\"}")).ValueOrThrow());
    EXPECT_THROW(matcher->Match(TYsonStringBuf("{deploy.engine='QYP'}")).ValueOrThrow(), ::NYT::TErrorException);
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("{deploy_engine=\"YP_LITE\"}")).ValueOrThrow());
    EXPECT_FALSE(matcher->Match(TYsonStringBuf("{deploy_engine=\"QYP\"}")).ValueOrThrow());
}

TEST(TFilterMatcherTest, ThreadSafety)
{
    auto matcher = CreateFilterMatcher("[/labels/deploy_engine] = 'YP_LITE'", {"/labels"});
    const size_t iterationCount = 10'000;
    const size_t threadCount = 10;

    auto task = [=] () {
        for (size_t i = 0; i < iterationCount; ++i) {
            if (RandomNumber<bool>()) {
                EXPECT_FALSE(matcher->Match(TYsonStringBuf("{deploy_engine=\"QYP\"}")).ValueOrThrow());
            } else {
                EXPECT_TRUE(matcher->Match(TYsonStringBuf("{deploy_engine=\"YP_LITE\"}")).ValueOrThrow());
            }
        }
    };

    auto threadPool = CreateThreadPool(threadCount, "FilterMatcherThreadSafety");

    std::vector<TFuture<void>> futures;
    futures.reserve(threadCount);
    auto invoker = threadPool->GetInvoker();
    for (size_t i = 0; i < threadCount; ++i) {
        futures.push_back(BIND(task)
            .AsyncVia(invoker)
            .Run());
    }
    WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();
    threadPool->Shutdown();
}

TEST(TFilterMatcherTest, FarmHash)
{
    auto matcher = CreateFilterMatcher("farm_hash(string([/labels/zone])) % 1 = 0", {"/labels"});
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("{zone=\"some_zone\"}")).ValueOrThrow());
    EXPECT_TRUE(matcher->Match(TYsonStringBuf("{not_zone_label=1}")).ValueOrThrow());
}

TEST(TFilterMatcherTest, TypedAttributePaths)
{
    auto matcher = CreateFilterMatcher(
        "([/labels/shard_id] = \"53\" AND NOT [/meta/id] IN (\"pod_1\", "
        "\"pod_2\", \"pod_3\", \"pod_4\", \"pod_5\", \"pod_6\")) AND "
        "[/meta/pod_set_id] = \"my_pod_set\"",
        /*typedAttributePaths*/ {
            TTypedAttributePath{
                .Path = "/meta/pod_set_id",
                .Type = NTableClient::EValueType::String,
            },
            TTypedAttributePath{
                .Path = "/meta/id",
                .Type = NTableClient::EValueType::String,
            },
            TTypedAttributePath{
                .Path = "/labels",
                .Type = NTableClient::EValueType::Any,
            },
        });

    auto buildAndMatch = [&] (const auto& podSetId, const auto& podId, const auto& shardId) {
        return matcher->Match({
            BuildYsonStringFluently().Value(podSetId),
            BuildYsonStringFluently().Value(podId),
            BuildYsonStringFluently().BeginMap()
                .Item("shard_id").Value(shardId)
            .EndMap(),
        }).ValueOrThrow();
    };

    EXPECT_TRUE(buildAndMatch("my_pod_set", "pod_123", "53"));
    EXPECT_TRUE(buildAndMatch("my_pod_set", "avdsd", "53"));
    EXPECT_FALSE(buildAndMatch("my_pod_set", "pod_123", "52"));
    EXPECT_FALSE(buildAndMatch("my_pod_set", "pod_1", "53"));
    EXPECT_FALSE(buildAndMatch("my_pod_set", "pod_2", "53"));
    EXPECT_FALSE(buildAndMatch("my_pod_set", "pod_5", "53"));
    EXPECT_FALSE(buildAndMatch("other_pod_set", "pod_123", "53"));

    EXPECT_THROW_WITH_SUBSTRING(
        buildAndMatch("my_pod_set", "pod_123", 53),
        "Cannot compare values of types");
}

TEST(TFilterMatcherTest, FullAttributePaths)
{
    {
        auto matcher = CreateFilterMatcher("[/meta/id] = 15 AND [/labels/a] = \"1\"", {"/meta/id", "/labels/a"});
        EXPECT_TRUE(matcher->Match({TYsonStringBuf("15"), TYsonStringBuf("\"1\"")}).ValueOrThrow());
    }
    {
        auto matcher = CreateFilterMatcher("[/meta/id] = 15 AND [/labels/a] = \"1\"");
        EXPECT_TRUE(matcher->Match(TYsonStringBuf("{meta={id=15}; labels={a=\"1\"}}")).ValueOrThrow());
    }
    {
        auto matcher = CreateFilterMatcher("[/meta/id] = 15 AND [/labels/a] = \"1\"");
        EXPECT_TRUE(matcher->Match(
            BuildYsonStringFluently()
                .BeginMap()
                    .Item("meta").Value(
                        BuildYsonStringFluently()
                            .BeginMap()
                                .Item("id").Value(15)
                            .EndMap()
                    )
                    .Item("labels").Value(
                        BuildYsonStringFluently()
                            .BeginMap()
                                .Item("a").Value("1")
                            .EndMap()
                    )
                .EndMap()
        ).ValueOrThrow());
    }
}

TEST(TFilterMatcherTest, Regex)
{
    {
        auto matcher = CreateFilterMatcher("regex_full_match('aaa', 'aaa')", {"/labels"});
        EXPECT_TRUE(matcher->Match(TYsonStringBuf("{foo=bar}")).ValueOrThrow());
    }
    {
        auto matcher = CreateFilterMatcher("regex_full_match('aaa', 'aab')", {"/labels"});
        EXPECT_FALSE(matcher->Match(TYsonStringBuf("{foo=bar}")).ValueOrThrow());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NQuery::NTests
