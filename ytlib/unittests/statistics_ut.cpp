#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/formats/format.h>
#include <yt/ytlib/formats/parser.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NJobTrackerClient {
namespace {

using namespace NFormats;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TStatistics, Summary)
{
    TSummary summary;

    summary.AddSample(10);
    summary.AddSample(20);

    EXPECT_EQ(10, summary.GetMin());
    EXPECT_EQ(20, summary.GetMax());
    EXPECT_EQ(2, summary.GetCount());
    EXPECT_EQ(30, summary.GetSum());

    summary.Update(summary);
    EXPECT_EQ(10, summary.GetMin());
    EXPECT_EQ(20, summary.GetMax());
    EXPECT_EQ(4, summary.GetCount());
    EXPECT_EQ(60, summary.GetSum());

    summary.Reset();
    EXPECT_EQ(0, summary.GetCount());
    EXPECT_EQ(0, summary.GetSum());
}

////////////////////////////////////////////////////////////////////////////////

TStatistics CreateStatistics(std::initializer_list<std::pair<NYPath::TYPath, i64>> data)
{
    TStatistics result;
    for (const auto& item : data) {
        result.AddSample(item.first, item.second);
    }
    return result;
}

TEST(TStatistics, AddSample)
{
    std::map<TString, int> origin = {{"x", 5}, {"y", 7}};

    TStatistics statistics;
    statistics.AddSample(
        "/key/subkey",
        origin);

    EXPECT_EQ(5, GetNumericValue(statistics, "/key/subkey/x"));
    EXPECT_EQ(7, GetNumericValue(statistics, "/key/subkey/y"));

    statistics.AddSample("/key/sub", 42);
    EXPECT_EQ(42, GetNumericValue(statistics, "/key/sub"));

    // Cannot add sample to the map node.
    EXPECT_THROW(statistics.AddSample("/key/subkey", 24), std::exception);

    statistics.Update(CreateStatistics({
        {"/key/subkey/x", 5},
        {"/key/subkey/z", 9}}));

    EXPECT_EQ(10, GetNumericValue(statistics, "/key/subkey/x"));
    EXPECT_EQ(7, GetNumericValue(statistics, "/key/subkey/y"));
    EXPECT_EQ(9, GetNumericValue(statistics, "/key/subkey/z"));

    EXPECT_THROW(
        statistics.Update(CreateStatistics({{"/key", 5}})),
        std::exception);

    statistics.AddSample("/key/subkey/x", 10);
    EXPECT_EQ(20, GetNumericValue(statistics, "/key/subkey/x"));

    auto ysonStatistics = ConvertToYsonString(statistics);
    auto deserializedStatistics = ConvertTo<TStatistics>(ysonStatistics);

    EXPECT_EQ(20, GetNumericValue(deserializedStatistics, "/key/subkey/x"));
    EXPECT_EQ(42, GetNumericValue(deserializedStatistics, "/key/sub"));
}

TEST(TStatistics, AddSuffixToNames)
{
    auto statistics = CreateStatistics({{"/key", 10}});

    statistics.AddSuffixToNames("/$/completed/map");

    EXPECT_EQ(10, GetNumericValue(statistics, "/key/$/completed/map"));
}

////////////////////////////////////////////////////////////////////////////////

class TStatisticsUpdater
{
public:
    void AddSample(const INodePtr& node)
    {
        Statistics_.AddSample("/custom", node);
    }

    const TStatistics& GetStatistics()
    {
        return Statistics_;
    }

private:
    TStatistics Statistics_;
};

TEST(TStatistics, Consumer)
{
    TStatisticsUpdater statisticsUpdater;
    TStatisticsConsumer consumer(BIND(&TStatisticsUpdater::AddSample, &statisticsUpdater));
    BuildYsonListFragmentFluently(&consumer)
        .Item()
            .BeginMap()
                .Item("k1").Value(4)
            .EndMap()
        .Item()
            .BeginMap()
                .Item("k2").Value(-7)
            .EndMap()
        .Item()
            .BeginMap()
                .Item("key")
                .BeginMap()
                    .Item("subkey")
                    .Value(42)
                .EndMap()
            .EndMap();

    const auto& statistics = statisticsUpdater.GetStatistics();
    EXPECT_EQ(4, GetNumericValue(statistics, "/custom/k1"));
    EXPECT_EQ(-7, GetNumericValue(statistics, "/custom/k2"));
    EXPECT_EQ(42, GetNumericValue(statistics, "/custom/key/subkey"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NJobTrackerClient
} // namespace NYT

