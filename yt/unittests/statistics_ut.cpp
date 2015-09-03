#include "stdafx.h"
#include "framework.h"

#include <ytlib/job_tracker_client/statistics.h>

#include <ytlib/formats/format.h>
#include <ytlib/formats/parser.h>

#include <core/ytree/convert.h>
#include <core/ytree/fluent.h>
#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NJobTrackerClient {
namespace {

using namespace NFormats;
using namespace NYTree;

////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////

i64 GetSum(const TStatistics& statistics, const NYPath::TYPath& path)
{
    auto getValue = [] (const TSummary& summary) {
        return summary.GetSum();
    };
    return GetValues<i64>(statistics, path, getValue);
}

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
    std::map<Stroka, int> origin = {{"x", 5}, {"y", 7}};

    TStatistics statistics;
    statistics.AddSample(
        "/key/subkey",
        origin);

    EXPECT_EQ(5, GetSum(statistics, "/key/subkey/x"));
    EXPECT_EQ(7, GetSum(statistics, "/key/subkey/y"));

    auto getValue = [] (const TSummary& summary) {
        return summary.GetSum();
    };
    auto stored = GetValues<std::map<Stroka, int>>(statistics, "/key/subkey", getValue);
    EXPECT_EQ(origin, stored);

    statistics.AddSample("/key/sub", 42);
    EXPECT_EQ(42, GetSum(statistics, "/key/sub"));

    // Cannot add sample to the map node.
    EXPECT_THROW(statistics.AddSample("/key/subkey", 24), std::exception);

    statistics.Update(CreateStatistics({
        {"/key/subkey/x", 5}, 
        {"/key/subkey/z", 9}}));

    EXPECT_EQ(10, GetSum(statistics, "/key/subkey/x"));
    EXPECT_EQ(7, GetSum(statistics, "/key/subkey/y"));
    EXPECT_EQ(9, GetSum(statistics, "/key/subkey/z"));

    EXPECT_THROW(
        statistics.Update(CreateStatistics({{"/key", 5}})), 
        std::exception);

    statistics.AddSample("/key/subkey/x", 10);
    EXPECT_EQ(20, GetSum(statistics, "/key/subkey/x"));

    NJobTrackerClient::NProto::TStatistics protoStatistics;
    ToProto(&protoStatistics, statistics);

    TStatistics deserializedStatistics;
    FromProto(&deserializedStatistics, protoStatistics);
    EXPECT_EQ(20, GetSum(deserializedStatistics, "/key/subkey/x"));
    EXPECT_EQ(42, GetSum(deserializedStatistics, "/key/sub"));
}

TEST(TStatistics, AddSuffixToNames)
{
    auto statistics = CreateStatistics({{"/key", 10}});

    statistics.AddSuffixToNames("/$/completed/map");

    EXPECT_EQ(10, GetSum(statistics, "/key/$/completed/map"));
}

////////////////////////////////////////////////////////////////////

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
    BuildYsonListFluently(&consumer)
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
    EXPECT_EQ(4, GetSum(statistics, "/custom/k1"));
    EXPECT_EQ(-7, GetSum(statistics, "/custom/k2"));
    EXPECT_EQ(42, GetSum(statistics, "/custom/key/subkey"));
}

////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NJobTrackerClient
} // namespace NYT

