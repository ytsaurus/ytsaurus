#include "stdafx.h"
#include "framework.h"

#include <server/job_proxy/table_output.h>

#include <ytlib/scheduler/statistics.h>

#include <ytlib/formats/format.h>
#include <ytlib/formats/parser.h>

#include <core/ytree/convert.h>

namespace NYT {
namespace NScheduler {
namespace {

using namespace NFormats;

////////////////////////////////////////////////////////////////////

TEST(TSummary, MergeBasic)
{
    TSummary sum;

    sum.AddSample(10);
    sum.AddSample(20);

    EXPECT_EQ(10, sum.GetMin());
    EXPECT_EQ(20, sum.GetMax());
    EXPECT_EQ(2, sum.GetCount());
    EXPECT_EQ(30, sum.GetSum());
}

////////////////////////////////////////////////////////////////////

TStatistics CreateStatistics(std::initializer_list<std::pair<NYPath::TYPath, i64>> data)
{
    TStatistics result;
    for (const auto& item : data) {
        result.Add(item.first, item.second);
    }
    return result;
}

TEST(TStatistics, Add)
{
    auto statistics = CreateStatistics({{"key", 10}});

    EXPECT_EQ(10, statistics.Get("key"));
}

TEST(TStatistics, AddComplex)
{
    yhash_map<Stroka, int> raw;
    raw["x"] = 5;
    raw["y"] = 7;

    TStatistics statistics;
    statistics.AddComplex(
        "/key/subkey",
        raw);

    EXPECT_EQ(5, statistics.Get("/key/subkey/x"));
    EXPECT_EQ(7, statistics.Get("/key/subkey/y"));
}

TEST(TStatistics, GetComplex)
{
    yhash_map<Stroka, int> raw;
    raw["x"] = 5;
    raw["y"] = 7;

    TStatistics statistics;
    statistics.AddComplex(
        "/key/subkey",
        raw);

    auto complexStatistics = statistics.GetComplex<yhash_map<Stroka, int>>("/key/subkey");
    EXPECT_EQ(5, complexStatistics["x"]);
    EXPECT_EQ(7, complexStatistics["y"]);
}

TEST(TStatistics, AddSuffixToNames)
{
    auto statistics = CreateStatistics({{"/key", 10}});

    statistics.AddSuffixToNames("/$/completed/map");

    EXPECT_EQ(10, statistics.Get("/key/$/completed/map"));
}

TEST(TStatistics, MergeDifferent)
{
    auto statistics = CreateStatistics({{"key", 10}});
    auto other = CreateStatistics({{"other_key", 40}});

    statistics.Merge(other);

    EXPECT_EQ(10, statistics.Get("key"));
    EXPECT_EQ(40, statistics.Get("other_key"));
}

TEST(TStatistics, MergeTheSameKey)
{
    auto statistics = CreateStatistics({{"key", 10}});
    auto other = CreateStatistics({{"key", 40}});

    statistics.Merge(other);

    EXPECT_EQ(40, statistics.Get("key"));
}

TEST(TStatistics, Serialization)
{
    auto statistics = CreateStatistics({
        {"/key", 10},
        {"/other_key", 40}});

    auto newStatistics = NYTree::ConvertTo<TStatistics>(NYTree::ConvertToYsonString(statistics));

    EXPECT_EQ(10, statistics.Get("/key"));
    EXPECT_EQ(40, statistics.Get("/other_key"));
}

////////////////////////////////////////////////////////////////////

class TMergeStatisticsConsumer
{
public:
    void Consume(const TStatistics& arg)
    {
        Statistics_.Merge(arg);
    }

    const TStatistics& GetStatistics()
    {
        return Statistics_;
    }

private:
    TStatistics Statistics_;
};

TEST(TStatisticsConsumer, Integration)
{
    TMergeStatisticsConsumer statisticsConsumer;
    auto consumer = std::make_unique<TStatisticsConsumer>(BIND(&TMergeStatisticsConsumer::Consume, &statisticsConsumer), "/something");
    auto parser = CreateParserForFormat(TFormat(EFormatType::Yson), EDataType::Tabular, consumer.get());
    NJobProxy::TTableOutput output(std::move(parser), std::move(consumer));
    output.Write("{k1=4}; {k2=-7}; {key={subkey=42}}");

    const auto& stats = statisticsConsumer.GetStatistics();
    EXPECT_EQ(4, stats.Get("/something/k1"));
    EXPECT_EQ(-7, stats.Get("/something/k2"));
    EXPECT_EQ(42, stats.Get("/something/key/subkey"));
}

////////////////////////////////////////////////////////////////////

TEST(TAggregatedStatistics, Integration)
{
    TAggregatedStatistics statistics;
    statistics.AddSample(CreateStatistics({{"key", 10}}));
    auto summary = statistics.Get("key");

    EXPECT_EQ(1, summary.GetCount());
    EXPECT_EQ(10, summary.GetSum());
}

////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NJobProxy
} // namespace NYT

