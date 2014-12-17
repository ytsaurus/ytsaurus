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
    TSummary sum1(10);
    TSummary sum2(20);

    sum1.Merge(sum2);

    EXPECT_EQ(10, sum1.GetMin());
    EXPECT_EQ(20, sum1.GetMax());
    EXPECT_EQ(2, sum1.GetCount());
    EXPECT_EQ(30, sum1.GetSum());
}

TEST(TStatistics, IsEmpty)
{
    TStatistics statistics;
    EXPECT_TRUE(statistics.IsEmpty());
}

TEST(TStatistics, Add)
{
    TStatistics statistics;
    statistics.Add("key", TSummary(10));

    EXPECT_EQ(10, statistics.Get("key").GetSum());
}

TEST(TStatistics, Clear)
{
    TStatistics statistics;
    statistics.Add("key", TSummary(10));
    statistics.Clear();

    EXPECT_TRUE(statistics.IsEmpty());
}

TEST(TStatistics, MergeDifferent)
{
    TStatistics statistics;
    statistics.Add("key", TSummary(10));

    TStatistics other;
    other.Add("other_key", TSummary(40));

    statistics.Merge(other);

    EXPECT_EQ(10, statistics.Get("key").GetSum());
    EXPECT_EQ(40, statistics.Get("other_key").GetSum());
}

TEST(TStatistics, MergeTheSameKey)
{
    TStatistics statistics;
    statistics.Add("key", TSummary(10));

    TStatistics other;
    other.Add("key", TSummary(40));

    statistics.Merge(other);

    EXPECT_EQ(2, statistics.Get("key").GetCount());
}

TEST(TStatistics, Serialization)
{
    TStatistics statistics;
    statistics.Add("/key", TSummary(10));
    statistics.Add("/other_key", TSummary(40));

    auto newStatistics = NYTree::ConvertTo<TStatistics>(NYTree::ConvertToYsonString(statistics));
}

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
    output.Write("{ k1=4}; {k2=-7}");

    const auto& stats = statisticsConsumer.GetStatistics();
    EXPECT_EQ(4, stats.Get("/something/k1").GetSum());
    EXPECT_EQ(-7, stats.Get("/something/k2").GetSum());
}

////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NJobProxy
} // namespace NYT

