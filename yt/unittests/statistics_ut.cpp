#include "stdafx.h"
#include "framework.h"

#include <server/job_proxy/statistics.h>
#include <server/job_proxy/table_output.h>

#include <ytlib/formats/format.h>
#include <ytlib/formats/parser.h>

#include <core/ytree/convert.h>

namespace NYT {
namespace NJobProxy {
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

TEST(TStatistics, Empty)
{
    TStatistics statistics;
    EXPECT_TRUE(statistics.Empty());
}

TEST(TStatistics, Add)
{
    TStatistics statistics;
    statistics.Add("key", TSummary(10));

    EXPECT_EQ(10, statistics.GetStatistic("key").GetSum());
}

TEST(TStatistics, Clear)
{
    TStatistics statistics;
    statistics.Add("key", TSummary(10));
    statistics.Clear();

    EXPECT_TRUE(statistics.Empty());
}

TEST(TStatistics, MergeDifferent)
{
    TStatistics statistics;
    statistics.Add("key", TSummary(10));

    TStatistics other;
    other.Add("other_key", TSummary(40));

    statistics.Merge(other);

    EXPECT_EQ(10, statistics.GetStatistic("key").GetSum());
    EXPECT_EQ(40, statistics.GetStatistic("other_key").GetSum());
}

TEST(TStatistics, MergeTheSameKey)
{
    TStatistics statistics;
    statistics.Add("key", TSummary(10));


    TStatistics other;
    other.Add("key", TSummary(40));

    statistics.Merge(other);

    EXPECT_EQ(2, statistics.GetStatistic("key").GetCount());
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

TEST(TStatisticsConvertor, Integration)
{
    TMergeStatisticsConsumer statisticsConsumer;
    auto consumer = std::make_unique<TStatisticsConvertor>(BIND(&TMergeStatisticsConsumer::Consume, &statisticsConsumer));
    auto parser = CreateParserForFormat(TFormat(EFormatType::Yson), EDataType::Tabular, consumer.get());
    TTableOutput output(std::move(parser), std::move(consumer));
    output.Write("{ k1=4}; {k2=-7}");

    const auto& stats = statisticsConsumer.GetStatistics();
    EXPECT_EQ(4, stats.GetStatistic("/k1").GetSum());
    EXPECT_EQ(-7, stats.GetStatistic("/k2").GetSum());
}

////////////////////////////////////////////////////////////////////

} // anon
} // NJobProxy
} // NYT

