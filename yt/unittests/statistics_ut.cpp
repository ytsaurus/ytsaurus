#include "stdafx.h"
#include "framework.h"

#include <server/job_proxy/statistics.h>
#include <server/job_proxy/table_output.h>

#include <ytlib/formats/format.h>
#include <ytlib/formats/parser.h>

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

    ASSERT_EQ(sum1.GetMin(), 10);
    ASSERT_EQ(sum1.GetMax(), 20);
    ASSERT_EQ(sum1.GetCount(), 2);
    ASSERT_EQ(sum1.GetSumm(), 30);
}

TEST(TStatistics, Empty)
{
    TStatistics statistics;
    ASSERT_TRUE(statistics.Statistics().empty());
}

TEST(TStatistics, Add)
{
    TStatistics statistics;
    statistics.Add("key", TSummary(10));

    ASSERT_EQ(statistics.Statistics().at("key").GetSumm(), 10);
}

TEST(TStatistics, Clear)
{
    TStatistics statistics;
    statistics.Add("key", TSummary(10));
    statistics.Clear();

    ASSERT_TRUE(statistics.Statistics().empty());
}

TEST(TStatistics, MergeDifferent)
{
    TStatistics statistics;
    statistics.Add("key", TSummary(10));


    TStatistics other;
    other.Add("other_key", TSummary(40));

    statistics.Merge(other);

    ASSERT_EQ(statistics.Statistics().size(), 2);
}

TEST(TStatistics, MergeTheSameKey)
{
    TStatistics statistics;
    statistics.Add("key", TSummary(10));


    TStatistics other;
    other.Add("key", TSummary(40));

    statistics.Merge(other);

    ASSERT_EQ(statistics.Statistics().at("key").GetCount(), 2);
}

class TMergeStatisticsConsumer
{
public:
    void Consume(const TStatistics& arg)
    {
        Statistics_.Merge(arg);
    }

    const TStatistics::TSummaryDict& GetStatistics()
    {
        return Statistics_.Statistics();
    }

private:
    TStatistics Statistics_;
};

TEST(TStatisticsConvertor, Integration)
{
    TMergeStatisticsConsumer statisticsConsumer;
    std::unique_ptr<NYson::IYsonConsumer> consumer(new TStatisticsConvertor(BIND(&TMergeStatisticsConsumer::Consume, &statisticsConsumer)));
    auto parser = CreateParserForFormat(TFormat(EFormatType::Yson), EDataType::Tabular, consumer.get());
    TTableOutput output(std::move(parser), std::move(consumer));
    output.Write("{ k1=4}; {k2=-7}");

    const auto& stats = statisticsConsumer.GetStatistics();
    ASSERT_EQ(stats.at("k1").GetSumm(), 4);
    ASSERT_EQ(stats.at("k2").GetSumm(), -7);
}

////////////////////////////////////////////////////////////////////

} // anon
} // NJobProxy
} // NYT

