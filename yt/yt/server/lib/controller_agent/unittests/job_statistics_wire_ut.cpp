#include <yt/yt/server/lib/controller_agent/job_statistics_wire.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/statistic_path.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NControllerAgent {
namespace {

using namespace NStatisticPath;
using NChunkClient::NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

TDataStatistics MakeDataStatistics(i64 rowCount)
{
    TDataStatistics statistics;
    statistics.set_row_count(rowCount);
    statistics.set_chunk_count(rowCount > 0 ? 1 : 0);
    statistics.set_uncompressed_data_size(rowCount * 10);
    statistics.set_compressed_data_size(rowCount * 3);
    statistics.set_data_weight(rowCount * 7);
    statistics.set_regular_disk_space(rowCount * 4);
    statistics.set_erasure_disk_space(rowCount * 5);
    statistics.set_unmerged_row_count(rowCount);
    statistics.set_unmerged_data_weight(rowCount * 6);
    statistics.set_encoded_row_batch_count(rowCount * 2);
    statistics.set_encoded_columnar_batch_count(rowCount * 3);
    return statistics;
}

TStatistics MakeStatisticsWithoutData()
{
    TStatistics statistics;
    statistics.AddSample("/data/input/not_fully_consumed"_SP, 0);
    statistics.AddSample("/data/custom"_SP, 17);
    statistics.AddSample("/custom/repeated"_SP, 3);
    statistics.AddSample("/custom/repeated"_SP, 5);
    statistics.AddSample("/user_job/woodpecker"_SP, 0);
    statistics.SetTimestamp(TInstant::Now());
    return statistics;
}

TStatistics MakeFullStatistics(
    TStatistics statistics,
    const TDataStatistics& input,
    const std::vector<TDataStatistics>& output,
    bool hasInput,
    int outputCount)
{
    if (hasInput) {
        statistics.AddSample("/data/input"_SP, input);
    }
    for (int index = 0; index < outputCount; ++index) {
        statistics.AddSample("/data/output"_SP / TStatisticPathLiteral(ToString(index)), output[index]);
    }
    return statistics;
}

NProto::TJobStatus MakeStatus(
    const TStatistics& statistics,
    const TDataStatistics& input,
    const std::vector<TDataStatistics>& output)
{
    NProto::TJobStatus status;
    status.set_statistics(NYson::ConvertToYsonString(statistics).ToString());
    *status.mutable_total_input_data_statistics() = input;
    for (const auto& dataStatistics : output) {
        *status.add_output_data_statistics() = dataStatistics;
    }
    return status;
}

void ExpectSameStatistics(NProto::TJobStatus original, NProto::TJobStatus encoded)
{
    TJobSummary originalSummary(&original);
    TJobSummary encodedSummary(&encoded);
    ASSERT_TRUE(originalSummary.Statistics);
    ASSERT_TRUE(encodedSummary.Statistics);
    EXPECT_EQ(originalSummary.Statistics->Data(), encodedSummary.Statistics->Data());
    EXPECT_EQ(originalSummary.Statistics->GetTimestamp(), encodedSummary.Statistics->GetTimestamp());
}

TEST(TJobStatisticsWireTest, DataOmissionRoundTrip)
{
    auto input = MakeDataStatistics(19);
    std::vector<TDataStatistics> output{
        MakeDataStatistics(11),
        MakeDataStatistics(8),
        MakeDataStatistics(5),
    };
    auto statisticsWithoutData = MakeStatisticsWithoutData();
    statisticsWithoutData.AddSample("/data/output/0/custom"_SP, 23);
    auto fullStatistics = MakeFullStatistics(statisticsWithoutData, input, output, true, 2);
    auto original = MakeStatus(fullStatistics, input, output);
    auto encoded = original;
    auto cachedYson = NYson::ConvertToYsonString(statisticsWithoutData);

    EncodeJobStatisticsForHeartbeat(
        GetPtr(encoded),
        cachedYson,
        EJobStatisticsWireFormat::OmitDataStatistics,
        /*inputDataStatisticsOmitted*/ true,
        /*outputDataStatisticsOmittedCount*/ 2);

    EXPECT_EQ(1u, encoded.statistics_wire_format());
    EXPECT_TRUE(encoded.input_data_statistics_omitted_from_yson());
    EXPECT_EQ(2, encoded.output_data_statistics_omitted_from_yson_count());
    EXPECT_EQ(cachedYson.ToString(), encoded.statistics());
    EXPECT_LT(encoded.statistics().size(), original.statistics().size());
    ExpectSameStatistics(std::move(original), std::move(encoded));
}

TEST(TJobStatisticsWireTest, RestoresFullStatisticsForNodeConsumers)
{
    auto input = MakeDataStatistics(19);
    input.set_unmerged_row_count(-1);
    std::vector<TDataStatistics> output{MakeDataStatistics(0), MakeDataStatistics(8)};
    auto statistics = MakeStatisticsWithoutData();
    auto expected = MakeFullStatistics(statistics, input, output, true, 1);

    AddJobDataStatistics(GetPtr(statistics), input, output, true, 1);

    EXPECT_EQ(expected.Data(), statistics.Data());
    EXPECT_EQ(expected.GetTimestamp(), statistics.GetTimestamp());
    EXPECT_EQ(0, GetNumericValue(statistics, "/data/output/0/compressed_data_size"_SP));
    EXPECT_FALSE(FindSummary(statistics, "/data/output/1/row_count"_SP));
}

TEST(TJobStatisticsWireTest, AbsentInputIsNotSynthesized)
{
    auto input = MakeDataStatistics(0);
    std::vector<TDataStatistics> output{MakeDataStatistics(7)};
    TStatistics statistics;
    statistics.AddSample("/custom/value"_SP, 1);
    auto encoded = MakeStatus(statistics, input, output);
    auto original = MakeStatus(MakeFullStatistics(statistics, input, output, false, 1), input, output);

    EncodeJobStatisticsForHeartbeat(
        GetPtr(encoded),
        NYson::ConvertToYsonString(statistics),
        EJobStatisticsWireFormat::OmitDataStatistics,
        /*inputDataStatisticsOmitted*/ false,
        /*outputDataStatisticsOmittedCount*/ 1);
    AddJobDataStatistics(GetPtr(statistics), input, output, false, 1);

    EXPECT_FALSE(encoded.input_data_statistics_omitted_from_yson());
    EXPECT_EQ(1, encoded.output_data_statistics_omitted_from_yson_count());
    EXPECT_FALSE(FindSummary(statistics, "/data/input/row_count"_SP));
    ExpectSameStatistics(std::move(original), std::move(encoded));
}

TEST(TJobStatisticsWireTest, TimeOnlyStatisticsDoNotCreateDataPaths)
{
    auto input = MakeDataStatistics(1);
    std::vector<TDataStatistics> output{MakeDataStatistics(2)};
    TStatistics statistics;
    statistics.AddSample("/time/prepare"_SP, 100);
    statistics.SetTimestamp(TInstant::Now());
    auto original = MakeStatus(statistics, input, output);
    auto encoded = original;

    EncodeJobStatisticsForHeartbeat(
        GetPtr(encoded),
        NYson::ConvertToYsonString(statistics),
        EJobStatisticsWireFormat::OmitDataStatistics,
        /*inputDataStatisticsOmitted*/ false,
        /*outputDataStatisticsOmittedCount*/ 0);
    AddJobDataStatistics(GetPtr(statistics), input, output, false, 0);

    EXPECT_EQ(1u, statistics.Data().size());
    ExpectSameStatistics(std::move(original), std::move(encoded));
}

TEST(TJobStatisticsWireTest, SuccessiveSnapshotsUseMatchingDataStatistics)
{
    for (i64 rowCount : {19, 0}) {
        auto input = MakeDataStatistics(rowCount);
        std::vector<TDataStatistics> output{MakeDataStatistics(rowCount)};
        auto statistics = MakeStatisticsWithoutData();
        auto expected = MakeFullStatistics(statistics, input, output, true, 1);
        auto encoded = MakeStatus(statistics, input, output);

        EncodeJobStatisticsForHeartbeat(
            GetPtr(encoded),
            NYson::ConvertToYsonString(statistics),
            EJobStatisticsWireFormat::OmitDataStatistics,
            /*inputDataStatisticsOmitted*/ true,
            /*outputDataStatisticsOmittedCount*/ 1);
        AddJobDataStatistics(GetPtr(statistics), input, output, true, 1);

        EXPECT_EQ(expected.Data(), statistics.Data());
        EXPECT_EQ(1, GetOrCrash(statistics.Data(), "/data/input/row_count"_SP).GetCount());
        ExpectSameStatistics(MakeStatus(expected, input, output), std::move(encoded));
    }
}

TEST(TJobStatisticsWireTest, FinalizationEnrichmentPreservesDataStatistics)
{
    auto input = MakeDataStatistics(19);
    std::vector<TDataStatistics> output{MakeDataStatistics(8)};
    auto cachedYson = NYson::ConvertToYsonString(MakeStatisticsWithoutData());
    auto statistics = NYTree::ConvertTo<TStatistics>(cachedYson);
    statistics.AddSample("/exec_agent/traffic/inbound/local"_SP, 42);
    cachedYson = NYson::ConvertToYsonString(statistics);
    auto expected = MakeFullStatistics(statistics, input, output, true, 1);
    auto encoded = MakeStatus(statistics, input, output);

    EncodeJobStatisticsForHeartbeat(
        GetPtr(encoded),
        cachedYson,
        EJobStatisticsWireFormat::OmitDataStatistics,
        /*inputDataStatisticsOmitted*/ true,
        /*outputDataStatisticsOmittedCount*/ 1);
    AddJobDataStatistics(GetPtr(statistics), input, output, true, 1);

    EXPECT_EQ(expected.Data(), statistics.Data());
    EXPECT_EQ(1, GetOrCrash(statistics.Data(), "/data/input/row_count"_SP).GetCount());
    EXPECT_EQ(42, GetNumericValue(statistics, "/exec_agent/traffic/inbound/local"_SP));
    ExpectSameStatistics(MakeStatus(expected, input, output), std::move(encoded));
}

TEST(TJobStatisticsWireTest, RejectsInvalidMetadata)
{
    NProto::TJobStatus status;
    status.set_statistics("{}");
    status.set_statistics_wire_format(1);
    status.set_output_data_statistics_omitted_from_yson_count(1);
    EXPECT_THROW(DecodeJobStatisticsFromHeartbeat(status), std::exception);

    status.set_output_data_statistics_omitted_from_yson_count(-1);
    EXPECT_THROW(DecodeJobStatisticsFromHeartbeat(status), std::exception);

    status.set_output_data_statistics_omitted_from_yson_count(0);
    status.set_input_data_statistics_omitted_from_yson(true);
    EXPECT_THROW(DecodeJobStatisticsFromHeartbeat(status), std::exception);

    status.set_statistics_wire_format(42);
    EXPECT_THROW(DecodeJobStatisticsFromHeartbeat(status), std::exception);

    status.clear_statistics();
    EXPECT_THROW(DecodeJobStatisticsFromHeartbeat(status), std::exception);
}

TEST(TJobStatisticsWireTest, LegacyModePreservesYsonBytes)
{
    auto input = MakeDataStatistics(2);
    auto statistics = MakeFullStatistics(MakeStatisticsWithoutData(), input, {}, true, 0);
    auto status = MakeStatus(statistics, input, {});
    auto cachedYson = NYson::ConvertToYsonString(statistics, NYson::EYsonFormat::Text);

    EncodeJobStatisticsForHeartbeat(
        GetPtr(status),
        cachedYson,
        EJobStatisticsWireFormat::Legacy,
        /*inputDataStatisticsOmitted*/ true,
        /*outputDataStatisticsOmittedCount*/ 0);

    EXPECT_EQ(cachedYson.ToString(), status.statistics());
    EXPECT_EQ(0u, status.statistics_wire_format());
    EXPECT_FALSE(status.has_statistics_wire_format());
    EXPECT_FALSE(status.input_data_statistics_omitted_from_yson());
}

TEST(TJobStatisticsWireTest, StatusWithoutYsonDoesNotCreateStatistics)
{
    NProto::TJobStatus status;
    *status.mutable_total_input_data_statistics() = MakeDataStatistics(1);
    status.set_statistics_wire_format(1);

    TJobSummary summary(&status);
    EXPECT_FALSE(summary.Statistics);
    ASSERT_TRUE(summary.TotalInputDataStatistics);
    EXPECT_EQ(1, summary.TotalInputDataStatistics->row_count());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent
