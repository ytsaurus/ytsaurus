#include <yt/yt/flow/extensions/static_table_export/group_by_system_timestamp/publisher.h>
#include <yt/yt/flow/extensions/static_table_export/group_by_system_timestamp/static_table_export.h>
#include <yt/yt/flow/extensions/static_table_export/group_by_system_timestamp/writer_storage.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TStaticTableExportParametersPtr MakeParameters(int windowCount = 4)
{
    return NYTree::ConvertTo<TStaticTableExportParametersPtr>(NYson::TYsonString(Format(R"({output_directory="//tmp/export";window_period="10s";closure_delta="2500ms";max_unpublished_window_count=%v;writer_resource_id=Writer})",
        windowCount)));
}

TEST(TStaticTableExportWindowTest, WindowsAreMonotoneAndRedirectLateMessages)
{
    const auto parameters = MakeParameters();
    EXPECT_EQ(AssignStaticTableExportWindow(TSystemTimestamp(125), std::nullopt, 100, parameters), 120u);
    // A write time regression stays in the partition's highest window.
    EXPECT_EQ(AssignStaticTableExportWindow(TSystemTimestamp(115), 130, 100, parameters), 130u);
    // A message of an already published window goes to the first unpublished one.
    EXPECT_EQ(AssignStaticTableExportWindow(TSystemTimestamp(95), std::nullopt, 100, parameters), 110u);
    EXPECT_EQ(AssignStaticTableExportWindow(TSystemTimestamp(105), 100, 120, parameters), 130u);
}

TEST(TStaticTableExportWindowTest, TableNamesUseFixedUtcOffset)
{
    // 2026-11-12T10:55:00Z.
    constexpr ui64 window = 1794480900;
    EXPECT_EQ(GetStaticTableExportTableName(window, MakeParameters()->TableNameUtcOffsetSeconds), "2026-11-12T13:55:00");
    EXPECT_EQ(GetStaticTableExportTableName(window, -(5 * 3600 + 30 * 60)), "2026-11-12T05:25:00");
}

TEST(TStaticTableExportWindowTest, BandBoundsAdmission)
{
    const auto parameters = MakeParameters();
    EXPECT_EQ(AssignStaticTableExportWindow(TSystemTimestamp(149), std::nullopt, 100, parameters), 140u);
    EXPECT_EQ(AssignStaticTableExportWindow(TSystemTimestamp(150), std::nullopt, 100, parameters), std::nullopt);
    EXPECT_EQ(AssignStaticTableExportWindow(TSystemTimestamp(101), 150, 100, parameters), std::nullopt);
}

TEST(TStaticTableExportStartTest, IdlePartitionsDoNotMoveTheStartAhead)
{
    TStaticTableExportPublisherInput input{
        .PartitionCount = 3,
        .EventWatermark = TSystemTimestamp(1000),
        .FirstWriteTimestamp = TSystemTimestamp(400),
        .FirstWriteReportCount = 2,
    };
    // The watermark of idle partitions is the current time, so the start waits for every report.
    EXPECT_EQ(GetStaticTableExportStart(input), std::nullopt);
    input.FirstWriteReportCount = 3;
    EXPECT_EQ(GetStaticTableExportStart(input), TSystemTimestamp(400));
}

TEST(TStaticTableExportStartTest, SilentDrainingPartitionsDoNotDelayTheStart)
{
    TStaticTableExportPublisherInput input{
        .PartitionCount = 3,
        .FirstWriteTimestamp = TSystemTimestamp(400),
        .FirstWriteReportCount = 1,
        .SilentDrainingCount = 2,
    };
    EXPECT_EQ(GetStaticTableExportStart(input), TSystemTimestamp(400));
    // A drain with no messages at all starts nothing.
    input.FirstWriteTimestamp.reset();
    input.FirstWriteReportCount = 0;
    input.SilentDrainingCount = 3;
    EXPECT_EQ(GetStaticTableExportStart(input), std::nullopt);
}

TEST(TStaticTableExportStartTest, EmptyFiniteSourceStartsNow)
{
    TStaticTableExportPublisherInput input{
        .PartitionCount = 2,
        .SilentDrainingCount = 2,
        .SilentCompletingCount = 1,
    };
    // A silent partition that only drains may still have a backlog to read.
    EXPECT_EQ(GetStaticTableExportStart(input), std::nullopt);
    input.SilentCompletingCount = 2;
    const auto start = GetStaticTableExportStart(input);
    ASSERT_TRUE(start);
    EXPECT_GE(start->Underlying(), TInstant::Now().Seconds() - 1);
}

TEST(TStaticTableExportWindowTest, ClosureRoundsDeltaUp)
{
    const auto parameters = MakeParameters();
    EXPECT_FALSE(IsStaticTableExportWindowClosable(100, TSystemTimestamp(112), parameters));
    EXPECT_TRUE(IsStaticTableExportWindowClosable(100, TSystemTimestamp(113), parameters));
    EXPECT_TRUE(IsStaticTableExportWindowClosable(100, TSystemTimestamp(1000), parameters));
}

TEST(TStaticTableExportWindowTest, BandMustExceedAlignment)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateStaticTableExportBand(MakeParameters(4), TDuration::Seconds(10)),
        "max_unpublished_window_count");
    EXPECT_NO_THROW(ValidateStaticTableExportBand(MakeParameters(5), TDuration::Seconds(10)));
    EXPECT_THROW(ValidateStaticTableExportBand(MakeParameters(5), TDuration::Seconds(20)), TErrorException);
}

TStaticTableExportCursorPtr MakeCursor(ui64 generation, i64 nextOffset, std::optional<ui64> highestWindow)
{
    auto cursor = New<TStaticTableExportCursor>();
    cursor->Generation = TUniqueSeqNo(generation);
    cursor->NextOffset = nextOffset;
    cursor->HighestWindow = highestWindow;
    return cursor;
}

TEST(TStaticTableExportCommitTest, ClassifiesUnknownOutcomeFromCursors)
{
    const std::vector<TStaticTableExportDelta> deltas{
        {{.Partition = 0, .Generation = TUniqueSeqNo(5)}, 10, 20},
        {{.Partition = 1, .Generation = TUniqueSeqNo(7)}, 0, 3},
    };
    auto classify = [&] (TStaticTableExportCursors cursors, std::vector<int> expectedFenced) {
        std::vector<int> fenced;
        auto outcome = ClassifyStaticTableExportCommit(110, deltas, cursors, &fenced);
        EXPECT_EQ(fenced, expectedFenced);
        return outcome;
    };
    // A lost reply of a successful commit must not be rewritten.
    EXPECT_EQ(classify({{0, MakeCursor(5, 20, 110)}, {1, MakeCursor(7, 3, 110)}}, {}), EStaticTableExportCommitOutcome::Committed);
    EXPECT_EQ(classify({{0, MakeCursor(5, 10, 100)}, {1, MakeCursor(7, 0, {})}}, {}), EStaticTableExportCommitOutcome::NotCommitted);
    // A newer owner may have written exactly the same offsets; only the other partition decides.
    EXPECT_EQ(classify({{0, MakeCursor(6, 20, 110)}, {1, MakeCursor(7, 0, {})}}, {0}), EStaticTableExportCommitOutcome::NotCommitted);
    EXPECT_EQ(classify({{0, MakeCursor(6, 20, 110)}, {1, MakeCursor(7, 3, 110)}}, {0}), EStaticTableExportCommitOutcome::Committed);
    EXPECT_EQ(classify({{0, MakeCursor(5, 20, 110)}, {1, MakeCursor(7, 0, {})}}, {}), EStaticTableExportCommitOutcome::Unknown);
    EXPECT_EQ(classify({{0, MakeCursor(6, 0, {})}, {1, MakeCursor(8, 0, {})}}, {0, 1}), EStaticTableExportCommitOutcome::NotCommitted);
}

TEST(TStaticTableExportCommitTest, FirstDeltaStartsAtTheFirstDeliveredOffset)
{
    // The source may start past zero, but a written cursor must be continued exactly.
    EXPECT_TRUE(IsStaticTableExportCursorAt(*MakeCursor(5, 0, {}), 1000));
    EXPECT_FALSE(IsStaticTableExportCursorAt(*MakeCursor(5, 10, 100), 1000));
    EXPECT_TRUE(IsStaticTableExportCursorAt(*MakeCursor(5, 10, 100), 10));
    const std::vector<TStaticTableExportDelta> deltas{{{.Partition = 0, .Generation = TUniqueSeqNo(5)}, 1000, 1010}};
    std::vector<int> fenced;
    EXPECT_EQ(ClassifyStaticTableExportCommit(110, deltas, {{0, MakeCursor(5, 0, {})}}, &fenced), EStaticTableExportCommitOutcome::NotCommitted);
    EXPECT_EQ(ClassifyStaticTableExportCommit(110, deltas, {{0, MakeCursor(5, 1010, 110)}}, &fenced), EStaticTableExportCommitOutcome::Committed);
}

TEST(TStaticTableExportParametersTest, Validation)
{
    EXPECT_THROW(NYTree::ConvertTo<TStaticTableExportParametersPtr>(NYson::TYsonString(TStringBuf(
        R"({output_directory="//tmp/export";window_period="1500ms"})"))),
        TErrorException);
    EXPECT_THROW(NYTree::ConvertTo<TStaticTableExportParametersPtr>(NYson::TYsonString(TStringBuf(
        R"({output_directory="//tmp/export";window_period="10s";closure_delta="20s"})"))),
        TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
