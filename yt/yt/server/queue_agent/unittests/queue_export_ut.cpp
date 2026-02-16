#include <yt/yt/server/queue_agent/helpers.h>
#include <yt/yt/server/queue_agent/queue_exporter.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <tuple>

namespace NYT::NQueueAgent {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TAggregateQueueProgressTest
    : public ::testing::TestWithParam<std::tuple<
        TQueueExportProgressPtr,
        TQueueExportProgressPtr,
        std::optional<std::vector<i64>>>>
{ };

TEST_P(TAggregateQueueProgressTest, BasicAggregation)
{
    auto [lhs, rhs, expectedResult] = GetParam();

    TAggregatedQueueExportsProgress progress;
    progress.MergeWith(TAggregatedQueueExportsProgress::FromQueueExportProrgess(lhs));
    progress.MergeWith(TAggregatedQueueExportsProgress::FromQueueExportProrgess(rhs));

    auto aggregateProgress = AggregateQueueExports(THashMap{
        std::pair{TString("lhs"), lhs},
        std::pair{TString("rhs"), rhs},
    });
    EXPECT_EQ(progress, aggregateProgress);

    ASSERT_EQ(progress.HasExports, expectedResult.has_value());
    if (expectedResult.has_value()) {
        auto expectedResultValue = *expectedResult;
        for (const auto& [tabletIndex, rowCount] : progress.TabletIndexToRowCount) {
            EXPECT_TRUE(tabletIndex >= 0 && tabletIndex < std::ssize(expectedResultValue) && expectedResultValue[tabletIndex] == rowCount);
        }
        for (int i = 0; i < std::ssize(expectedResultValue); i++) {
            if (expectedResultValue[i] == -1) {
                EXPECT_FALSE(progress.TabletIndexToRowCount.contains(i));
            } else {
                EXPECT_EQ(expectedResultValue[i], GetOrDefault(progress.TabletIndexToRowCount, i, -1));
            }
        }
    }
}

const auto LhsExportProgress = ConvertTo<TQueueExportProgressPtr>(TYsonString(TStringBuf(R"({tablets={"1"={row_count=1};"3"={row_count=1};"4"={row_count=2}}})")));
const auto RhsExportProgress = ConvertTo<TQueueExportProgressPtr>(TYsonString(TStringBuf(R"({tablets={"2"={row_count=1};"3"={row_count=2};"4"={row_count=1}}})")));

INSTANTIATE_TEST_SUITE_P(
    TAggregateQueueProgressTest,
    TAggregateQueueProgressTest,
    ::testing::Values(
        std::tuple{nullptr, nullptr, std::nullopt},
        std::tuple{LhsExportProgress, nullptr, std::vector<i64>{-1, 1, -1, 1, 2}},
        std::tuple{nullptr, RhsExportProgress, std::vector<i64>{-1, -1, 1, 2, 1}},
        std::tuple{LhsExportProgress, RhsExportProgress, std::vector<i64>{-1, 0, 0, 1, 1}}
    ));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueueAgent
