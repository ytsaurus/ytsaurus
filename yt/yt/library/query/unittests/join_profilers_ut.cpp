
#include <yt/yt/library/query/base/join_profiler.h>

#include <yt/yt/library/query/unittests/evaluate/test_evaluate.h>

#include <yt/yt/client/table_client/unversioned_reader.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NQueryClient {
namespace {

using namespace NTableClient;

const NLogging::TLogger Logger("Test");

////////////////////////////////////////////////////////////////////////////////

const auto NativeSplit = MakeSplit({
    {"jlk1", EValueType::Int64},
    {"jlk2", EValueType::Int64},
});

const auto ForeignSplit = MakeSplit({
    {"jrk1", EValueType::Int64},
    {"jrk2", EValueType::Int64},
    {"fc", EValueType::Int64},
});

std::vector<TUnversionedRow> ReadAllRows(const ISchemafulUnversionedReaderPtr& reader)
{
    std::vector<TUnversionedRow> result;
    while (auto batch = reader->Read()) {
        if (!batch) {
            break;
        }
        auto rows = batch->MaterializeRows();
        result.insert(result.end(), rows.begin(), rows.end());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TJoinRowsetProfilerTest, HashTableJoin)
{
    auto foreignOwningRows = YsonToRows({
        "jrk1=3; jrk2=4; fc=100",
        "jrk1=5; jrk2=6; fc=200",
        "jrk1=3; jrk2=4; fc=400",
        "jrk1=1; jrk2=2; fc=300",
        "jrk1=3; jrk2=4; fc=500",
    }, ForeignSplit);

    auto joinOwningKeys = YsonToRows({
        "jlk1=2; jlk2=1",
        "jlk1=3; jlk2=4",
        "jlk1=5; jlk2=5",
        "jlk1=5; jlk2=6",
    }, NativeSplit);

    auto foreignRowset = MakeSharedRange(
        std::vector<TRow>(foreignOwningRows.begin(), foreignOwningRows.end()));

    auto joinKeys = std::vector<TRow>(joinOwningKeys.begin(), joinOwningKeys.end());

    auto producer = CreateJoinRowsetProfiler(foreignRowset, 0, 2, Logger)->Profile();
    auto reader = producer->FetchJoinedRows(std::move(joinKeys), nullptr);
    auto result = ReadAllRows(reader);

    ASSERT_EQ(result.size(), 4u);
    EXPECT_EQ(result[0], YsonToRow("jrk1=3; jrk2=4; fc=100", ForeignSplit));
    EXPECT_EQ(result[1], YsonToRow("jrk1=3; jrk2=4; fc=400", ForeignSplit));
    EXPECT_EQ(result[2], YsonToRow("jrk1=3; jrk2=4; fc=500", ForeignSplit));
    EXPECT_EQ(result[3], YsonToRow("jrk1=5; jrk2=6; fc=200", ForeignSplit));
}

TEST(TJoinRowsetProfilerTest, MergeJoin)
{
    auto foreignOwningRows = YsonToRows({
        "jrk1=1; jrk2=2; fc=100",
        "jrk1=3; jrk2=4; fc=200",
        "jrk1=5; jrk2=6; fc=300",
        "jrk1=5; jrk2=6; fc=300",
        "jrk1=5; jrk2=6; fc=300",
    }, ForeignSplit);

    auto joinOwningKeys = YsonToRows({
        "jlk1=1; jlk2=2",
        "jlk1=5; jlk2=6",
        "jlk1=7; jlk2=8",
    }, NativeSplit);

    auto foreignRowset = MakeSharedRange(
        std::vector<TRow>(foreignOwningRows.begin(), foreignOwningRows.end()));

    auto joinKeys = std::vector<TRow>(joinOwningKeys.begin(), joinOwningKeys.end());

    auto producer = CreateJoinRowsetProfiler(foreignRowset, 2, 2, Logger)->Profile();
    auto reader = producer->FetchJoinedRows(std::move(joinKeys), nullptr);
    auto result = ReadAllRows(reader);

    ASSERT_EQ(result.size(), 4u);
    EXPECT_EQ(result[0], YsonToRow("jrk1=1; jrk2=2; fc=100", ForeignSplit));
    EXPECT_EQ(result[1], YsonToRow("jrk1=5; jrk2=6; fc=300", ForeignSplit));
    EXPECT_EQ(result[2], YsonToRow("jrk1=5; jrk2=6; fc=300", ForeignSplit));
    EXPECT_EQ(result[3], YsonToRow("jrk1=5; jrk2=6; fc=300", ForeignSplit));
}

TEST(TJoinRowsetProfilerTest, HybridJoin)
{
    auto foreignOwningRows = YsonToRows({
        "jrk1=1; jrk2=3; fc=100",
        "jrk1=1; jrk2=2; fc=100",
        "jrk1=1; jrk2=1; fc=100",
        "jrk1=1; jrk2=3; fc=100",
        "jrk1=3; jrk2=4; fc=200",
        "jrk1=5; jrk2=6; fc=300",
    }, ForeignSplit);

    auto joinOwningKeys = YsonToRows({
        "jlk1=1; jlk2=0",
        "jlk1=1; jlk2=2",
        "jlk1=1; jlk2=3",
        "jlk1=5; jlk2=6",
        "jlk1=5; jlk2=6",
        "jlk1=5; jlk2=6",
    }, NativeSplit);

    auto foreignRowset = MakeSharedRange(
        std::vector<TRow>(foreignOwningRows.begin(), foreignOwningRows.end()));

    auto joinKeys = std::vector<TRow>(joinOwningKeys.begin(), joinOwningKeys.end());

    auto producer = CreateJoinRowsetProfiler(foreignRowset, 1, 2, Logger)->Profile();
    auto reader = producer->FetchJoinedRows(std::move(joinKeys), nullptr);
    auto result = ReadAllRows(reader);

    ASSERT_EQ(result.size(), 6u);
    EXPECT_EQ(result[0], YsonToRow("jrk1=1; jrk2=2; fc=100", ForeignSplit));
    EXPECT_EQ(result[1], YsonToRow("jrk1=1; jrk2=3; fc=100", ForeignSplit));
    EXPECT_EQ(result[2], YsonToRow("jrk1=1; jrk2=3; fc=100", ForeignSplit));
    EXPECT_EQ(result[3], YsonToRow("jrk1=5; jrk2=6; fc=300", ForeignSplit));
    EXPECT_EQ(result[4], YsonToRow("jrk1=5; jrk2=6; fc=300", ForeignSplit));
    EXPECT_EQ(result[5], YsonToRow("jrk1=5; jrk2=6; fc=300", ForeignSplit));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
