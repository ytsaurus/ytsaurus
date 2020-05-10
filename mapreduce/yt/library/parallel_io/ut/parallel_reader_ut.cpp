#include <mapreduce/yt/library/parallel_io/parallel_reader.h>

#include <mapreduce/yt/library/parallel_io/ut/test_message.pb.h>

#include <mapreduce/yt/http/abortable_http_response.h>

#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

#define DECLARE_PARALLEL_READER_SUIT(suiteName) \
    UNIT_TEST_SUITE(suiteName); \
    UNIT_TEST(SimpleYson); \
    UNIT_TEST(SimpleSkiff); \
    UNIT_TEST(SimpleProtobuf); \
    UNIT_TEST(EmptyTable); \
    UNIT_TEST(SmallTable); \
    UNIT_TEST(RangesSingleTable); \
    UNIT_TEST(RangesSeveralTables); \
    UNIT_TEST(NetworkProblemsFullOutage); \
    UNIT_TEST(NetworkProblemsRetriableOutage); \
    UNIT_TEST(WithColumnSelector); \
    UNIT_TEST_SUITE_END()

bool operator==(const TTestMessage& left, const TTestMessage& right)
{
    return left.GetKey() == right.GetKey() && left.GetValue() == right.GetValue();
}

template <bool Ordered>
class TParallelReaderTest
    : public TTestBase
{
public:
    void SimpleYson()
    {
        TestNodeReader(ENodeReaderFormat::Yson);
    }

    void SimpleSkiff()
    {
        TestNodeReader(ENodeReaderFormat::Skiff);
    }

    void SimpleProtobuf()
    {
        TVector<TTestMessage> rows;
        constexpr size_t rowCount = 100;
        rows.reserve(rowCount);
        for (size_t i = 0; i != rowCount; ++i) {
            TTestMessage row;
            row.SetKey("x");
            row.SetValue(i);
            rows.push_back(std::move(row));
        }
        TestReader(
            "//testing/table",
            rows,
            rows,
            /* ranges */ {{0, rowCount}},
            TParallelTableReaderOptions()
                .Ordered(Ordered)
                .ThreadCount(10)
                .BufferedRowCountLimit(20));
    }

    void EmptyTable()
    {
        TestReader(
            "//testing/table",
            TVector<TNode>{},
            TVector<TNode>{},
            /* ranges */ {{0,0}},
            TParallelTableReaderOptions()
                .Ordered(Ordered)
                .ThreadCount(10)
                .BufferedRowCountLimit(20));
    }

    void SmallTable()
    {
        TVector<TNode> rows{
            TNode()("x", 1),
            TNode()("x", 2),
            TNode()("x", 3),
        };
        TestReader(
            "//testing/table",
            rows,
            rows,
            /* ranges */ {{0, rows.size()}},
            TParallelTableReaderOptions()
                .Ordered(Ordered)
                .ThreadCount(2)
                .BufferedRowCountLimit(2000));
    }

    void RangesSingleTable()
    {
        TestRanges(1);
    }

    void RangesSeveralTables()
    {
        TestRanges(3);
    }

    void NetworkProblemsFullOutage()
    {
        try {
            TestWithOutage(3, std::numeric_limits<size_t>::max());
        } catch (const yexception& exception) {
            // We cannot catch the TAbortedForTestReason exception here as it is caught
            // inside the client reader and propagated as simple yexception.
            UNIT_ASSERT_STRING_CONTAINS(exception.what(), "response was aborted");
        }
    }

    void NetworkProblemsRetriableOutage()
    {
        TestWithOutage(3, 2);
    }

    void WithColumnSelector() {
        TVector<TNode> rows{
            TNode()("x", 1)("y", 4)("z", 7),
            TNode()("x", 2)("y", 5)("z", 8),
            TNode()("x", 3)("y", 6)("z", 9),
        };

        TVector<TNode> correctResult{
            TNode()("x", 1)("z", 7),
            TNode()("x", 2)("z", 8),
            TNode()("x", 3)("z", 9),
        };

        auto client = CreateTestClient();
        auto path = TRichYPath("//testing/table").Columns({"x", "z"});
        TestReader(
            path,
            rows,
            correctResult,
            {{0, rows.size()}},
            TParallelTableReaderOptions().Ordered(Ordered));
    }

private:
    template <typename T>
    struct TRowWithInfo
    {
        ui32 TableIndex;
        ui64 RowIndex;
        T Row;
    };

    template <typename T>
    friend bool operator < (const TRowWithInfo<T>& lhs, const TRowWithInfo<T>& rhs)
    {
        return std::tie(lhs.TableIndex, lhs.RowIndex) < std::tie(rhs.TableIndex, rhs.RowIndex);
    }

    template <typename T>
    void WriteRows(
        const IClientBasePtr& client,
        const TVector<TRichYPath>& paths,
        const TVector<TVector<T>>& writtenRows)
    {
        Y_ENSURE(paths.size() == writtenRows.size());
        for (size_t tableIndex = 0; tableIndex < paths.size(); ++tableIndex) {
            auto writer = client->CreateTableWriter<T>(paths[tableIndex]);
            for (const auto& row : writtenRows[tableIndex]) {
                writer->AddRow(row);
            };
            writer->Finish();
        }
    }

    template <typename T>
    TVector<TRowWithInfo<T>> ReadInParallel(
        const IClientBasePtr& client,
        const TVector<TRichYPath>& paths,
        const TParallelTableReaderOptions& options)
    {
        TVector<TRowWithInfo<T>> result;
        {
            auto reader = CreateParallelTableReader<T>(client, paths, options);
            for (; reader->IsValid(); reader->Next()) {
                TRowWithInfo<T> row;
                row.TableIndex = reader->GetTableIndex();
                row.RowIndex = reader->GetRowIndex();
                row.Row = reader->MoveRow();
                result.push_back(std::move(row));
            }
        }
        return result;
    }


    template <typename T>
    void TestReader(
        TVector<TRichYPath> paths,
        const TVector<TVector<T>>& rows,
        const TVector<TVector<T>>& expectedRows,
        const TVector<std::pair<size_t, size_t>>& ranges,
        const TParallelTableReaderOptions& options)
    {
        Y_ENSURE(!ranges.empty());

        auto client = CreateTestClient();
        WriteRows(client, paths, rows);

        if (!ranges.empty()) {
            for (auto& path : paths) {
                Y_ENSURE(path.Ranges_.empty());
                for (const auto& range : ranges) {
                    path.AddRange(TReadRange()
                        .LowerLimit(TReadLimit().RowIndex(range.first))
                        .UpperLimit(TReadLimit().RowIndex(range.second)));
                }
            }
        }

        auto result = ReadInParallel<T>(client, paths, options);
        if (!options.Ordered_) {
            Sort(result);
        }

        TVector<std::pair<size_t, size_t>> actualRanges = ranges;
        if (actualRanges.empty()) {
            actualRanges.emplace_back(0, rows.front().size());
        }

        auto resultIt = result.begin();
        for (size_t tableIndex = 0; tableIndex != paths.size(); ++tableIndex) {
            const auto& tableRows = expectedRows[tableIndex];
            for (const auto& range : actualRanges) {
                for (size_t rowIndex = range.first; rowIndex != range.second; ++rowIndex) {
                    UNIT_ASSERT_VALUES_EQUAL(resultIt->TableIndex, tableIndex);
                    UNIT_ASSERT_VALUES_EQUAL(resultIt->RowIndex, rowIndex);
                    UNIT_ASSERT_VALUES_EQUAL(resultIt->Row, tableRows[rowIndex]);
                    ++resultIt;
                }
            }
        }
        UNIT_ASSERT_EQUAL(resultIt, result.end());
    }

    template <typename T>
    void TestReader(
        const TRichYPath& paths,
        const TVector<T>& rows,
        const TVector<T>& expectedRows,
        const TVector<std::pair<size_t, size_t>>& ranges,
        const TParallelTableReaderOptions& options)
    {
        TestReader<T>(TVector<TRichYPath>{paths}, TVector<TVector<T>>{rows}, TVector<TVector<T>>{expectedRows}, ranges, options);
    }

    void TestRanges(int tableCount)
    {
        constexpr size_t rowCount = 100;

        TVector<TRichYPath> paths;
        for (int i = 0; i != tableCount; ++i) {
            paths.push_back(TStringBuilder() << "//testing/table_" << i);
        }

        TVector<std::pair<size_t, size_t>> ranges = {{3, 27}, {33, 59}, {66, 67}, {67, 98}, {99, 100}};
        TVector<TVector<TNode>> writtenRows;
        for (size_t tableIndex = 0; tableIndex != paths.size(); ++tableIndex) {
            writtenRows.emplace_back();
            writtenRows.back().reserve(rowCount);
            for (size_t i = 0; i != rowCount; ++i) {
                writtenRows.back().push_back(TNode()("x", rowCount * tableIndex + i));
            }
        }

        TestReader(
            paths,
            writtenRows,
            writtenRows,
            ranges,
            TParallelTableReaderOptions()
                .Ordered(Ordered)
                .BufferedRowCountLimit(10)
                .ThreadCount(5));
    }

    void TestNodeReader(ENodeReaderFormat format)
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->NodeReaderFormat = format;

        TVector<TNode> rows;
        constexpr size_t rowCount = 100;
        rows.reserve(rowCount);
        for (size_t i = 0; i != rowCount; ++i) {
            rows.push_back(TNode()("x", i)("y", i * i));
        }
        TestReader(
            TRichYPath("//testing/table")
                .Schema(TTableSchema()
                    .AddColumn(TColumnSchema().Name("x").Type(EValueType::VT_UINT64))
                    .AddColumn(TColumnSchema().Name("y").Type(EValueType::VT_UINT64))),
            rows,
            rows,
            /* ranges */ {{0, rowCount}},
            TParallelTableReaderOptions()
                .Ordered(Ordered)
                .ThreadCount(10)
                .BufferedRowCountLimit(20));
    }

    void TestWithOutage(size_t readRetryCount, size_t responseCount = std::numeric_limits<size_t>::max())
    {
        TConfigSaverGuard configGuard;
        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->RetryInterval = TDuration::MilliSeconds(10);
        TConfig::Get()->ReadRetryCount = readRetryCount;
        TConfig::Get()->RetryCount = readRetryCount;

        auto client = CreateTestClient();
        constexpr size_t rowCount = 100;
        TRichYPath path("//testing/table");
        {
            auto writer = client->CreateTableWriter<TNode>(path);
            for (size_t i = 0; i != rowCount; ++i) {
                writer->AddRow(TNode()("x", i));
            };
            writer->Finish();
        }

        size_t rangeSize = 2;
        auto reader = CreateParallelTableReader<TNode>(
            client,
            path,
            TParallelTableReaderOptions()
                .Ordered(Ordered)
                .ThreadCount(5)
                .BufferedRowCountLimit(5 * rangeSize));

        TVector<std::pair<ui64, TNode>> result;

        // Skip rangeSize rows. We hope that only these first rows will be read
        // by the main thread and the following outage will affect parallel threads.
        for (size_t i = 0; i != rangeSize; ++i) {
            UNIT_ASSERT(reader->IsValid());
            result.emplace_back(reader->GetRowIndex(), reader->MoveRow());
            reader->Next();
        }

        auto outage = TAbortableHttpResponse::StartOutage("/read_table", responseCount);

        for (; reader->IsValid(); reader->Next()) {
            result.emplace_back(reader->GetRowIndex(), reader->MoveRow());
        }
        if (!Ordered) {
            SortBy(result, [] (const auto& p) { return p.first; });
        }
        UNIT_ASSERT_VALUES_EQUAL(result.size(), rowCount);
        for (size_t i = 0; i != rowCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i, result[i].first);
            UNIT_ASSERT_VALUES_EQUAL(TNode()("x", i), result[i].second);
        }
    }
};

class TOrderedParallelReaderTest
    : public TParallelReaderTest<true>
{
public:
    DECLARE_PARALLEL_READER_SUIT(TOrderedParallelReaderTest);
};
UNIT_TEST_SUITE_REGISTRATION(TOrderedParallelReaderTest);


class TUnorderedParallelReaderTest
    : public TParallelReaderTest<false>
{
public:
    DECLARE_PARALLEL_READER_SUIT(TUnorderedParallelReaderTest);
};
UNIT_TEST_SUITE_REGISTRATION(TUnorderedParallelReaderTest);

#undef DECLARE_PARALLEL_READER_SUIT
