#include <mapreduce/yt/library/parallel_io/parallel_reader.h>

#include <mapreduce/yt/library/parallel_io/ut/test_message.pb.h>

#include <mapreduce/yt/http/abortable_http_response.h>

#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

#define DECLARE_PARALLEL_READER_SUIT(suiteName) \
    UNIT_TEST_SUITE(suiteName); \
    UNIT_TEST(SimpleYson); \
    UNIT_TEST(SimpleSkiff); \
    UNIT_TEST(SimpleProtobuf); \
    UNIT_TEST(SimpleYaMR); \
    UNIT_TEST(EmptyTable); \
    UNIT_TEST(SmallTable); \
    UNIT_TEST(RangesSingleTable); \
    UNIT_TEST(RangesSeveralTables); \
    UNIT_TEST(NetworkProblemsFullOutage); \
    UNIT_TEST(NetworkProblemsRetriableOutage); \
    UNIT_TEST(WithColumnSelector); \
    UNIT_TEST(LargeSingleTable); \
    UNIT_TEST(LargeSeveralTables); \
    UNIT_TEST(LargeYaMR); \
    UNIT_TEST(ReadWithProcessor); \
    UNIT_TEST(ReadWithProcessorError); \
    UNIT_TEST_SUITE_END()

bool operator==(const TTestMessage& left, const TTestMessage& right)
{
    return left.GetKey() == right.GetKey() && left.GetValue() == right.GetValue();
}

template <typename T>
struct TActualRow
{
    using TActual = T;
};

template <>
struct TActualRow<TOwningYaMRRow>
{
    using TActual = TYaMRRow;
};

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
            "table",
            rows,
            rows,
            /* ranges */ {{0, rowCount}},
            TParallelTableReaderOptions()
                .Ordered(Ordered)
                .ThreadCount(10)
                .BufferedRowCountLimit(20));
    }

    void SimpleYaMR()
    {
        TVector<TOwningYaMRRow> rows;
        constexpr size_t rowCount = 100;
        rows.reserve(rowCount);
        for (size_t i = 0; i != rowCount; ++i) {
            rows.emplace_back("key" + ToString(i), "subkey" + ToString(i), "value" + ToString(i));
        }
        TestReader(
            "table",
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
            "table",
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
            "table",
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

    void WithColumnSelector()
    {
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
        auto path = TRichYPath("table").Columns({"x", "z"});
        TestReader(
            path,
            rows,
            correctResult,
            {{0, rows.size()}},
            TParallelTableReaderOptions().Ordered(Ordered));
    }

    void LargeSingleTable()
    {
        TestLarge(1);
    }

    void LargeSeveralTables()
    {
        TestLarge(3);
    }

    void LargeYaMR()
    {
        constexpr int RowCount = 12343;

        TString key(1000, 'k');
        TString subkey(1000, 's');
        TString value(1000, 'v');

        TVector<TOwningYaMRRow> rows;
        rows.reserve(RowCount);
        for (int i = 0; i < RowCount; ++i) {
            auto iString = ToString(i);
            rows.emplace_back(key + iString, subkey + iString, value + iString);
        };

        TestReader(
            "table",
            rows,
            rows,
            {},
            TParallelTableReaderOptions()
                .Ordered(Ordered)
                .ThreadCount(3)
                .MemoryLimit(1 << 20)
                .RangeCount(10));
    }

    void ReadWithProcessor()
    {
        constexpr int RowCount = 1000;
        constexpr int ThreadCount = 10;

        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TVector<TRichYPath> paths = {
            workingDir + "/table1",
            workingDir + "/table2",
        };

        std::atomic<int> index = 0;
        TVector<TVector<TNode>> batches(paths.size() * ThreadCount);

        TParallelReaderRowProcessor<TNode> processor = [&] (TTableReaderPtr<TNode> reader, int tableIndex) {
            UNIT_ASSERT(tableIndex == 0 || tableIndex == 1);
            auto myIndex = index.fetch_add(1);
            UNIT_ASSERT_LT(myIndex, std::ssize(paths) * ThreadCount);
            for (; reader->IsValid(); reader->Next()) {
                batches[myIndex].push_back(reader->MoveRow());
            }
        };

        if (Ordered) {
            UNIT_ASSERT_EXCEPTION(
                ReadTablesInParallel(client, paths, processor),
                TApiUsageError);
            return;
        }

        for (const auto& path : paths) {
            auto writer = client->CreateTableWriter<TNode>(path);
            for (auto i = 0; i < RowCount; ++i) {
                writer->AddRow(TNode()("key", i));
            }
        }

        ReadTablesInParallel(
            client,
            paths,
            processor,
            TParallelTableReaderOptions()
                .Ordered(Ordered)
                .ThreadCount(ThreadCount)
                .MemoryLimit(1 << 20));

        TVector<TNode> rows;
        rows.reserve(paths.size() * RowCount);
        for (const auto& batch : batches) {
            rows.insert(rows.end(), batch.begin(), batch.end());
        }
        SortBy(rows, [] (const TNode& n) {
            return n["key"].AsInt64();
        });
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), paths.size() * RowCount);
        for (auto i = 0; i < std::ssize(rows); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(rows[i], TNode()("key", i / std::ssize(paths)));
        }
    }

    class TMyTestException
        : public yexception
    { };

    void ReadWithProcessorError()
    {
        constexpr int RowCount = 100;
        constexpr int ThreadCount = 10;

        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        auto path = workingDir + "/table";

        TParallelReaderRowProcessor<TNode> processor = [&] (TTableReaderPtr<TNode>, int) {
            throw TMyTestException();
        };

        if (Ordered) {
            UNIT_ASSERT_EXCEPTION(
                ReadTableInParallel(client, path, processor),
                TApiUsageError);
            return;
        }

        {
            auto writer = client->CreateTableWriter<TNode>(path);
            for (auto i = 0; i < RowCount; ++i) {
                writer->AddRow(TNode()("key", i));
            }
        }

        UNIT_ASSERT_EXCEPTION(
            ReadTableInParallel(
                client,
                path,
                processor,
                TParallelTableReaderOptions()
                    .Ordered(Ordered)
                    .ThreadCount(ThreadCount)
                    .MemoryLimit(1 << 20)),
            TMyTestException);
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
        using TActual = typename TActualRow<T>::TActual;
        Y_ENSURE(paths.size() == writtenRows.size());
        for (size_t tableIndex = 0; tableIndex < paths.size(); ++tableIndex) {
            auto writer = client->CreateTableWriter<TActual>(paths[tableIndex]);
            for (const auto& row : writtenRows[tableIndex]) {
                writer->AddRow(static_cast<TActual>(row));
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
        using TActual = typename TActualRow<T>::TActual;
        TVector<TRowWithInfo<T>> result;
        {
            auto reader = CreateParallelTableReader<TActual>(client, paths, options);
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

    // paths must be suffixes of actual path
    // (actual path is built by prepending working dir path).
    template <typename T>
    void TestReader(
        TVector<TRichYPath> paths,
        const TVector<TVector<T>>& rows,
        const TVector<TVector<T>>& expectedRows,
        const TVector<std::pair<size_t, size_t>>& ranges,
        const TParallelTableReaderOptions& options)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        for (auto& path : paths) {
            path.Path_ = fixture.GetWorkingDir() + "/" + path.Path_;
        }

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
                    UNIT_ASSERT_UNEQUAL(resultIt, result.end());
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
            paths.push_back("table_" + ToString(i));
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
            TRichYPath("table")
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
        TTestFixture fixture;
        auto workingDir = fixture.GetWorkingDir();
        auto client = fixture.GetClient();

        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->RetryInterval = TDuration::MilliSeconds(10);
        TConfig::Get()->ReadRetryCount = readRetryCount;
        TConfig::Get()->RetryCount = readRetryCount;

        constexpr size_t rowCount = 100;
        TRichYPath path(workingDir + "/table");
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

    void TestLarge(int tableCount)
    {
        constexpr int RowCount = 12343;

        TVector<TRichYPath> paths;
        TVector<TVector<TNode>> allRows;
        for (int tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
            paths.push_back("table_" + ToString(tableIndex));
            auto& rows = allRows.emplace_back();
            for (int i = 0; i < RowCount; ++i) {
                rows.push_back(TNode()("a", i));
            };
        }

        TestReader(
            paths,
            allRows,
            allRows,
            {},
            TParallelTableReaderOptions()
                .Ordered(Ordered)
                .BufferedRowCountLimit(10)
                .ThreadCount(5)
                .RangeCount(10));
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
