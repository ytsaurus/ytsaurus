#include <yt/cpp/mapreduce/library/parallel_io/parallel_reader.h>

#include <yt/cpp/mapreduce/library/parallel_io/ut/proto/test_message.pb.h>

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NTesting;

bool operator==(const TTestMessage& left, const TTestMessage& right)
{
    return left.key() == right.key() && left.value() == right.value();
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

////////////////////////////////////////////////////////////////////////////////

struct TTestSkiffRow
{
    ui64 Num = 0;
    TString Str;
    std::optional<double> PointNum;

    TTestSkiffRow() = default;

    TTestSkiffRow(ui64 num, const TString& str, const std::optional<double>& pointNum)
        : Num(num)
        , Str(str)
        , PointNum(pointNum)
    { }

    bool operator==(const TTestSkiffRow& other) const
    {
        return Num == other.Num && Str == other.Str && PointNum == other.PointNum;
    }
};

IOutputStream& operator<<(IOutputStream& ss, const TTestSkiffRow& row)
{
    ss << "{ Num: " << row.Num
        << ", Str: '" << row.Str
        << "', PointNum: " << (row.PointNum ? std::to_string(*row.PointNum) : "nullopt") << " }";
    return ss;
}

template <>
struct TIsSkiffRow<TTestSkiffRow>
    : std::true_type
{ };

class TTestSkiffRowParser
    : public ISkiffRowParser
{
public:
    TTestSkiffRowParser(TTestSkiffRow* row, const TMaybe<TSkiffRowHints>& hints)
        : Hints_(hints)
        , Row_(row)
    { }

    virtual ~TTestSkiffRowParser() override = default;

    virtual void Parse(NSkiff::TCheckedInDebugSkiffParser* parser) override {
        Row_->Num = parser->ParseUint64();

        if (Hints_ && Hints_->TableIndex_ && *Hints_->TableIndex_ == 0) {
            Row_->Str = parser->ParseString32();
        }

        auto tag = parser->ParseVariant8Tag();
        if (tag == 1) {
            Row_->PointNum = parser->ParseDouble();
        } else {
            Row_->PointNum = std::nullopt;
            Y_ENSURE(tag == 0, "tag value must be equal 0 or 1");
        }
    }

private:
    TMaybe<TSkiffRowHints> Hints_;

    TTestSkiffRow* Row_;
};

template <>
ISkiffRowParserPtr NYT::CreateSkiffParser<TTestSkiffRow>(TTestSkiffRow* row, const TMaybe<TSkiffRowHints>& hints)
{
    return ::MakeIntrusive<TTestSkiffRowParser>(row, hints);
}

template <>
NSkiff::TSkiffSchemaPtr NYT::GetSkiffSchema<TTestSkiffRow>(const TMaybe<TSkiffRowHints>& hints)
{
    if (hints && hints->TableIndex_ && *hints->TableIndex_ == 0) {
        return NSkiff::CreateTupleSchema({
            NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Uint64)->SetName("num"),
            NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::String32)->SetName("str"),
            NSkiff::CreateVariant8Schema({
                CreateSimpleTypeSchema(NSkiff::EWireType::Nothing),
                CreateSimpleTypeSchema(NSkiff::EWireType::Double)})
            ->SetName("pointNum")});
    } else {
        return NSkiff::CreateTupleSchema({
            NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Uint64)->SetName("num"),
            NSkiff::CreateVariant8Schema({
                CreateSimpleTypeSchema(NSkiff::EWireType::Nothing),
                CreateSimpleTypeSchema(NSkiff::EWireType::Double)})
            ->SetName("pointNum")});
    }
}

////////////////////////////////////////////////////////////////////////////////

class TParallelReaderTest
    : public ::testing::TestWithParam<bool>
{
public:
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
    template <typename U, typename T>
    void TestReader(
        TVector<TRichYPath> paths,
        const TVector<TVector<U>>& rows,
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
                Y_ENSURE(path.GetRanges().Empty());
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
                for (size_t rowIndex = range.first; rowIndex < range.second; ++rowIndex) {
                    EXPECT_NE(resultIt, result.end());

                    EXPECT_EQ(resultIt->TableIndex, tableIndex);
                    EXPECT_EQ(resultIt->RowIndex, rowIndex);
                    EXPECT_EQ(resultIt->Row, tableRows[rowIndex]);

                    ++resultIt;
                }
            }
        }
        EXPECT_EQ(resultIt, result.end());
    }

    template <typename U, typename T>
    void TestReader(
        const TRichYPath& paths,
        const TVector<U>& rows,
        const TVector<T>& expectedRows,
        const TVector<std::pair<size_t, size_t>>& ranges,
        const TParallelTableReaderOptions& options)
    {
        TestReader<U, T>(TVector<TRichYPath>{paths}, TVector<TVector<U>>{rows}, TVector<TVector<T>>{expectedRows}, ranges, options);
    }

    void TestRanges(int tableCount, bool ordered)
    {
        constexpr size_t rowCount = 100;

        TVector<TRichYPath> paths;
        for (int i = 0; i != tableCount; ++i) {
            paths.push_back("table_" + ToString(i));
        }

        TVector<std::pair<size_t, size_t>> ranges = {{3, 27}, {29, 28}, {33, 59}, {62, 60}, {66, 67}, {67, 98}, {99, 100}};
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
                .Ordered(ordered)
                .BufferedRowCountLimit(10)
                .ThreadCount(5));
    }

    void TestNodeReader(ENodeReaderFormat format, bool ordered)
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
                .Ordered(ordered)
                .ThreadCount(10)
                .BufferedRowCountLimit(20));
    }

    void TestWithOutage(size_t readRetryCount, bool ordered, size_t responseCount = std::numeric_limits<size_t>::max())
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
                .Ordered(ordered)
                .ThreadCount(5)
                .BufferedRowCountLimit(5 * rangeSize));

        TVector<std::pair<ui64, TNode>> result;

        // Skip rangeSize rows. We hope that only these first rows will be read
        // by the main thread and the following outage will affect parallel threads.
        for (size_t i = 0; i != rangeSize; ++i) {
            EXPECT_TRUE(reader->IsValid());
            result.emplace_back(reader->GetRowIndex(), reader->MoveRow());
            reader->Next();
        }

        auto outage = TAbortableHttpResponse::StartOutage("/read_table", responseCount);

        for (; reader->IsValid(); reader->Next()) {
            result.emplace_back(reader->GetRowIndex(), reader->MoveRow());
        }
        if (!ordered) {
            SortBy(result, [] (const auto& p) { return p.first; });
        }
        EXPECT_EQ(result.size(), rowCount);
        for (size_t i = 0; i != rowCount; ++i) {
            EXPECT_EQ(i, result[i].first);
            EXPECT_EQ(TNode()("x", i), result[i].second);
        }
    }

    void TestLarge(int tableCount, bool ordered)
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
                .Ordered(ordered)
                .BufferedRowCountLimit(10)
                .ThreadCount(5)
                .RangeCount(10));
    }
};

TEST_P(TParallelReaderTest, SimpleYson)
{
    TestNodeReader(ENodeReaderFormat::Yson, GetParam());
}

TEST_P(TParallelReaderTest, SimpleSkiff)
{
    TestNodeReader(ENodeReaderFormat::Skiff, GetParam());
}

TEST_P(TParallelReaderTest, SimpleSkiffRow)
{
    TVector<TNode> rows;
    constexpr size_t rowCount = 100;
    rows.reserve(rowCount);
    TVector<TTestSkiffRow> expectedRows;
    expectedRows.reserve(rowCount);
    for (size_t i = 0; i != rowCount; ++i) {
        rows.push_back(TNode()("num", i)("str", ToString(i * i)));
        expectedRows.push_back(TTestSkiffRow(i, ToString(i * i), std::nullopt));
    }

    TestReader(
        "table",
        rows,
        expectedRows,
        {{0, rowCount}},
        TParallelTableReaderOptions()
            .Ordered(GetParam())
            .ThreadCount(10)
            .BufferedRowCountLimit(20)
            .FormatHints(TFormatHints()
                .SkiffRowHints(TSkiffRowHints())));
}


TEST_P(TParallelReaderTest, SimpleSkiffRowSeveralTables)
{
    constexpr size_t rowCount = 100;
    constexpr int tableCount = 3;

    TVector<TRichYPath> paths;
    for (int i = 0; i != tableCount; ++i) {
        paths.push_back("table_" + ToString(i));
    }

    TVector<TVector<TNode>> writtenRows;
    TVector<TVector<TTestSkiffRow>> expectedRows;

    for (size_t tableIndex = 0; tableIndex != paths.size(); ++tableIndex) {
        writtenRows.emplace_back();
        writtenRows.back().reserve(rowCount);
        expectedRows.emplace_back();
        expectedRows.back().reserve(rowCount);
        for (size_t i = 0; i != rowCount; ++i) {
            auto row = TNode()("num", i);
            TTestSkiffRow expectedRow;
            expectedRow.Num = i;
            if (tableIndex == 0) {
                auto strValue = ToString(i * i);
                row = row("str", strValue);
                expectedRow.Str = strValue;
            }
            writtenRows.back().push_back(row);
            expectedRows.back().push_back(expectedRow);
        }
    }

    TestReader(
        paths,
        writtenRows,
        expectedRows,
        {{0, rowCount}},
        TParallelTableReaderOptions()
            .Ordered(GetParam())
            .BufferedRowCountLimit(10)
            .ThreadCount(5)
            .FormatHints(TFormatHints()
                .SkiffRowHints(TSkiffRowHints())));
}

TEST_P(TParallelReaderTest, SimpleProtobuf)
{
    TVector<TTestMessage> rows;
    constexpr size_t rowCount = 100;
    rows.reserve(rowCount);
    for (size_t i = 0; i != rowCount; ++i) {
        TTestMessage row;
        row.set_key("x");
        row.set_value(i);
        rows.push_back(std::move(row));
    }
    TestReader(
        "table",
        rows,
        rows,
        /* ranges */ {{0, rowCount}},
        TParallelTableReaderOptions()
            .Ordered(GetParam())
            .ThreadCount(10)
            .BufferedRowCountLimit(20));
}

TEST_P(TParallelReaderTest, SimpleYaMR)
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
            .Ordered(GetParam())
            .ThreadCount(10)
            .BufferedRowCountLimit(20));
}

TEST_P(TParallelReaderTest, EmptyTable)
{
    TestReader(
        "table",
        TVector<TNode>{},
        TVector<TNode>{},
        /* ranges */ {{0,0}},
        TParallelTableReaderOptions()
            .Ordered(GetParam())
            .ThreadCount(10)
            .BufferedRowCountLimit(20));
}

TEST_P(TParallelReaderTest, EmptyRanges)
{
    TVector<TNode> rows;

    for (int64_t i = 0; i < 10; ++i) {
        rows.push_back(TNode()("x", i));
    }

    TestReader(
        "table",
        rows,
        rows,
        /* ranges */ {{4, 2}, {0, 2}, {5, 5}, {3, 0}, {3, 4}},
        TParallelTableReaderOptions()
            .Ordered(GetParam())
            .ThreadCount(10)
            .RangeCount(1)
            .BufferedRowCountLimit(20));
}

TEST_P(TParallelReaderTest, SmallTable)
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
            .Ordered(GetParam())
            .ThreadCount(2)
            .BufferedRowCountLimit(2000));
}

TEST_P(TParallelReaderTest, RangesSingleTable)
{
    TestRanges(1, GetParam());
}

TEST_P(TParallelReaderTest, RangesSeveralTables)
{
    TestRanges(3, GetParam());
}

TEST_P(TParallelReaderTest, NetworkProblemsFullOutage)
{
    try {
        TestWithOutage(3, GetParam(), std::numeric_limits<size_t>::max());
    } catch (const std::exception& exception) {
        // We cannot catch the TAbortedForTestReason exception here as it is caught
        // inside the client reader and propagated as simple yexception.
        EXPECT_THAT(exception.what(), testing::HasSubstr("response was aborted"));
    }
}

TEST_P(TParallelReaderTest, NetworkProblemsRetriableOutage)
{
    TestWithOutage(3, GetParam(), 2);
}

TEST_P(TParallelReaderTest, WithColumnSelector)
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
        TParallelTableReaderOptions().Ordered(GetParam()));
}

TEST_P(TParallelReaderTest, LargeSingleTable)
{
    TestLarge(1, GetParam());
}

TEST_P(TParallelReaderTest, LargeSeveralTables)
{
    TestLarge(3, GetParam());
}

TEST_P(TParallelReaderTest, LargeYaMR)
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
            .Ordered(GetParam())
            .ThreadCount(3)
            .MemoryLimit(1 << 20)
            .RangeCount(10));
}

TEST_P(TParallelReaderTest, ReadWithProcessor)
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
        EXPECT_TRUE(tableIndex == 0 || tableIndex == 1);
        auto myIndex = index.fetch_add(1);
        EXPECT_LT(myIndex, std::ssize(paths) * ThreadCount);
        for (; reader->IsValid(); reader->Next()) {
            batches[myIndex].push_back(reader->MoveRow());
        }
    };

    if (GetParam()) {
        EXPECT_THROW(
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
            .Ordered(GetParam())
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
    EXPECT_EQ(rows.size(), paths.size() * RowCount);
    for (auto i = 0; i < std::ssize(rows); ++i) {
        EXPECT_EQ(rows[i], TNode()("key", i / std::ssize(paths)));
    }
}

class TMyTestException
    : public yexception
{ };

TEST_P(TParallelReaderTest, ReadWithProcessorError)
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

    if (GetParam()) {
        EXPECT_THROW(
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

    EXPECT_THROW(
        ReadTableInParallel(
            client,
            path,
            processor,
            TParallelTableReaderOptions()
                .Ordered(GetParam())
                .ThreadCount(ThreadCount)
                .MemoryLimit(1 << 20)),
        TMyTestException);
}

TEST_P(TParallelReaderTest, ExceptionInFirstReadRow)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();

    auto testTable = fixture.GetWorkingDir() + "/test-table";

    // Row with integer key is not valid TYaMRRow
    NYT::NTesting::WriteTable(client, testTable, std::vector{TNode()("key", 1), TNode()("key", 2)});

    auto failingFunction = [&] {
        auto reader = CreateParallelTableReader<TYaMRRow>(client, testTable, TParallelTableReaderOptions().Ordered(GetParam()));
        reader->Next();
    };

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(failingFunction(), std::exception, "YAMR");
}

INSTANTIATE_TEST_SUITE_P(Ordered, TParallelReaderTest, ::testing::Values(true));
INSTANTIATE_TEST_SUITE_P(Unordered, TParallelReaderTest, ::testing::Values(true));

TEST(EstimateRowWeight, Simple)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();

    auto pathOne = fixture.GetWorkingDir() + "/table1";
    auto pathTwo = fixture.GetWorkingDir() + "/table2";

    {
        for (size_t chunkId = 0; chunkId < 3; ++chunkId) {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(pathOne).Append(true));
            for (size_t rowId = 0; rowId < 1000; ++rowId) {
                writer->AddRow(TNode()("x", rowId)("y", rowId + 10));
            }
            writer->Finish();
        }
    }

    {
        for (size_t chunkId = 0; chunkId < 2; ++chunkId) {
            auto writer = client->CreateTableWriter<TNode>(TRichYPath(pathTwo).Append(true));
            for (size_t rowId = 0; rowId < 500; ++rowId) {
                writer->AddRow(TNode()("a", rowId)("b", rowId + 10));
            }
            writer->Finish();
        }
    }

    auto pathWithoutRangesOne = TRichYPath(pathOne).Columns({"x", "y"});
    auto pathWithoutRangesTwo = TRichYPath(pathTwo).Columns({"a", "b"});

    auto weightWithoutRanges = NYT::NDetail::EstimateTableRowWeight(client, {pathWithoutRangesOne, pathWithoutRangesTwo});
    auto columnarStatisticsWithoutRanges = client->GetTableColumnarStatistics({pathWithoutRangesOne, pathWithoutRangesTwo});

    auto pathWithRangesOne = pathWithoutRangesOne.AddRange(TReadRange()
                    .LowerLimit(TReadLimit().RowIndex(0))
                    .UpperLimit(TReadLimit().RowIndex(10)));

    auto pathWithRangesTwo = pathWithoutRangesTwo.AddRange(TReadRange()
                    .LowerLimit(TReadLimit().RowIndex(0))
                    .UpperLimit(TReadLimit().RowIndex(20)));

    auto weightWithRanges = NYT::NDetail::EstimateTableRowWeight(client, {pathWithRangesOne, pathWithRangesTwo});
    auto columnarStatisticsWithRanges = client->GetTableColumnarStatistics({pathWithRangesOne, pathWithRangesTwo});

    EXPECT_EQ(2u, columnarStatisticsWithoutRanges.size());
    EXPECT_EQ(2u, columnarStatisticsWithRanges.size());

    // Check that columns statistics are different if request it with ranges.
    EXPECT_TRUE(columnarStatisticsWithRanges[0].ColumnDataWeight["x"] != columnarStatisticsWithoutRanges[0].ColumnDataWeight["x"]);
    EXPECT_EQ(weightWithRanges, weightWithoutRanges);

    auto weightOnlyOneTable = NYT::NDetail::EstimateTableRowWeight(client, {pathWithRangesOne});
    EXPECT_EQ(weightOnlyOneTable, weightWithRanges);
}
