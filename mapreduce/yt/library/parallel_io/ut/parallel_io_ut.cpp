#include <mapreduce/yt/library/parallel_io/parallel_reader.h>

#include <mapreduce/yt/library/parallel_io/ut/test_message.pb.h>

#include <mapreduce/yt/http/abortable_http_response.h>

#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

#define DECLARE_PARALLEL_READER_SUIT(suiteName) \
    UNIT_TEST_SUITE(suiteName); \
    UNIT_TEST(SimpleYson); \
    UNIT_TEST(SimpleSkiff); \
    UNIT_TEST(SimpleProtobuf); \
    UNIT_TEST(EmptyTable); \
    UNIT_TEST(SmallTable); \
    UNIT_TEST(ReadRanges); \
    UNIT_TEST(NetworkProblemsFullOutage); \
    UNIT_TEST(NetworkProblemsRetriableOutage); \
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
            TParallelTableReaderOptions()
                .Ordered(Ordered)
                .ThreadCount(2)
                .BufferedRowCountLimit(2000));
    }

    void ReadRanges()
    {
        TVector<TNode> writtenRows;
        constexpr size_t rowCount = 100;
        writtenRows.reserve(rowCount);
        for (size_t i = 0; i != rowCount; ++i) {
            writtenRows.push_back(TNode()("x", i));
        }

        TVector<std::pair<size_t, size_t>> ranges = {{3, 27}, {33, 59}, {66, 67}, {67, 98}, {99, 100}};
        TVector<TReadRange> pathRanges;
        for (const auto& range : ranges) {
            pathRanges.push_back(TReadRange()
                .LowerLimit(TReadLimit().RowIndex(range.first))
                .UpperLimit(TReadLimit().RowIndex(range.second)));
        }
        TRichYPath path("//testing/table");
        path.Ranges_ = pathRanges;

        auto result = WriteAndRead(
            path,
            writtenRows,
            TParallelTableReaderOptions()
                .Ordered(Ordered)
                .BufferedRowCountLimit(10)
                .ThreadCount(5));
        if (!Ordered) {
            SortBy(result, [] (const auto& p) { return p.first; });
        }

        auto it = result.begin();
        for (const auto& range : ranges) {
            for (size_t i = range.first; i != range.second; ++i) {
                UNIT_ASSERT_UNEQUAL(it, result.end());
                UNIT_ASSERT_VALUES_EQUAL(it->first, i);
                UNIT_ASSERT_VALUES_EQUAL(it->second["x"].AsUint64(), i);
                ++it;
            }
        }
        UNIT_ASSERT_EQUAL(it, result.end());
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

private:
    template <typename T>
    TVector<std::pair<ui64, T>> WriteAndRead(
        const TRichYPath& path,
        const TVector<T>& writtenRows,
        const TParallelTableReaderOptions& options)
    {
        auto client = CreateTestClient();
        {
            auto writer = client->CreateTableWriter<T>(path);
            for (const auto& row : writtenRows) {
                writer->AddRow(row);
            };
            writer->Finish();
        }

        TVector<std::pair<ui64, T>> result;
        {
            auto reader = CreateParallelTableReader<T>(client, path, options);
            for (; reader->IsValid(); reader->Next()) {
                result.emplace_back(reader->GetRowIndex(), reader->MoveRow());
            }
        }
        return result;
    }

    template <typename T>
    void TestReader(
        const TRichYPath& path,
        const TVector<T>& rows,
        const TParallelTableReaderOptions& options)
    {
        auto result = WriteAndRead(path, rows, options);
        UNIT_ASSERT_VALUES_EQUAL(result.size(), rows.size());
        if (!options.Ordered_) {
            SortBy(result, [] (const auto& p) { return p.first; });
        }

        for (size_t i = 0; i != rows.size(); ++i) {
            const auto& [resultRowIndex, resultRow] = result[i];
            UNIT_ASSERT_VALUES_EQUAL(resultRowIndex, i);
            UNIT_ASSERT_VALUES_EQUAL(resultRow, rows[i]);
        }
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
