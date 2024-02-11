#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/sorted_merging_reader.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <random>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TResultStorage
{
public:
    void OnUnreadRows(const std::vector<TUnversionedRow>& unreadRows)
    {
        UnreadRows_.reserve(unreadRows.size());
        for (const auto& row : unreadRows) {
            UnreadRows_.push_back(TUnversionedOwningRow(row));
        }
    }

    int GetUnreadRowCount() const
    {
        return UnreadRows_.size();
    }

    TString GetFirstUnreadRow() const
    {
        return ToString(UnreadRows_.front());
    }

    const std::vector<TUnversionedOwningRow>& GetUnreadRows() const
    {
        return UnreadRows_;
    }

private:
    std::vector<TUnversionedOwningRow> UnreadRows_;
};

////////////////////////////////////////////////////////////////////////////////

struct TRawTableData
{
    TString Schema;
    std::vector<TString> Rows;
};

struct TTableData
{
    TTableSchema Schema;
    std::vector<TUnversionedOwningRow> Rows;

    TTableData() = default;

    TTableData(const TRawTableData& rawTableData)
    {
        Schema = ConvertTo<TTableSchema>(TYsonString(rawTableData.Schema));
        Rows.reserve(rawTableData.Rows.size());
        for (const auto& rawRow : rawTableData.Rows) {
            Rows.push_back(YsonToSchemafulRow(rawRow, Schema, /*treatMissingAsNull*/false));
        }
    }
};

//! Generate a random table with #columnCount columns sorted by #comparator and #rowCount rows.
//! Table values are integers from [0, #valuesRange).
TTableData RandomTable(int columnCount, TComparator comparator, int rowCount, int valuesRange, int seed)
{
    std::mt19937 rng(seed);

    TTableData tableData;

    int keyColumnCount = comparator.GetLength();

    std::vector<TColumnSchema> columns;
    columns.reserve(columnCount);
    for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
        std::optional<ESortOrder> sortOrder;
        if (columnIndex < keyColumnCount) {
            sortOrder = comparator.SortOrders()[columnIndex];
        }
        TColumnSchema columnSchema(
            /*name*/Format("c%v", columnIndex),
            /*type*/EValueType::Int64,
            /*sortOrder*/sortOrder);
        columns.push_back(std::move(columnSchema));
    }

    tableData.Schema = TTableSchema(std::move(columns));

    tableData.Rows.reserve(rowCount);
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        TUnversionedOwningRowBuilder builder;
        for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
            builder.AddValue(MakeUnversionedInt64Value(rng() % valuesRange));
        }
        tableData.Rows.push_back(builder.FinishRow());
    }

    std::sort(tableData.Rows.begin(), tableData.Rows.end(), [&] (auto lhs, auto rhs) {
        auto lhsKey = TKey::FromRow(lhs, keyColumnCount);
        auto rhsKey = TKey::FromRow(rhs, keyColumnCount);
        return comparator.CompareKeys(lhsKey, rhsKey) < 0;
    });

    return tableData;
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow AddTableIndex(
    TUnversionedRow row,
    int tableIndex,
    int tableIndexId)
{
    TUnversionedOwningRowBuilder builder;
    for (const auto& value : row) {
        builder.AddValue(value);
    }
    builder.AddValue(MakeUnversionedInt64Value(tableIndex, tableIndexId));

    return builder.FinishRow();
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessMultiChunkFakeReader
    : public ISchemalessMultiChunkReader
{
public:
    TSchemalessMultiChunkFakeReader(
        TTableData tableData,
        int inputTableIndex,
        TResultStorage* resultStorage = nullptr)
        : TableSchema_(std::move(tableData.Schema))
        , KeyColumns_(TableSchema_.GetKeyColumns())
        , ResultStorage_(resultStorage)
    {
        for (int i = 0; i < TableSchema_.GetColumnCount(); ++i) {
            NameTable_->RegisterName(TableSchema_.Columns()[i].Name());
        }
        int tableIndexId = NameTable_->RegisterName(TableIndexColumnName);

        for (const auto& row : tableData.Rows) {
            TableRows_.push_back(AddTableIndex(row, inputTableIndex, tableIndexId));
        }
    }

    TFuture<void> GetReadyEvent() const override
    {
        return VoidFuture;
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        YT_ABORT();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        YT_UNIMPLEMENTED();
    }

    NTableClient::TTimingStatistics GetTimingStatistics() const override
    {
        return {};
    }

    bool IsFetchingCompleted() const override
    {
        return true;
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        YT_ABORT();
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        Rows_.clear();
        if (Interrupted_ || RowIndex_ >= std::ssize(TableRows_)) {
            return nullptr;
        }
        std::vector<TUnversionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);
        while (std::ssize(rows) < options.MaxRowsPerRead && RowIndex_ < std::ssize(TableRows_)) {
            auto row = TableRows_[RowIndex_];
            Rows_.push_back(row);
            rows.push_back(row);
            ++RowIndex_;
        }
        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    i64 GetTableRowIndex() const override
    {
        return RowIndex_;
    }

    TInterruptDescriptor GetInterruptDescriptor(TRange<TUnversionedRow> unreadRows) const override
    {
        std::vector<TUnversionedRow> rows(unreadRows.begin(), unreadRows.end());
        for (int rowIndex = RowIndex_; rowIndex < std::ssize(TableRows_); ++rowIndex) {
            rows.push_back(TableRows_[rowIndex]);
        }
        ResultStorage_->OnUnreadRows(rows);
        return {};
    }

    i64 GetSessionRowIndex() const override
    {
        return RowIndex_;
    }

    i64 GetTotalRowCount() const override
    {
        return TableRows_.size();
    }

    void Interrupt() override
    {
        Interrupted_ = true;
    }

    void SkipCurrentReader() override
    {
        YT_ABORT();
    }

    const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    std::vector<TUnversionedOwningRow> TableRows_;
    const TTableSchema TableSchema_;
    const TKeyColumns KeyColumns_;
    const TNameTablePtr NameTable_ = New<TNameTable>();

    TResultStorage* ResultStorage_ = nullptr;

    int RowIndex_ = 0;
    bool Interrupted_ = false;
    std::vector<TUnversionedOwningRow> Rows_;
};

////////////////////////////////////////////////////////////////////////////////

class TSortedMergingReaderTest
    : public ::testing::Test
{
protected:
    using TReaderFactory = std::function<ISchemalessMultiChunkReaderPtr(std::vector<TResultStorage>* resultStorage)>;

    void ReadAndCheckResult(
        TReaderFactory createReader,
        std::vector<TResultStorage>* resultStorage,
        int rowsPerRead,
        int interruptRowCount,
        int expectedReadRowCount,
        TString expectedLastReadRow,
        std::vector<std::pair<int, TString>> expectedResult)
    {
        auto reader = createReader(resultStorage);

        TRowBatchReadOptions options{
            .MaxRowsPerRead = rowsPerRead
        };

        int readRowCount = 0;
        TString lastReadRow;

        bool interrupted = false;
        auto maybeInterrupt = [&] {
            if (readRowCount >= interruptRowCount && !interrupted) {
                reader->Interrupt();
                interrupted = true;
            }
        };

        maybeInterrupt();

        while (auto batch = reader->Read(options)) {
            if (batch->IsEmpty()) {
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            auto rows = batch->MaterializeRows();
            lastReadRow = ToString(rows.Back());
            readRowCount += rows.size();
            maybeInterrupt();
        }

        reader->GetInterruptDescriptor(NYT::TRange<TUnversionedRow>());
        EXPECT_EQ(readRowCount, expectedReadRowCount);
        EXPECT_EQ(lastReadRow, expectedLastReadRow);
        for (int primaryTableId = 0; primaryTableId < static_cast<int>(resultStorage->size()); ++primaryTableId) {
            EXPECT_EQ((*resultStorage)[primaryTableId].GetUnreadRowCount(), expectedResult[primaryTableId].first);
            if ((*resultStorage)[primaryTableId].GetUnreadRowCount() != 0) {
                EXPECT_EQ((*resultStorage)[primaryTableId].GetFirstUnreadRow(), expectedResult[primaryTableId].second);
            }
        }
    }

    std::vector<TString> ReadAll(
        TReaderFactory createReader,
        std::vector<TResultStorage>* resultStorage)
    {
        std::vector<TString> result;
        auto reader = createReader(resultStorage);
        while (auto batch = reader->Read()) {
            if (batch->IsEmpty()) {
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }
            for (auto row : batch->MaterializeRows()) {
                result.push_back(ToString(row));
            }
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

const TRawTableData tableData0 = {
    "<strict=%false>["
        "{name = c0; type = string; sort_order = ascending};"
        "{name = c1; type = int64; sort_order = ascending};"
        "{name = c2; type = uint64; sort_order = ascending}; ]",
    {
        "c0=ab; c1=1; c2=21u",
        "c0=ab; c1=1; c2=22u",
        "c0=bb; c1=2; c2=23u",
        "c0=bb; c1=2; c2=24u",
        "c0=cb; c1=3; c2=25u",
        "c0=cb; c1=3; c2=26u",
    }
};

const TRawTableData tableData1 = {
    "<strict=%false>["
        "{name = c0; type = string; sort_order = ascending};"
        "{name = c1; type = int64; sort_order = ascending};"
        "{name = c2; type = uint64; sort_order = ascending}; ]",
    {
        "c0=aa; c1=1; c2=1u",
        "c0=ab; c1=3; c2=3u",
        "c0=ac; c1=5; c2=5u",
        "c0=ba; c1=7; c2=7u",
        "c0=bb; c1=9; c2=9u",
        "c0=bc; c1=11; c2=11u",
        "c0=ca; c1=13; c2=13u",
        "c0=cb; c1=15; c2=15u",
        "c0=cc; c1=17; c2=17u",
    }
};

const TRawTableData tableData2 = {
    "<strict=%false>["
        "{name = c0; type = string; sort_order = ascending};"
        "{name = c1; type = int64};"
        "{name = c2; type = uint64}; ]",
    {
        "c0=aa; c1=2; c2=2u",
        "c0=ab; c1=4; c2=4u",
        "c0=ac; c1=6; c2=6u",
        "c0=ba; c1=8; c2=8u",
        "c0=bb; c1=10; c2=10u",
        "c0=bc; c1=12; c2=12u",
        "c0=ca; c1=14; c2=14u",
        "c0=cb; c1=16; c2=16u",
        "c0=cc; c1=18; c2=18u",
    }
};

const TRawTableData tableData3 = {
    "<strict=%false>["
        "{name = c0; type = string; sort_order = ascending};"
        "{name = c1; type = int64}; ]",
    {
        "c0=a; c1=1",
        "c0=a; c1=3",
        "c0=a; c1=5",
    }
};

const TRawTableData tableData4 = {
    "<strict=%false>["
        "{name = c0; type = string; sort_order = ascending};"
        "{name = c1; type = int64}; ]",
    {
        "c0=a; c1=2",
        "c0=a; c1=4",
        "c0=a; c1=6",
    }
};

const TRawTableData tableData5 = {
    "<strict=%false>["
        "{name = c0; type = string; sort_order = ascending}; ]",
    {
        "c0=a; c1=3",
        "c0=a; c1=3",
        "c0=a; c1=3",
        "c0=b; c1=3",
        "c0=b; c1=3",
        "c0=b; c1=3",
    }
};

const TRawTableData tableData6 = {
    "<strict=%false>["
        "{name = c0; type = string; sort_order = ascending}; ]",
    {
        "c0=a; c1=4",
        "c0=b; c1=4",
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedMergingReaderTest, SortedMergingReaderSingleTable)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(1);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData0, 0, &(*resultStorage)[0]));

        TComparator sortComparator(std::vector<ESortOrder>(2, ESortOrder::Ascending));
        TComparator reduceComparator(std::vector<ESortOrder>(2, ESortOrder::Ascending));
        return CreateSortedMergingReader(primaryReaders, sortComparator, reduceComparator, false);
    };

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);
    for (int interruptRowCount = 0; interruptRowCount < static_cast<int>(rows.size()); ++interruptRowCount) {
        int rowsPerRead = 1;
        auto lastRow = interruptRowCount != 0 ? rows[interruptRowCount - 1] : TString("");
        ReadAndCheckResult(
            createReader,
            &resultStorage,
            rowsPerRead,
            interruptRowCount,
            interruptRowCount,
            lastRow,
            {
                {rows.size() - interruptRowCount, rows[interruptRowCount]},
            });
    }
}

TEST_F(TSortedMergingReaderTest, SortedMergingReaderMultipleTablesInterruptAtKeyEdge)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(2);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders{
            // NB: Table indexes are not passed to readers.
            New<TSchemalessMultiChunkFakeReader>(tableData1, 0, &(*resultStorage)[0]),
            New<TSchemalessMultiChunkFakeReader>(tableData2, 0, &(*resultStorage)[1])
        };

        TComparator sortComparator(std::vector<ESortOrder>(3, ESortOrder::Ascending));
        TComparator reduceComparator(std::vector<ESortOrder>(2, ESortOrder::Ascending));
        return CreateSortedMergingReader(primaryReaders, sortComparator, reduceComparator, true);
    };

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 4;
    int rowsPerRead = 1;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        interruptRowCount,
        rows[interruptRowCount - 1],
        {
            {7, rows[4]},
            {7, rows[5]},
        });

    interruptRowCount = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        interruptRowCount,
        rows[interruptRowCount - 1],
        {
            {6, rows[6]},
            {7, rows[5]},
        });
}

TEST_F(TSortedMergingReaderTest, SortedMergingReaderStressTest)
{
    std::mt19937 rng(42);

    constexpr int TableCount = 3;
    constexpr int RowCount = 20;
    constexpr int ColumnCount = 2;
    constexpr int KeyColumnCount = 2;
    constexpr int ReduceColumnCount = 1;
    constexpr int ValuesRange = 30;
    constexpr int MaxRowsPerRead = 4;
    constexpr int Iterations = 15000;

    for (int iteration = 0; iteration < Iterations; ++iteration) {
        std::vector<ESortOrder> sortOrders;
        for (int columnIndex = 0; columnIndex < KeyColumnCount; ++columnIndex) {
            if (rng() % 2 == 0) {
                sortOrders.push_back(ESortOrder::Ascending);
            } else {
                sortOrders.push_back(ESortOrder::Descending);
            }
        }

        TComparator sortComparator(sortOrders);
        sortOrders.resize(ReduceColumnCount);
        TComparator reduceComparator(sortOrders);

        std::vector<TTableData> inputTables;

        std::vector<TUnversionedOwningRow> expected;
        for (int tableIndex = 0; tableIndex < TableCount; ++tableIndex) {
            inputTables.push_back(
                RandomTable(
                    ColumnCount,
                    sortComparator,
                    rng() % RowCount,
                    (rng() % ValuesRange) + 1,
                    iteration * TableCount + tableIndex));
            for (const auto& row : inputTables.back().Rows) {
                expected.emplace_back(AddTableIndex(row, tableIndex, ColumnCount));
            }
        }

        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.reserve(TableCount);
        std::vector<TResultStorage> resultStorages;
        resultStorages.resize(TableCount);
        for (int tableIndex = 0; tableIndex < TableCount; ++tableIndex) {
            primaryReaders.push_back(
                New<TSchemalessMultiChunkFakeReader>(inputTables[tableIndex], tableIndex, &resultStorages[tableIndex]));
        }

        // Key edge interrupt is more interesting case.
        bool interruptAtKeyEdge = (rng() % 10 < 7);

        auto reader = CreateSortedMergingReader(primaryReaders, sortComparator, reduceComparator, interruptAtKeyEdge);

        std::sort(expected.begin(), expected.end(), [&] (auto lhs, auto rhs) {
            auto lhsKey = TKey::FromRow(lhs, KeyColumnCount);
            auto rhsKey = TKey::FromRow(rhs, KeyColumnCount);
            int comparisonResult = sortComparator.CompareKeys(lhsKey, rhsKey);
            if (comparisonResult != 0) {
                return comparisonResult < 0;
            }
            auto lhsTableIndex = lhs[lhs.GetCount() - 1];
            auto rhsTableIndex = rhs[rhs.GetCount() - 1];
            return lhsTableIndex < rhsTableIndex;
        });

        int interruptIndex = rng() % (expected.size() + 1);
        if (interruptIndex == 0) {
            reader->Interrupt();
            while (true) {
                TRowBatchReadOptions options{
                    .MaxRowsPerRead = static_cast<i64>(rng() % 2)
                };
                auto batch = reader->Read(options);
                if (!batch) {
                    break;
                }
                EXPECT_EQ(batch->GetRowCount(), 0);
            }
            continue;
        }

        auto interruptKey = TKey::FromRow(expected[interruptIndex - 1], ReduceColumnCount);
        int expectedReadRowCount = interruptIndex;

        if (interruptAtKeyEdge) {
            while (expectedReadRowCount < std::ssize(expected) &&
                reduceComparator.CompareKeys(
                    TKey::FromRow(expected[expectedReadRowCount], ReduceColumnCount),
                    interruptKey) == 0)
            {
                ++expectedReadRowCount;
            }
        }

        std::vector<TUnversionedOwningRow> rowsRead;
        while (std::ssize(rowsRead) < interruptIndex) {
            int rowsLeft = interruptIndex - rowsRead.size();
            int maxRowsPerRead = rng() % std::min(rowsLeft + 1, MaxRowsPerRead + 1);
            TRowBatchReadOptions options{
                .MaxRowsPerRead = maxRowsPerRead
            };

            auto batch = reader->Read(options);
            ASSERT_TRUE(static_cast<bool>(batch));
            if (batch->IsEmpty()) {
                auto waitResult = WaitFor(reader->GetReadyEvent());
                EXPECT_TRUE(waitResult.IsOK());
                continue;
            }

            auto rows = batch->MaterializeRows();
            for (const auto& row : rows) {
                rowsRead.push_back(TUnversionedOwningRow(row));
            }
        }

        EXPECT_EQ(std::ssize(rowsRead), interruptIndex);
        reader->Interrupt();

        while (true) {
            int maxRowsPerRead = rng() % (MaxRowsPerRead + 1);
            TRowBatchReadOptions options{
                .MaxRowsPerRead = maxRowsPerRead
            };

            auto batch = reader->Read(options);
            if (!batch) {
                break;
            }

            if (batch->IsEmpty()) {
                auto waitResult = WaitFor(reader->GetReadyEvent());
                EXPECT_TRUE(waitResult.IsOK());
                continue;
            }

            auto rows = batch->MaterializeRows();
            for (const auto& row : rows) {
                rowsRead.push_back(TUnversionedOwningRow(row));
            }
        }

        std::vector<TUnversionedOwningRow> expectedRead(expected.begin(), expected.begin() + expectedReadRowCount);

        EXPECT_EQ(expectedRead, rowsRead);

        reader->GetInterruptDescriptor(NYT::TRange<TUnversionedRow>());

        std::vector<std::vector<TUnversionedOwningRow>> expectedUnreadRows(TableCount);
        for (int rowIndex = expectedReadRowCount; rowIndex < std::ssize(expected); ++rowIndex) {
            const auto& row = expected[rowIndex];
            int tableIndex;
            FromUnversionedValue(&tableIndex, row[row.GetCount() - 1]);
            expectedUnreadRows[tableIndex].push_back(row);
        }

        for (int tableIndex = 0; tableIndex < TableCount; ++tableIndex) {
            EXPECT_EQ(resultStorages[tableIndex].GetUnreadRows(), expectedUnreadRows[tableIndex]);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedMergingReaderTest, SortedJoiningReaderForeignBeforeMultiplePrimary)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(2);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders{
            New<TSchemalessMultiChunkFakeReader>(tableData0, 1, &(*resultStorage)[0]),
            New<TSchemalessMultiChunkFakeReader>(tableData1, 2, &(*resultStorage)[1])
        };

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders{
            New<TSchemalessMultiChunkFakeReader>(tableData2, 0)
        };

        TComparator sortComparator(std::vector<ESortOrder>(3, ESortOrder::Ascending));
        TComparator reduceComparator(std::vector<ESortOrder>(2, ESortOrder::Ascending));
        TComparator joinComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        return CreateSortedJoiningReader(
            primaryReaders,
            sortComparator,
            reduceComparator,
            foreignReaders,
            joinComparator,
            true);
    };

    // Expected sequence of rows:
    // ["aa", 2, 2u, 0]
    // ["aa", 1, 1u, 2]
    // ["ab", 4, 4u, 0]
    // ["ab", 1, 21u, 1]
    // ["ab", 1, 22u, 1]
    // ["ab", 3, 3u, 2]
    // ["ac", 6, 6u, 0]
    // ["ac", 5, 5u, 2]
    // ["ba", 8, 8u, 0]
    // ["ba", 7, 7u, 2]
    // ["bb", 10, 10u, 0]
    // ["bb", 2, 23u, 1]
    // ["bb", 2, 24u, 1]
    // ["bb", 9, 9u, 2]
    // ["bc", 12, 12u, 0]
    // ["bc", 11, 11u, 2]
    // ["ca", 14, 14u, 0]
    // ["ca", 13, 13u, 2]
    // ["cb", 16, 16u, 0]
    // ["cb", 3, 25u, 1]
    // ["cb", 3, 26u, 1]
    // ["cb", 15, 15u, 2]
    // ["cc", 18, 18u, 0]
    // ["cc", 17, 17u, 2]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 3;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        3,
        rows[2],
        {
            {6, TString("[0#\"ab\", 1#1, 2#21u, 3#1]")},
            {8, TString("[0#\"ab\", 1#3, 2#3u, 3#2]")},
        });
    interruptRowCount = 4;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        5,
        rows[4],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#1]")},
            {8, TString("[0#\"ab\", 1#3, 2#3u, 3#2]")},
        });
    interruptRowCount = 5;
    rowsPerRead = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        5,
        rows[4],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#1]")},
            {8, TString("[0#\"ab\", 1#3, 2#3u, 3#2]")},
        });
    interruptRowCount = 6;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        6,
        rows[5],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#1]")},
            {7, TString("[0#\"ac\", 1#5, 2#5u, 3#2]")},
        });
    interruptRowCount = 7;
    rowsPerRead = 7;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        7,
        rows[6],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#1]")},
            {7, TString("[0#\"ac\", 1#5, 2#5u, 3#2]")},
        });
}

TEST_F(TSortedMergingReaderTest, SortedJoiningReaderMultiplePrimaryBeforeForeign)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(2);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData0, 0, &(*resultStorage)[0]));
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData1, 1, &(*resultStorage)[1]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData2, 2));

        TComparator sortComparator(std::vector<ESortOrder>(3, ESortOrder::Ascending));
        TComparator reduceComparator(std::vector<ESortOrder>(2, ESortOrder::Ascending));
        TComparator joinComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        return CreateSortedJoiningReader(
            primaryReaders,
            sortComparator,
            reduceComparator,
            foreignReaders,
            joinComparator,
            true);
    };

    // Expected sequence of rows:
    // ["aa", 1, 1u, 1]
    // ["aa", 2, 2u, 2]
    // ["ab", 1, 21u, 0]
    // ["ab", 1, 22u, 0]
    // ["ab", 3, 3u, 1]
    // ["ab", 4, 4u, 2]
    // ["ac", 5, 5u, 1]
    // ["ac", 6, 6u, 2]
    // ["ba", 7, 7u, 1]
    // ["ba", 8, 8u, 2]
    // ["bb", 2, 23u, 0]
    // ["bb", 2, 24u, 0]
    // ["bb", 9, 9u, 1]
    // ["bb", 10, 10u, 2]
    // ["bc", 11, 11u, 1]
    // ["bc", 12, 12u, 2]
    // ["ca", 13, 13u, 1]
    // ["ca", 14, 14u, 2]
    // ["cb", 3, 25u, 0]
    // ["cb", 3, 26u, 0]
    // ["cb", 15, 15u, 1]
    // ["cb", 16, 16u, 2]
    // ["cc", 17, 17u, 1]
    // ["cc", 18, 18u, 2]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 3;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        5,
        rows[5],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
            {8, TString("[0#\"ab\", 1#3, 2#3u, 3#1]")},
        });
    interruptRowCount = 4;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        5,
        rows[5],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
            {8, TString("[0#\"ab\", 1#3, 2#3u, 3#1]")},
        });
    interruptRowCount = 5;
    rowsPerRead = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        6,
        rows[5],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
            {7, TString("[0#\"ac\", 1#5, 2#5u, 3#1]")},
        });
    interruptRowCount = 6;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        6,
        rows[5],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
            {7, TString("[0#\"ac\", 1#5, 2#5u, 3#1]")},
        });
    interruptRowCount = 7;
    rowsPerRead = 7;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
            {6, TString("[0#\"ba\", 1#7, 2#7u, 3#1]")},
        });
}

TEST_F(TSortedMergingReaderTest, SortedJoiningReaderMultipleForeignBeforePrimary)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(1);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData0, 2, &(*resultStorage)[0]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData1, 0));
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData2, 1));

        TComparator sortComparator(std::vector<ESortOrder>(3, ESortOrder::Ascending));
        TComparator reduceComparator(std::vector<ESortOrder>(2, ESortOrder::Ascending));
        TComparator joinComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        return CreateSortedJoiningReader(
            primaryReaders,
            sortComparator,
            reduceComparator,
            foreignReaders,
            joinComparator,
            true);
    };

    // Expected sequence of rows:
    // ["ab", 3, 3u, 0]
    // ["ab", 4, 4u, 1]
    // ["ab", 1, 21u, 2]
    // ["ab", 1, 22u, 2]
    // ["bb", 9, 9u, 0]
    // ["bb", 10, 10u, 1]
    // ["bb", 2, 23u, 2]
    // ["bb", 2, 24u, 2]
    // ["cb", 15, 15u, 0]
    // ["cb", 16, 16u, 1]
    // ["cb", 3, 25u, 2]
    // ["cb", 3, 26u, 2]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 3;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 4;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 5;
    rowsPerRead = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        5,
        rows[4],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 6;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        6,
        rows[5],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 7;
    rowsPerRead = 7;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {2, TString("[0#\"cb\", 1#3, 2#25u, 3#2]")},
        });
}

TEST_F(TSortedMergingReaderTest, SortedJoiningReaderPrimaryBeforeMultipleForeign)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(1);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData0, 0, &(*resultStorage)[0]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData1, 1));
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData2, 2));

        TComparator sortComparator(std::vector<ESortOrder>(3, ESortOrder::Ascending));
        TComparator reduceComparator(std::vector<ESortOrder>(2, ESortOrder::Ascending));
        TComparator joinComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        return CreateSortedJoiningReader(
            primaryReaders,
            sortComparator,
            reduceComparator,
            foreignReaders,
            joinComparator,
            true);
    };

    // Expected sequence of rows:
    // ["ab", 1, 21u, 0]
    // ["ab", 1, 22u, 0]
    // ["ab", 3, 3u, 1]
    // ["ab", 4, 4u, 2]
    // ["bb", 2, 23u, 0]
    // ["bb", 2, 24u, 0]
    // ["bb", 9, 9u, 1]
    // ["bb", 10, 10u, 2]
    // ["cb", 3, 25u, 0]
    // ["cb", 3, 26u, 0]
    // ["cb", 15, 15u, 1]
    // ["cb", 16, 16u, 2]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 3;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
        });
    interruptRowCount = 4;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
        });
    interruptRowCount = 5;
    rowsPerRead = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {2, TString("[0#\"cb\", 1#3, 2#25u, 3#0]")},
        });
    interruptRowCount = 6;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {2, TString("[0#\"cb\", 1#3, 2#25u, 3#0]")},
        });
    interruptRowCount = 7;
    rowsPerRead = 7;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {2, TString("[0#\"cb\", 1#3, 2#25u, 3#0]")},
        });
}

TEST_F(TSortedMergingReaderTest, SortedJoiningReaderForeignBeforePrimary)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(1);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData0, 2, &(*resultStorage)[0]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData1, 0));
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData2, 1));

        TComparator sortComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        TComparator reduceComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        TComparator joinComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        return CreateSortedJoiningReader(
            primaryReaders,
            sortComparator,
            reduceComparator,
            foreignReaders,
            joinComparator,
            false);
    };

    // Expected sequence of rows:
    // ["ab", 3, 3u, 0]
    // ["ab", 4, 4u, 1]
    // ["ab", 1, 21u, 2]
    // ["ab", 1, 22u, 2]
    // ["bb", 9, 9u, 0]
    // ["bb", 10, 10u, 1]
    // ["bb", 2, 23u, 2]
    // ["bb", 2, 24u, 2]
    // ["cb", 15, 15u, 0]
    // ["cb", 16, 16u, 1]
    // ["cb", 3, 25u, 2]
    // ["cb", 3, 26u, 2]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 3;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        3,
        rows[2],
        {
            {5, TString("[0#\"ab\", 1#1, 2#22u, 3#2]")},
        });
    interruptRowCount = 4;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 5;
    rowsPerRead = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        5,
        rows[4],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 6;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        6,
        rows[5],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 7;
    rowsPerRead = 7;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        7,
        rows[6],
        {
            {3, TString("[0#\"bb\", 1#2, 2#24u, 3#2]")},
        });
}

TEST_F(TSortedMergingReaderTest, SortedJoiningReaderPrimaryBeforeForeign)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(1);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData0, 0, &(*resultStorage)[0]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData1, 1));
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData2, 2));

        TComparator sortComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        TComparator reduceComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        TComparator joinComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        return CreateSortedJoiningReader(
            primaryReaders,
            sortComparator,
            reduceComparator,
            foreignReaders,
            joinComparator,
            false);
    };

    // Expected sequence of rows:
    // ["ab", 1, 21u, 0]
    // ["ab", 1, 22u, 0]
    // ["ab", 3, 3u, 1]
    // ["ab", 4, 4u, 2]
    // ["bb", 2, 23u, 0]
    // ["bb", 2, 24u, 0]
    // ["bb", 9, 9u, 1]
    // ["bb", 10, 10u, 2]
    // ["cb", 3, 25u, 0]
    // ["cb", 3, 26u, 0]
    // ["cb", 15, 15u, 1]
    // ["cb", 16, 16u, 2]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 3;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
        });
    interruptRowCount = 4;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
        });
    interruptRowCount = 5;
    rowsPerRead = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        7,
        rows[7], // Note: rows[6] should be skipped
        {
            {3, TString("[0#\"bb\", 1#2, 2#24u, 3#0]")},
        });
    interruptRowCount = 6;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {2, TString("[0#\"cb\", 1#3, 2#25u, 3#0]")},
        });
    interruptRowCount = 7;
    rowsPerRead = 7;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {2, TString("[0#\"cb\", 1#3, 2#25u, 3#0]")},
        });
}

TEST_F(TSortedMergingReaderTest, InterruptOnReduceKeyChange)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(1);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData3, 0, &(*resultStorage)[0]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData4, 1));

        TComparator sortComparator(std::vector<ESortOrder>(2, ESortOrder::Ascending));
        TComparator reduceComparator(std::vector<ESortOrder>(2, ESortOrder::Ascending));
        TComparator joinComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        return CreateSortedJoiningReader(
            primaryReaders,
            sortComparator,
            reduceComparator,
            foreignReaders,
            joinComparator,
            true);
    };

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 1;
    int rowsPerRead = 1;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[5],
        {
            {2, TString("[0#\"a\", 1#3, 2#0]")},
        });
}

TEST_F(TSortedMergingReaderTest, SortedJoiningReaderEqualKeys)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(2);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders{
            New<TSchemalessMultiChunkFakeReader>(tableData3, 0, &(*resultStorage)[0]),
            New<TSchemalessMultiChunkFakeReader>(tableData4, 1, &(*resultStorage)[1])
        };

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders{
            New<TSchemalessMultiChunkFakeReader>(tableData1, 0)
        };

        TComparator sortComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        TComparator reduceComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        TComparator joinComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        return CreateSortedJoiningReader(
            primaryReaders,
            sortComparator,
            reduceComparator,
            foreignReaders,
            joinComparator,
            false);
    };

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 2;
    int rowsPerRead = 1;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        interruptRowCount,
        rows[interruptRowCount - 1],
        {
            {1, rows[2]},
            {3, rows[3]},
        });

    interruptRowCount = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        interruptRowCount,
        rows[interruptRowCount - 1],
        {
            {0, ""},
            {1, rows[5]},
        });
}

TEST_F(TSortedMergingReaderTest, SortedJoiningReaderCheckLastRows)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(1);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData5, 1, &(*resultStorage)[0]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData6, 0));

        TComparator sortComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        TComparator reduceComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        TComparator joinComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        return CreateSortedJoiningReader(
            primaryReaders,
            sortComparator,
            reduceComparator,
            foreignReaders,
            joinComparator,
            true);
    };

    // Expected sequence of rows:
    // ["a", 3, 0]
    // ["a", 3, 0]
    // ["a", 3, 0]
    // ["a", 4, 1]
    // ["b", 3, 0]
    // ["b", 3, 0]
    // ["b", 3, 0]
    // ["b", 4, 1]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 8;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {0, TString("")},
        });
}

TEST_F(TSortedMergingReaderTest, SortedJoiningReaderStressTest)
{
    std::mt19937 rng(42);

    constexpr int TableCount = 5;
    constexpr int RowCount = 20;
    constexpr int ColumnCount = 3;
    constexpr int KeyColumnCount = 3;
    constexpr int ReduceColumnCount = 2;
    constexpr int JoinColumnCount = 1;
    constexpr int ValuesRange = 50;
    constexpr int MaxRowsPerRead = 4;
    constexpr int Iterations = 15000;

    for (int iteration = 0; iteration < Iterations; ++iteration) {
        std::vector<ESortOrder> sortOrders;
        for (int columnIndex = 0; columnIndex < KeyColumnCount; ++columnIndex) {
            if (rng() % 2 == 0) {
                sortOrders.push_back(ESortOrder::Ascending);
            } else {
                sortOrders.push_back(ESortOrder::Descending);
            }
        }

        TComparator sortComparator(sortOrders);
        sortOrders.resize(ReduceColumnCount);
        TComparator reduceComparator(sortOrders);
        sortOrders.resize(JoinColumnCount);
        TComparator joinComparator(sortOrders);

        std::vector<TTableData> inputTables;
        std::vector<bool> isPrimaryTable;

        auto getTableIndex = [&] (const TUnversionedOwningRow& row) {
            int tableIndex;
            FromUnversionedValue(&tableIndex, row[row.GetCount() - 1]);
            return tableIndex;
        };

        auto isPrimaryRow = [&] (const TUnversionedOwningRow& row) {
            return isPrimaryTable[getTableIndex(row)];
        };

        std::vector<TUnversionedOwningRow> allPrimaryRows;
        std::vector<TUnversionedOwningRow> allForeignRows;
        for (int tableIndex = 0; tableIndex < TableCount; ++tableIndex) {
            inputTables.push_back(
                RandomTable(
                    ColumnCount,
                    sortComparator,
                    rng() % RowCount,
                    (rng() % ValuesRange) + 1,
                    iteration * TableCount + tableIndex));
            isPrimaryTable.push_back(static_cast<bool>(rng() % 2));
        }

        {
            int primaryTableCount = 0;
            for (auto isPrimary : isPrimaryTable) {
                if (isPrimary) {
                    ++primaryTableCount;
                }
            }

            // At least one primary table should exist.
            if (primaryTableCount == 0) {
                isPrimaryTable[rng() % isPrimaryTable.size()] = true;
            }
        }

        for (int tableIndex = 0; tableIndex < TableCount; ++tableIndex) {
            const auto& table = inputTables[tableIndex];
            bool isPrimary = isPrimaryTable[tableIndex];
            for (const auto& row : table.Rows) {
                auto rowWithTableIndex = AddTableIndex(row, tableIndex, ColumnCount);
                if (isPrimary) {
                    allPrimaryRows.push_back(rowWithTableIndex);
                } else {
                    allForeignRows.push_back(rowWithTableIndex);
                }
            }
        }

        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        std::vector<TResultStorage> resultStorages(TableCount);
        for (int tableIndex = 0; tableIndex < TableCount; ++tableIndex) {
            auto tableReader = New<TSchemalessMultiChunkFakeReader>(
                inputTables[tableIndex],
                tableIndex,
                &resultStorages[tableIndex]);
            if (isPrimaryTable[tableIndex]) {
                primaryReaders.push_back(std::move(tableReader));
            } else {
                foreignReaders.push_back(std::move(tableReader));
            }
        }

        // Key edge interrupt is more interesting case.
        bool interruptAtKeyEdge = (rng() % 10 < 7);

        auto reader = CreateSortedJoiningReader(
            primaryReaders,
            sortComparator,
            reduceComparator,
            foreignReaders,
            joinComparator,
            interruptAtKeyEdge);

        {
            THashSet<TKey> allPrimaryJoinKeys;
            for (const auto& primaryRow : allPrimaryRows) {
                auto joinKey = TKey::FromRow(primaryRow, joinComparator.GetLength());
                allPrimaryJoinKeys.insert(joinKey);
            }

            std::vector<TUnversionedOwningRow> allJoinedForeignRows;
            for (const auto& foreignRow : allForeignRows) {
                auto joinKey = TKey::FromRow(foreignRow, joinComparator.GetLength());
                if (allPrimaryJoinKeys.contains(joinKey)) {
                    allJoinedForeignRows.push_back(foreignRow);
                }
            }

            allForeignRows = allJoinedForeignRows;
        }

        std::sort(allPrimaryRows.begin(), allPrimaryRows.end(), [&] (auto lhs, auto rhs) {
            auto lhsKey = TKey::FromRow(lhs, KeyColumnCount);
            auto rhsKey = TKey::FromRow(rhs, KeyColumnCount);
            int comparisonResult = sortComparator.CompareKeys(lhsKey, rhsKey);
            if (comparisonResult != 0) {
                return comparisonResult < 0;
            }
            auto lhsTableIndex = lhs[lhs.GetCount() - 1];
            auto rhsTableIndex = rhs[rhs.GetCount() - 1];
            return lhsTableIndex < rhsTableIndex;
        });

        std::vector<TUnversionedOwningRow> allRows;
        for (const auto& row : allPrimaryRows) {
            allRows.push_back(row);
        }
        for (const auto& row : allForeignRows) {
            allRows.push_back(row);
        }

        // XXX(gritukan): That's weird.
        int primaryStreamTableIndex = -1;
        if (!allPrimaryRows.empty()) {
            primaryStreamTableIndex = getTableIndex(allPrimaryRows.front());
        }

        // NB(gritukan): stable sort is required here, since primary rows should
        // be sorted by stricter comparator.
        std::stable_sort(allRows.begin(), allRows.end(), [&] (auto lhs, auto rhs) {
            auto lhsKey = TKey::FromRow(lhs, joinComparator.GetLength());
            auto rhsKey = TKey::FromRow(rhs, joinComparator.GetLength());
            int comparisonResult = joinComparator.CompareKeys(lhsKey, rhsKey);
            if (comparisonResult != 0) {
                return comparisonResult < 0;
            }
            auto lhsTableIndex = getTableIndex(lhs);
            auto rhsTableIndex = getTableIndex(rhs);
            if (isPrimaryTable[lhsTableIndex]) {
                lhsTableIndex = primaryStreamTableIndex;
            }
            if (isPrimaryTable[rhsTableIndex]) {
                rhsTableIndex = primaryStreamTableIndex;
            }
            return lhsTableIndex < rhsTableIndex;
        });

        int interruptIndex = rng() % (allRows.size() + 1);
        bool emptyOutput = (interruptIndex == 0);
        int lastReadPrimaryRowIndex = interruptIndex - 1;
        if (interruptIndex) {
            while (lastReadPrimaryRowIndex >= 0 && !isPrimaryRow(allRows[lastReadPrimaryRowIndex])) {
                --lastReadPrimaryRowIndex;
            }
            if (lastReadPrimaryRowIndex < 0) {
                emptyOutput = true;
            }
        }
        if (emptyOutput) {
            reader->Interrupt();
            while (true) {
                TRowBatchReadOptions options{
                    .MaxRowsPerRead = static_cast<i64>(rng() % 2)
                };
                auto batch = reader->Read(options);
                if (!batch) {
                    break;
                }
                EXPECT_EQ(batch->GetRowCount(), 0);
            }
            continue;
        }

        YT_VERIFY(lastReadPrimaryRowIndex >= 0);
        auto lastReadPrimaryKey = TKey::FromRow(allRows[lastReadPrimaryRowIndex], reduceComparator.GetLength());

        auto readPrimaryRow = [&] (int rowIndex) {
            const auto& row = allRows[rowIndex];
            YT_VERIFY(isPrimaryRow(row));
            if (interruptAtKeyEdge) {
                auto primaryKey = TKey::FromRow(row, reduceComparator.GetLength());
                return reduceComparator.CompareKeys(primaryKey, lastReadPrimaryKey) <= 0;
            } else {
                return rowIndex <= lastReadPrimaryRowIndex;
            }
        };

        THashSet<TKey> readPrimaryJoinKeys;
        for (int rowIndex = 0; rowIndex < std::ssize(allRows); ++rowIndex) {
            const auto& row = allRows[rowIndex];
            if (isPrimaryRow(row) && readPrimaryRow(rowIndex)) {
                auto joinKey = TKey::FromRow(row, joinComparator.GetLength());
                readPrimaryJoinKeys.insert(joinKey);
            }
        }

        std::vector<TUnversionedOwningRow> expectedReadRows;
        for (int rowIndex = 0; rowIndex < std::ssize(allRows); ++rowIndex) {
            const auto& row = allRows[rowIndex];
            if (isPrimaryRow(row)) {
                if (readPrimaryRow(rowIndex)) {
                    auto primaryKey = TKey::FromRow(row, reduceComparator.GetLength());
                    if (reduceComparator.CompareKeys(primaryKey, lastReadPrimaryKey) <= 0) {
                        expectedReadRows.push_back(row);
                    }
                }
            } else {
                auto joinKey = TKey::FromRow(row, joinComparator.GetLength());
                if (readPrimaryJoinKeys.contains(joinKey) || rowIndex < interruptIndex) {
                    expectedReadRows.push_back(row);
                }
            }
        }

        std::vector<TUnversionedOwningRow> rowsRead;
        while (std::ssize(rowsRead) < interruptIndex) {
            int rowsLeft = interruptIndex - rowsRead.size();
            int maxRowsPerRead = rng() % std::min(rowsLeft + 1, MaxRowsPerRead + 1);
            TRowBatchReadOptions options{
                .MaxRowsPerRead = maxRowsPerRead
            };

            auto batch = reader->Read(options);
            EXPECT_TRUE(static_cast<bool>(batch));
            if (batch->IsEmpty()) {
                auto waitResult = WaitFor(reader->GetReadyEvent());
                EXPECT_TRUE(waitResult.IsOK());
                continue;
            }

            auto rows = batch->MaterializeRows();
            for (const auto& row : rows) {
                rowsRead.push_back(TUnversionedOwningRow(row));
            }
        }

        EXPECT_EQ(std::ssize(rowsRead), interruptIndex);
        reader->Interrupt();

        while (true) {
            int maxRowsPerRead = rng() % (MaxRowsPerRead + 1);
            TRowBatchReadOptions options{
                .MaxRowsPerRead = maxRowsPerRead
            };

            auto batch = reader->Read(options);
            if (!batch) {
                break;
            }

            if (batch->IsEmpty()) {
                auto waitResult = WaitFor(reader->GetReadyEvent());
                EXPECT_TRUE(waitResult.IsOK());
                continue;
            }

            auto rows = batch->MaterializeRows();
            for (const auto& row : rows) {
                rowsRead.push_back(TUnversionedOwningRow(row));
            }
        }

        EXPECT_EQ(expectedReadRows, rowsRead);

        reader->GetInterruptDescriptor(NYT::TRange<TUnversionedRow>());

        std::vector<std::vector<TUnversionedOwningRow>> expectedUnreadRows(TableCount);
        for (int rowIndex = 0; rowIndex < std::ssize(allRows); ++rowIndex) {
            const auto& row = allRows[rowIndex];
            if (isPrimaryRow(row) && !readPrimaryRow(rowIndex)) {
                expectedUnreadRows[getTableIndex(row)].push_back(row);
            }
        }

        for (int tableIndex = 0; tableIndex < TableCount; ++tableIndex) {
            if (isPrimaryTable[tableIndex]) {
                EXPECT_EQ(resultStorages[tableIndex].GetUnreadRows(), expectedUnreadRows[tableIndex]);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
