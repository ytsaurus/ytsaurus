#include <yt/yt/ytlib/push_based_shuffle_client/sorted_merging_reader.h>

#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <utility>
#include <vector>

namespace NYT::NPushBasedShuffleClient {
namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

constexpr int WriterIdColumnId = 10;
constexpr int RowIdColumnId = 11;

struct TSourceBatchHolder
    : public TRefCounted
{
    TRowBufferPtr RowBuffer = New<TRowBuffer>();
};

DEFINE_REFCOUNTED_TYPE(TSourceBatchHolder)

////////////////////////////////////////////////////////////////////////////////

class TMockSchemalessMultiChunkReader
    : public ISchemalessMultiChunkReader
{
public:
    TMockSchemalessMultiChunkReader(
        std::vector<TUnversionedOwningRow> rows,
        TNameTablePtr nameTable,
        int maxRowsPerBatch = std::numeric_limits<int>::max())
        : Rows_(std::move(rows))
        , NameTable_(std::move(nameTable))
        , MaxRowsPerBatch_(maxRowsPerBatch)
    { }

    TFuture<void> GetReadyEvent() const override
    {
        return OKFuture;
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        NChunkClient::NProto::TDataStatistics statistics;
        statistics.set_row_count(RowIndex_);
        return statistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return {};
    }

    bool IsFetchingCompleted() const override
    {
        return RowIndex_ == std::ssize(Rows_);
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (RowIndex_ == std::ssize(Rows_)) {
            return nullptr;
        }

        auto holder = New<TSourceBatchHolder>();
        std::vector<TUnversionedRow> rows;
        while (
            RowIndex_ < std::ssize(Rows_) &&
            std::ssize(rows) < std::min<i64>(options.MaxRowsPerRead, MaxRowsPerBatch_))
        {
            rows.push_back(holder->RowBuffer->CaptureRow(
                Rows_[RowIndex_],
                /*captureValues*/ true));
            ++RowIndex_;
        }
        return CreateBatchFromUnversionedRows(
            MakeSharedRange(std::move(rows), std::move(holder)));
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    i64 GetTableRowIndex() const override
    {
        return RowIndex_;
    }

    TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> /*unreadRows*/) const override
    {
        return {};
    }

    const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        return DataSliceDescriptor_;
    }

    TTimingStatistics GetTimingStatistics() const override
    {
        return {};
    }

    i64 GetSessionRowIndex() const override
    {
        return RowIndex_;
    }

    i64 GetTotalRowCount() const override
    {
        return std::ssize(Rows_);
    }

    void Interrupt() override
    {
        YT_ABORT();
    }

    void SkipCurrentReader() override
    {
        YT_ABORT();
    }

private:
    const TDataSliceDescriptor DataSliceDescriptor_;

    const std::vector<TUnversionedOwningRow> Rows_;
    const TNameTablePtr NameTable_;
    const int MaxRowsPerBatch_;

    int RowIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TNameTablePtr CreateNameTable()
{
    auto nameTable = New<TNameTable>();
    for (int id = 0; id < WriterIdColumnId; ++id) {
        EXPECT_EQ(id, nameTable->RegisterName(Format("column_%v", id)));
    }
    EXPECT_EQ(
        WriterIdColumnId,
        nameTable->RegisterName("$push_writer_id"));
    EXPECT_EQ(
        RowIdColumnId,
        nameTable->RegisterName("$push_row_id"));
    return nameTable;
}

TUnversionedOwningRow MakePhysicalRow(
    i64 key,
    i64 writerId,
    i64 rowId,
    TUnversionedValue payload)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(key, /*id*/ 0));
    builder.AddValue(MakeUnversionedInt64Value(writerId, WriterIdColumnId));
    builder.AddValue(MakeUnversionedInt64Value(rowId, RowIdColumnId));
    builder.AddValue(payload);
    return builder.FinishRow();
}

TUnversionedOwningRow MakeRow(std::vector<TUnversionedValue> values)
{
    TUnversionedOwningRowBuilder builder;
    for (const auto& value : values) {
        builder.AddValue(value);
    }
    return builder.FinishRow();
}

TComparator MakePhysicalSortComparator(ESortOrder keySortOrder)
{
    return TComparator({
        keySortOrder,
        ESortOrder::Ascending,
        ESortOrder::Ascending,
    });
}

std::vector<TUnversionedOwningRow> ReadAll(
    const ISchemalessMultiChunkReaderPtr& reader,
    int maxRowsPerRead = 1024)
{
    std::vector<TUnversionedOwningRow> rows;
    TRowBatchReadOptions options;
    options.MaxRowsPerRead = maxRowsPerRead;
    while (true) {
        auto batch = reader->Read(options);
        if (!batch) {
            break;
        }

        auto batchRows = batch->MaterializeRows();
        if (batchRows.empty()) {
            WaitFor(reader->GetReadyEvent()).ThrowOnError();
            continue;
        }

        for (const auto& row : batchRows) {
            rows.push_back(TUnversionedOwningRow(row));
        }
    }
    return rows;
}

ISchemalessMultiChunkReaderPtr CreateReader(
    std::vector<ISchemalessMultiChunkReaderPtr> readers,
    TComparator comparator,
    TIdentityColumnIds identityColumnIds = {
        .WriterId = WriterIdColumnId,
        .RowId = RowIdColumnId,
    },
    TValidWriterIds validWriterIds = {1, 2, 3})
{
    return CreateIdentityAwareSortedMergingReader(
        readers,
        std::move(comparator),
        identityColumnIds,
        std::move(validWriterIds));
}

IUnversionedRowBatchPtr ReadUntilReadyError(
    const ISchemalessMultiChunkReaderPtr& reader,
    const TRowBatchReadOptions& options)
{
    while (true) {
        auto batch = reader->Read(options);
        if (!batch || !batch->IsEmpty()) {
            ADD_FAILURE() << "Expected an empty non-null batch before the terminal error";
            return batch;
        }
        try {
            WaitFor(reader->GetReadyEvent()).ThrowOnError();
        } catch (const TErrorException&) {
            return batch;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TPushBasedSortedMergingReaderTest
    : public ::testing::Test
{ };

TEST_F(TPushBasedSortedMergingReaderTest, MergesMultipleStreamsAndRemovesIdentities)
{
    auto nameTable = CreateNameTable();
    std::vector<ISchemalessMultiChunkReaderPtr> readers{
        New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(1, 1, 1, MakeUnversionedInt64Value(101, /*id*/ 12)),
                MakePhysicalRow(4, 1, 4, MakeUnversionedInt64Value(104, /*id*/ 12)),
            },
            nameTable),
        New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(2, 2, 2, MakeUnversionedInt64Value(202, /*id*/ 12)),
                MakePhysicalRow(5, 2, 5, MakeUnversionedInt64Value(205, /*id*/ 12)),
            },
            nameTable),
        New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(3, 3, 3, MakeUnversionedInt64Value(303, /*id*/ 12)),
                MakePhysicalRow(6, 3, 6, MakeUnversionedInt64Value(306, /*id*/ 12)),
            },
            nameTable),
    };

    auto rows = ReadAll(CreateReader(std::move(readers), MakePhysicalSortComparator(ESortOrder::Ascending)));

    ASSERT_EQ(std::ssize(rows), 6);
    const std::vector<i64> expectedPayloads{101, 202, 303, 104, 205, 306};
    for (int index = 0; index < std::ssize(rows); ++index) {
        const auto& row = rows[index];
        ASSERT_EQ(row.GetCount(), 2u);
        EXPECT_EQ(row[0].Id, 0);
        EXPECT_EQ(row[0].Data.Int64, index + 1);
        EXPECT_EQ(row[1].Id, 12);
        EXPECT_EQ(row[1].Data.Int64, expectedPayloads[index]);
        for (const auto& value : row) {
            EXPECT_NE(value.Id, WriterIdColumnId);
            EXPECT_NE(value.Id, RowIdColumnId);
        }
    }
}

TEST_F(TPushBasedSortedMergingReaderTest, AppendsAscendingIdentitiesToDescendingUserComparator)
{
    auto nameTable = CreateNameTable();
    std::vector<ISchemalessMultiChunkReaderPtr> readers{
        New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(5, 1, 1, MakeUnversionedInt64Value(101, /*id*/ 12)),
                MakePhysicalRow(3, 1, 3, MakeUnversionedInt64Value(103, /*id*/ 12)),
            },
            nameTable),
        New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(5, 1, 2, MakeUnversionedInt64Value(102, /*id*/ 12)),
                MakePhysicalRow(3, 2, 1, MakeUnversionedInt64Value(201, /*id*/ 12)),
            },
            nameTable),
        New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(5, 2, 1, MakeUnversionedInt64Value(201, /*id*/ 12)),
                MakePhysicalRow(2, 3, 1, MakeUnversionedInt64Value(301, /*id*/ 12)),
            },
            nameTable),
    };

    auto rows = ReadAll(CreateReader(std::move(readers), MakePhysicalSortComparator(ESortOrder::Descending)));

    ASSERT_EQ(std::ssize(rows), 6);
    EXPECT_EQ(rows[0][0].Data.Int64, 5);
    EXPECT_EQ(rows[1][0].Data.Int64, 5);
    EXPECT_EQ(rows[2][0].Data.Int64, 5);
    EXPECT_EQ(rows[3][0].Data.Int64, 3);
    EXPECT_EQ(rows[4][0].Data.Int64, 3);
    EXPECT_EQ(rows[5][0].Data.Int64, 2);
    EXPECT_EQ(rows[0][1].Data.Int64, 101);
    EXPECT_EQ(rows[1][1].Data.Int64, 102);
    EXPECT_EQ(rows[2][1].Data.Int64, 201);
    EXPECT_EQ(rows[3][1].Data.Int64, 103);
    EXPECT_EQ(rows[4][1].Data.Int64, 201);
}

TEST_F(TPushBasedSortedMergingReaderTest, FiltersInvalidWritersAfterMerge)
{
    auto nameTable = CreateNameTable();
    std::vector<ISchemalessMultiChunkReaderPtr> readers{
        New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(1, 1, 1, MakeUnversionedInt64Value(101, /*id*/ 12)),
                MakePhysicalRow(1, 3, 1, MakeUnversionedInt64Value(301, /*id*/ 12)),
            },
            nameTable),
        New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(1, 2, 1, MakeUnversionedInt64Value(201, /*id*/ 12)),
                MakePhysicalRow(2, 1, 2, MakeUnversionedInt64Value(102, /*id*/ 12)),
            },
            nameTable),
    };

    auto rows = ReadAll(CreateReader(
        std::move(readers),
        MakePhysicalSortComparator(ESortOrder::Ascending),
        {.WriterId = WriterIdColumnId, .RowId = RowIdColumnId},
        TValidWriterIds{1, 3}));

    ASSERT_EQ(std::ssize(rows), 3);
    EXPECT_EQ(rows[0][1].Data.Int64, 101);
    EXPECT_EQ(rows[1][1].Data.Int64, 301);
    EXPECT_EQ(rows[2][1].Data.Int64, 102);
}

TEST_F(TPushBasedSortedMergingReaderTest, DeduplicatesWithinOneStream)
{
    auto nameTable = CreateNameTable();
    auto rows = ReadAll(CreateReader(
        {New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(1, 1, 1, MakeUnversionedInt64Value(101, /*id*/ 12)),
                MakePhysicalRow(1, 1, 1, MakeUnversionedInt64Value(102, /*id*/ 12)),
            },
            nameTable)},
        MakePhysicalSortComparator(ESortOrder::Ascending)));

    ASSERT_EQ(std::ssize(rows), 1);
    EXPECT_EQ(rows[0][1].Data.Int64, 101);
}

TEST_F(TPushBasedSortedMergingReaderTest, DeduplicatesAcrossStreams)
{
    auto nameTable = CreateNameTable();
    auto rows = ReadAll(CreateReader(
        {
            New<TMockSchemalessMultiChunkReader>(
                std::vector<TUnversionedOwningRow>{
                    MakePhysicalRow(1, 1, 1, MakeUnversionedInt64Value(101, /*id*/ 12)),
                },
                nameTable),
            New<TMockSchemalessMultiChunkReader>(
                std::vector<TUnversionedOwningRow>{
                    MakePhysicalRow(1, 1, 1, MakeUnversionedInt64Value(102, /*id*/ 12)),
                },
                nameTable),
        },
        MakePhysicalSortComparator(ESortOrder::Ascending)));

    ASSERT_EQ(std::ssize(rows), 1);
    EXPECT_EQ(rows[0][1].Data.Int64, 101);
}

TEST_F(TPushBasedSortedMergingReaderTest, DeduplicatesAcrossSourceBatchBoundaries)
{
    auto nameTable = CreateNameTable();
    auto reader = CreateReader(
        {New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(1, 1, 1, MakeUnversionedInt64Value(101, /*id*/ 12)),
                MakePhysicalRow(1, 1, 1, MakeUnversionedInt64Value(102, /*id*/ 12)),
                MakePhysicalRow(2, 1, 2, MakeUnversionedInt64Value(201, /*id*/ 12)),
            },
            nameTable,
            /*maxRowsPerBatch*/ 1)},
        MakePhysicalSortComparator(ESortOrder::Ascending));

    auto rows = ReadAll(reader, /*maxRowsPerRead*/ 1);

    ASSERT_EQ(std::ssize(rows), 2);
    EXPECT_EQ(rows[0][1].Data.Int64, 101);
    EXPECT_EQ(rows[1][1].Data.Int64, 201);
}

TEST_F(TPushBasedSortedMergingReaderTest, KeepsDistinctIdentitiesUnderOneUserKey)
{
    auto nameTable = CreateNameTable();
    auto rows = ReadAll(CreateReader(
        {New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(1, 1, 2, MakeUnversionedInt64Value(102, /*id*/ 12)),
                MakePhysicalRow(1, 2, 1, MakeUnversionedInt64Value(201, /*id*/ 12)),
                MakePhysicalRow(1, 2, 2, MakeUnversionedInt64Value(202, /*id*/ 12)),
            },
            nameTable)},
        MakePhysicalSortComparator(ESortOrder::Ascending)));

    ASSERT_EQ(std::ssize(rows), 3);
    EXPECT_EQ(rows[0][1].Data.Int64, 102);
    EXPECT_EQ(rows[1][1].Data.Int64, 201);
    EXPECT_EQ(rows[2][1].Data.Int64, 202);
}

TEST_F(TPushBasedSortedMergingReaderTest, UsesWriterAndRowIdAsDeduplicationKey)
{
    auto nameTable = CreateNameTable();
    auto rows = ReadAll(CreateReader(
        {New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(1, 1, 1, MakeUnversionedInt64Value(101, /*id*/ 12)),
                MakePhysicalRow(1, 2, 1, MakeUnversionedInt64Value(201, /*id*/ 12)),
            },
            nameTable)},
        MakePhysicalSortComparator(ESortOrder::Ascending)));

    ASSERT_EQ(std::ssize(rows), 2);
    EXPECT_EQ(rows[0][1].Data.Int64, 101);
    EXPECT_EQ(rows[1][1].Data.Int64, 201);
}

TEST_F(TPushBasedSortedMergingReaderTest, ReturnsEmptyNonEosBatchWhenAllRowsAreRejected)
{
    auto nameTable = CreateNameTable();
    auto reader = CreateReader(
        {New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(1, 2, 1, MakeUnversionedInt64Value(201, /*id*/ 12)),
                MakePhysicalRow(2, 1, 2, MakeUnversionedInt64Value(102, /*id*/ 12)),
            },
            nameTable,
            /*maxRowsPerBatch*/ 1)},
        MakePhysicalSortComparator(ESortOrder::Ascending),
        {.WriterId = WriterIdColumnId, .RowId = RowIdColumnId},
        TValidWriterIds{1});
    TRowBatchReadOptions options;

    auto rejectedBatch = reader->Read(options);
    ASSERT_TRUE(rejectedBatch);
    EXPECT_TRUE(rejectedBatch->MaterializeRows().empty());

    WaitFor(reader->GetReadyEvent()).ThrowOnError();
    auto acceptedBatch = reader->Read(options);
    ASSERT_TRUE(acceptedBatch);
    ASSERT_EQ(std::ssize(acceptedBatch->MaterializeRows()), 1);
    EXPECT_EQ(acceptedBatch->MaterializeRows()[0][1].Data.Int64, 102);
}

TEST_F(TPushBasedSortedMergingReaderTest, ReportsStableTerminalErrorWithoutCommittingPartialBatchState)
{
    auto nameTable = CreateNameTable();
    auto reader = CreateReader(
        {New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(1, 1, 1, MakeUnversionedInt64Value(101, /*id*/ 12)),
                MakeRow({
                    MakeUnversionedInt64Value(2, /*id*/ 0),
                    MakeUnversionedInt64Value(1, /*id*/ 12),
                    MakeUnversionedInt64Value(2, RowIdColumnId),
                    MakeUnversionedInt64Value(1, WriterIdColumnId),
                }),
            },
            nameTable,
            /*maxRowsPerBatch*/ 2)},
        MakePhysicalSortComparator(ESortOrder::Ascending));
    TRowBatchReadOptions options;
    options.MaxRowsPerRead = 2;

    auto batch = reader->Read(options);
    ASSERT_TRUE(batch);
    EXPECT_TRUE(batch->IsEmpty());

    EXPECT_EQ(reader->GetSessionRowIndex(), 0);
    const auto statistics = reader->GetDataStatistics();
    EXPECT_EQ(statistics.row_count(), 0);
    EXPECT_EQ(statistics.data_weight(), 0);
    EXPECT_EQ(reader->GetTotalRowCount(), 2);

    TError firstError;
    try {
        WaitFor(reader->GetReadyEvent()).ThrowOnError();
        ADD_FAILURE() << "Expected the malformed row to fail readiness";
    } catch (const TErrorException& error) {
        firstError = error.Error();
    }

    auto subsequentBatch = reader->Read(options);
    ASSERT_TRUE(subsequentBatch);
    EXPECT_TRUE(subsequentBatch->IsEmpty());

    TError subsequentError;
    try {
        WaitFor(reader->GetReadyEvent()).ThrowOnError();
        ADD_FAILURE() << "Expected terminal readiness to remain failed";
    } catch (const TErrorException& error) {
        subsequentError = error.Error();
    }
    EXPECT_EQ(firstError, subsequentError);
}

TEST_F(TPushBasedSortedMergingReaderTest, ReportsRowWithoutIdentityColumns)
{
    auto nameTable = CreateNameTable();
    auto reader = CreateReader(
        {New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{MakeRow({
                MakeUnversionedInt64Value(1, /*id*/ 0),
            })},
            nameTable)},
        MakePhysicalSortComparator(ESortOrder::Ascending));
    TRowBatchReadOptions options;

    auto batch = ReadUntilReadyError(reader, options);
    ASSERT_TRUE(batch);
    EXPECT_TRUE(batch->IsEmpty());
    try {
        WaitFor(reader->GetReadyEvent()).ThrowOnError();
        ADD_FAILURE() << "Expected a row without identity columns to fail readiness";
    } catch (const TErrorException& error) {
        EXPECT_NE(TString(error.what()).find("too short"), TString::npos);
    }
}

TEST_F(TPushBasedSortedMergingReaderTest, ReportsMisplacedRowIdentity)
{
    auto nameTable = CreateNameTable();
    auto reader = CreateReader(
        {New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{MakeRow({
                MakeUnversionedInt64Value(1, /*id*/ 0),
                MakeUnversionedInt64Value(1, WriterIdColumnId),
                MakeUnversionedInt64Value(1, /*id*/ 12),
                MakeUnversionedInt64Value(1, RowIdColumnId),
            })},
            nameTable)},
        MakePhysicalSortComparator(ESortOrder::Ascending));
    TRowBatchReadOptions options;

    auto batch = ReadUntilReadyError(reader, options);
    ASSERT_TRUE(batch);
    EXPECT_TRUE(batch->IsEmpty());
    EXPECT_THROW(WaitFor(reader->GetReadyEvent()).ThrowOnError(), TErrorException);
}

TEST_F(TPushBasedSortedMergingReaderTest, ReportsDuplicateIdentityColumnId)
{
    auto nameTable = CreateNameTable();
    auto reader = CreateReader(
        {New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{MakeRow({
                MakeUnversionedInt64Value(1, /*id*/ 0),
                MakeUnversionedInt64Value(1, WriterIdColumnId),
                MakeUnversionedInt64Value(1, RowIdColumnId),
                MakeUnversionedInt64Value(1, WriterIdColumnId),
            })},
            nameTable)},
        MakePhysicalSortComparator(ESortOrder::Ascending));
    TRowBatchReadOptions options;

    auto batch = ReadUntilReadyError(reader, options);
    ASSERT_TRUE(batch);
    EXPECT_TRUE(batch->IsEmpty());
    EXPECT_THROW(WaitFor(reader->GetReadyEvent()).ThrowOnError(), TErrorException);
}

TEST_F(TPushBasedSortedMergingReaderTest, ReportsIncorrectIdentityTypes)
{
    auto nameTable = CreateNameTable();
    for (const auto& row : std::vector<TUnversionedOwningRow>{
        MakeRow({
            MakeUnversionedInt64Value(1, /*id*/ 0),
            MakeUnversionedStringValue("writer", WriterIdColumnId),
            MakeUnversionedInt64Value(1, RowIdColumnId),
        }),
        MakeRow({
            MakeUnversionedInt64Value(1, /*id*/ 0),
            MakeUnversionedInt64Value(1, WriterIdColumnId),
            MakeUnversionedStringValue("row", RowIdColumnId),
        }),
    }) {
        auto reader = CreateReader(
            {New<TMockSchemalessMultiChunkReader>(
                std::vector<TUnversionedOwningRow>{row},
                nameTable)},
            MakePhysicalSortComparator(ESortOrder::Ascending));
        TRowBatchReadOptions options;

        auto batch = ReadUntilReadyError(reader, options);
        ASSERT_TRUE(batch);
        EXPECT_TRUE(batch->IsEmpty());
        EXPECT_THROW(WaitFor(reader->GetReadyEvent()).ThrowOnError(), TErrorException);
    }
}

TEST_F(TPushBasedSortedMergingReaderTest, ReportsWriterIdOutsideInt32Range)
{
    auto nameTable = CreateNameTable();
    for (i64 writerId : {
        static_cast<i64>(std::numeric_limits<i32>::min()) - 1,
        static_cast<i64>(std::numeric_limits<i32>::max()) + 1,
    }) {
        auto reader = CreateReader(
            {New<TMockSchemalessMultiChunkReader>(
                std::vector<TUnversionedOwningRow>{MakePhysicalRow(
                    1,
                    writerId,
                    1,
                    MakeUnversionedInt64Value(1, /*id*/ 12))},
                nameTable)},
            MakePhysicalSortComparator(ESortOrder::Ascending));
        TRowBatchReadOptions options;

        auto batch = ReadUntilReadyError(reader, options);
        ASSERT_TRUE(batch);
        EXPECT_TRUE(batch->IsEmpty());
        EXPECT_THROW(WaitFor(reader->GetReadyEvent()).ThrowOnError(), TErrorException);
    }
}

TEST_F(TPushBasedSortedMergingReaderTest, ReportsProjectedOutputStatistics)
{
    auto nameTable = CreateNameTable();
    auto reader = CreateReader(
        {New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(1, 1, 1, MakeUnversionedInt64Value(101, /*id*/ 12)),
                MakePhysicalRow(2, 2, 2, MakeUnversionedInt64Value(202, /*id*/ 12)),
                MakePhysicalRow(3, 1, 1, MakeUnversionedInt64Value(103, /*id*/ 12)),
                MakePhysicalRow(4, 1, 4, MakeUnversionedInt64Value(104, /*id*/ 12)),
            },
            nameTable,
            /*maxRowsPerBatch*/ 1)},
        MakePhysicalSortComparator(ESortOrder::Ascending),
        {.WriterId = WriterIdColumnId, .RowId = RowIdColumnId},
        TValidWriterIds{1});
    TRowBatchReadOptions options;
    options.MaxRowsPerRead = 1;

    i64 expectedRowCount = 0;
    i64 expectedDataWeight = 0;
    while (auto batch = reader->Read(options)) {
        auto rows = batch->MaterializeRows();
        if (rows.empty()) {
            WaitFor(reader->GetReadyEvent()).ThrowOnError();
            continue;
        }

        for (const auto& row : rows) {
            auto projectedOwningRow = TUnversionedOwningRow(row);
            expectedDataWeight += GetDataWeight(projectedOwningRow);
            ++expectedRowCount;
        }
        EXPECT_EQ(reader->GetSessionRowIndex(), expectedRowCount);
        const auto statistics = reader->GetDataStatistics();
        EXPECT_EQ(statistics.row_count(), expectedRowCount);
        EXPECT_EQ(statistics.data_weight(), expectedDataWeight);
    }

    EXPECT_EQ(expectedRowCount, 2);
}

TEST_F(TPushBasedSortedMergingReaderTest, TotalRowCountConvergesAfterDrain)
{
    auto nameTable = CreateNameTable();
    auto reader = CreateReader(
        {New<TMockSchemalessMultiChunkReader>(
            std::vector<TUnversionedOwningRow>{
                MakePhysicalRow(1, 1, 1, MakeUnversionedInt64Value(101, /*id*/ 12)),
                MakePhysicalRow(2, 2, 2, MakeUnversionedInt64Value(202, /*id*/ 12)),
                MakePhysicalRow(3, 1, 1, MakeUnversionedInt64Value(103, /*id*/ 12)),
                MakePhysicalRow(4, 1, 4, MakeUnversionedInt64Value(104, /*id*/ 12)),
            },
            nameTable,
            /*maxRowsPerBatch*/ 1)},
        MakePhysicalSortComparator(ESortOrder::Ascending),
        {.WriterId = WriterIdColumnId, .RowId = RowIdColumnId},
        TValidWriterIds{1});
    TRowBatchReadOptions options;
    options.MaxRowsPerRead = 1;

    const i64 initialTotalRowCount = reader->GetTotalRowCount();
    EXPECT_EQ(initialTotalRowCount, 4);
    const std::vector<i64> expectedTotalRowCounts{4, 3, 2, 2};
    int batchIndex = 0;
    while (auto batch = reader->Read(options)) {
        ASSERT_LT(batchIndex, std::ssize(expectedTotalRowCounts));
        EXPECT_EQ(reader->GetTotalRowCount(), expectedTotalRowCounts[batchIndex]);
        ++batchIndex;
        if (batch->IsEmpty()) {
            WaitFor(reader->GetReadyEvent()).ThrowOnError();
        }
    }

    EXPECT_EQ(batchIndex, std::ssize(expectedTotalRowCounts));
    EXPECT_EQ(reader->GetTotalRowCount(), reader->GetSessionRowIndex());
    EXPECT_EQ(reader->GetTotalRowCount(), 2);
}

TEST_F(TPushBasedSortedMergingReaderTest, HandlesEmptyInputs)
{
    auto nameTable = CreateNameTable();
    std::vector<ISchemalessMultiChunkReaderPtr> readers{
        New<TMockSchemalessMultiChunkReader>(std::vector<TUnversionedOwningRow>{}, nameTable),
        New<TMockSchemalessMultiChunkReader>(std::vector<TUnversionedOwningRow>{}, nameTable),
    };
    auto reader = CreateReader(std::move(readers), MakePhysicalSortComparator(ESortOrder::Ascending));

    EXPECT_TRUE(ReadAll(reader).empty());
    TRowBatchReadOptions options;
    EXPECT_FALSE(reader->Read(options));
}

TEST_F(TPushBasedSortedMergingReaderTest, PreservesStringAnyAndCompositePayloadsWithinBatchContract)
{
    auto nameTable = CreateNameTable();
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(1, /*id*/ 0));
    builder.AddValue(MakeUnversionedInt64Value(1, WriterIdColumnId));
    builder.AddValue(MakeUnversionedInt64Value(1, RowIdColumnId));
    builder.AddValue(MakeUnversionedStringValue("string", /*id*/ 12));
    builder.AddValue(MakeUnversionedAnyValue("[any;]", /*id*/ 13));
    builder.AddValue(MakeUnversionedCompositeValue("[composite;]", /*id*/ 14));
    std::vector<TUnversionedOwningRow> blueprints{builder.FinishRow()};

    std::vector<ISchemalessMultiChunkReaderPtr> readers{
        New<TMockSchemalessMultiChunkReader>(blueprints, nameTable),
    };
    auto reader = CreateReader(std::move(readers), MakePhysicalSortComparator(ESortOrder::Ascending));
    TRowBatchReadOptions options;
    auto batch = reader->Read(options);
    ASSERT_TRUE(batch);
    ASSERT_EQ(std::ssize(batch->MaterializeRows()), 1);

    blueprints.clear();
    reader.Reset();

    auto row = batch->MaterializeRows()[0];
    EXPECT_EQ(TStringBuf(row[1].Data.String, row[1].Length), "string");
    EXPECT_EQ(TStringBuf(row[2].Data.String, row[2].Length), "[any;]");
    EXPECT_EQ(TStringBuf(row[3].Data.String, row[3].Length), "[composite;]");
}

TEST_F(TPushBasedSortedMergingReaderTest, RejectsEmptyReaderList)
{
    EXPECT_THROW(
        CreateReader({}, MakePhysicalSortComparator(ESortOrder::Ascending)),
        TErrorException);
    try {
        CreateReader({}, MakePhysicalSortComparator(ESortOrder::Ascending));
        ADD_FAILURE();
    } catch (const TErrorException& error) {
        EXPECT_NE(TString(error.what()).find("input readers"), TString::npos);
    }
}

TEST_F(TPushBasedSortedMergingReaderTest, RejectsComparatorWithoutUserKey)
{
    auto nameTable = CreateNameTable();
    EXPECT_THROW(
        CreateReader(
            {New<TMockSchemalessMultiChunkReader>(std::vector<TUnversionedOwningRow>{}, nameTable)},
            TComparator({
                ESortOrder::Ascending,
                ESortOrder::Ascending,
            })),
        TErrorException);
    try {
        CreateReader(
            {New<TMockSchemalessMultiChunkReader>(std::vector<TUnversionedOwningRow>{}, nameTable)},
            TComparator({
                ESortOrder::Ascending,
                ESortOrder::Ascending,
            }));
        ADD_FAILURE();
    } catch (const TErrorException& error) {
        EXPECT_NE(TString(error.what()).find("at least three sort columns"), TString::npos);
    }
}

TEST_F(TPushBasedSortedMergingReaderTest, RejectsInvalidIdentityColumnIds)
{
    auto nameTable = CreateNameTable();
    EXPECT_THROW(
        CreateReader(
            {New<TMockSchemalessMultiChunkReader>(std::vector<TUnversionedOwningRow>{}, nameTable)},
            MakePhysicalSortComparator(ESortOrder::Ascending),
            {.WriterId = -1, .RowId = RowIdColumnId}),
        TErrorException);
    try {
        CreateReader(
            {New<TMockSchemalessMultiChunkReader>(std::vector<TUnversionedOwningRow>{}, nameTable)},
            MakePhysicalSortComparator(ESortOrder::Ascending),
            {.WriterId = -1, .RowId = RowIdColumnId});
        ADD_FAILURE();
    } catch (const TErrorException& error) {
        EXPECT_NE(TString(error.what()).find("Invalid identity column ids"), TString::npos);
    }
}

TEST_F(TPushBasedSortedMergingReaderTest, RejectsEqualIdentityColumnIds)
{
    auto nameTable = CreateNameTable();
    EXPECT_THROW(
        CreateReader(
            {New<TMockSchemalessMultiChunkReader>(std::vector<TUnversionedOwningRow>{}, nameTable)},
            MakePhysicalSortComparator(ESortOrder::Ascending),
            {.WriterId = WriterIdColumnId, .RowId = WriterIdColumnId}),
        TErrorException);
    try {
        CreateReader(
            {New<TMockSchemalessMultiChunkReader>(std::vector<TUnversionedOwningRow>{}, nameTable)},
            MakePhysicalSortComparator(ESortOrder::Ascending),
            {.WriterId = WriterIdColumnId, .RowId = WriterIdColumnId});
        ADD_FAILURE();
    } catch (const TErrorException& error) {
        EXPECT_NE(TString(error.what()).find("Invalid identity column ids"), TString::npos);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TPushBasedSortedMergingReaderDeathTest
    : public TPushBasedSortedMergingReaderTest
{
protected:
    void SetUp() override
    {
        ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    }
};

TEST_F(TPushBasedSortedMergingReaderDeathTest, InterruptAborts)
{
    EXPECT_DEATH(
        {
            auto nameTable = CreateNameTable();
            auto reader = CreateReader(
                {New<TMockSchemalessMultiChunkReader>(std::vector<TUnversionedOwningRow>{}, nameTable)},
                MakePhysicalSortComparator(ESortOrder::Ascending));
            reader->Interrupt();
        },
        "");
}

TEST_F(TPushBasedSortedMergingReaderDeathTest, GetInterruptDescriptorAborts)
{
    EXPECT_DEATH(
        {
            auto nameTable = CreateNameTable();
            auto reader = CreateReader(
                {New<TMockSchemalessMultiChunkReader>(std::vector<TUnversionedOwningRow>{}, nameTable)},
                MakePhysicalSortComparator(ESortOrder::Ascending));
            reader->GetInterruptDescriptor({});
        },
        "");
}

TEST_F(TPushBasedSortedMergingReaderDeathTest, GetCurrentReaderDescriptorAborts)
{
    EXPECT_DEATH(
        {
            auto nameTable = CreateNameTable();
            auto reader = CreateReader(
                {New<TMockSchemalessMultiChunkReader>(std::vector<TUnversionedOwningRow>{}, nameTable)},
                MakePhysicalSortComparator(ESortOrder::Ascending));
            reader->GetCurrentReaderDescriptor();
        },
        "");
}

TEST_F(TPushBasedSortedMergingReaderDeathTest, SkipCurrentReaderAborts)
{
    EXPECT_DEATH(
        {
            auto nameTable = CreateNameTable();
            auto reader = CreateReader(
                {
                    New<TMockSchemalessMultiChunkReader>(std::vector<TUnversionedOwningRow>{}, nameTable),
                    New<TMockSchemalessMultiChunkReader>(std::vector<TUnversionedOwningRow>{}, nameTable),
                },
                MakePhysicalSortComparator(ESortOrder::Ascending));
            reader->SkipCurrentReader();
        },
        "");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NPushBasedShuffleClient
