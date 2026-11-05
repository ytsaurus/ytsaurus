#include "sorted_merging_reader.h"

#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/sorted_merging_reader.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/misc/error.h>

#include <algorithm>
#include <atomic>
#include <exception>
#include <limits>
#include <optional>
#include <utility>

namespace NYT::NPushBasedShuffleClient {

using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TRowIdentity
{
    i32 WriterId = 0;
    i64 RowId = 0;

    bool operator==(const TRowIdentity& other) const = default;
};

TRowIdentity ValidateAndGetIdentity(
    TUnversionedRow row,
    int keyColumnCount,
    TIdentityColumnIds identityColumnIds)
{
    if (static_cast<int>(row.GetCount()) < keyColumnCount + IdentityColumnCount) {
        THROW_ERROR_EXCEPTION("Row is too short to carry identity columns")
            .With("value_count", row.GetCount())
            .With("expected_min_value_count", keyColumnCount + IdentityColumnCount);
    }

    const auto& writerValue = row[keyColumnCount];
    const auto& rowValue = row[keyColumnCount + 1];
    if (writerValue.Id != identityColumnIds.WriterId) {
        THROW_ERROR_EXCEPTION("Unexpected writer identity column id")
            .With("expected_column_id", identityColumnIds.WriterId)
            .With("actual_column_id", writerValue.Id)
            .With("row_position", keyColumnCount);
    }
    if (rowValue.Id != identityColumnIds.RowId) {
        THROW_ERROR_EXCEPTION("Unexpected row identity column id")
            .With("expected_column_id", identityColumnIds.RowId)
            .With("actual_column_id", rowValue.Id)
            .With("row_position", keyColumnCount + 1);
    }
    if (writerValue.Type != EValueType::Int64) {
        THROW_ERROR_EXCEPTION("Unexpected writer identity value type")
            .With("value_type", writerValue.Type)
            .With("row_position", keyColumnCount);
    }
    if (rowValue.Type != EValueType::Int64) {
        THROW_ERROR_EXCEPTION("Unexpected row identity value type")
            .With("value_type", rowValue.Type)
            .With("row_position", keyColumnCount + 1);
    }

    const auto writerId = writerValue.Data.Int64;
    if (writerId < std::numeric_limits<i32>::min() ||
        writerId > std::numeric_limits<i32>::max())
    {
        THROW_ERROR_EXCEPTION("Writer identity value is outside the Int32 range")
            .With("writer_value", writerId)
            .With("row_position", keyColumnCount);
    }

    const int rowValueCount = static_cast<int>(row.GetCount());
    for (int index = 0; index < rowValueCount; ++index) {
        if (index == keyColumnCount || index == keyColumnCount + 1) {
            continue;
        }
        const auto& value = row[index];
        if (value.Id == identityColumnIds.WriterId || value.Id == identityColumnIds.RowId) {
            THROW_ERROR_EXCEPTION("Duplicate identity column id")
                .With("actual_column_id", value.Id)
                .With("row_position", index);
        }
    }

    return {
        .WriterId = static_cast<i32>(writerId),
        .RowId = rowValue.Data.Int64,
    };
}

struct TBatchHolder
    : public TRefCounted
{
    IUnversionedRowBatchPtr SourceBatch;
    TRowBufferPtr RowBuffer = New<TRowBuffer>();
};

DEFINE_REFCOUNTED_TYPE(TBatchHolder)

////////////////////////////////////////////////////////////////////////////////

class TIdentityAwareSortedMergingReader
    : public ISchemalessMultiChunkReader
{
public:
    TIdentityAwareSortedMergingReader(
        ISchemalessMultiChunkReaderPtr underlyingReader,
        int keyColumnCount,
        TIdentityColumnIds identityColumnIds,
        TValidWriterIds validWriterIds)
        : UnderlyingReader_(std::move(underlyingReader))
        , KeyColumnCount_(keyColumnCount)
        , IdentityColumnIds_(identityColumnIds)
        , ValidWriterIds_(std::move(validWriterIds))
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (ErrorPromise_.IsSet()) {
            return CreateEmptyUnversionedRowBatch();
        }

        auto sourceBatch = UnderlyingReader_->Read(options);
        if (!sourceBatch) {
            return nullptr;
        }

        auto sourceRows = sourceBatch->MaterializeRows();
        if (sourceRows.empty()) {
            return sourceBatch;
        }

        try {
            auto batchHolder = New<TBatchHolder>();
            batchHolder->SourceBatch = std::move(sourceBatch);

            std::vector<TUnversionedRow> outputRows;
            outputRows.reserve(sourceRows.size());
            auto lastEmittedIdentity = LastEmittedIdentity_;
            i64 emittedRowCount = 0;
            i64 emittedDataWeight = 0;
            i64 rejectedRowCount = 0;
            for (const auto& row : sourceRows) {
                auto identity = ValidateAndGetIdentity(
                    row,
                    KeyColumnCount_,
                    IdentityColumnIds_);
                if (!ValidWriterIds_.contains(identity.WriterId)) {
                    ++rejectedRowCount;
                    continue;
                }
                if (lastEmittedIdentity == identity) {
                    ++rejectedRowCount;
                    continue;
                }

                lastEmittedIdentity = identity;
                auto outputRow = batchHolder->RowBuffer->AllocateUnversioned(
                    row.GetCount() - IdentityColumnCount);
                std::copy(
                    row.Begin(),
                    row.Begin() + KeyColumnCount_,
                    outputRow.Begin());
                std::copy(
                    row.Begin() + KeyColumnCount_ + IdentityColumnCount,
                    row.End(),
                    outputRow.Begin() + KeyColumnCount_);
                emittedDataWeight += GetDataWeight(outputRow);
                ++emittedRowCount;
                outputRows.push_back(outputRow);
            }

            LastEmittedIdentity_ = lastEmittedIdentity;
            EmittedRowCount_.fetch_add(emittedRowCount);
            EmittedDataWeight_.fetch_add(emittedDataWeight);
            RejectedRowCount_.fetch_add(rejectedRowCount);
            return CreateBatchFromUnversionedRows(
                MakeSharedRange(std::move(outputRows), std::move(batchHolder)));
        } catch (const std::exception& ex) {
            ErrorPromise_.Set(ex);
            return CreateEmptyUnversionedRowBatch();
        }
    }

    TFuture<void> GetReadyEvent() const override
    {
        return ErrorPromise_.IsSet()
            ? ErrorPromise_.ToFuture()
            : UnderlyingReader_->GetReadyEvent();
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        auto statistics = UnderlyingReader_->GetDataStatistics();
        statistics.set_row_count(EmittedRowCount_.load());
        statistics.set_data_weight(EmittedDataWeight_.load());
        return statistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return UnderlyingReader_->IsFetchingCompleted();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return UnderlyingReader_->GetFailedChunkIds();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return UnderlyingReader_->GetNameTable();
    }

    i64 GetTableRowIndex() const override
    {
        return UnderlyingReader_->GetTableRowIndex();
    }

    TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> /*unreadRows*/) const override
    {
        YT_ABORT();
    }

    const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_ABORT();
    }

    TTimingStatistics GetTimingStatistics() const override
    {
        return UnderlyingReader_->GetTimingStatistics();
    }

    i64 GetSessionRowIndex() const override
    {
        return EmittedRowCount_.load();
    }

    i64 GetTotalRowCount() const override
    {
        return UnderlyingReader_->GetTotalRowCount() - RejectedRowCount_.load();
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
    const ISchemalessMultiChunkReaderPtr UnderlyingReader_;
    const int KeyColumnCount_;
    const TIdentityColumnIds IdentityColumnIds_;
    const TValidWriterIds ValidWriterIds_;
    const TPromise<void> ErrorPromise_ = NewPromise<void>();

    std::optional<TRowIdentity> LastEmittedIdentity_;
    std::atomic<i64> EmittedRowCount_ = 0;
    std::atomic<i64> EmittedDataWeight_ = 0;
    std::atomic<i64> RejectedRowCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

ISchemalessMultiChunkReaderPtr CreateIdentityAwareSortedMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    TComparator sortComparator,
    TIdentityColumnIds identityColumnIds,
    TValidWriterIds validWriterIds)
{
    if (readers.empty()) {
        THROW_ERROR_EXCEPTION("Cannot create identity-aware sorted merging reader without input readers");
    }
    if (sortComparator.GetLength() < IdentityColumnCount + 1) {
        THROW_ERROR_EXCEPTION(
            "Identity-aware sorted merging reader requires at least three sort columns")
            .With("sort_column_count", sortComparator.GetLength());
    }
    if (!identityColumnIds.AreValid()) {
        THROW_ERROR_EXCEPTION("Invalid identity column ids")
            .With("writer_id_column_id", identityColumnIds.WriterId)
            .With("row_id_column_id", identityColumnIds.RowId);
    }

    const int keyColumnCount = sortComparator.GetLength() - IdentityColumnCount;

    // Merge comparator is only used for key-edge interruptions, which are disabled.
    auto underlyingReader = CreateSortedMergingReader(
        readers,
        std::move(sortComparator),
        /*mergeComparator*/ {},
        /*interruptAtKeyEdge*/ false);
    return New<TIdentityAwareSortedMergingReader>(
        std::move(underlyingReader),
        keyColumnCount,
        identityColumnIds,
        std::move(validWriterIds));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
