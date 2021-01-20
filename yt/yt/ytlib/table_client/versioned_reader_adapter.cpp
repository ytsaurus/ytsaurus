#include "versioned_reader_adapter.h"

#include <yt/core/misc/chunked_memory_pool.h>

#include <yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/client/table_client/versioned_reader.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/unversioned_reader.h>
#include <yt/client/table_client/row_batch.h>

#include <yt/ytlib/chunk_client/public.h>

namespace NYT::NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

struct TVersionedReaderAdapterPoolTag { };

class TVersionedReaderAdapter
    : public IVersionedReader
{
public:
    TVersionedReaderAdapter(
        ISchemafulUnversionedReaderPtr underlyingReader,
        TTableSchemaPtr schema,
        TTimestamp timestamp)
        : UnderlyingReader_(std::move(underlyingReader))
        , KeyColumnCount_(schema->GetKeyColumnCount())
        , Timestamp_(timestamp)
        , MemoryPool_(TVersionedReaderAdapterPoolTag())
    { }

    virtual TFuture<void> Open() override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        MemoryPool_.Clear();

        auto batch = UnderlyingReader_->Read(options);
        if (!batch) {
            return nullptr;
        }

        std::vector<TVersionedRow> rows;
        rows.reserve(batch->GetRowCount());

        for (auto row : batch->MaterializeRows()) {
            rows.push_back(MakeVersionedRow(row));
        }

        return CreateBatchFromVersionedRows(MakeSharedRange(rows, MakeStrong(this)));
    }

    virtual TFuture<void> GetReadyEvent() const override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    virtual bool IsFetchingCompleted() const override
    {
        return UnderlyingReader_->IsFetchingCompleted();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return UnderlyingReader_->GetFailedChunkIds();
    }

private:
    const ISchemafulUnversionedReaderPtr UnderlyingReader_;
    const int KeyColumnCount_;
    const TTimestamp Timestamp_;
    TChunkedMemoryPool MemoryPool_;

    TVersionedRow MakeVersionedRow(TUnversionedRow row)
    {
        if (!row) {
            return TVersionedRow();
        }

        for (int index = 0; index < KeyColumnCount_; ++index) {
            YT_ASSERT(row[index].Id == index);
        }

        auto versionedRow = TMutableVersionedRow::Allocate(
            &MemoryPool_,
            KeyColumnCount_,
            row.GetCount() - KeyColumnCount_,
            1,
            0);

        ::memcpy(versionedRow.BeginKeys(), row.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);

        TVersionedValue* currentValue = versionedRow.BeginValues();
        for (int index = KeyColumnCount_; index < row.GetCount(); ++index) {
            YT_VERIFY(row[index].Id >= KeyColumnCount_);
            *currentValue = MakeVersionedValue(row[index], Timestamp_);
            ++currentValue;
        }

        versionedRow.BeginWriteTimestamps()[0] = Timestamp_;

        return versionedRow;
    }
};

DEFINE_REFCOUNTED_TYPE(TVersionedReaderAdapter)

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedReaderAdapter(
    TSchemafulReaderFactory createReader,
    const TTableSchemaPtr& schema,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp)
{
    TColumnFilter filter;
    if (!columnFilter.IsUniversal()) {
        TColumnFilter::TIndexes columnFilterIndexes;

        for (int i = 0; i < schema->GetKeyColumnCount(); ++i) {
            columnFilterIndexes.push_back(i);
        }
        for (int index : columnFilter.GetIndexes()) {
            if (index >= schema->GetKeyColumnCount()) {
                columnFilterIndexes.push_back(index);
            }
        }
        filter = TColumnFilter(std::move(columnFilterIndexes));
    }

    auto reader = createReader(schema, filter);
    return New<TVersionedReaderAdapter>(std::move(reader), schema, timestamp);
}

////////////////////////////////////////////////////////////////////////////////

class TTimestampResettingAdapter
    : public IVersionedReader
{
public:
    TTimestampResettingAdapter(
        IVersionedReaderPtr underlyingReader,
        TTimestamp timestamp)
        : UnderlyingReader_(std::move(underlyingReader))
        , Timestamp_(timestamp)
    { }

    virtual TFuture<void> Open() override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        auto batch = UnderlyingReader_->Read(options);
        if (!batch) {
            return nullptr;
        }

        for (auto row : batch->MaterializeRows()) {
            if (row) {
                ResetTimestamp(row);
            }
        }

        return batch;
    }

    virtual TFuture<void> GetReadyEvent() const override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    virtual bool IsFetchingCompleted() const override
    {
        YT_ABORT();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        YT_ABORT();
    }

private:
    const IVersionedReaderPtr UnderlyingReader_;
    const TTimestamp Timestamp_;

    void ResetTimestamp(TVersionedRow row) const
    {
        TMutableVersionedRow mutableRow(row.ToTypeErasedRow());

        YT_VERIFY(row.GetWriteTimestampCount() <= 1);
        for (int i = 0; i < row.GetWriteTimestampCount(); ++i) {
            mutableRow.BeginWriteTimestamps()[i] = Timestamp_;
        }

        YT_VERIFY(row.GetDeleteTimestampCount() <= 1);
        for (int i = 0; i < row.GetDeleteTimestampCount(); ++i) {
            mutableRow.BeginDeleteTimestamps()[i] = Timestamp_;
        }

        for (auto* value = mutableRow.BeginValues(); value != mutableRow.EndValues(); ++value) {
            value->Timestamp = Timestamp_;
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TTimestampResettingAdapter)

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateTimestampResettingAdapter(
    IVersionedReaderPtr underlyingReader,
    TTimestamp timestamp)
{
    return New<TTimestampResettingAdapter>(
        std::move(underlyingReader),
        timestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
