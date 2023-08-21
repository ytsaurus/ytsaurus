#include "versioned_reader_adapter.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

namespace NYT::NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

struct TVersionedReaderAdapterPoolTag { };

template <class TRow, class TUnderlyingReaderPtr>
class TVersionedReaderAdapterBase
    : public IVersionedReader
{
public:
    TVersionedReaderAdapterBase(TUnderlyingReaderPtr underlyingReader)
        : MemoryPool_(TVersionedReaderAdapterPoolTag())
        , UnderlyingReader_(std::move(underlyingReader))
    { }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
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

        return CreateBatchFromVersionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
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

protected:
    TChunkedMemoryPool MemoryPool_;
    const TUnderlyingReaderPtr UnderlyingReader_;

private:
    virtual TVersionedRow MakeVersionedRow(TRow row) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(lukyan): Use THorizontalSchemalessVersionedBlockReader
class TVersionedReaderAdapter
    : public TVersionedReaderAdapterBase<TUnversionedRow, ISchemafulUnversionedReaderPtr>
{
public:
    TVersionedReaderAdapter(
        ISchemafulUnversionedReaderPtr underlyingReader,
        int keyColumnCount,
        TTimestamp timestamp)
        : TVersionedReaderAdapterBase(std::move(underlyingReader))
        , KeyColumnCount_(keyColumnCount)
        , Timestamp_(timestamp)
    { }

    TFuture<void> Open() override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

private:
    const int KeyColumnCount_;
    const TTimestamp Timestamp_;

    TVersionedRow MakeVersionedRow(TUnversionedRow row) override
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

        auto* currentValue = versionedRow.BeginValues();
        for (int index = KeyColumnCount_; index < static_cast<int>(row.GetCount()); ++index) {
            YT_VERIFY(row[index].Id >= KeyColumnCount_);
            *currentValue = MakeVersionedValue(row[index], Timestamp_);
            ++currentValue;
        }

        versionedRow.WriteTimestamps()[0] = Timestamp_;

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
    return New<TVersionedReaderAdapter>(std::move(reader), schema->GetKeyColumnCount(), timestamp);
}

////////////////////////////////////////////////////////////////////////////////

class TTimestampResettingAdapter
    : public TVersionedReaderAdapterBase<TVersionedRow, IVersionedReaderPtr>
{
public:
    TTimestampResettingAdapter(
        IVersionedReaderPtr underlyingReader,
        TTimestamp timestamp)
        : TVersionedReaderAdapterBase(std::move(underlyingReader))
        , Timestamp_(timestamp)
    { }

    TFuture<void> Open() override
    {
        return UnderlyingReader_->Open();
    }

private:
    const TTimestamp Timestamp_;

    TVersionedRow MakeVersionedRow(TVersionedRow row) override
    {
        if (!row) {
            return TVersionedRow();
        }

        YT_VERIFY(row.GetWriteTimestampCount() <= 1);
        YT_VERIFY(row.GetDeleteTimestampCount() <= 1);

        auto versionedRow = TMutableVersionedRow::Allocate(
            &MemoryPool_,
            row.GetKeyCount(),
            row.GetValueCount(),
            row.GetWriteTimestampCount(),
            row.GetDeleteTimestampCount());

        // Keys.
        ::memcpy(
            versionedRow.BeginKeys(),
            row.BeginKeys(),
            sizeof(TUnversionedValue) * row.GetKeyCount());

        // Values.
        ::memcpy(
            versionedRow.BeginValues(),
            row.BeginValues(),
            sizeof(TVersionedValue) * row.GetValueCount());
        for (auto& value : versionedRow.Values()) {
            value.Timestamp = Timestamp_;
        }

        // Write/delete timestamps.
        if (row.GetWriteTimestampCount() > 0) {
            versionedRow.WriteTimestamps()[0] = Timestamp_;
        }
        if (row.GetDeleteTimestampCount() > 0) {
            versionedRow.DeleteTimestamps()[0] = Timestamp_;
        }

        return versionedRow;
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
