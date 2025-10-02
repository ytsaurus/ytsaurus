#include "versioned_reader_adapter.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/versioned_reader.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include "yt/yt/ytlib/table_client/helpers.h"

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

        for (int index = 0; index < schema->GetKeyColumnCount(); ++index) {
            columnFilterIndexes.push_back(index);
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
        TTimestamp timestamp,
        bool isChunkVersioned)
        : TVersionedReaderAdapterBase(std::move(underlyingReader))
        , Timestamp_(timestamp)
        , IsChunkVersioned_(isChunkVersioned)
    { }

    TFuture<void> Open() override
    {
        return UnderlyingReader_->Open();
    }

private:
    const TTimestamp Timestamp_;
    // NB: Versioned chunks produced with unversioned_update schema modification contain fake timestamp equal to MinTimestamp.
    // Versioned map-reduce may produce versioned chunks where some values have genuine timestamps and other have fake ones.
    // Unversioned chunks may contain arbitrary timestamps but they are never genuine.
    // So we reset only MinTimestamp for versioned chunks and any timestamp for unversioned ones.
    const bool IsChunkVersioned_;

    int GetWriteTimestampCountAfterResetting(const TVersionedRow& row) const
    {
        if (!IsChunkVersioned_) {
            return row.GetWriteTimestampCount() > 0 ? 1 : 0;
        }

        bool minTimestampExists = false;
        bool substituteTimestampExists = false;
        for (auto writeTimestamp : row.WriteTimestamps()) {
            minTimestampExists |= writeTimestamp == MinTimestamp;
            substituteTimestampExists |= writeTimestamp == Timestamp_;
        }

        // All MinTimestamp's will be replaced with Timestamp_, so if we already have Timestamp_, it will be counted twice.
        return row.GetWriteTimestampCount() - static_cast<int>(minTimestampExists && substituteTimestampExists);
    }

    TVersionedRow MakeVersionedRow(TVersionedRow row) override
    {
        if (!row) {
            return TVersionedRow();
        }

        YT_VERIFY(row.GetWriteTimestampCount() <= 1 || IsChunkVersioned_);
        YT_VERIFY(row.GetDeleteTimestampCount() <= 1);

        auto versionedRow = TMutableVersionedRow::Allocate(
            &MemoryPool_,
            row.GetKeyCount(),
            row.GetValueCount(),
            GetWriteTimestampCountAfterResetting(row),
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
            if (!IsChunkVersioned_ || value.Timestamp == MinTimestamp) {
                value.Timestamp = Timestamp_;
            }
        }

        // Write/delete timestamps.
        if (row.GetWriteTimestampCount() > 0) {
            if (!IsChunkVersioned_) {
                *versionedRow.BeginWriteTimestamps() = Timestamp_;
            } else if (versionedRow.GetWriteTimestampCount() == row.GetWriteTimestampCount() - 1 || // Timestamp_ equals to one of row timestamps.
                *(row.EndWriteTimestamps() - 1) != MinTimestamp)
            {
                ::memcpy(
                    versionedRow.BeginWriteTimestamps(),
                    row.BeginWriteTimestamps(),
                    sizeof(TTimestamp) * versionedRow.GetWriteTimestampCount());
            } else {
                auto writeTimestamps = row.BeginWriteTimestamps();
                auto versionedWriteTimestamps = versionedRow.BeginWriteTimestamps();

                // Here we find position of first timestamp less then Timestamp_ (there is no equal timestamp since we check it in previous statement).
                // We copy prefix of write timestamps, set Timestamp_ on this position and copy suffix of timestamps.
                for (int index = 0; index < row.GetWriteTimestampCount(); ++index) {
                    if (writeTimestamps[index] < Timestamp_) {
                        versionedWriteTimestamps[index] = Timestamp_;
                        if (index != row.GetWriteTimestampCount() - 1) {
                            ::memcpy(
                                versionedRow.BeginWriteTimestamps() + index + 1,
                                row.BeginWriteTimestamps() + index,
                                sizeof(TTimestamp) * (row.GetWriteTimestampCount() - index - 1));
                        }

                        break;
                    }

                    versionedWriteTimestamps[index] = writeTimestamps[index];
                }
            }
        }

        if (row.GetDeleteTimestampCount() > 0) {
            *versionedRow.BeginDeleteTimestamps() = Timestamp_;
        }

        return versionedRow;
    }
};

DEFINE_REFCOUNTED_TYPE(TTimestampResettingAdapter)

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateTimestampResettingAdapter(
    IVersionedReaderPtr underlyingReader,
    TTimestamp timestamp,
    EChunkFormat chunkFormat)
{
    return New<TTimestampResettingAdapter>(
        std::move(underlyingReader),
        timestamp,
        IsTableChunkFormatVersioned(chunkFormat));
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TAnyEncodingReaderAdapter)

class TAnyEncodingReaderAdapter
    : public TVersionedReaderAdapterBase<TVersionedRow, IVersionedReaderPtr>
{
public:
    TAnyEncodingReaderAdapter(
        IVersionedReaderPtr underlyingReader,
        TTableSchemaPtr schema)
        : TVersionedReaderAdapterBase(std::move(underlyingReader))
        , TargetSchema_(std::move(schema))
    { }

    TFuture<void> Open() override
    {
        return UnderlyingReader_->Open();
    }

private:
    const TTableSchemaPtr TargetSchema_;

    TVersionedRow MakeVersionedRow(TVersionedRow row) override
    {
        if (!row) {
            return TVersionedRow();
        }

        auto result = TMutableVersionedRow::Allocate(&MemoryPool_, row.GetKeyCount(), row.GetValueCount(), row.GetWriteTimestampCount(), row.GetDeleteTimestampCount());

        ::memcpy(result.BeginKeys(), row.BeginKeys(), sizeof(TUnversionedValue) * row.GetKeyCount());
        ::memcpy(result.BeginValues(), row.BeginValues(), sizeof(TVersionedValue) * row.GetValueCount());
        ::memcpy(result.BeginWriteTimestamps(), row.BeginWriteTimestamps(), sizeof(TTimestamp) * row.GetWriteTimestampCount());
        ::memcpy(result.BeginDeleteTimestamps(), row.BeginDeleteTimestamps(), sizeof(TTimestamp) * row.GetDeleteTimestampCount());

        for (auto& value : result.Values()) {
            auto initialAggregateFlags = value.Flags & EValueFlags::Aggregate;
            value.Flags &= ~EValueFlags::Aggregate;
            EnsureAnyValueEncoded(&value, *TargetSchema_, &MemoryPool_, true);
            value.Flags |= initialAggregateFlags;
        }

        return result;
    }
};

DEFINE_REFCOUNTED_TYPE(TAnyEncodingReaderAdapter)

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr MaybeWrapWithAnyEncodingAdapter(
    IVersionedReaderPtr underlyingReader,
    const TTableSchemaPtr& tableSchema,
    const TTableSchemaPtr& chunkSchema,
    const std::vector<TColumnIdMapping>& schemaIdMapping)
{
    for (const auto& idMapping : schemaIdMapping) {
        auto tableType = tableSchema->Columns()[idMapping.ReaderSchemaIndex].GetWireType();
        auto chunkType = chunkSchema->Columns()[idMapping.ChunkSchemaIndex].GetWireType();
        if ((tableType == EValueType::Any) && (chunkType != EValueType::Any)) {
            return New<TAnyEncodingReaderAdapter>(
                std::move(underlyingReader),
                tableSchema);
        }
    }

    return underlyingReader;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
