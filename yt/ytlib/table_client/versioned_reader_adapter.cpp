#include "versioned_reader_adapter.h"

#include <yt/core/misc/chunked_memory_pool.h>

#include <yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/client/table_client/versioned_reader.h>
#include <yt/client/table_client/schemaful_reader.h>

#include <yt/ytlib/chunk_client/public.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

struct TVersionedReaderAdapterPoolTag { };

class TVersionedReaderAdapter
    : public IVersionedReader
{
public:
    TVersionedReaderAdapter(
        ISchemafulReaderPtr underlyingReader,
        const TTableSchema& schema,
        TTimestamp timestamp)
        : UnderlyingReader_(std::move(underlyingReader))
        , KeyColumnCount_(schema.GetKeyColumnCount())
        , Timestamp_(timestamp)
        , MemoryPool_(TVersionedReaderAdapterPoolTag())
    { }

    virtual TFuture<void> Open() override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        rows->clear();
        MemoryPool_.Clear();
        Rows_.resize(rows->capacity());
        Rows_.shrink_to_fit();

        auto hasMore = UnderlyingReader_->Read(&Rows_);
        if (Rows_.empty()) {
            return hasMore;
        }

        YCHECK(hasMore);

        for (auto row : Rows_) {
            rows->push_back(MakeVersionedRow(row));
        }

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
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
        Y_UNREACHABLE();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        Y_UNREACHABLE();
    }

private:
    const ISchemafulReaderPtr UnderlyingReader_;
    const int KeyColumnCount_;
    const TTimestamp Timestamp_;
    TChunkedMemoryPool MemoryPool_;
    std::vector<TUnversionedRow> Rows_;

    TVersionedRow MakeVersionedRow(const TUnversionedRow row)
    {
        if (!row) {
            return TVersionedRow();
        }

        for (int index = 0; index < KeyColumnCount_; ++index) {
            Y_ASSERT(row[index].Id == index);
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
            YCHECK(row[index].Id >= KeyColumnCount_);
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
    const TTableSchema& schema,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp)
{
    TColumnFilter filter;

    if (!columnFilter.All) {
        filter = TColumnFilter(schema.GetKeyColumnCount());

        for (int index : columnFilter.Indexes) {
            if (index >= schema.GetKeyColumnCount()) {
                filter.Indexes.push_back(index);
            }
        }
    }

    auto reader = createReader(schema, filter);
    return New<TVersionedReaderAdapter>(std::move(reader), schema, timestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
