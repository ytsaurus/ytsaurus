#include "sort_reader_adapter.h"

#include "sort_reader.h"

#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NPushBasedShuffleClient {

using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSortReaderAdapter
    : public ISchemalessMultiChunkReader
{
public:
    TSortReaderAdapter(
        ISortReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        i64 totalRowCount)
        : UnderlyingReader_(std::move(underlyingReader))
        , NameTable_(std::move(nameTable))
        , TotalRowCount_(totalRowCount)
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& /*options*/) override
    {
        if (Finished_) {
            return nullptr;
        }

        if (!ReadFuture_) {
            ReadFuture_ = UnderlyingReader_->Read();
        }

        if (!ReadFuture_.IsSet()) {
            return CreateEmptyUnversionedRowBatch();
        }

        if (!ReadFuture_.GetOrCrash().IsOK()) {
            // The failed future stays in place, so the ready event keeps reporting it.
            return CreateEmptyUnversionedRowBatch();
        }

        auto rows = ReadFuture_.GetOrCrash().Value();
        ReadFuture_ = {};

        if (rows.empty()) {
            Finished_ = true;
            return nullptr;
        }

        DataWeight_ += GetDataWeight(rows);
        RowCount_ += std::ssize(rows);

        return CreateBatchFromUnversionedRows(std::move(rows));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return ReadFuture_
            ? ReadFuture_.AsVoid()
            : OKFuture;
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        NChunkClient::NProto::TDataStatistics dataStatistics;
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return {};
    }

    bool IsFetchingCompleted() const override
    {
        return Finished_;
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    i64 GetTableRowIndex() const override
    {
        // Not supported: the shuffled rows have no source table.
        return -1;
    }

    i64 GetSessionRowIndex() const override
    {
        return RowCount_;
    }

    i64 GetTotalRowCount() const override
    {
        return TotalRowCount_;
    }

    TTimingStatistics GetTimingStatistics() const override
    {
        return {};
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

    void Interrupt() override
    {
        YT_ABORT();
    }

    void SkipCurrentReader() override
    {
        YT_ABORT();
    }

private:
    const ISortReaderPtr UnderlyingReader_;
    const TNameTablePtr NameTable_;
    const i64 TotalRowCount_;

    TFuture<TSharedRange<TUnversionedRow>> ReadFuture_;
    bool Finished_ = false;

    std::atomic<i64> RowCount_ = 0;
    std::atomic<i64> DataWeight_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSortReaderAdapter(
    ISortReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    i64 totalRowCount)
{
    return New<TSortReaderAdapter>(
        std::move(underlyingReader),
        std::move(nameTable),
        totalRowCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
