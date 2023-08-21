#pragma once

#include <yt/yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/yt/ytlib/table_client/chunk_reader_base.h>

#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

namespace NYT::NChunkClient {

// TODO(max42): move implementation to .cpp

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderMock
    : public NTableClient::ISchemalessChunkReader
{
public:
    TChunkReaderMock(std::vector<std::vector<NTableClient::TUnversionedOwningRow>> data, TDuration delay)
        : Data_(std::move(data))
        , Delay_(delay)
    { }

    NTableClient::IUnversionedRowBatchPtr Read(const NTableClient::TRowBatchReadOptions& /*options*/) override
    {
        if (CurrentDataIndex_ >= std::ssize(Data_)) {
            return nullptr;
        }
        std::vector<NTableClient::TUnversionedRow> rows;
        rows.insert(rows.end(), Data_[CurrentDataIndex_].begin(), Data_[CurrentDataIndex_].end());
        ++CurrentDataIndex_;
        return NTableClient::CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        if (Error_.load()) {
            return MakeFuture(TError("Mock error"));
        }
        return NConcurrency::TDelayedExecutor::MakeDelayed(Delay_);
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        NProto::TDataStatistics dataStatistics;
        dataStatistics.set_chunk_count(1);
        dataStatistics.set_uncompressed_data_size(1);
        dataStatistics.set_compressed_data_size(1);
        dataStatistics.set_row_count(1);
        dataStatistics.set_data_weight(1);
        return dataStatistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        TCodecStatistics statistics;
        return statistics;
    }

    NTableClient::TTimingStatistics GetTimingStatistics() const override
    {
        return {};
    }

    bool IsFetchingCompleted() const override
    {
        return CurrentDataIndex_ == std::ssize(Data_);
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

    const NTableClient::TNameTablePtr& GetNameTable() const override
    {
        YT_UNIMPLEMENTED();
    }

    i64 GetTableRowIndex() const override
    {
        YT_UNIMPLEMENTED();
    }

    TInterruptDescriptor GetInterruptDescriptor(TRange<NTableClient::TUnversionedRow> /*unreadRows*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_UNIMPLEMENTED();
    }

protected:
    const std::vector<std::vector<NTableClient::TUnversionedOwningRow>> Data_;
    const TDuration Delay_;

    int CurrentDataIndex_ = 0;
    std::atomic<bool> Error_ = false;
};

class TChunkReaderWithErrorMock
    : public TChunkReaderMock
{
public:
    using TChunkReaderMock::TChunkReaderMock;

    NTableClient::IUnversionedRowBatchPtr Read(const NTableClient::TRowBatchReadOptions& options) override
    {
        if (Error_.load()) {
            return NTableClient::CreateEmptyUnversionedRowBatch();
        }

        auto batch = TChunkReaderMock::Read(options);
        if (batch) {
            Error_ = true;
        }

        return batch;
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {TChunkId()};
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
