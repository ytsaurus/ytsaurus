#pragma once

#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/chunk_reader_base.h>

#include <yt/core/concurrency/delayed_executor.h>

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

    bool Read(std::vector<NTableClient::TUnversionedRow>* rows) override
    {
        rows->clear();
        if (CurrentDataIndex_ >= Data_.size()) {
            return false;
        }
        rows->insert(rows->end(), Data_[CurrentDataIndex_].begin(), Data_[CurrentDataIndex_].end());
        ++CurrentDataIndex_;
        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        if (Error_.load()) {
            return MakeFuture(TError("Mock error"));
        }
        return NConcurrency::TDelayedExecutor::MakeDelayed(Delay_);
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        NProto::TDataStatistics dataStatistics;
        dataStatistics.set_chunk_count(1);
        dataStatistics.set_uncompressed_data_size(1);
        dataStatistics.set_compressed_data_size(1);
        dataStatistics.set_row_count(1);
        dataStatistics.set_data_weight(1);
        return dataStatistics;
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        TCodecStatistics statistics;
        return statistics;
    }

    virtual bool IsFetchingCompleted() const override
    {
        return CurrentDataIndex_ == Data_.size();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

    virtual const NTableClient::TNameTablePtr& GetNameTable() const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual const NTableClient::TKeyColumns& GetKeyColumns() const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual i64 GetTableRowIndex() const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual TInterruptDescriptor GetInterruptDescriptor(TRange<NTableClient::TUnversionedRow> unreadRows) const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_UNIMPLEMENTED();
    }

protected:
    const std::vector<std::vector<NTableClient::TUnversionedOwningRow>> Data_;
    TDuration Delay_;
    int CurrentDataIndex_ = 0;
    std::atomic_bool Error_{false};
};

class TChunkReaderWithErrorMock
    : public TChunkReaderMock
{
public:
    using TChunkReaderMock::TChunkReaderMock;

    bool Read(std::vector<NTableClient::TUnversionedRow>* rows) override
    {
        if (Error_.load()) {
            return true;
        }

        auto readerNotFinished = TChunkReaderMock::Read(rows);
        if (!readerNotFinished) {
            Error_ = true;
        }

        return true;
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {TChunkId()};
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
