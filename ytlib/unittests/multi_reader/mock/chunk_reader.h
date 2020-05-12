#pragma once

#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/chunk_reader_base.h>

namespace NYT::NChunkClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderMock
    : public ISchemalessChunkReader
{
public:
    TChunkReaderMock(std::vector<std::vector<TUnversionedOwningRow>> data)
        : Data_(std::move(data))
    { }

    bool Read(std::vector<TUnversionedRow>* rows) override
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
        return VoidFuture;
    }

    virtual NProto::TDataStatistics GetDataStatistics() const override
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

    virtual const TNameTablePtr& GetNameTable() const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual const TKeyColumns& GetKeyColumns() const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual i64 GetTableRowIndex() const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual TInterruptDescriptor GetInterruptDescriptor(TRange<TUnversionedRow> unreadRows) const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_UNIMPLEMENTED();
    }

protected:
    const std::vector<std::vector<TUnversionedOwningRow>> Data_;
    int CurrentDataIndex_ = 0;
    std::atomic_bool Error_{false};
};

class TChunkReaderWithErrorMock
    : public TChunkReaderMock
{
public:
    using TChunkReaderMock::TChunkReaderMock;

    bool Read(std::vector<TUnversionedRow>* rows) override
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
