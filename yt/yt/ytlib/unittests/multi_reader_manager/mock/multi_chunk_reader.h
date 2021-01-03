#pragma once

#include <yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/ytlib/chunk_client/multi_reader_manager.h>
#include <yt/ytlib/chunk_client/config.h>

#include <yt/client/table_client/unversioned_row_batch.h>

namespace NYT::NChunkClient {

// TODO(max42): move to .cpp

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMultiChunkReaderMock)

class TMultiChunkReaderMock
    : public NTableClient::ISchemalessMultiChunkReader
{
public:
    explicit TMultiChunkReaderMock(IMultiReaderManagerPtr multiReaderManager)
        : MultiReaderManager_(std::move(multiReaderManager))
    {
        MultiReaderManager_->SubscribeReaderSwitched(BIND(&TMultiChunkReaderMock::OnReaderSwitched, MakeWeak(this)));
        MultiReaderManager_->Open();
    }

    virtual NTableClient::IUnversionedRowBatchPtr Read(const NTableClient::TRowBatchReadOptions& options) override
    {
        if (!MultiReaderManager_->GetReadyEvent().IsSet() || !MultiReaderManager_->GetReadyEvent().Get().IsOK()) {
            return NTableClient::CreateEmptyUnversionedRowBatch();
        }

        if (Finished_) {
            return nullptr;
        }

        auto batch = CurrentReader_->Read(options);
        if (batch && !batch->IsEmpty()) {
            return batch;
        }

        if (!MultiReaderManager_->OnEmptyRead(!batch)) {
            Finished_ = true;
        }

        return NTableClient::CreateEmptyUnversionedRowBatch();
    }

    virtual i64 GetSessionRowIndex() const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual i64 GetTotalRowCount() const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual i64 GetTableRowIndex() const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual const NTableClient::TNameTablePtr& GetNameTable() const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual void Interrupt() override
    {
        MultiReaderManager_->Interrupt();
    }

    virtual void SkipCurrentReader() override
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

    virtual TFuture<void> GetReadyEvent() const override
    {
        return MultiReaderManager_->GetReadyEvent();
    }

    virtual NProto::TDataStatistics GetDataStatistics() const override
    {
        return MultiReaderManager_->GetDataStatistics();
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return MultiReaderManager_->GetDecompressionStatistics();
    }

    virtual NTableClient::TTimingStatistics GetTimingStatistics() const override
    {
        return MultiReaderManager_->GetTimingStatistics();
    }

    virtual bool IsFetchingCompleted() const override
    {
        return MultiReaderManager_->IsFetchingCompleted();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return MultiReaderManager_->GetFailedChunkIds();
    }

private:
    const IMultiReaderManagerPtr MultiReaderManager_;

    NTableClient::ISchemalessChunkReaderPtr CurrentReader_;
    std::atomic<bool> Finished_ = false;

    void OnReaderSwitched()
    {
        CurrentReader_ = dynamic_cast<ISchemalessChunkReader*>(MultiReaderManager_->GetCurrentSession().Reader.Get());
        YT_VERIFY(CurrentReader_);
    }
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkReaderMock)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

