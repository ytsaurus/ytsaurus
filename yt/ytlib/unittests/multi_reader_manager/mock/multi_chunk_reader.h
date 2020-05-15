#pragma once

#include <yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/ytlib/chunk_client/multi_reader_manager.h>
#include <yt/ytlib/chunk_client/config.h>

namespace NYT::NChunkClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkReaderMock
    : public ISchemalessMultiChunkReader
{
public:
    TMultiChunkReaderMock(IMultiReaderManagerPtr multiReaderManager):
        MultiReaderManager_(std::move(multiReaderManager))
    {
        MultiReaderManager_->SubscribeReaderSwitched(BIND(&TMultiChunkReaderMock::OnReaderSwitched, MakeWeak(this)));
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        rows->clear();

        if (!MultiReaderManager_->GetReadyEvent().IsSet() || !MultiReaderManager_->GetReadyEvent().Get().IsOK()) {
            return true;
        }

        if (Finished_) {
            return false;
        }

        bool readerFinished = !CurrentReader_->Read(rows);
        if (!rows->empty()) {
            return true;
        }

        if (!MultiReaderManager_->OnEmptyRead(readerFinished)) {
            Finished_ = true;
        }

        return true;
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

    virtual const TNameTablePtr& GetNameTable() const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual const TKeyColumns& GetKeyColumns() const override
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

    virtual TInterruptDescriptor GetInterruptDescriptor(TRange<TUnversionedRow> unreadRows) const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_UNIMPLEMENTED();
    }

    void Open()
    {
        MultiReaderManager_->Open();
    }

    virtual TFuture<void> GetReadyEvent() override
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

    virtual bool IsFetchingCompleted() const override
    {
        return MultiReaderManager_->IsFetchingCompleted();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return MultiReaderManager_->GetFailedChunkIds();
    }

private:
    IMultiReaderManagerPtr MultiReaderManager_;
    ISchemalessChunkReaderPtr CurrentReader_;

    std::atomic<bool> Finished_ = {false};

    void OnReaderSwitched()
    {
        CurrentReader_ = dynamic_cast<ISchemalessChunkReader*>(MultiReaderManager_->GetCurrentSession().Reader.Get());
        YT_VERIFY(CurrentReader_);
    }
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkReaderMock);
DECLARE_REFCOUNTED_CLASS(TMultiChunkReaderMock);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

