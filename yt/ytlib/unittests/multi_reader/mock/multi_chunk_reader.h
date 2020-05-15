#pragma once

#include <yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/ytlib/chunk_client/multi_reader_base.h>
#include <yt/ytlib/chunk_client/config.h>

namespace NYT::NChunkClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkReaderMock
    : public ISchemalessMultiChunkReader
    , public TSequentialMultiReaderBase
{
public:
    using TSequentialMultiReaderBase::TSequentialMultiReaderBase;

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        rows->clear();

        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return true;
        }

        if (Finished_) {
            return false;
        }

        bool readerFinished = !CurrentReader_->Read(rows);
        if (!rows->empty()) {
            return true;
        }

        if (!TSequentialMultiReaderBase::OnEmptyRead(readerFinished)) {
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
        OnInterrupt();
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

private:
    virtual void OnReaderSwitched() override
    {
        CurrentReader_ = dynamic_cast<ISchemalessChunkReader*>(CurrentSession_.Reader.Get());
        YT_VERIFY(CurrentReader_);
    }

    ISchemalessChunkReaderPtr CurrentReader_;

    std::atomic<bool> Finished_ = {false};
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkReaderMock);
DECLARE_REFCOUNTED_CLASS(TMultiChunkReaderMock);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

