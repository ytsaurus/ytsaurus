#include "stdafx.h"
#include "pipe.h"
#include "schemed_reader.h"
#include "schemed_writer.h"

#include <core/misc/chunked_memory_pool.h>
#include <core/misc/ring_queue.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

static auto PresetResult = MakeFuture(TError());

////////////////////////////////////////////////////////////////////////////////

struct TSchemedPipe::TData
    : public TIntrinsicRefCounted
{
    TData()
        : WriterOpened(NewPromise())
        , ReaderReadyEvent(NewPromise<TError>())
        , WriterReadyEvent(NewPromise<TError>())
        , RowsWritten(0)
        , RowsRead(0)
        , ReaderOpened(false)
        , WriterClosed(false)
        , Failed(false)
    { }

    TSpinLock SpinLock;
    
    TPromise<void> WriterOpened;

    TTableSchema Schema;

    TChunkedMemoryPool AlignedPool;
    TChunkedMemoryPool UnalignedPool;

    TRingQueue<TUnversionedRow> Rows;
    
    TPromise<TError> ReaderReadyEvent;
    TPromise<TError> WriterReadyEvent;

    int RowsWritten;
    int RowsRead;
    bool ReaderOpened;
    bool WriterClosed;
    bool Failed;

};

////////////////////////////////////////////////////////////////////////////////

class TSchemedPipe::TReader
    : public ISchemedReader
{
public:
    explicit TReader(TDataPtr data)
        : Data_(std::move(data))
        , ReadyEvent_(PresetResult)
    { }

    virtual TAsyncError Open(const TTableSchema& schema) override
    {
        return Data_->WriterOpened.ToFuture().Apply(BIND(
            &TReader::OnOpened,
            MakeStrong(this),
            schema));
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        YASSERT(rows->capacity() > 0);
        rows->clear();

        {
            TGuard<TSpinLock> guard(Data_->SpinLock);
            
            YCHECK(Data_->ReaderOpened);

            if (Data_->WriterClosed && Data_->RowsWritten == Data_->RowsRead) {
                return false;
            }

            if (!Data_->Failed) {
                auto& dataRows = Data_->Rows;
                while (!dataRows.empty() && rows->size() < rows->capacity()) {
                    rows->push_back(dataRows.front());
                    dataRows.pop();
                    ++Data_->RowsRead;
                }
            }

            if (rows->empty()) {
                ReadyEvent_ = Data_->ReaderReadyEvent.ToFuture();
            }
        }

        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        return ReadyEvent_;
    }

private:
    TDataPtr Data_;
    TAsyncError ReadyEvent_;


    TError OnOpened(const TTableSchema& readerSchema)
    {
        {
            TGuard<TSpinLock> guard(Data_->SpinLock);
            
            YCHECK(!Data_->ReaderOpened);
            Data_->ReaderOpened = true;
        }

        if (readerSchema != Data_->Schema) {
            return TError("Reader/writer schema mismatch");
        }

        return TError();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TSchemedPipe::TWriter
    : public ISchemedWriter
{
public:
    explicit TWriter(TDataPtr data)
        : Data_(std::move(data))
    { }

    virtual TAsyncError Open(
        const TTableSchema& schema,
        const TNullable<TKeyColumns>& /*keyColumns*/) override
    {
        Data_->Schema = schema;
        Data_->WriterOpened.Set();
        return PresetResult;
    }

    virtual TAsyncError Close() override
    {
        TPromise<TError> readerReadyEvent;
        TPromise<TError> writerReadyEvent;

        bool doClose = false;

        {
            TGuard<TSpinLock> guard(Data_->SpinLock);

            YCHECK(!Data_->WriterClosed);
            Data_->WriterClosed = true;

            if (!Data_->Failed) {
                doClose = true;
            }

            readerReadyEvent = Data_->ReaderReadyEvent;
            writerReadyEvent = Data_->WriterReadyEvent;
        }

        readerReadyEvent.Set(TError());
        if (doClose) {
            writerReadyEvent.Set(TError());
        }

        return writerReadyEvent;
    }

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override
    {
        // Copy data (no lock).
        auto capturedRows = CaptureRows(
            rows,
            &Data_->AlignedPool,
            &Data_->UnalignedPool);

        // Enqueue rows (with lock).
        TPromise<TError> readerReadyEvent;

        {
            TGuard<TSpinLock> guard(Data_->SpinLock);

            YCHECK(Data_->WriterOpened.IsSet());
            YCHECK(!Data_->WriterClosed);

            if (Data_->Failed) {
                return false;
            }

            for (auto row : capturedRows) {
                Data_->Rows.push(row);
                ++Data_->RowsWritten;
            }

            readerReadyEvent = std::move(Data_->ReaderReadyEvent);
            Data_->ReaderReadyEvent = NewPromise<TError>();
        }

        // Signal readers.
        readerReadyEvent.Set(TError());

        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        // TODO(babenko): implement backpressure from reader
        TGuard<TSpinLock> guard(Data_->SpinLock);
        YCHECK(Data_->Failed);
        return Data_->WriterReadyEvent;
    }

private:
    TDataPtr Data_;

};

////////////////////////////////////////////////////////////////////////////////

class TSchemedPipe::TImpl
    : public TIntrinsicRefCounted
{
public:
    TImpl()
        : Data_(New<TData>())
        , Reader_(New<TReader>(Data_))
        , Writer_(New<TWriter>(Data_))
    { }

    ISchemedReaderPtr GetReader() const
    {
        return Reader_;
    }

    ISchemedWriterPtr  GetWriter() const
    {
        return Writer_;
    }

    void Fail(const TError& error)
    {
        YCHECK(!error.IsOK());

        TPromise<TError> readerReadyEvent;
        TPromise<TError> writerReadyEvent;

        {
            TGuard<TSpinLock> guard(Data_->SpinLock);
            if (Data_->WriterClosed || Data_->Failed)
                return;

            Data_->Failed = true;
            readerReadyEvent = Data_->ReaderReadyEvent;
            writerReadyEvent = Data_->WriterReadyEvent;
        }

        readerReadyEvent.Set(error);
        writerReadyEvent.Set(error);
    }

private:
    TDataPtr Data_;
    TReaderPtr Reader_;
    TWriterPtr Writer_;

};

////////////////////////////////////////////////////////////////////////////////

TSchemedPipe::TSchemedPipe()
    : Impl_(New<TImpl>())
{ }

TSchemedPipe::~TSchemedPipe()
{ }

ISchemedReaderPtr TSchemedPipe::GetReader() const
{
    return Impl_->GetReader();
}

ISchemedWriterPtr TSchemedPipe::GetWriter() const
{
    return Impl_->GetWriter();
}

void TSchemedPipe::Fail(const TError& error)
{
    Impl_->Fail(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
