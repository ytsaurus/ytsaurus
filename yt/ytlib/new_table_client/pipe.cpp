#include "stdafx.h"
#include "pipe.h"
#include "schemaful_reader.h"
#include "schemaful_writer.h"
#include "row_buffer.h"

#include <core/actions/future.h>

#include <core/misc/ring_queue.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TSchemafulPipe::TData
    : public TIntrinsicRefCounted
{
    TSpinLock SpinLock;
    
    TPromise<void> WriterOpened = NewPromise<void>();

    TTableSchema Schema;

    TRowBuffer RowBuffer;
    TRingQueue<TUnversionedRow> RowQueue;
    
    TPromise<void> ReaderReadyEvent = NewPromise<void>();
    TPromise<void> WriterReadyEvent = NewPromise<void>();

    int RowsWritten = 0;
    int RowsRead = 0;
    bool ReaderOpened = false;
    bool WriterClosed = false;
    bool Failed = false;

};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulPipe::TReader
    : public ISchemafulReader
{
public:
    explicit TReader(TDataPtr data)
        : Data_(std::move(data))
    { }

    virtual TFuture<void> Open(const TTableSchema& schema) override
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
                auto& rowQueue = Data_->RowQueue;
                while (!rowQueue.empty() && rows->size() < rows->capacity()) {
                    rows->push_back(rowQueue.front());
                    rowQueue.pop();
                    ++Data_->RowsRead;
                }
            }

            if (rows->empty()) {
                ReadyEvent_ = Data_->ReaderReadyEvent.ToFuture();
            }
        }

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

private:
    const TDataPtr Data_;

    TFuture<void> ReadyEvent_ = VoidFuture;


    void OnOpened(const TTableSchema& readerSchema)
    {
        {
            TGuard<TSpinLock> guard(Data_->SpinLock);
            
            YCHECK(!Data_->ReaderOpened);
            Data_->ReaderOpened = true;
        }

        if (readerSchema != Data_->Schema) {
            THROW_ERROR_EXCEPTION("Reader/writer schema mismatch");
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulPipe::TWriter
    : public ISchemafulWriter
{
public:
    explicit TWriter(TDataPtr data)
        : Data_(std::move(data))
    { }

    virtual TFuture<void> Open(
        const TTableSchema& schema,
        const TNullable<TKeyColumns>& /*keyColumns*/) override
    {
        Data_->Schema = schema;
        Data_->WriterOpened.Set();
        return VoidFuture;
    }

    virtual TFuture<void> Close() override
    {
        TPromise<void> readerReadyEvent;
        TPromise<void> writerReadyEvent;

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
        auto capturedRows = Data_->RowBuffer.Capture(rows);

        // Enqueue rows (with lock).
        TPromise<void> readerReadyEvent;

        {
            TGuard<TSpinLock> guard(Data_->SpinLock);

            YCHECK(Data_->WriterOpened.IsSet());
            YCHECK(!Data_->WriterClosed);

            if (Data_->Failed) {
                return false;
            }

            for (auto row : capturedRows) {
                Data_->RowQueue.push(row);
                ++Data_->RowsWritten;
            }

            readerReadyEvent = std::move(Data_->ReaderReadyEvent);
            Data_->ReaderReadyEvent = NewPromise<void>();
        }

        // Signal readers.
        readerReadyEvent.Set(TError());

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        // TODO(babenko): implement backpressure from reader
        TGuard<TSpinLock> guard(Data_->SpinLock);
        YCHECK(Data_->Failed);
        return Data_->WriterReadyEvent;
    }

private:
    const TDataPtr Data_;

};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulPipe::TImpl
    : public TIntrinsicRefCounted
{
public:
    TImpl()
        : Data_(New<TData>())
        , Reader_(New<TReader>(Data_))
        , Writer_(New<TWriter>(Data_))
    { }

    ISchemafulReaderPtr GetReader() const
    {
        return Reader_;
    }

    ISchemafulWriterPtr  GetWriter() const
    {
        return Writer_;
    }

    void Fail(const TError& error)
    {
        YCHECK(!error.IsOK());

        TPromise<void> readerReadyEvent;
        TPromise<void> writerReadyEvent;

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

TSchemafulPipe::TSchemafulPipe()
    : Impl_(New<TImpl>())
{ }

TSchemafulPipe::~TSchemafulPipe()
{ }

ISchemafulReaderPtr TSchemafulPipe::GetReader() const
{
    return Impl_->GetReader();
}

ISchemafulWriterPtr TSchemafulPipe::GetWriter() const
{
    return Impl_->GetWriter();
}

void TSchemafulPipe::Fail(const TError& error)
{
    Impl_->Fail(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
