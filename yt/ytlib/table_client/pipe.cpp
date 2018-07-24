#include "pipe.h"

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/schemaful_writer.h>

#include <yt/core/misc/ring_queue.h>

namespace NYT {
namespace NTableClient {

using NChunkClient::NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

struct TSchemafulPipeBufferTag
{ };

struct TSchemafulPipe::TData
    : public TIntrinsicRefCounted
{
    TSpinLock SpinLock;

    const TRowBufferPtr RowBuffer = New<TRowBuffer>(TSchemafulPipeBufferTag());
    TRingQueue<TUnversionedRow> RowQueue;

    TPromise<void> ReaderReadyEvent;
    TPromise<void> WriterReadyEvent = NewPromise<void>();

    int RowsWritten = 0;
    int RowsRead = 0;
    bool WriterClosed = false;
    bool Failed = false;

    TData()
    {
        ResetReaderReadyEvent();
    }

    void ResetReaderReadyEvent()
    {
        ReaderReadyEvent = NewPromise<void>();
        ReaderReadyEvent.OnCanceled(BIND([=, this_ = MakeStrong(this)] () mutable {
            Fail(TError(NYT::EErrorCode::Canceled, "Table reader canceled"));
        }));
    }

    void Fail(const TError& error)
    {
        YCHECK(!error.IsOK());

        TPromise<void> readerReadyEvent;
        TPromise<void> writerReadyEvent;

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (WriterClosed || Failed)
                return;

            Failed = true;
            readerReadyEvent = ReaderReadyEvent;
            writerReadyEvent = WriterReadyEvent;
        }

        readerReadyEvent.TrySet(error);
        writerReadyEvent.TrySet(error);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulPipe::TReader
    : public ISchemafulReader
{
public:
    explicit TReader(TDataPtr data)
        : Data_(std::move(data))
    { }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        Y_ASSERT(rows->capacity() > 0);
        rows->clear();

        {
            TGuard<TSpinLock> guard(Data_->SpinLock);

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

    virtual TDataStatistics GetDataStatistics() const override
    {
        return TDataStatistics();
    }

    virtual NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return NChunkClient::TCodecStatistics();
    }

private:
    const TDataPtr Data_;

    TFuture<void> ReadyEvent_ = VoidFuture;

};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulPipe::TWriter
    : public ISchemafulWriter
{
public:
    explicit TWriter(TDataPtr data)
        : Data_(std::move(data))
    { }

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

        readerReadyEvent.TrySet(TError());
        if (doClose) {
            writerReadyEvent.TrySet(TError());
        }

        return writerReadyEvent;
    }

    virtual bool Write(TRange<TUnversionedRow> rows) override
    {
        // Copy data (no lock).
        auto capturedRows = Data_->RowBuffer->Capture(rows);

        // Enqueue rows (with lock).
        TPromise<void> readerReadyEvent;

        {
            TGuard<TSpinLock> guard(Data_->SpinLock);

            YCHECK(!Data_->WriterClosed);

            if (Data_->Failed) {
                return false;
            }

            for (auto row : capturedRows) {
                Data_->RowQueue.push(row);
                ++Data_->RowsWritten;
            }

            readerReadyEvent = std::move(Data_->ReaderReadyEvent);
            Data_->ResetReaderReadyEvent();
        }

        // Signal readers.
        readerReadyEvent.TrySet(TError());

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
        Data_->Fail(error);
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

} // namespace NTableClient
} // namespace NYT
