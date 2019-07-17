#include "adapters.h"

#include <yt/client/api/table_writer.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/periodic_yielder.h>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TApiFromSchemalessWriterAdapter
    : public NApi::ITableWriter
{
public:
    explicit TApiFromSchemalessWriterAdapter(IUnversionedWriterPtr underlyingWriter)
        : UnderlyingWriter_(std::move(underlyingWriter))
    { }

    virtual bool Write(TRange<NTableClient::TUnversionedRow> rows) override
    {
        return UnderlyingWriter_->Write(rows);
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return UnderlyingWriter_->GetReadyEvent();
    }

    virtual TFuture<void> Close() override
    {
        return UnderlyingWriter_->Close();
    }

    virtual const NTableClient::TNameTablePtr& GetNameTable() const override
    {
        return UnderlyingWriter_->GetNameTable();
    }

    virtual const NTableClient::TTableSchema& GetSchema() const override
    {
        return UnderlyingWriter_->GetSchema();
    }

private:
    const IUnversionedWriterPtr UnderlyingWriter_;
};

NApi::ITableWriterPtr CreateApiFromSchemalessWriterAdapter(
    IUnversionedWriterPtr underlyingWriter)
{
    return New<TApiFromSchemalessWriterAdapter>(std::move(underlyingWriter));
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessApiFromWriterAdapter
    : public IUnversionedWriter
{
public:
    explicit TSchemalessApiFromWriterAdapter(NApi::ITableWriterPtr underlyingWriter)
        : UnderlyingWriter_(std::move(underlyingWriter))
    { }

    virtual bool Write(TRange<NTableClient::TUnversionedRow> rows) override
    {
        return UnderlyingWriter_->Write(rows);
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return UnderlyingWriter_->GetReadyEvent();
    }

    virtual TFuture<void> Close() override
    {
        return UnderlyingWriter_->Close();
    }

    virtual const NTableClient::TNameTablePtr& GetNameTable() const override
    {
        return UnderlyingWriter_->GetNameTable();
    }

    virtual const NTableClient::TTableSchema& GetSchema() const override
    {
        return UnderlyingWriter_->GetSchema();
    }

private:
    const NApi::ITableWriterPtr UnderlyingWriter_;
};

IUnversionedWriterPtr CreateSchemalessFromApiWriterAdapter(
    NApi::ITableWriterPtr underlyingWriter)
{
    return New<TSchemalessApiFromWriterAdapter>(std::move(underlyingWriter));
}

////////////////////////////////////////////////////////////////////////////////

void PipeReaderToWriter(
    const ITableReaderPtr& reader,
    const IUnversionedRowsetWriterPtr& writer,
    const TPipeReaderToWriterOptions& options)
{
    TPeriodicYielder yielder(TDuration::Seconds(1));

    std::vector<TUnversionedRow> rows;
    rows.reserve(options.BufferRowCount);

    while (reader->Read(&rows)) {
        yielder.TryYield();

        if (rows.empty()) {
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
            continue;
        }

        if (options.ValidateValues) {
            for (const auto row : rows) {
                for (const auto& value : row) {
                    ValidateStaticValue(value);
                }
            }
        }

        if (options.Throttler) {
            i64 dataWeight = 0;
            for (const auto row : rows) {
                dataWeight += GetDataWeight(row);
            }
            WaitFor(options.Throttler->Throttle(dataWeight))
                .ThrowOnError();
        }

        if (!rows.empty() && options.PipeDelay) {
            TDelayedExecutor::WaitForDuration(options.PipeDelay);
        }

        if (!writer->Write(rows)) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }

    WaitFor(writer->Close())
        .ThrowOnError();

    YT_VERIFY(rows.empty());
}

void PipeInputToOutput(
    IInputStream* input,
    IOutputStream* output,
    i64 bufferBlockSize)
{
    struct TWriteBufferTag { };
    TBlob buffer(TWriteBufferTag(), bufferBlockSize);

    TPeriodicYielder yielder(TDuration::Seconds(1));

    while (true) {
        yielder.TryYield();

        size_t length = input->Read(buffer.Begin(), buffer.Size());
        if (length == 0)
            break;

        output->Write(buffer.Begin(), length);
    }

    output->Finish();
}

void PipeInputToOutput(
    const NConcurrency::IAsyncInputStreamPtr& input,
    IOutputStream* output,
    i64 bufferBlockSize)
{
    struct TWriteBufferTag { };
    auto buffer = TSharedMutableRef::Allocate<TWriteBufferTag>(bufferBlockSize, false);

    while (true) {
        auto length = WaitFor(input->Read(buffer))
            .ValueOrThrow();

        if (length == 0) {
            break;
        }

        output->Write(buffer.Begin(), length);
    }

    output->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
