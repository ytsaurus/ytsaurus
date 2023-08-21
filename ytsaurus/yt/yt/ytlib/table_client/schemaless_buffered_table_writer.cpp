#include "schemaless_buffered_table_writer.h"
#include "private.h"
#include "config.h"
#include "schemaless_chunk_writer.h"

#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <queue>
#include <array>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NRpc;
using namespace NApi;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

struct TSchemalessBufferedTableWriterBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

class TSchemalessBufferedTableWriter
    : public IUnversionedWriter
{
public:
    TSchemalessBufferedTableWriter(
        TBufferedTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        NNative::IClientPtr client,
        TNameTablePtr nameTable,
        const TYPath& path)
        : Config_(config)
        , Options_(options)
        , Client_(client)
        , NameTable_(nameTable)
        , Path_(path)
        , FlushBufferInvoker_(CreateSerializedInvoker(NChunkClient::TDispatcher::Get()->GetWriterInvoker()))
        , FlushExecutor_(New<TPeriodicExecutor>(
            FlushBufferInvoker_,
            BIND(&TSchemalessBufferedTableWriter::OnPeriodicFlush, Unretained(this)), Config_->FlushPeriod))
    {
        for (auto& buffer : Buffers_) {
            buffer = std::make_unique<TBuffer>(Config_->RowBufferChunkSize);
            EmptyBuffers_.push(buffer.get());
        }

        Logger.AddTag("Path: %v", Path_);

        FlushExecutor_->Start();
    }

    TFuture<void> GetReadyEvent() override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> Close() override
    {
        auto guard = Guard(SpinLock_);
        Closed_ = true;
        RotateBuffers();
        return AllSucceeded(std::vector<TFuture<void>>(BufferFlushedFutures_.begin(), BufferFlushedFutures_.end()));
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        auto guard = Guard(SpinLock_);

        YT_VERIFY(!Closed_);

        if (!CurrentBuffer_) {
            if (EmptyBuffers_.empty()) {
                YT_LOG_DEBUG("Buffer overflown; dropping rows");
                DroppedRowCount_ += rows.Size();
                return true;
            }

            ++CurrentBufferIndex_;
            CurrentBuffer_ = EmptyBuffers_.front();
            EmptyBuffers_.pop();
            CurrentBuffer_->SetIndex(CurrentBufferIndex_);
        }

        CurrentBuffer_->Write(rows);

        if (CurrentBuffer_->GetSize() > Config_->DesiredChunkSize) {
            RotateBuffers();
        }

        return true;
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

private:
    const TBufferedTableWriterConfigPtr Config_;
    const TTableWriterOptionsPtr Options_;
    const NNative::IClientPtr Client_;
    const TNameTablePtr NameTable_;
    const TYPath Path_;

    IInvokerPtr FlushBufferInvoker_;
    const TPeriodicExecutorPtr FlushExecutor_;

    const TTableSchemaPtr Schema_ = New<TTableSchema>();

    class TBuffer
    {
    public:
        DEFINE_BYREF_RO_PROPERTY(std::vector<TUnversionedRow>, Rows);
        DEFINE_BYVAL_RW_PROPERTY(int, Index);

    public:
        explicit TBuffer(i64 rowBufferChunkSize)
            : RowBuffer_(New<TRowBuffer>(
                TSchemalessBufferedTableWriterBufferTag(),
                rowBufferChunkSize))
        { }

        void Write(TRange<TUnversionedRow> rows)
        {
            auto capturedRows = RowBuffer_->CaptureRows(rows);
            Rows_.insert(Rows_.end(), capturedRows.begin(), capturedRows.end());
        }

        void Clear()
        {
            Rows_.clear();
            RowBuffer_->Clear();
        }

        i64 GetSize() const
        {
            return RowBuffer_->GetSize();
        }

        bool IsEmpty() const
        {
            return Rows_.empty();
        }

    private:
        const TRowBufferPtr RowBuffer_;

    };


    // Double buffering.
    std::array<std::unique_ptr<TBuffer>, 2> Buffers_;

    // Guards the following section of members.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    int CurrentBufferIndex_ = -1;
    i64 DroppedRowCount_ = 0;
    TBuffer* CurrentBuffer_ = nullptr;
    std::queue<TBuffer*> EmptyBuffers_;
    std::deque<TFuture<void>> BufferFlushedFutures_;

    // Accessed under spinlock.
    int PendingFlushes_ = 0;
    bool Closed_ = false;

    NLogging::TLogger Logger = TableClientLogger;


    void OnPeriodicFlush()
    {
        auto guard = Guard(SpinLock_);

        if (PendingFlushes_ > 0) {
            return;
        }

        if (CurrentBuffer_ && !CurrentBuffer_->IsEmpty()) {
            RotateBuffers();
        }
    }

    void RotateBuffers()
    {
        if (CurrentBuffer_) {
            ++PendingFlushes_;
            ScheduleBufferFlush(CurrentBuffer_);
            CurrentBuffer_ = nullptr;
        }
    }

    void ScheduleBufferFlush(TBuffer* buffer)
    {
        YT_LOG_DEBUG("Scheduling table chunk flush (BufferIndex: %v)",
            buffer->GetIndex());

        BufferFlushedFutures_.push_back(BIND(&TSchemalessBufferedTableWriter::FlushBuffer, MakeWeak(this), buffer)
            .AsyncVia(FlushBufferInvoker_)
            .Run());

        // Let's clean all the set futures
        while (!BufferFlushedFutures_.empty() && BufferFlushedFutures_.front().IsSet()) {
            BufferFlushedFutures_.pop_front();
        }
    }

    void FlushBuffer(TBuffer* buffer)
    {
        // NB(mrkastep): Here we use endless loop instead of chained callbacks
        // because we want to set the corresponding FlushBuffer future only when
        // the buffer is successfully flushed.
        while (true) {
            try {
                YT_LOG_DEBUG("Started flushing table chunk (BufferIndex: %v, BufferSize: %v)",
                    buffer->GetIndex(),
                    buffer->GetSize());

                TRichYPath richPath(Path_);
                richPath.SetAppend(true);

                auto asyncWriter = CreateSchemalessTableWriter(
                    Config_,
                    Options_,
                    richPath,
                    NameTable_,
                    Client_,
                    /*localHostName*/ TString(), // Locality is not important during table upload.
                    /*transaction*/ nullptr
                );

                auto writer = WaitFor(asyncWriter)
                    .ValueOrThrow();

                writer->Write(buffer->Rows());
                WaitFor(writer->Close())
                    .ThrowOnError();

                YT_LOG_DEBUG("Finished flushing table chunk (BufferIndex: %v)",
                    buffer->GetIndex());

                buffer->Clear();

                {
                    auto guard = Guard(SpinLock_);
                    EmptyBuffers_.push(buffer);
                    --PendingFlushes_;
                }

                return;
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to flush table chunk; will retry later (BufferIndex: %v)",
                    buffer->GetIndex());
                TDelayedExecutor::WaitForDuration(Config_->RetryBackoffTime);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IUnversionedWriterPtr CreateSchemalessBufferedTableWriter(
    TBufferedTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    NNative::IClientPtr client,
    TNameTablePtr nameTable,
    const TYPath& path)
{
    return New<TSchemalessBufferedTableWriter>(
        std::move(config),
        std::move(options),
        std::move(client),
        std::move(nameTable),
        path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
