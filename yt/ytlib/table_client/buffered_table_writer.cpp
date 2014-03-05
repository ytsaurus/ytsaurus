#include "stdafx.h"

#include "buffered_table_writer.h"

#include "async_writer.h"
#include "config.h"
#include "private.h"
#include "table_writer.h"

#include <ytlib/chunk_client/dispatcher.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction.h>

#include <core/concurrency/delayed_executor.h>

#include <core/misc/chunked_memory_pool.h>
#include <core/misc/property.h>

#include <core/rpc/channel.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NYPath;

static auto& Logger = TableWriterLogger;

////////////////////////////////////////////////////////////////////////////////

namespace  NDetail {

class TBuffer
{
    DEFINE_BYREF_RO_PROPERTY(std::vector<TRow>, Rows);
    DEFINE_BYVAL_RW_PROPERTY(i64, Index);

public:
    void AddRow(const TRow& row)
    {
        Rows_.emplace_back();
        auto& newRow = Rows_.back();

        for (const auto& pair : row) {
            newRow.push_back(std::make_pair(
                DeepCopy(pair.first),
                DeepCopy(pair.first)));
        }
    }

    void Clear()
    {
        Rows_.clear();
        MemoryPool_.Clear();
    }

    i64 GetSize() const
    {
        return MemoryPool_.GetSize();
    }

private:
    TChunkedMemoryPool MemoryPool_;

    TStringBuf DeepCopy(const TStringBuf& value)
    {
        char* ptr = MemoryPool_.AllocateUnaligned(value.size());
        std::memcpy(ptr, value.begin(), value.size());
        return TStringBuf(ptr, value.size());
    }

};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

using NDetail::TBuffer;

class TBufferedTableWriter
    : public IAsyncWriter
{
public:
    TBufferedTableWriter(
        TBufferedTableWriterConfigPtr config,
        IChannelPtr masterChannel,
        TTransactionManagerPtr transactionManager,
        const TRichYPath& richPath,
        const TNullable<TKeyColumns>& keyColumns)
        : Config_(config)
        , MasterChannel_(masterChannel)
        , TransactionManager_(transactionManager)
        , Path_(richPath)
        , KeyColumns_(keyColumns)
        , CurrentBufferIndex_(-1)
        , FlushedBufferCount_(0)
        , RowCount_(0)
        , DroppedRowCount_(0)
        , CurrentBuffer_(nullptr)
    {
        EmptyBuffers_.Enqueue(Buffers_);
        EmptyBuffers_.Enqueue(Buffers_ + 1);
    }

    virtual void Open() override
    {
        // Do nothing.
    }

    virtual bool IsReady() override
    {
        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        YUNREACHABLE();
    }

    virtual void Close() override
    {
        YUNREACHABLE();
    }

    virtual void WriteRow(const TRow& row) override
    {
        ++RowCount_;

        if (!CurrentBuffer_) {
            if (EmptyBuffers_.IsEmpty()) {
                // Buffer overfilled - drop row.
                ++DroppedRowCount_;
                return;
            }

            ++CurrentBufferIndex_;
            EmptyBuffers_.Dequeue(&CurrentBuffer_);
            CurrentBuffer_->SetIndex(CurrentBufferIndex_);
        }

        CurrentBuffer_->AddRow(row);

        if (CurrentBuffer_->GetSize() > Config_->DesiredChunkSize) {
            ScheduleBufferFlush(CurrentBuffer_);
            CurrentBuffer_ = nullptr;
        }
    }

    virtual i64 GetRowCount() const override
    {
        return RowCount_;
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        YUNREACHABLE();
    }

    virtual const TNullable<TKeyColumns>& GetKeyColumns() const override
    {
        return KeyColumns_;
    }

private:
    TBufferedTableWriterConfigPtr Config_;
    IChannelPtr MasterChannel_;
    TTransactionManagerPtr TransactionManager_;
    TRichYPath Path_;
    TNullable<TKeyColumns> KeyColumns_;

    i64 CurrentBufferIndex_;
    i64 FlushedBufferCount_;

    i64 RowCount_;
    i64 DroppedRowCount_;

    // Double buffering.
    NDetail::TBuffer Buffers_[2];

    NDetail::TBuffer* CurrentBuffer_;
    TLockFreeQueue<TBuffer*> EmptyBuffers_;


    void ScheduleBufferFlush(TBuffer* buffer)
    {
        TDispatcher::Get()->GetWriterInvoker()->Invoke(BIND(
            &TBufferedTableWriter::FlushBuffer,
            MakeWeak(this),
            buffer));
    }

    void ScheduleDelayedRetry(TBuffer* buffer)
    {
        TDelayedExecutor::Submit(
            BIND(&TBufferedTableWriter::ScheduleBufferFlush, MakeWeak(this), buffer),
            Config_->RetryBackoffTime);
    }

    void FlushBuffer(TBuffer* buffer)
    {
        if (buffer->GetIndex() > FlushedBufferCount_) {
            // Previous chunk not yet flushed
            ScheduleDelayedRetry(buffer);
            return;
        }

        try {
            auto writer = CreateAsyncTableWriter(
                Config_,
                MasterChannel_,
                nullptr,
                TransactionManager_,
                Path_,
                KeyColumns_);

            writer->Open();

            for (const auto& row : buffer->Rows()) {
                writer->WriteRow(row);
            }

            writer->Close();
            LOG_DEBUG(
                "Buffered chunk successfully flushed (Path: %s, BufferIndex: %" PRId64 ")",
                ~Path_.GetPath(),
                buffer->GetIndex());

            buffer->Clear();
            ++FlushedBufferCount_;
        } catch (const std::exception& ex) {
            ScheduleDelayedRetry(buffer);

            LOG_WARNING(
                ex,
                "Failed to write buffered chunk, scheduling retry (Path: %s, BufferIndex: %" PRId64 ")",
                ~Path_.GetPath(),
                buffer->GetIndex());
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

IAsyncWriterPtr CreateBufferedTableWriter(
    TBufferedTableWriterConfigPtr config,
    IChannelPtr masterChannel,
    TTransactionManagerPtr transactionManager,
    const TRichYPath richPath,
    const TNullable<TKeyColumns>& keyColumns)
{
    return New<TBufferedTableWriter>(
        config,
        masterChannel,
        transactionManager,
        richPath,
        keyColumns);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
