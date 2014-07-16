#include "stdafx.h"
#include "buffered_table_writer.h"
#include "async_writer.h"
#include "config.h"
#include "private.h"
#include "table_writer.h"

#include <core/concurrency/delayed_executor.h>
#include <core/concurrency/periodic_executor.h>

#include <core/misc/chunked_memory_pool.h>
#include <core/misc/property.h>

#include <core/rpc/channel.h>

#include <core/logging/log.h>

#include <ytlib/chunk_client/dispatcher.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <queue>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TBuffer
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<TRow>, Rows);
    DEFINE_BYVAL_RW_PROPERTY(int, Index);

public:
    void AddRow(const TRow& row)
    {
        Rows_.emplace_back();
        auto& newRow = Rows_.back();

        for (const auto& pair : row) {
            newRow.push_back(std::make_pair(
                Capture(pair.first),
                Capture(pair.second)));
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

    bool IsEmpty() const
    {
        return Rows_.empty();
    }

private:
    TChunkedMemoryPool MemoryPool_;

    TStringBuf Capture(const TStringBuf& value)
    {
        char* ptr = MemoryPool_.AllocateUnaligned(value.size());
        std::memcpy(ptr, value.begin(), value.size());
        return TStringBuf(ptr, value.size());
    }

};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TBufferedTableWriter
    : public IAsyncWriter
{
public:
    TBufferedTableWriter(
        TBufferedTableWriterConfigPtr config,
        IChannelPtr masterChannel,
        TTransactionManagerPtr transactionManager,
        const TYPath& path)
        : Config_(config)
        , MasterChannel_(masterChannel)
        , TransactionManager_(transactionManager)
        , Path_(path)
        , FlushExecutor_(New<TPeriodicExecutor>(
            TDispatcher::Get()->GetWriterInvoker(),
            BIND(&TBufferedTableWriter::OnPeriodicFlush, Unretained(this)), Config_->FlushPeriod))
        , CurrentBufferIndex_(-1)
        , TotalRowCount_(0)
        , DroppedRowCount_(0)
        , CurrentBuffer_(nullptr)
        , FlushedBufferCount_(0)
        , Logger(TableClientLogger)
    {
        EmptyBuffers_.push(Buffers_);
        EmptyBuffers_.push(Buffers_ + 1);
        
        Logger.AddTag("Path: %v", Path_);
    }

    virtual void Open() override
    {
        FlushExecutor_->Start();
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
        TGuard<TSpinLock> guard(SpinLock_);

        ++TotalRowCount_;

        if (!CurrentBuffer_) {
            if (EmptyBuffers_.empty()) {
                LOG_DEBUG("Buffer overflown; dropping row");
                ++DroppedRowCount_;
                return;
            }

            ++CurrentBufferIndex_;
            CurrentBuffer_ = EmptyBuffers_.front();
            EmptyBuffers_.pop();
            CurrentBuffer_->SetIndex(CurrentBufferIndex_);
        }

        CurrentBuffer_->AddRow(row);

        if (CurrentBuffer_->GetSize() > Config_->DesiredChunkSize) {
            RotateBuffers();
        }
    }

    virtual i64 GetRowCount() const override
    {
        return TotalRowCount_;
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        YUNREACHABLE();
    }

private:
    TBufferedTableWriterConfigPtr Config_;
    IChannelPtr MasterChannel_;
    TTransactionManagerPtr TransactionManager_;
    TYPath Path_;

    TPeriodicExecutorPtr FlushExecutor_;

    // Double buffering.
    TBuffer Buffers_[2];

    // Guards the following section of members.
    TSpinLock SpinLock_;
    int CurrentBufferIndex_;
    i64 TotalRowCount_;
    i64 DroppedRowCount_;
    TBuffer* CurrentBuffer_;
    std::queue<TBuffer*> EmptyBuffers_;

    // Only accessed in writer thread.
    int FlushedBufferCount_;

    NLog::TLogger Logger;


    void OnPeriodicFlush()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        
        if (CurrentBuffer_ && !CurrentBuffer_->IsEmpty()) {
            RotateBuffers();
        }
    }

    void RotateBuffers()
    {
        if (CurrentBuffer_) {
            ScheduleBufferFlush(CurrentBuffer_);
            CurrentBuffer_ = nullptr;
        }
    }

    void ScheduleBufferFlush(TBuffer* buffer)
    {
        LOG_DEBUG("Scheduling table chunk flush (BufferIndex: %d)",
            buffer->GetIndex());

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
            // Previous chunk not yet flushed.
            ScheduleDelayedRetry(buffer);
            return;
        }

        try {
            TRichYPath richPath(Path_);
            richPath.Attributes().Set("append", true);

            auto writer = CreateAsyncTableWriter(
                Config_,
                MasterChannel_,
                nullptr,
                TransactionManager_,
                richPath,
                Null);

            writer->Open();

            for (const auto& row : buffer->Rows()) {
                writer->WriteRow(row);
            }

            writer->Close();
            LOG_DEBUG("Buffered table chunk flushed successfully (BufferIndex: %d)",
                buffer->GetIndex());

            buffer->Clear();
            ++FlushedBufferCount_;

            {
                TGuard<TSpinLock> guard(SpinLock_);
                EmptyBuffers_.push(buffer);
            }
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Buffered table chunk write failed, will retry later (BufferIndex: %d)",
                buffer->GetIndex());

            ScheduleDelayedRetry(buffer);
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

IAsyncWriterPtr CreateBufferedTableWriter(
    TBufferedTableWriterConfigPtr config,
    IChannelPtr masterChannel,
    TTransactionManagerPtr transactionManager,
    const TYPath& path)
{
    return New<TBufferedTableWriter>(
        config,
        masterChannel,
        transactionManager,
        path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
