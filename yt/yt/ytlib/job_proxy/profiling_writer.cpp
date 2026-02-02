#include "private.h"
#include "profiling_writer.h"

#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NJobProxy {

using namespace NChunkClient;
using namespace NFormats;
using namespace NProfiling;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TProfilingMultiChunkWriter
    : public IProfilingMultiChunkWriter
    , public TFirstBatchTimeTrackingBase
{
public:
    TProfilingMultiChunkWriter(ISchemalessMultiChunkWriterPtr underlying, TInstant start)
        : TFirstBatchTimeTrackingBase(start)
        , Underlying_(std::move(underlying))
    { }

    std::optional<TDuration> GetTimeToFirstBatch() const override
    {
        return TFirstBatchTimeTrackingBase::GetTimeToFirstBatch();
    }

    TWriterTimingStatistics GetTimingStatistics() const override
    {
        auto guard = Guard(Lock_);
        TWriterTimingStatistics statistics;
        statistics.WriteTime = WriteTimer_.GetElapsedTime();
        statistics.WaitTime = WaitTimer_.GetElapsedTime();
        statistics.CloseTime = CloseTimer_.GetElapsedTime();
        statistics.IdleTime = TotalTimer_.GetElapsedTime() - statistics.WriteTime - statistics.WaitTime - statistics.CloseTime;
        return statistics;
    }

    // IWriterBase
    TFuture<void> GetReadyEvent() override
    {
        {
            auto guard = Guard(Lock_);
            int oldWaiterCount = WaiterCount_++;
            if (oldWaiterCount == 0) {
                WaitTimer_.Start();
            }
        }

        auto stopTimer = BIND_NO_PROPAGATE([this, weakThis = MakeWeak(this)] (const TError& /*error*/) {
            auto this_ = weakThis.Lock();
            if (!this_) {
                return;
            }

            auto guard = Guard(Lock_);
            --WaiterCount_;
            YT_VERIFY(WaiterCount_ >= 0);
            if (WaiterCount_ == 0) {
                WaitTimer_.Stop();
            }
        });

        auto readyEvent = Underlying_->GetReadyEvent();
        readyEvent.Subscribe(std::move(stopTimer));
        return readyEvent;
    }

    TFuture<void> Close() override
    {
        {
            auto guard = Guard(Lock_);
            YT_VERIFY(
                CloseTimer_.GetElapsedTime() == TDuration::Zero(),
                "Close must only be called once");
            YT_VERIFY(CloseTimer_.Start());
        }

        auto stopTimer = BIND_NO_PROPAGATE([this, weakThis = MakeWeak(this), startTime = GetInstant()] (const TError& /*error*/) {
            auto this_ = weakThis.Lock();
            if (!this_) {
                return;
            }
            auto guard = Guard(Lock_);
            YT_VERIFY(CloseTimer_.Stop());
        });

        auto closeFuture = Underlying_->Close();
        closeFuture.Subscribe(std::move(stopTimer));
        return closeFuture;
    }

    // IMultiChunkWriter
    const std::vector<NChunkClient::NProto::TChunkSpec>& GetWrittenChunkSpecs() const override
    {
        return Underlying_->GetWrittenChunkSpecs();
    }

    const TWrittenChunkReplicasInfoList& GetWrittenChunkReplicasInfos() const override
    {
        return Underlying_->GetWrittenChunkReplicasInfos();
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }

    TCodecStatistics GetCompressionStatistics() const override
    {
        return Underlying_->GetCompressionStatistics();
    }

    // IUnversionedRowsetWriter
    bool Write(TRange<TUnversionedRow> rows) override
    {
        {
            auto guard = Guard(Lock_);
            YT_VERIFY(WriteTimer_.Start());
        }

        if (!rows.Empty()) {
            TryUpdateFirstBatchTime();
        }

        auto result = Underlying_->Write(rows);

        auto guard = Guard(Lock_);
        YT_VERIFY(WriteTimer_.Stop());
        return result;
    }

    std::optional<TRowsDigest> GetDigest() const override
    {
        return Underlying_->GetDigest();
    }

    // IUnversionedWriter
    const TNameTablePtr& GetNameTable() const override
    {
        return Underlying_->GetNameTable();
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Underlying_->GetSchema();
    }

private:
    const ISchemalessMultiChunkWriterPtr Underlying_;

    //! Lock_ protects everything below.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    int WaiterCount_ = 0;
    TWallTimer WriteTimer_ = TWallTimer(/*start*/ false);
    TWallTimer TotalTimer_ = TWallTimer();
    TWallTimer WaitTimer_ = TWallTimer(/*start*/ false);
    TWallTimer CloseTimer_ = TWallTimer(/*start*/ false);
};

////////////////////////////////////////////////////////////////////////////////

class TProfilingSchemalessFormatWriter
    : public IProfilingSchemalessFormatWriter
    , public TFirstBatchTimeTrackingBase
{
public:
    TProfilingSchemalessFormatWriter(ISchemalessFormatWriterPtr underlying, TInstant start)
        : TFirstBatchTimeTrackingBase(start)
        , Underlying_(std::move(underlying))
    { }

    std::optional<TDuration> GetTimeToFirstBatch() const override
    {
        return TFirstBatchTimeTrackingBase::GetTimeToFirstBatch();
    }

    // IUnversionedRowsetWriter
    bool Write(TRange<TUnversionedRow> rows) override
    {
        if (!rows.Empty()) {
            TryUpdateFirstBatchTime();
        }
        return Underlying_->Write(rows);
    }

    std::optional<TRowsDigest> GetDigest() const override
    {
        return Underlying_->GetDigest();
    }

    // IWriterBase
    TFuture<void> GetReadyEvent() override
    {
        return Underlying_->GetReadyEvent();
    }

    TFuture<void> Close() override
    {
        return Underlying_->Close();
    }

    // ISchemalessFormatWriter
    virtual TBlob GetContext() const override
    {
        return Underlying_->GetContext();
    }

    virtual i64 GetWrittenSize() const override
    {
        return Underlying_->GetWrittenSize();
    }

    virtual i64 GetEncodedRowBatchCount() const override
    {
        return Underlying_->GetEncodedRowBatchCount();
    }

    virtual i64 GetEncodedColumnarBatchCount() const override
    {
        return Underlying_->GetEncodedColumnarBatchCount();
    }

    virtual TFuture<void> Flush() override
    {
        return Underlying_->Flush();
    }

    virtual bool WriteBatch(IUnversionedRowBatchPtr rowBatch) override
    {
        if (rowBatch && !rowBatch->IsEmpty()) {
            TryUpdateFirstBatchTime();
        }
        return Underlying_->WriteBatch(std::move(rowBatch));
    }

private:
    const ISchemalessFormatWriterPtr Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

IProfilingMultiChunkWriterPtr CreateProfilingMultiChunkWriter(
    NTableClient::ISchemalessMultiChunkWriterPtr underlying,
    TInstant start)
{
    return New<TProfilingMultiChunkWriter>(std::move(underlying), start);
}

IProfilingSchemalessFormatWriterPtr CreateProfilingSchemalessFormatWriter(
    ISchemalessFormatWriterPtr underlying,
    TInstant start)
{
    return New<TProfilingSchemalessFormatWriter>(std::move(underlying), start);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
