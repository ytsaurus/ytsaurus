#include "private.h"
#include "profiling_reader.h"

#include <yt/yt/client/table_client/row_batch.h>

namespace NYT::NJobProxy {

using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TProfilingMultiChunkReader
    : public IProfilingMultiChunkReader
    , public TFirstBatchTimeTrackingBase
{
public:
    TProfilingMultiChunkReader(ISchemalessMultiChunkReaderPtr underlying, TCpuInstant start)
        : TFirstBatchTimeTrackingBase(start)
        , Underlying_(std::move(underlying))
    { }

    std::optional<TCpuDuration> GetTimeToFirstBatch() const override
    {
        return TFirstBatchTimeTrackingBase::GetTimeToFirstBatch();
    }

    // ISchemalessMultiChunkReader
    i64 GetSessionRowIndex() const override
    {
        return Underlying_->GetSessionRowIndex();
    }

    i64 GetTotalRowCount() const override
    {
        return Underlying_->GetTotalRowCount();
    }

    void Interrupt() override
    {
        return Underlying_->Interrupt();
    }

    void SkipCurrentReader() override
    {
        return Underlying_->SkipCurrentReader();
    }

    // ISchemalessChunkReader
    i64 GetTableRowIndex() const override
    {
        return Underlying_->GetTableRowIndex();
    }

    NChunkClient::TInterruptDescriptor GetInterruptDescriptor(
        TRange<NTableClient::TUnversionedRow> unreadRows) const override
    {
        return Underlying_->GetInterruptDescriptor(unreadRows);
    }

    const NChunkClient::TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        return Underlying_->GetCurrentReaderDescriptor();
    }

    // ITimingReader
    TTimingStatistics GetTimingStatistics() const override
    {
        return Underlying_->GetTimingStatistics();
    }

    // IUnversionedReaderBase
    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options = {}) override
    {
        auto batch = Underlying_->Read(options);
        if (batch && !batch->IsEmpty()) {
            TryUpdateFirstBatchTime();
        }
        return batch;
    }

    // IUnversionedReader
    const TNameTablePtr& GetNameTable() const override
    {
        return Underlying_->GetNameTable();
    }

    // IReaderBase
    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }
    TCodecStatistics GetDecompressionStatistics() const override
    {
        return Underlying_->GetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return Underlying_->IsFetchingCompleted();
    }
    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return Underlying_->GetFailedChunkIds();
    }

    // IReadyEventReaderBase
    TFuture<void> GetReadyEvent() const override
    {
        return Underlying_->GetReadyEvent();
    }

private:
    ISchemalessMultiChunkReaderPtr Underlying_;

};

IProfilingMultiChunkReaderPtr CreateProfilingMultiChunkReader(
    ISchemalessMultiChunkReaderPtr underlying, TCpuInstant start)
{
    return New<TProfilingMultiChunkReader>(std::move(underlying), start);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
