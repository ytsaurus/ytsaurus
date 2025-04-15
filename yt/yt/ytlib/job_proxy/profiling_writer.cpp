#include "private.h"
#include "profiling_writer.h"

#include <yt/yt/client/table_client/row_batch.h>


namespace NYT::NJobProxy {

using namespace NChunkClient;
using namespace NFormats;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TProfilingMultiChunkWriter
    : public IProfilingMultiChunkWriter
    , public TFirstBatchTimeTrackingBase
{
public:
    TProfilingMultiChunkWriter(ISchemalessMultiChunkWriterPtr underlying, TCpuInstant start)
        : TFirstBatchTimeTrackingBase(start)
        , Underlying_(std::move(underlying))
    { }

    std::optional<TCpuDuration> GetTimeToFirstBatch() const override
    {
        return TFirstBatchTimeTrackingBase::GetTimeToFirstBatch();
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
        if (!rows.Empty()) {
            TryUpdateFirstBatchTime();
        }
        return Underlying_->Write(rows);
    }

    std::optional<NCrypto::TMD5Hash> GetDigest() const override
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
    ISchemalessMultiChunkWriterPtr Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

class TProfilingSchemalessFormatWriter
    : public IProfilingSchemalessFormatWriter
    , public TFirstBatchTimeTrackingBase
{
public:
    TProfilingSchemalessFormatWriter(ISchemalessFormatWriterPtr underlying, TCpuInstant start)
        : TFirstBatchTimeTrackingBase(start)
        , Underlying_(std::move(underlying))
    { }

    std::optional<TCpuDuration> GetTimeToFirstBatch() const override
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

    std::optional<NCrypto::TMD5Hash> GetDigest() const override
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
    ISchemalessFormatWriterPtr Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

IProfilingMultiChunkWriterPtr CreateProfilingMultiChunkWriter(
    NTableClient::ISchemalessMultiChunkWriterPtr underlying, TCpuInstant start)
{
    return New<TProfilingMultiChunkWriter>(std::move(underlying), start);
}

IProfilingSchemalessFormatWriterPtr CreateProfilingSchemalessFormatWriter(
    ISchemalessFormatWriterPtr underlying, TCpuInstant start)
{
    return New<TProfilingSchemalessFormatWriter>(std::move(underlying), start);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
