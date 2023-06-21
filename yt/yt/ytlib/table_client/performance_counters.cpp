#include "performance_counters.h"
#include "private.h"

#include <yt/yt/client/table_client/versioned_reader.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

namespace NYT::NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

void UpdatePerformanceCounters(
    const NChunkClient::NProto::TDataStatistics& statistics,
    const TTabletPerformanceCountersPtr& performanceCounters,
    EDataSource source,
    ERequestType type)
{
    if (source == EDataSource::DynamicStore && type == ERequestType::Lookup) {
        performanceCounters->DynamicRowLookupCount.fetch_add(statistics.row_count(), std::memory_order::relaxed);
        performanceCounters->DynamicRowLookupDataWeight.fetch_add(statistics.data_weight(), std::memory_order::relaxed);
    } else if (source == EDataSource::DynamicStore && type == ERequestType::Read) {
        performanceCounters->DynamicRowReadCount.fetch_add(statistics.row_count(), std::memory_order::relaxed);
        performanceCounters->DynamicRowReadDataWeight.fetch_add(statistics.data_weight(), std::memory_order::relaxed);
    } else if (type == ERequestType::Lookup) {
        performanceCounters->StaticChunkRowLookupCount.fetch_add(statistics.row_count(), std::memory_order::relaxed);
        performanceCounters->StaticChunkRowLookupDataWeight.fetch_add(statistics.data_weight(), std::memory_order::relaxed);
    } else {
        performanceCounters->StaticChunkRowReadCount.fetch_add(statistics.row_count(), std::memory_order::relaxed);
        performanceCounters->StaticChunkRowReadDataWeight.fetch_add(statistics.data_weight(), std::memory_order::relaxed);
    }
}

class TPerformanceCountingReaderBase
    : public virtual IReaderBase
{
public:
    explicit TPerformanceCountingReaderBase(IReaderBasePtr reader)
        : Reader_(std::move(reader))
    { }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return Reader_->GetDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return Reader_->GetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return Reader_->IsFetchingCompleted();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return Reader_->GetFailedChunkIds();
    }

    TFuture<void> GetReadyEvent() const override
    {
        return Reader_->GetReadyEvent();
    }

private:
    IReaderBasePtr Reader_;
};

class TVersionedPerformanceCountingReader
    : public IVersionedReader
    , public TPerformanceCountingReaderBase
{
public:
    TVersionedPerformanceCountingReader(
        IVersionedReaderPtr reader,
        TTabletPerformanceCountersPtr performanceCounters,
        EDataSource source,
        ERequestType type)
        : TPerformanceCountingReaderBase(reader)
        , Reader_(std::move(reader))
        , PerformanceCounters_(std::move(performanceCounters))
        , DataSource_(source)
        , RequestType_(type)
    { }

    ~TVersionedPerformanceCountingReader()
    {
        UpdatePerformanceCounters(
            Reader_->GetDataStatistics(),
            PerformanceCounters_,
            DataSource_,
            RequestType_);
    }

    TFuture<void> Open() override
    {
        return Reader_->Open();
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return Reader_->Read(options);
    }

private:
    const IVersionedReaderPtr Reader_;
    const TTabletPerformanceCountersPtr PerformanceCounters_;
    const EDataSource DataSource_;
    const ERequestType RequestType_;
};

class TSchemafulPerformanceCountingReader
    : public ISchemafulUnversionedReader
    , public TPerformanceCountingReaderBase
{
public:
    TSchemafulPerformanceCountingReader(
        ISchemafulUnversionedReaderPtr reader,
        TTabletPerformanceCountersPtr performanceCounters,
        EDataSource source,
        ERequestType type)
        : TPerformanceCountingReaderBase(reader)
        , Reader_(std::move(reader))
        , PerformanceCounters_(std::move(performanceCounters))
        , DataSource_(source)
        , RequestType_(type)
    { }

    ~TSchemafulPerformanceCountingReader()
    {
        UpdatePerformanceCounters(
            Reader_->GetDataStatistics(),
            PerformanceCounters_,
            DataSource_,
            RequestType_);
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return Reader_->Read(options);
    }

private:
    const ISchemafulUnversionedReaderPtr Reader_;
    const TTabletPerformanceCountersPtr PerformanceCounters_;
    const EDataSource DataSource_;
    const ERequestType RequestType_;
};

IVersionedReaderPtr CreateVersionedPerformanceCountingReader(
    IVersionedReaderPtr reader,
    TTabletPerformanceCountersPtr performanceCounters,
    EDataSource source,
    ERequestType type)
{
    YT_ASSERT(!DynamicPointerCast<TVersionedPerformanceCountingReader>(reader));
    return New<TVersionedPerformanceCountingReader>(
        std::move(reader),
        std::move(performanceCounters),
        source,
        type);
}

ISchemafulUnversionedReaderPtr CreateSchemafulPerformanceCountingReader(
    ISchemafulUnversionedReaderPtr reader,
    TTabletPerformanceCountersPtr performanceCounters,
    EDataSource source,
    ERequestType type)
{
    YT_ASSERT(!DynamicPointerCast<TSchemafulPerformanceCountingReader>(reader));
    return New<TSchemafulPerformanceCountingReader>(
        std::move(reader),
        std::move(performanceCounters),
        source,
        type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

