#include "performance_counters.h"

#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/private.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

namespace NYT::NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

void TPerformanceCountersEma::UpdateEma()
{
    Ema.Update(Counter.load(std::memory_order::relaxed));
}

void TChunkReaderPerformanceCounters::IncrementHunkDataWeight(
    EPerformanceCountedRequestType requestType,
    i64 value,
    EWorkloadCategory workloadCategory)
{
    // NB: Do not account background activities in user read performance counters.
    if (IsSystemWorkloadCategory(workloadCategory)) {
        return;
    }

    if (requestType == EPerformanceCountedRequestType::Lookup) {
        StaticHunkChunkRowLookupDataWeight.Counter.fetch_add(value, std::memory_order::relaxed);
    } else {
        StaticHunkChunkRowReadDataWeight.Counter.fetch_add(value, std::memory_order::relaxed);
    }
}

void UpdatePerformanceCounters(
    const NChunkClient::NProto::TDataStatistics& statistics,
    const TTabletPerformanceCountersPtr& performanceCounters,
    EDataSource source,
    EPerformanceCountedRequestType type)
{
    if (source == EDataSource::DynamicStore && type == EPerformanceCountedRequestType::Lookup) {
        performanceCounters->DynamicRowLookup.Counter.fetch_add(statistics.row_count(), std::memory_order::relaxed);
        performanceCounters->DynamicRowLookupDataWeight.Counter.fetch_add(statistics.data_weight(), std::memory_order::relaxed);
    } else if (source == EDataSource::DynamicStore && type == EPerformanceCountedRequestType::Read) {
        performanceCounters->DynamicRowRead.Counter.fetch_add(statistics.row_count(), std::memory_order::relaxed);
        performanceCounters->DynamicRowReadDataWeight.Counter.fetch_add(statistics.data_weight(), std::memory_order::relaxed);
    } else if (type == EPerformanceCountedRequestType::Lookup) {
        performanceCounters->StaticChunkRowLookup.Counter.fetch_add(statistics.row_count(), std::memory_order::relaxed);
        performanceCounters->StaticChunkRowLookupDataWeight.Counter.fetch_add(statistics.data_weight(), std::memory_order::relaxed);
    } else {
        performanceCounters->StaticChunkRowRead.Counter.fetch_add(statistics.row_count(), std::memory_order::relaxed);
        performanceCounters->StaticChunkRowReadDataWeight.Counter.fetch_add(statistics.data_weight(), std::memory_order::relaxed);
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
    const IReaderBasePtr Reader_;
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
        EPerformanceCountedRequestType type)
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
    const EPerformanceCountedRequestType RequestType_;
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
        EPerformanceCountedRequestType type)
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
    const EPerformanceCountedRequestType RequestType_;
};

IVersionedReaderPtr CreateVersionedPerformanceCountingReader(
    IVersionedReaderPtr reader,
    TTabletPerformanceCountersPtr performanceCounters,
    EDataSource source,
    EPerformanceCountedRequestType type)
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
    EPerformanceCountedRequestType type)
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
