#include "performance_counters.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/ytlib/table_client/hunks.h>

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

void TPerformanceCountersEma::Merge(const TEmaCounter<i64>& other)
{
    Counter.fetch_add(other.Count, std::memory_order::relaxed);
    Ema.Merge(other);
}

void TChunkReaderPerformanceCounters::IncrementHunkDataWeight(
    EInitialQueryKind initialQueryKind,
    i64 value,
    EWorkloadCategory workloadCategory)
{
    // NB: Do not account background activities in user read performance counters.
    if (IsSystemWorkloadCategory(workloadCategory)) {
        return;
    }

    if (initialQueryKind == EInitialQueryKind::LookupRows) {
        StaticHunkChunkRowLookupDataWeight.Counter.fetch_add(value, std::memory_order::relaxed);
    } else {
        StaticHunkChunkRowReadDataWeight.Counter.fetch_add(value, std::memory_order::relaxed);
    }
}

void TChunkReaderPerformanceCounters::Increment(
    const TClientChunkReadOptions& chunkReadOptions,
    bool isSystemWorkload)
{
    // Only DataBytesTransmitted is taken from chunk read options for now.
    auto& sensor = isSystemWorkload ? SystemDataBytesTransmitted : UserDataBytesTransmitted;

    sensor.Counter.fetch_add(
        chunkReadOptions.ChunkReaderStatistics->DataBytesTransmitted.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    if (const auto& hunkStatistics = chunkReadOptions.HunkChunkReaderStatistics) {
        sensor.Counter.fetch_add(
            hunkStatistics->GetChunkReaderStatistics()->DataBytesTransmitted.load(std::memory_order_relaxed),
            std::memory_order_relaxed);
    }
}

void UpdatePerformanceCounters(
    const NChunkClient::NProto::TDataStatistics& statistics,
    const TTabletPerformanceCountersPtr& performanceCounters,
    EDataSource source,
    EInitialQueryKind initialQueryKind)
{
    if (source == EDataSource::DynamicStore && initialQueryKind == EInitialQueryKind::LookupRows) {
        performanceCounters->DynamicRowLookup.Counter.fetch_add(statistics.row_count(), std::memory_order::relaxed);
        performanceCounters->DynamicRowLookupDataWeight.Counter.fetch_add(statistics.data_weight(), std::memory_order::relaxed);
    } else if (source == EDataSource::DynamicStore) {
        performanceCounters->DynamicRowRead.Counter.fetch_add(statistics.row_count(), std::memory_order::relaxed);
        performanceCounters->DynamicRowReadDataWeight.Counter.fetch_add(statistics.data_weight(), std::memory_order::relaxed);
    } else if (initialQueryKind == EInitialQueryKind::LookupRows) {
        performanceCounters->StaticChunkRowLookup.Counter.fetch_add(statistics.row_count(), std::memory_order::relaxed);
        performanceCounters->StaticChunkRowLookupDataWeight.Counter.fetch_add(statistics.data_weight(), std::memory_order::relaxed);
    } else {
        performanceCounters->StaticChunkRowRead.Counter.fetch_add(statistics.row_count(), std::memory_order::relaxed);
        performanceCounters->StaticChunkRowReadDataWeight.Counter.fetch_add(statistics.data_weight(), std::memory_order::relaxed);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TPerformanceCountingReaderBase
    : public virtual IReaderBase
{
public:
    TPerformanceCountingReaderBase(
        IReaderBasePtr reader,
        TTabletPerformanceCountersPtr performanceCounters,
        EDataSource source,
        EInitialQueryKind initialQueryKind)
        : Reader_(std::move(reader))
        , PerformanceCounters_(std::move(performanceCounters))
        , DataSource_(source)
        , InitialQueryKind_(initialQueryKind)
    { }

    ~TPerformanceCountingReaderBase()
    {
        UpdatePerformanceCounters(
            Reader_->GetDataStatistics(),
            PerformanceCounters_,
            DataSource_,
            InitialQueryKind_);
    }

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
    const TTabletPerformanceCountersPtr PerformanceCounters_;
    const EDataSource DataSource_;
    const EInitialQueryKind InitialQueryKind_;
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedPerformanceCountingReader
    : public IVersionedReader
    , public TPerformanceCountingReaderBase
{
public:
    TVersionedPerformanceCountingReader(
        IVersionedReaderPtr reader,
        TTabletPerformanceCountersPtr performanceCounters,
        EDataSource source,
        EInitialQueryKind initialQueryKind)
        : TPerformanceCountingReaderBase(
            reader,
            std::move(performanceCounters),
            source,
            initialQueryKind)
        , Reader_(std::move(reader))
    { }

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
        EInitialQueryKind initialQueryKind)
        : TPerformanceCountingReaderBase(
            reader,
            std::move(performanceCounters),
            source,
            initialQueryKind)
        , Reader_(std::move(reader))
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return Reader_->Read(options);
    }

private:
    const ISchemafulUnversionedReaderPtr Reader_;
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedPerformanceCountingReader(
    IVersionedReaderPtr reader,
    TTabletPerformanceCountersPtr performanceCounters,
    EDataSource source,
    EInitialQueryKind initialQueryKind)
{
    YT_ASSERT(!DynamicPointerCast<TVersionedPerformanceCountingReader>(reader));
    return New<TVersionedPerformanceCountingReader>(
        std::move(reader),
        std::move(performanceCounters),
        source,
        initialQueryKind);
}

ISchemafulUnversionedReaderPtr CreateSchemafulPerformanceCountingReader(
    ISchemafulUnversionedReaderPtr reader,
    TTabletPerformanceCountersPtr performanceCounters,
    EDataSource source,
    EInitialQueryKind initialQueryKind)
{
    YT_ASSERT(!DynamicPointerCast<TSchemafulPerformanceCountingReader>(reader));
    return New<TSchemafulPerformanceCountingReader>(
        std::move(reader),
        std::move(performanceCounters),
        source,
        initialQueryKind);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
