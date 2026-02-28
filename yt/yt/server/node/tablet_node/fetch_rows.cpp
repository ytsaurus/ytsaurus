#include "fetch_rows.h"

#include "failing_on_rotation_reader.h"
#include "tablet.h"
#include "private.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

struct TFetchRowsBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

class TSimpleFetchRowsResultWriter
{
public:
    TSimpleFetchRowsResultWriter(
        const TTabletSnapshotPtr& /*tabletSnapshot*/,
        const TClientChunkReadOptions& /*chunkReadOptions*/)
    { }

    i64 GetRowsetDataWeight(TRange<NTableClient::TUnversionedRow> rowset) const
    {
        return GetDataWeight(rowset);
    }

    void WriteRowset(TRange<NTableClient::TUnversionedRow> rowset)
    {
        Writer_->WriteUnversionedRowset(rowset);
    }

    TFuture<std::vector<TSharedRef>> Postprocess(const IInvokerPtr& /*invoker*/)
    {
        YT_VERIFY(!std::exchange(Postprocessed_, true));

        return MakeFuture(Writer_->Finish());
    }

private:
    const std::unique_ptr<IWireProtocolWriter> Writer_ = CreateWireProtocolWriter();

    bool Postprocessed_ = false;
};

class THunkDecodingFetchRowsResultWriter
{
public:
    THunkDecodingFetchRowsResultWriter(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TClientChunkReadOptions& chunkReadOptions)
        : Schema_(tabletSnapshot->QuerySchema)
        , ChunkFragmentReader_(tabletSnapshot->ChunkFragmentReader)
        , ChunkReadOptions_(chunkReadOptions)
        , PerformanceCounters_(tabletSnapshot->PerformanceCounters)
        , RowBuffer_(New<TRowBuffer>(
            TFetchRowsBufferTag(),
            TChunkedMemoryPool::DefaultStartChunkSize,
            chunkReadOptions.MemoryUsageTracker))
    {
        YT_VERIFY(Schema_->HasHunkColumns());
    }

    i64 GetRowsetDataWeight(TRange<NTableClient::TUnversionedRow> rowset) const
    {
        return ComputeSchemafulRowsDataWeightAfterHunkDecoding(rowset, Schema_);
    }

    void WriteRowset(TRange<NTableClient::TUnversionedRow> rowset)
    {
        for (auto row : rowset) {
            HunkEncodedRows_.push_back(RowBuffer_->CaptureRow(row));
        }
    }

    TFuture<std::vector<TSharedRef>> Postprocess(const IInvokerPtr& invoker)
    {
        YT_VERIFY(!std::exchange(Postprocessed_, true));

        auto sharedRows = MakeSharedRange(std::move(HunkEncodedRows_), std::move(RowBuffer_));
        return DecodeHunksInSchemafulUnversionedRows(
            Schema_,
            TColumnFilter::MakeUniversal(),
            std::move(ChunkFragmentReader_),
            /*dictionaryCompressionFactory*/ nullptr,
            std::move(ChunkReadOptions_),
            std::move(PerformanceCounters_),
            EPerformanceCountedRequestType::Read,
            std::move(sharedRows))
            .Apply(BIND([] (const TSharedRange<TMutableUnversionedRow>& rowset) {
                auto writer = CreateWireProtocolWriter();
                std::vector<TUnversionedRow> unversionedRowset;
                unversionedRowset.reserve(rowset.size());
                for (auto row : rowset) {
                    unversionedRowset.push_back(row);
                }
                writer->WriteUnversionedRowset(unversionedRowset);
                return writer->Finish();
            }).AsyncVia(invoker));
    }

private:
    const TTableSchemaPtr Schema_;

    IChunkFragmentReaderPtr ChunkFragmentReader_;
    TClientChunkReadOptions ChunkReadOptions_;
    TTabletPerformanceCountersPtr PerformanceCounters_;

    TRowBufferPtr RowBuffer_;
    std::vector<TMutableUnversionedRow> HunkEncodedRows_;

    bool Postprocessed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

template <class TWriter>
class TFetchRowsSession
    : public TRefCounted
{
public:
    TFetchRowsSession(
        TTabletSnapshotPtr tabletSnapshot,
        i64 rowIndex,
        i64 maxRowCount,
        i64 maxDataWeight,
        i64 maxPullQueueResponseDataWeight,
        TClientChunkReadOptions chunkReadOptions,
        std::optional<std::string> profilingUser,
        IInvokerPtr invoker)
        : TabletSnapshot_(std::move(tabletSnapshot))
        , RowIndex_(rowIndex)
        , MaxPullQueueResponseDataWeight_(maxPullQueueResponseDataWeight)
        , ProfilingUser_(std::move(profilingUser))
        , Invoker_(std::move(invoker))
        , Logger(TabletNodeLogger().WithTag("ReadSessionId: %v", chunkReadOptions.ReadSessionId))
        , ChunkReadOptions_(std::move(chunkReadOptions))
        , Writer_(TabletSnapshot_, ChunkReadOptions_)
        , LeftRowCount_(maxRowCount)
        , LeftDataWeight_(maxDataWeight)
    { }

    TFuture<TFetchRowsFromOrderedStoreResult> Run(
        const IOrderedStorePtr& store,
        int tabletIndex)
    {
        YT_LOG_DEBUG("Fetching rows from ordered store (TabletId: %v, Store: %v, StartingRowIndex: %v, RowIndex: %v, "
            "MaxRowCount: %v, MaxDataWeight: %v, MaxResponseDataWeight: %v, HasHunkColumns: %v)",
            TabletSnapshot_->TabletId,
            store->GetId(),
            store->GetStartingRowIndex(),
            RowIndex_,
            LeftRowCount_,
            LeftDataWeight_,
            MaxPullQueueResponseDataWeight_,
            TabletSnapshot_->PhysicalSchema->HasHunkColumns());

        Reader_ = store->CreateReader(
            TabletSnapshot_,
            tabletIndex,
            /*lowerRowIndex*/ RowIndex_,
            /*upperRowIndex*/ std::numeric_limits<i64>::max(),
            AsyncLastCommittedTimestamp,
            TColumnFilter::MakeUniversal(),
            ChunkReadOptions_,
            ChunkReadOptions_.WorkloadDescriptor.Category);

        if (TabletSnapshot_->CommitOrdering == NTransactionClient::ECommitOrdering::Strong &&
            TabletSnapshot_->Settings.MountConfig->RetryReadOnOrderedStoreRotation)
        {
            Reader_ = CreateFailingOnRotationReader(std::move(Reader_), TabletSnapshot_);
        }

        UpdateReadOptions();

        DoRun(TError{});

        return ResultPromise_;
    }

private:
    const TTabletSnapshotPtr TabletSnapshot_;
    const i64 RowIndex_;
    const i64 MaxPullQueueResponseDataWeight_;
    const std::optional<std::string> ProfilingUser_;
    const IInvokerPtr Invoker_;

    const TPromise<TFetchRowsFromOrderedStoreResult> ResultPromise_ = NewPromise<TFetchRowsFromOrderedStoreResult>();

    const NLogging::TLogger Logger;

    TClientChunkReadOptions ChunkReadOptions_;

    TWriter Writer_;

    i64 LeftRowCount_;
    i64 ReadRowCount_ = 0;
    i64 LeftDataWeight_;
    i64 ReadDataWeight_ = 0;

    TRowBatchReadOptions ReadOptions_;
    ISchemafulUnversionedReaderPtr Reader_;


    void UpdateReadOptions()
    {
        ReadOptions_.MaxRowsPerRead = std::min(ReadOptions_.MaxRowsPerRead, LeftRowCount_);
        ReadOptions_.MaxDataWeightPerRead = std::min(ReadOptions_.MaxDataWeightPerRead, LeftDataWeight_);
    }

    void DoRun(const TError& error)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (ResultPromise_.IsSet()) {
            return;
        }

        if (!error.IsOK()) {
            YT_LOG_DEBUG(error, "Failed to fetch rows from ordered store (TabletId: %v)",
                TabletSnapshot_->TabletId);
            ResultPromise_.TrySet(error);
            return;
        }

        while (auto batch = Reader_->Read(ReadOptions_)) {
            if (batch->IsEmpty()) {
                YT_LOG_DEBUG("Waiting for rows from ordered store (TabletId: %v, RowIndex: %v)",
                    TabletSnapshot_->TabletId,
                    RowIndex_ + ReadRowCount_);

                Reader_->GetReadyEvent().Subscribe(BIND(
                    &TFetchRowsSession::DoRun,
                    MakeStrong(this))
                    .Via(Invoker_));

                return;
            }

            auto rows = batch->MaterializeRows();

            ReadRowCount_ += std::ssize(rows);
            LeftRowCount_ -= std::ssize(rows);

            i64 currentReadDataWeight = Writer_.GetRowsetDataWeight(rows);
            ReadDataWeight_ += currentReadDataWeight;
            LeftDataWeight_ -= currentReadDataWeight;

            Writer_.WriteRowset(rows);

            if (LeftRowCount_ <= 0 ||
                LeftDataWeight_ <= 0 ||
                ReadDataWeight_ >= MaxPullQueueResponseDataWeight_)
            {
                break;
            }

            UpdateReadOptions();
        }

        auto rowsetFuture = Writer_.Postprocess(Invoker_);
        if (auto maybeRowset = rowsetFuture.AsUnique().TryGet()) {
            FinalizeSession(*maybeRowset);
        } else {
            rowsetFuture.AsUnique().Subscribe(BIND(
                &TFetchRowsSession::FinalizeSession,
                MakeStrong(this)));
        }
    }

    void FinalizeSession(TErrorOr<std::vector<TSharedRef>> resultOrError)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto counters = TabletSnapshot_->TableProfiler->GetFetchTableRowsCounters(ProfilingUser_);

        if (!resultOrError.IsOK()) {
            YT_LOG_DEBUG(resultOrError, "Failed to finalize fetching rows from ordered store (TabletId: %v)",
                TabletSnapshot_->TabletId);

            counters->ChunkReaderStatisticsCounters.Increment(
                ChunkReadOptions_.ChunkReaderStatistics,
                /*failed*/ true);
            counters->HunkChunkReaderCounters.Increment(
                ChunkReadOptions_.HunkChunkReaderStatistics,
                /*failed*/ true);

            ResultPromise_.TrySet(resultOrError);
            return;
        }

        YT_LOG_DEBUG(
            "Fetched rows from ordered store (TabletId: %v, RowCount: %v, DataWeight: %v)",
            TabletSnapshot_->TabletId,
            ReadRowCount_,
            ReadDataWeight_);

        counters->RowCount.Increment(ReadRowCount_);
        counters->DataWeight.Increment(ReadDataWeight_);

        counters->ChunkReaderStatisticsCounters.Increment(
            ChunkReadOptions_.ChunkReaderStatistics,
            /*failed*/ false);
        counters->HunkChunkReaderCounters.Increment(
            ChunkReadOptions_.HunkChunkReaderStatistics,
            /*failed*/ false);

        ResultPromise_.TrySet(TFetchRowsFromOrderedStoreResult{
            .Rowsets = std::move(resultOrError.Value()),
            .RowCount = ReadRowCount_,
            .DataWeight = ReadDataWeight_,
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<TFetchRowsFromOrderedStoreResult> FetchRowsFromOrderedStore(
    TTabletSnapshotPtr tabletSnapshot,
    const IOrderedStorePtr& store,
    int tabletIndex,
    i64 rowIndex,
    i64 maxRowCount,
    i64 maxDataWeight,
    i64 maxPullQueueResponseDataWeight,
    TClientChunkReadOptions chunkReadOptions,
    std::optional<std::string> profilingUser,
    IInvokerPtr invoker)
{
    if (tabletSnapshot->PhysicalSchema->HasHunkColumns()) {
        chunkReadOptions.HunkChunkReaderStatistics = CreateHunkChunkReaderStatistics(
            tabletSnapshot->Settings.MountConfig->EnableHunkColumnarProfiling,
            tabletSnapshot->PhysicalSchema);

        auto session = New<TFetchRowsSession<THunkDecodingFetchRowsResultWriter>>(
            std::move(tabletSnapshot),
            rowIndex,
            maxRowCount,
            maxDataWeight,
            maxPullQueueResponseDataWeight,
            std::move(chunkReadOptions),
            std::move(profilingUser),
            std::move(invoker));
        return session->Run(store, tabletIndex);
    } else {
        auto session = New<TFetchRowsSession<TSimpleFetchRowsResultWriter>>(
            std::move(tabletSnapshot),
            rowIndex,
            maxRowCount,
            maxDataWeight,
            maxPullQueueResponseDataWeight,
            std::move(chunkReadOptions),
            std::move(profilingUser),
            std::move(invoker));
        return session->Run(store, tabletIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
