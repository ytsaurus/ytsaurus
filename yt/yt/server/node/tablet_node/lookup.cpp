#include "lookup.h"
#include "error_manager.h"
#include "hedging_manager_registry.h"
#include "private.h"
#include "store.h"
#include "tablet.h"
#include "tablet_profiling.h"
#include "tablet_reader.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"
#include "sorted_dynamic_store.h"

#include <yt/yt/server/node/query_agent/helpers.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/key_filter.h>
#include <yt/yt/ytlib/table_client/row_merger.h>
#include <yt/yt/ytlib/table_client/versioned_row_merger.h>

#include <yt/yt/client/table_client/pipe.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/versioned_reader.h>

#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/range_formatters.h>
#include <yt/yt/core/misc/tls_cache.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <optional>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NCompression;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NServer;

using NYT::FromProto;

using NTransactionClient::TReadTimestampRange;

////////////////////////////////////////////////////////////////////////////////

static constexpr i64 RowBufferCapacity = 1000;

////////////////////////////////////////////////////////////////////////////////

class TLookupSession;
using TLookupSessionPtr = TIntrusivePtr<TLookupSession>;

struct TTabletLookupRequest;

template <class TPipeline>
class TTabletLookupSession;

struct TPartitionSession;

class TStoreSession;

struct TLookupRowsBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EInitialQueryKind,
    ((LookupRows)    (0))
    ((SelectRows)    (1))
);

ETabletDistributedThrottlerKind GetThrottlerKindFromQueryKind(EInitialQueryKind queryKind)
{
    switch (queryKind) {
        case EInitialQueryKind::LookupRows:
            return ETabletDistributedThrottlerKind::Lookup;
        case EInitialQueryKind::SelectRows:
            return ETabletDistributedThrottlerKind::Select;
        default:
            YT_ABORT();
    }
}

ERequestType GetRequestTypeFromQueryKind(EInitialQueryKind queryKind)
{
    switch (queryKind) {
        case EInitialQueryKind::LookupRows:
            return ERequestType::Lookup;

        case EInitialQueryKind::SelectRows:
            return ERequestType::Read;

        default:
            YT_ABORT();
    }
}

class TAdapterBase
{
protected:
    TDataStatistics DataStatistics_;
    TCodecStatistics DecompressionStatistics_;
    TDuration ResponseCompressionTime_;
    TDuration HunksDecodingTime_;
    int FoundRowCount_ = 0;
    int FoundDataWeight_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TCompressingAdapterBase
    : public TAdapterBase
{
protected:
    static constexpr auto QueryKind = EInitialQueryKind::LookupRows;

    ICodec* const Codec_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;
    const std::unique_ptr<IWireProtocolWriter> Writer_ = CreateWireProtocolWriter();

    const TPromise<TSharedRef> ResultPromise_ = NewPromise<TSharedRef>();

    TCompressingAdapterBase(ICodec* codec, IMemoryUsageTrackerPtr memoryUsageTracker)
        : Codec_(codec)
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
    { }

    TCompressingAdapterBase(TCompressingAdapterBase&& other)
        : Codec_(other.Codec_)
        , MemoryUsageTracker_(std::move(other.MemoryUsageTracker_))
    { }

    TCompressingAdapterBase(const TCompressingAdapterBase&) = delete;

    void Finish(TWallTimer* timer)
    {
        HunksDecodingTime_ = timer->GetElapsedTime();

        timer->Restart();
        ResultPromise_.TrySet(TrackMemory(
            MemoryUsageTracker_,
            Codec_->Compress(Writer_->Finish())));
        ResponseCompressionTime_ = timer->GetElapsedTime();
        timer->Restart();
    }
};

class TUnversionedAdapter
    : public TCompressingAdapterBase
{
public:
    TUnversionedAdapter(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TColumnFilter& columnFilter,
        const TReadTimestampRange& timestampRange,
        ICodec* const codec,
        TRowBufferPtr rowBuffer,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        TTimestampColumnMapping timestampColumnMapping)
        : TCompressingAdapterBase(codec, std::move(memoryUsageTracker))
        , Merger_(std::make_unique<TSchemafulRowMerger>(
            std::move(rowBuffer),
            tabletSnapshot->PhysicalSchema->GetColumnCount() + (!timestampColumnMapping.empty()
                ? tabletSnapshot->PhysicalSchema->GetValueColumnCount()
                : 0),
            tabletSnapshot->PhysicalSchema->GetKeyColumnCount(),
            columnFilter,
            tabletSnapshot->ColumnEvaluator,
            timestampRange.RetentionTimestamp,
            timestampColumnMapping))
    { }

    TUnversionedAdapter(TUnversionedAdapter&& other) = default;
    TUnversionedAdapter& operator=(TUnversionedAdapter&&) = default;

protected:
    using TMutableRow = TMutableUnversionedRow;

    std::unique_ptr<TSchemafulRowMerger> Merger_;

    void WriteRow(TUnversionedRow row)
    {
        FoundRowCount_ += static_cast<bool>(row);
        FoundDataWeight_ += GetDataWeight(row);
        Writer_->WriteSchemafulRow(row);
    }
};

class TVersionedAdapter
    : public TCompressingAdapterBase
{
public:
    TVersionedAdapter(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TColumnFilter& columnFilter,
        const TRetentionConfigPtr& retentionConfig,
        const TReadTimestampRange& timestampRange,
        ICodec* const codec,
        TRowBufferPtr rowBuffer,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : TCompressingAdapterBase(codec, std::move(memoryUsageTracker))
        , Merger_(CreateVersionedRowMerger(
            tabletSnapshot->Settings.MountConfig->RowMergerType,
            std::move(rowBuffer),
            tabletSnapshot->PhysicalSchema,
            columnFilter,
            retentionConfig,
            timestampRange.Timestamp,
            MinTimestamp,
            tabletSnapshot->ColumnEvaluator,
            tabletSnapshot->CustomRuntimeData,
            /*mergeRowsOnFlush*/ false))
    { }

    TVersionedAdapter(TVersionedAdapter&&) = default;
    TVersionedAdapter& operator=(TVersionedAdapter&&) = default;

protected:
    using TMutableRow = TMutableVersionedRow;

    std::unique_ptr<IVersionedRowMerger> Merger_;

    void WriteRow(TVersionedRow row)
    {
        FoundRowCount_ += static_cast<bool>(row);
        FoundDataWeight_ += GetDataWeight(row);
        Writer_->WriteVersionedRow(row);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TRowAdapter>
class TSimplePipeline
    : public TRowAdapter
{
protected:
    using TAdapter = TRowAdapter;
    using typename TRowAdapter::TMutableRow;

    TSimplePipeline(
        TRowAdapter&& adapter,
        const TTabletSnapshotPtr& /*tabletSnapshot*/,
        const TColumnFilter& /*columnFilter*/,
        const TReadTimestampRange& timestampRange,
        const NChunkClient::TClientChunkReadOptions& /*chunkReadOptions*/,
        const std::optional<std::string>& /*profilingUser*/,
        TRowBufferPtr /*rowBuffer*/,
        NLogging::TLogger /*logger*/)
        : TRowAdapter(std::move(adapter))
        , Timestamp_(timestampRange.Timestamp)
    { }

    TSharedRange<TUnversionedRow> Initialize(TSharedRange<TUnversionedRow> lookupKeys)
    {
        return lookupKeys;
    }

    bool IsLookupInChunkNeeded(int /*index*/) const
    {
        return true;
    }

    TTimestamp GetReadTimestamp() const
    {
        return Timestamp_;
    }

    void AddPartialRow(TVersionedRow partialRow, TTimestamp timestamp, bool /*activeStore*/)
    {
        Merger_->AddPartialRow(partialRow, timestamp);
    }

    TMutableRow GetMergedRow()
    {
        return Merger_->BuildMergedRow();
    }

    void FinishRow()
    {
        auto mergedRow = GetMergedRow();
        TRowAdapter::WriteRow(mergedRow);
    }

    TFuture<void> PostprocessTabletLookup(TRefCountedPtr /*owner*/, TWallTimer* timer)
    {
        TRowAdapter::Finish(timer);
        return VoidFuture;
    }

private:
    using TRowAdapter::Merger_;

    const TTimestamp Timestamp_;
};

template <class TRowAdapter>
class TRowCachePipeline
    : public TRowAdapter
{
protected:
    using TAdapter = TRowAdapter;
    using TMutableRow = TMutableVersionedRow;

    TRowCachePipeline(
        TRowAdapter&& adapter,
        const TTabletSnapshotPtr& tabletSnapshot,
        const TColumnFilter& /*columnFilter*/,
        const TReadTimestampRange& timestampRange,
        const NChunkClient::TClientChunkReadOptions& /*chunkReadOptions*/,
        const std::optional<std::string>& profilingUser,
        TRowBufferPtr rowBuffer,
        NLogging::TLogger logger)
        : TRowAdapter(std::move(adapter))
        , TabletId_(tabletSnapshot->TabletId)
        , TableProfiler_(tabletSnapshot->TableProfiler)
        , RowCache_(tabletSnapshot->RowCache)
        , ProfilingUser_(profilingUser)
        , Timestamp_(timestampRange.Timestamp)
        , RetainedTimestamp_(tabletSnapshot->RetainedTimestamp)
        , StoreFlushIndex_(tabletSnapshot->StoreFlushIndex)
        , RowBuffer_(std::move(rowBuffer))
        , Logger(std::move(logger))
        , CacheRowMerger_(CreateVersionedRowMerger(
            tabletSnapshot->Settings.MountConfig->RowMergerType,
            RowBuffer_,
            tabletSnapshot->PhysicalSchema,
            TColumnFilter::MakeUniversal(),
            tabletSnapshot->Settings.MountConfig,
            GetCompactionTimestamp(tabletSnapshot->Settings.MountConfig, RetainedTimestamp_, Logger),
            MaxTimestamp, // Do not consider major timestamp.
            tabletSnapshot->ColumnEvaluator,
            tabletSnapshot->CustomRuntimeData,
            /*mergeRowsOnFlush*/ true)) // Always merge rows on flush.
    { }

    ~TRowCachePipeline()
    {
        auto flushIndex = RowCache_->GetFlushIndex();

        YT_LOG_DEBUG("Lookup in row cache finished "
            "(CacheHits: %v, CacheMisses: %v, CacheOutdated: %v, CacheInserts: %v, FailedInserts: %v, SuccessfulInserts: %v, "
            "FailedSealAttemptsByRevision: %v, NotSealedRows: %v, "
            "FailedFlushIndex: %v, MaxInsertedTimestamp: %v, FailedUpdates: %v, SuccessfulReinserts: %v, FailedReinserts: %v, "
            "StoreFlushIndex: %v, CacheFlushIndex: %v)",
            CacheHits_,
            CacheMisses_,
            CacheOutdated_,
            CacheInserts_,
            FailedInserts_,
            SuccessfulInserts_,
            FailedSealAttemptsByRevision_,
            NotSealedRows_,
            FailedFlushIndex_,
            MaxInsertedTimestamp_,
            FailedUpdates_,
            SuccessfulReinserts_,
            FailedReinserts_,
            StoreFlushIndex_,
            flushIndex);

        switch (TRowAdapter::QueryKind) {
            case EInitialQueryKind::LookupRows: {
                auto* counters = TableProfiler_->GetLookupCounters(ProfilingUser_);

                counters->CacheHits.Increment(CacheHits_);
                counters->CacheOutdated.Increment(CacheOutdated_);
                counters->CacheMisses.Increment(CacheMisses_);
                counters->CacheInserts.Increment(CacheInserts_);
                break;
            }

            case EInitialQueryKind::SelectRows: {
                auto* counters = TableProfiler_->GetSelectRowsCounters(ProfilingUser_);

                counters->CacheHits.Increment(CacheHits_);
                counters->CacheOutdated.Increment(CacheOutdated_);
                counters->CacheMisses.Increment(CacheMisses_);
                counters->CacheInserts.Increment(CacheInserts_);
                break;
            }

            default:
                YT_ABORT();
        }
    }

    TSharedRange<TUnversionedRow> Initialize(const TSharedRange<TUnversionedRow>& lookupKeys)
    {
        std::vector<TUnversionedRow> chunkLookupKeys;

        auto flushIndex = RowCache_->GetFlushIndex();

        YT_LOG_DEBUG("Lookup in row cache started (StoreFlushIndex: %v, RowCacheFlushIndex: %v)",
            StoreFlushIndex_,
            flushIndex);

        auto lookuper = RowCache_->GetCache()->GetLookuper();
        CacheInserter_ = RowCache_->GetCache()->GetInserter();
        for (auto key : lookupKeys) {
            auto foundItemRef = lookuper(key);

            if (auto foundItem = foundItemRef.Get()) {
                auto latestItem = GetLatestRow(foundItem);

                auto outdated = latestItem->Outdated.load(std::memory_order::acquire);

                if (!foundItemRef.IsSealed()) {
                    ++NotSealedRows_;
                } else if (outdated) {
                    ++CacheOutdated_;
                } else {
                    ++CacheHits_;
                    YT_LOG_TRACE("Row found (Key: %v)", key);

                    auto insertTable = CacheInserter_.GetTable();
                    if (insertTable == foundItemRef.Origin) {
                        YT_LOG_TRACE("Updating row");
                        if (!foundItemRef.Replace(latestItem, foundItem.Get(), true)) {
                            ++FailedUpdates_;
                        }
                    } else if (insertTable->Next == foundItemRef.Origin) {
                        YT_LOG_TRACE("Reinserting row");
                        if (auto insertedRef = insertTable->Insert(latestItem)) {
                            if (RowCache_->GetCache()->IsHead(insertTable)) {
                                insertedRef.SealItem();
                            }

                            ++SuccessfulReinserts_;
                        } else {
                            ++FailedReinserts_;
                        }
                    }

                    RowsFromCache_.push_back(std::move(latestItem));
                    continue;
                }
            } else {
                ++CacheMisses_;
                YT_LOG_TRACE("Row not found (Key: %v)", key);
            }

            chunkLookupKeys.push_back(key);
            RowsFromCache_.emplace_back();
        }

        RowsFromActiveStore_.resize(RowsFromCache_.size());
        return MakeSharedRange(std::move(chunkLookupKeys), lookupKeys);
    }

    TTimestamp GetReadTimestamp() const
    {
        // When using lookup cache we must read all versions.
        // It is safe to change fixed timestamp to SyncLastCommitted and drop newer than timestamp versions
        // in row merger.
        return Timestamp_ != AsyncLastCommittedTimestamp
            ? SyncLastCommittedTimestamp
            : Timestamp_;
    }

    bool IsLookupInChunkNeeded(int keyIndex) const
    {
        return !RowsFromCache_[keyIndex];
    }

    void AddPartialRow(TVersionedRow partialRow, TTimestamp /*timestamp*/, bool activeStore)
    {
        if (IsLookupInChunkNeeded(CurrentRowIndex_)) {
            // The only purpose of it is memory consumption optimization.
            // It does not affect correctness.
            // Make sense if row is absent in cache.
            // We must include values from active dynamic store in result, but we want to
            // minimize memory consumption in row cache and do not add values in CacheRowMerger_.
            // So we preserve row from active store and add only key to row cache.
            if (activeStore) {
                // Add key without values.
                CacheRowMerger_->AddPartialRow(partialRow, MinTimestamp);

                if (partialRow) {
                    for (const auto& value : partialRow.Values()) {
                        YT_VERIFY(None(value.Flags & EValueFlags::Hunk));
                    }
                }

                RowsFromActiveStore_[CurrentRowIndex_] = RowBuffer_->CaptureRow(partialRow);
            } else {
                CacheRowMerger_->AddPartialRow(partialRow, MaxTimestamp);
            }
        } else {
            // SimpleRowMerger_ performs compaction with MergeRowsOnFlush option and uses max MajorTimestamp.
            // It can be done if we have all versions of row.
            // Otherwise it can drop delete timestamps before earliestWriteTimestamp.
            // In this case some versions are read from cache.
            // So we need to use row merger without compaction.
            SimpleRowMerger_.AddPartialRow(partialRow);
        }
    }

    TMutableVersionedRow GetMergedRow()
    {
        // For non cached rows (IsLookupInChunkNeeded() == true) use CacheRowMerger_.
        // For cached rows use simple SimpleRowMerger_ which merges rows into one without compaction.

        auto mergedRow = IsLookupInChunkNeeded(CurrentRowIndex_)
            ? CacheRowMerger_->BuildMergedRow(true)
            : SimpleRowMerger_.BuildMergedRow(RowBuffer_);

        ++CurrentRowIndex_;
        return mergedRow;
    }

    void FinishRow()
    {
        auto mergedRow = GetMergedRow();
        WriteRow(mergedRow);
    }

    void WriteRow(TVersionedRow lookupedRow)
    {
        if (lookupedRow) {
            for (const auto& value : TRange(lookupedRow.BeginValues(), lookupedRow.EndValues())) {
                YT_VERIFY(None(value.Flags & EValueFlags::Hunk));
            }
        }

        Merger_->AddPartialRow(lookupedRow, Timestamp_ + 1);

        if (const auto* cachedItem = RowsFromCache_[WriteRowIndex_].Get()) {
            if (Timestamp_ < cachedItem->RetainedTimestamp) {
                THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::TimestampOutOfRange,
                    "Timestamp %v is less than retained timestamp %v of cached row in tablet %v",
                    Timestamp_,
                    cachedItem->RetainedTimestamp,
                    TabletId_);
            }

            YT_LOG_TRACE("Using row from cache (CacheRow: %v, Outdated: %v, ReadTimestamp: %v)",
                cachedItem->GetVersionedRow(),
                cachedItem->Outdated.load(),
                Timestamp_);

            Merger_->AddPartialRow(cachedItem->GetVersionedRow(), Timestamp_ + 1);
        } else {
            Merger_->AddPartialRow(RowsFromActiveStore_[WriteRowIndex_], Timestamp_ + 1);

            auto newItem = CachedRowFromVersionedRow(
                RowCache_->GetAllocator(),
                lookupedRow,
                RetainedTimestamp_);

            if (newItem) {
                YT_VERIFY(newItem->GetVersionedRow().GetKeyCount() > 0);

                newItem->InsertTime = GetInstant();

                if (newItem->GetVersionedRow().GetWriteTimestampCount() > 0) {
                    MaxInsertedTimestamp_ = std::max(
                        MaxInsertedTimestamp_,
                        newItem->GetVersionedRow().BeginWriteTimestamps()[0]);
                }

                YT_LOG_TRACE("Populating cache (Row: %v, Revision: %v)",
                    newItem->GetVersionedRow(),
                    StoreFlushIndex_);

                ++CacheInserts_;

                auto insertTable = CacheInserter_.GetTable();
                if (auto insertedRef = insertTable->Insert(newItem)) {
                    auto flushIndex = RowCache_->GetFlushIndex();

                    // Row revision is equal to flushRevision if the last passive dynamic store has started flushing.
                    if (flushIndex <= StoreFlushIndex_) {
                        insertedRef.SealItem();
                        ++SuccessfulInserts_;
                    } else {
                        FailedFlushIndex_ = flushIndex;
                        ++FailedSealAttemptsByRevision_;
                    }
                } else {
                    ++FailedInserts_;
                }
            }
        }

        ++WriteRowIndex_;

        auto mergedRow = Merger_->BuildMergedRow();
        TRowAdapter::WriteRow(mergedRow);
    }

    TFuture<void> PostprocessTabletLookup(TRefCountedPtr /*owner*/, TWallTimer* timer)
    {
        TRowAdapter::Finish(timer);
        return VoidFuture;
    }

private:
    class TSimpleRowMerger
    {
    public:
        void AddPartialRow(TVersionedRow row)
        {
            if (!row) {
                return;
            }

            if (!Started_) {
                Started_ = true;
                Keys_.resize(row.GetKeyCount());
                std::copy(row.BeginKeys(), row.EndKeys(), Keys_.data());
            } else {
                YT_VERIFY(std::ssize(Keys_) == row.GetKeyCount());
            }

            for (const auto& value : row.Values()) {
                Values_.push_back(value);
            }

            for (auto timestamp : row.DeleteTimestamps()) {
                DeleteTimestamps_.push_back(timestamp);
            }

            for (auto timestamp : row.WriteTimestamps()) {
                WriteTimestamps_.push_back(timestamp);
            }
        }

        TMutableVersionedRow BuildMergedRow(const TRowBufferPtr& rowBuffer)
        {
            if (!Started_) {
                return {};
            }

            std::sort(DeleteTimestamps_.begin(), DeleteTimestamps_.end(), [] (auto lhs, auto rhs) {
                return lhs > rhs;
            });
            DeleteTimestamps_.erase(
                std::unique(DeleteTimestamps_.begin(), DeleteTimestamps_.end()),
                DeleteTimestamps_.end());

            std::sort(WriteTimestamps_.begin(), WriteTimestamps_.end(), [] (auto lhs, auto rhs) {
                return lhs > rhs;
            });
            WriteTimestamps_.erase(
                std::unique(WriteTimestamps_.begin(), WriteTimestamps_.end()),
                WriteTimestamps_.end());

            // Sort input values by |(id, timestamp)| and remove duplicates.
            std::sort(
                Values_.begin(),
                Values_.end(),
                [&] (const TVersionedValue& lhs, const TVersionedValue& rhs) {
                    return lhs.Id != rhs.Id ? lhs.Id < rhs.Id : lhs.Timestamp > rhs.Timestamp;
                });
            Values_.erase(
                std::unique(
                    Values_.begin(),
                    Values_.end(),
                    [] (const TVersionedValue& lhs, const TVersionedValue& rhs) {
                        return std::tie(lhs.Id, lhs.Timestamp) == std::tie(rhs.Id, rhs.Timestamp);
                    }),
                Values_.end());


            // Construct output row.
            auto row = rowBuffer->AllocateVersioned(
                Keys_.size(),
                Values_.size(),
                WriteTimestamps_.size(),
                DeleteTimestamps_.size());

            // Construct output keys.
            std::copy(Keys_.begin(), Keys_.end(), row.BeginKeys());

            // Construct output values.
            std::copy(Values_.begin(), Values_.end(), row.BeginValues());

            // Construct output timestamps.
            std::copy(WriteTimestamps_.begin(), WriteTimestamps_.end(), row.BeginWriteTimestamps());
            std::copy(DeleteTimestamps_.begin(), DeleteTimestamps_.end(), row.BeginDeleteTimestamps());

            Cleanup();

            return row;
        }

        void Cleanup()
        {
            Started_ = false;

            Keys_.clear();
            Values_.clear();
            WriteTimestamps_.clear();
            DeleteTimestamps_.clear();
        }

    private:
        bool Started_ = false;

        std::vector<TUnversionedValue> Keys_;
        std::vector<TVersionedValue> Values_;
        std::vector<TTimestamp> WriteTimestamps_;
        std::vector<TTimestamp> DeleteTimestamps_;
    };

    using TRowAdapter::Merger_;

    const TTabletId TabletId_;
    const TTableProfilerPtr TableProfiler_;
    const TRowCachePtr RowCache_;
    const std::optional<std::string> ProfilingUser_;
    const TTimestamp Timestamp_;
    const TTimestamp RetainedTimestamp_;
    const ui32 StoreFlushIndex_;
    const TRowBufferPtr RowBuffer_;
    const NLogging::TLogger Logger;
    const std::unique_ptr<IVersionedRowMerger> CacheRowMerger_;

    TSimpleRowMerger SimpleRowMerger_;

    // Holds references to lookup tables.
    TConcurrentCache<TCachedRow>::TInserter CacheInserter_;
    std::vector<TCachedRowPtr> RowsFromCache_;
    std::vector<TVersionedRow> RowsFromActiveStore_;

    // Assume that rows are finished and written in order.
    int CurrentRowIndex_ = 0;
    int WriteRowIndex_ = 0;

    int CacheHits_ = 0;
    int CacheMisses_ = 0;
    int CacheOutdated_ = 0;
    int CacheInserts_ = 0;

    int FailedUpdates_ = 0;
    int FailedReinserts_ = 0;
    int SuccessfulReinserts_ = 0;

    int FailedInserts_ = 0;
    int SuccessfulInserts_ = 0;
    int FailedSealAttemptsByRevision_ = 0;
    int NotSealedRows_ = 0;
    ui32 FailedFlushIndex_ = 0;

    TTimestamp MaxInsertedTimestamp_ = 0;

    static TTimestamp GetCompactionTimestamp(
        const TTableMountConfigPtr& mountConfig,
        TTimestamp retainedTimestamp,
        const NLogging::TLogger& Logger)
    {
        auto compactionTimestamp = NTransactionClient::InstantToTimestamp(
            NTransactionClient::TimestampToInstant(retainedTimestamp).first + mountConfig->MinDataTtl).first;

        YT_LOG_DEBUG("Creating row merger for row cache (CompactionTimestamp: %v)",
            compactionTimestamp);

        return compactionTimestamp;
    }
};

template <class TBasePipeline>
class THunkDecodingPipeline
    : public TBasePipeline
{
protected:
    THunkDecodingPipeline(
        TBasePipeline::TAdapter&& adapter,
        const TTabletSnapshotPtr& tabletSnapshot,
        const TColumnFilter& columnFilter,
        const TReadTimestampRange& timestampRange,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        const std::optional<std::string>& profilingUser,
        TRowBufferPtr rowBuffer,
        const NLogging::TLogger Logger)
        : TBasePipeline(
            std::move(adapter),
            tabletSnapshot,
            columnFilter,
            timestampRange,
            chunkReadOptions,
            profilingUser,
            rowBuffer,
            Logger)
        , Schema_(tabletSnapshot->PhysicalSchema)
        , ColumnFilter_(columnFilter)
        , RowBuffer_(std::move(rowBuffer))
        , ChunkFragmentReader_(tabletSnapshot->ChunkFragmentReader)
        , DictionaryCompressionFactory_(tabletSnapshot->DictionaryCompressionFactory)
        , ChunkReadOptions_(chunkReadOptions)
        , PerformanceCounters_(tabletSnapshot->PerformanceCounters)
    {
        if (const auto& hedgingManagerRegistry = tabletSnapshot->HedgingManagerRegistry) {
            ChunkReadOptions_.HedgingManager = hedgingManagerRegistry->GetOrCreateHedgingManager(
                THedgingUnit{
                    // TODO(babenko): migrate to std::string
                    .UserTag = profilingUser ? std::optional<TString>(profilingUser) : std::nullopt,
                    .HunkChunk = true,
                });
        }
    }

    void FinishRow()
    {
        auto mergedRow = TBasePipeline::GetMergedRow();
        RowBuffer_->CaptureValues(mergedRow);
        HunkEncodedRows_.push_back(mergedRow);
    }

    TFuture<void> PostprocessTabletLookup(TRefCountedPtr owner, TWallTimer* timer)
    {
        auto sharedRows = MakeSharedRange(std::move(HunkEncodedRows_), std::move(RowBuffer_));

        // Being rigorous we should wrap the callback into AsyncVia but that does not matter in practice.
        return DecodeHunks(std::move(sharedRows))
            // NB: Owner captures this by strong ref.
            .Apply(BIND([this, timer, owner = std::move(owner)] (const TSharedRange<TMutableRow>& rows) {
                for (auto row : rows) {
                    TBasePipeline::WriteRow(row);
                }

                return TBasePipeline::PostprocessTabletLookup(owner, timer);
            }));
    }

private:
    using typename TBasePipeline::TMutableRow;

    const TTableSchemaPtr Schema_;
    const TColumnFilter ColumnFilter_;
    const TRowBufferPtr RowBuffer_;

    NChunkClient::IChunkFragmentReaderPtr ChunkFragmentReader_;
    NTableClient::IDictionaryCompressionFactoryPtr DictionaryCompressionFactory_;
    NChunkClient::TClientChunkReadOptions ChunkReadOptions_;
    NTableClient::TTabletPerformanceCountersPtr PerformanceCounters_;

    std::vector<TMutableRow> HunkEncodedRows_;
    bool HunksDecoded_ = false;


    TFuture<TSharedRange<TMutableUnversionedRow>> DecodeHunks(
        TSharedRange<TMutableUnversionedRow> rows)
    {
        YT_VERIFY(!std::exchange(HunksDecoded_, true));

        return DecodeHunksInSchemafulUnversionedRows(
            Schema_,
            ColumnFilter_,
            std::move(ChunkFragmentReader_),
            std::move(DictionaryCompressionFactory_),
            std::move(ChunkReadOptions_),
            std::move(PerformanceCounters_),
            GetRequestTypeFromQueryKind(TBasePipeline::TAdapter::QueryKind),
            std::move(rows));
    }

    TFuture<TSharedRange<TMutableVersionedRow>> DecodeHunks(
        TSharedRange<TMutableVersionedRow> rows)
    {
        YT_VERIFY(!std::exchange(HunksDecoded_, true));

        return DecodeHunksInVersionedRows(
            std::move(ChunkFragmentReader_),
            std::move(DictionaryCompressionFactory_),
            std::move(ChunkReadOptions_),
            std::move(PerformanceCounters_),
            GetRequestTypeFromQueryKind(TBasePipeline::TAdapter::QueryKind),
            std::move(rows));
    }
};

////////////////////////////////////////////////////////////////////////////////

bool GetUseLookupCache(const TTabletSnapshotPtr& tabletSnapshot, std::optional<bool> useLookupCache)
{
    return
        tabletSnapshot->RowCache &&
        useLookupCache.value_or(tabletSnapshot->Settings.MountConfig->EnableLookupCacheByDefault);
}

NTableClient::TColumnFilter DecodeColumnFilter(
    std::unique_ptr<NTableClient::NProto::TColumnFilter> protoColumnFilter,
    int columnCount)
{
    auto columnFilter = protoColumnFilter
        ? TColumnFilter(FromProto<TColumnFilter::TIndexes>(protoColumnFilter->indexes()))
        : TColumnFilter();
    ValidateColumnFilter(columnFilter, columnCount);
    return columnFilter;
}

////////////////////////////////////////////////////////////////////////////////

class TStoreSession
{
public:
    explicit TStoreSession(IVersionedReaderPtr reader)
        : Reader_(std::move(reader))
    { }

    TStoreSession(const TStoreSession& otherSession) = delete;
    TStoreSession(TStoreSession&& otherSession) = default;

    TStoreSession& operator=(const TStoreSession& otherSession) = delete;
    TStoreSession& operator=(TStoreSession&& otherSession)
    {
        YT_VERIFY(!Reader_);
        YT_VERIFY(!otherSession.Reader_);
        return *this;
    }

    TFuture<void> Open() const
    {
        return Reader_->Open();
    }

    TVersionedRow FetchRow()
    {
        YT_ASSERT(IsReaderReady());
        return Rows_[RowIndex_++];
    }

    bool PrepareBatch()
    {
        if (IsReaderReady()) {
            return true;
        }

        RowIndex_ = 0;

        auto rowBatch = Reader_->Read(TRowBatchReadOptions{
            .MaxRowsPerRead = RowBufferCapacity
        });

        YT_VERIFY(rowBatch);
        if (rowBatch->IsEmpty()) {
            Rows_.Reset();
            return false;
        }

        Rows_ = rowBatch->MaterializeRows();
        return true;
    }

    TFuture<void> GetReadyEvent() const
    {
        return Reader_->GetReadyEvent();
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const
    {
        return Reader_->GetDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const
    {
        return Reader_->GetDecompressionStatistics();
    }

private:
    const IVersionedReaderPtr Reader_;

    TSharedRange<TVersionedRow> Rows_;
    int RowIndex_ = -1;

    bool IsReaderReady() const
    {
        return Rows_ && RowIndex_ < std::ssize(Rows_);
    }
};

static constexpr int TypicalStoreSessionCount = 16;
using TStoreSessionList = TCompactVector<TStoreSession, TypicalStoreSessionCount>;

////////////////////////////////////////////////////////////////////////////////

struct TPartitionSession
{
    int CurrentKeyIndex;
    int EndKeyIndex;

    const TPartitionSnapshotPtr PartitionSnapshot;
    const TSharedRange<TLegacyKey> ChunkLookupKeys;

    // TODO(akozhikhov): Proper block fetcher: Create all partition sessions at the beginning of the lookup session.
    // Right know we cannot do that because chunk reader may call Open in ctor and start reading blocks.
    bool SessionStarted = false;

    TStoreSessionList StoreSessions;
    bool StoreSessionsPrepared = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TTabletLookupRequest
{
    const TTabletId TabletId;
    const TCellId CellId;
    const NHydra::TRevision MountRevision;
    const TSharedRef RequestData;

    std::vector<TError> InnerErrors;

    TFuture<TSharedRef> RunTabletLookupSession(const TLookupSessionPtr& lookupSession);
};

class TLookupSession
    : public ILookupSession
{
public:
    TLookupSession(
        EInMemoryMode inMemoryMode,
        int tabletRequestCount,
        ICodec* responseCodec,
        int maxRetryCount,
        int maxConcurrentSubqueries,
        TReadTimestampRange timestampRange,
        std::optional<bool> useLookupCache,
        NChunkClient::TClientChunkReadOptions chunkReadOptions,
        TRetentionConfigPtr retentionConfig,
        bool enablePartialResult,
        TVersionedReadOptions versionedReadOptions,
        const ITabletSnapshotStorePtr& snapshotStore,
        const std::optional<std::string>& profilingUser,
        IInvokerPtr invoker);

    ~TLookupSession();

    void AddTabletRequest(
        TTabletId tabletId,
        TCellId cellId,
        NHydra::TRevision mountRevision,
        TSharedRef requestData) override;

    TFuture<std::vector<TSharedRef>> Run() override;

private:
    friend struct TTabletLookupRequest;

    template <class TPipeline>
    friend class TTabletLookupSession;

    const EInMemoryMode InMemoryMode_;
    const TReadTimestampRange TimestampRange_;
    ICodec* const ResponseCodec_;
    const int MaxRetryCount_;
    const int MaxConcurrentSubqueries_;
    const std::optional<bool> UseLookupCache_;
    const TRetentionConfigPtr RetentionConfig_;
    const bool EnablePartialResult_;
    const TVersionedReadOptions VersionedReadOptions_;
    const ITabletSnapshotStorePtr& SnapshotStore_;
    const std::optional<std::string> ProfilingUser_;
    const IInvokerPtr Invoker_;

    const NLogging::TLogger Logger;

    TWallTimer WallTimer_;
    NChunkClient::TClientChunkReadOptions ChunkReadOptions_;
    std::optional<std::pair<TTabletSnapshotPtr, TServiceProfilerGuard>> ProfilerGuard_;

    std::vector<TTabletLookupRequest> TabletRequests_;

    THazardPtrReclaimGuard HazardPtrReclaimGuard_;

    std::optional<TDuration> CpuTime_;
    // This flag is used to increment wasted_* profiling counters in case of failed lookup.
    bool FinishedSuccessfully_ = false;

    // NB: These counters are updated within TTabletLookupSession dtor
    // and used for profiling within TLookupSession dtor.
    std::atomic<int> FoundRowCount_ = 0;
    std::atomic<i64> FoundDataWeight_ = 0;
    std::atomic<int> MissingRowCount_ = 0;
    std::atomic<int> UnmergedRowCount_ = 0;
    std::atomic<int> UnmergedMissingRowCount_ = 0;
    std::atomic<i64> UnmergedDataWeight_ = 0;
    std::atomic<TDuration::TValue> DecompressionCpuTime_ = 0;
    std::atomic<int> RetryCount_ = 0;


    TFuture<TSharedRef> RunTabletRequest(
        int requestIndex);
    TFuture<TSharedRef> OnTabletLookupAttemptFinished(
        int requestIndex,
        const TErrorOr<TSharedRef>& resultOrError);
    TFuture<TSharedRef> OnTabletLookupAttemptFailed(
        int requestIndex,
        const TError& error);
    TFuture<TSharedRef> OnTabletLookupFailed(TTabletId tabletId, TError error);

    std::vector<TSharedRef> ProcessResults(std::vector<TErrorOr<TSharedRef>>&& resultOrErrors);
};

////////////////////////////////////////////////////////////////////////////////

template <class TPipeline>
class TTabletLookupSession
    : public TRefCounted
    , public TPipeline
{
public:
    TTabletLookupSession(
        TPipeline::TAdapter&& adapter,
        TTabletSnapshotPtr tabletSnapshot,
        bool produceAllVersions,
        TColumnFilter columnFilter,
        TSharedRange<TUnversionedRow> lookupKeys,
        TLookupSessionPtr lookupSession,
        TRowBufferPtr rowBuffer);

    TTabletLookupSession(
        TPipeline::TAdapter&& adapter,
        TTabletSnapshotPtr tabletSnapshot,
        TSharedRange<TUnversionedRow> lookupKeys,
        bool produceAllVersions,
        TColumnFilter columnFilter,
        const TReadTimestampRange& readTimestampRange,
        const TClientChunkReadOptions& chunkReadOptions,
        IInvokerPtr invoker,
        const std::optional<std::string>& profilingUser,
        TRowBufferPtr rowBuffer,
        NLogging::TLogger logger);

    ~TTabletLookupSession();

    auto Run() -> TFuture<typename decltype(TPipeline::TAdapter::ResultPromise_)::TValueType>;

private:
    const IInvokerPtr Invoker_;
    const TTabletSnapshotPtr TabletSnapshot_;
    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const TClientChunkReadOptions ChunkReadOptions_;
    const TColumnFilter ColumnFilter_;
    const TSharedRange<TUnversionedRow> LookupKeys_;
    const TSharedRange<TUnversionedRow> ChunkLookupKeys_;
    const std::function<void()> OnDestruction_;

    const NLogging::TLogger Logger;

    int ActiveStoreIndex_ = -1;

    TStoreSessionList DynamicEdenSessions_;
    TStoreSessionList ChunkEdenSessions_;

    int CurrentPartitionSessionIndex_ = 0;
    std::vector<TPartitionSession> PartitionSessions_;

    using TPipeline::FoundRowCount_;
    using TPipeline::FoundDataWeight_;
    using TPipeline::DataStatistics_;
    using TPipeline::DecompressionStatistics_;
    using TPipeline::HunksDecodingTime_;
    using TPipeline::ResponseCompressionTime_;
    using TPipeline::TAdapter::ResultPromise_;

    int RequestedUnmergedRowCount_ = 0;

    TWallTimer Timer_;
    TDuration InitializationDuration_;
    TDuration PartitionsLookupDuration_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CancelationSpinLock_);
    std::optional<TError> CancelationError_;
    TFuture<void> SessionFuture_ = VoidFuture;


    TPartitionSession CreatePartitionSession(
        decltype(LookupKeys_)::iterator* currentIt,
        int* startChunkKeyIndex);

    TStoreSessionList CreateStoreSessions(
        const std::vector<ISortedStorePtr>& stores,
        const TSharedRange<TLegacyKey>& keys);

    std::vector<TFuture<void>> OpenStoreSessions(const TStoreSessionList& sessions);

    void LookupInPartitions(const TError& error);

    //! These return |true| if caller should stop due to error or scheduled asynchronous execution.
    bool LookupInCurrentPartition();
    bool DoLookupInCurrentPartition();

    void OnStoreSessionsPrepared();
    void LookupFromStoreSessions(TStoreSessionList* sessions, int activeStoreIndex);

    void FinishSession(const TError& error);

    void UpdateUnmergedStatistics(const TStoreSessionList& sessions);

    void OnCanceled(const TError& error);
    void SetSessionFuture(TFuture<void> sessionFuture);
};

////////////////////////////////////////////////////////////////////////////////

TLookupSession::TLookupSession(
    EInMemoryMode inMemoryMode,
    int tabletRequestCount,
    ICodec* responseCodec,
    int maxRetryCount,
    int maxConcurrentSubqueries,
    TReadTimestampRange timestampRange,
    std::optional<bool> useLookupCache,
    NChunkClient::TClientChunkReadOptions chunkReadOptions,
    TRetentionConfigPtr retentionConfig,
    bool enablePartialResult,
    TVersionedReadOptions versionedReadOptions,
    const ITabletSnapshotStorePtr& snapshotStore,
    const std::optional<std::string>& profilingUser,
    IInvokerPtr invoker)
    : InMemoryMode_(inMemoryMode)
    , TimestampRange_(timestampRange)
    , ResponseCodec_(responseCodec)
    , MaxRetryCount_(maxRetryCount)
    , MaxConcurrentSubqueries_(maxConcurrentSubqueries)
    , UseLookupCache_(useLookupCache)
    , RetentionConfig_(std::move(retentionConfig))
    , EnablePartialResult_(enablePartialResult)
    , VersionedReadOptions_(std::move(versionedReadOptions))
    , SnapshotStore_(snapshotStore)
    , ProfilingUser_(profilingUser)
    , Invoker_(std::move(invoker))
    , Logger(TabletNodeLogger().WithTag("ReadSessionId: %v", chunkReadOptions.ReadSessionId))
    , ChunkReadOptions_(std::move(chunkReadOptions))
{
    TabletRequests_.reserve(tabletRequestCount);
}

void TLookupSession::AddTabletRequest(
    TTabletId tabletId,
    TCellId cellId,
    NHydra::TRevision mountRevision,
    TSharedRef requestData)
{
    TabletRequests_.push_back(TTabletLookupRequest{
        .TabletId = tabletId,
        .CellId = cellId,
        .MountRevision = mountRevision,
        .RequestData = std::move(requestData),
    });

    if (!ProfilerGuard_) {
        // NB: Any tablet snapshot will suffice.
        if (auto tabletSnapshot = SnapshotStore_->FindTabletSnapshot(tabletId, mountRevision)) {
            const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
            ChunkReadOptions_.MultiplexingParallelism = mountConfig->LookupRpcMultiplexingParallelism;
            ChunkReadOptions_.HunkChunkReaderStatistics = CreateHunkChunkReaderStatistics(
                mountConfig->EnableHunkColumnarProfiling,
                tabletSnapshot->PhysicalSchema);

            ChunkReadOptions_.KeyFilterStatistics = mountConfig->EnableKeyFilterForLookup
                ? New<TKeyFilterStatistics>()
                : nullptr;

            if (InMemoryMode_ == EInMemoryMode::None) {
                if (const auto& hedgingManagerRegistry = tabletSnapshot->HedgingManagerRegistry) {
                    ChunkReadOptions_.HedgingManager = hedgingManagerRegistry->GetOrCreateHedgingManager(
                        THedgingUnit{
                            // TODO(babenko): migrate to std::string
                            .UserTag = ProfilingUser_ ? std::optional<TString>(ProfilingUser_) : std::nullopt,
                            .HunkChunk = false,
                        });
                }
            }

            auto counters = tabletSnapshot->TableProfiler->GetQueryServiceCounters(ProfilingUser_);
            ProfilerGuard_.emplace(std::pair(std::move(tabletSnapshot), TServiceProfilerGuard{}));
            ProfilerGuard_->second.Start(counters->Multiread);
        }
    }
}

TFuture<std::vector<TSharedRef>> TLookupSession::Run()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    if (TabletRequests_.empty()) {
        return MakeFuture<std::vector<TSharedRef>>({});
    }

    if (InMemoryMode_ == EInMemoryMode::Uncompressed) {
        std::vector<TFuture<TSharedRef>> futures;
        futures.reserve(TabletRequests_.size());

        std::vector<TErrorOr<TSharedRef>> results;
        results.reserve(TabletRequests_.size());

        for (int requestIndex = 0; requestIndex < std::ssize(TabletRequests_); ++requestIndex) {
            futures.push_back(RunTabletRequest(requestIndex));
            if (futures.back().IsSet()) {
                results.push_back(futures.back().Get());
            }
        }

        // TODO(akozhikhov): Proper block fetcher: we may face unset futures here
        // presumably due to some issues with block fetching logic in old columnar readers.
        if (futures.size() != results.size()) {
            return AllSet(std::move(futures)).ApplyUnique(BIND(
                &TLookupSession::ProcessResults,
                MakeStrong(this)));
        }

        return MakeFuture(ProcessResults(std::move(results)));
    }

    std::vector<TCallback<TFuture<TSharedRef>()>> callbacks;
    callbacks.reserve(TabletRequests_.size());

    for (int requestIndex = 0; requestIndex < std::ssize(TabletRequests_); ++requestIndex) {
        callbacks.push_back(BIND(
            &TLookupSession::RunTabletRequest,
            MakeStrong(this),
            requestIndex)
            .AsyncVia(Invoker_));
    }

    return CancelableRunWithBoundedConcurrency(
        std::move(callbacks),
        MaxConcurrentSubqueries_)
        .ApplyUnique(BIND(
            &TLookupSession::ProcessResults,
            MakeStrong(this)));
}

TFuture<TSharedRef> TLookupSession::RunTabletRequest(int requestIndex)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    TFuture<TSharedRef> future;
    try {
        future = TabletRequests_[requestIndex].RunTabletLookupSession(this);
    } catch (const std::exception& ex) {
        return OnTabletLookupAttemptFailed(requestIndex, TError(ex));
    }

    if (auto maybeResult = future.TryGet()) {
        return OnTabletLookupAttemptFinished(requestIndex, *maybeResult);
    }
    return future.Apply(BIND(
        &TLookupSession::OnTabletLookupAttemptFinished,
        MakeStrong(this),
        requestIndex));
}

TFuture<TSharedRef> TLookupSession::OnTabletLookupAttemptFinished(
    int requestIndex,
    const TErrorOr<TSharedRef>& resultOrError)
{
    if (resultOrError.IsOK()) {
        return MakeFuture(resultOrError.Value());
    } else {
        return BIND(
            &TLookupSession::OnTabletLookupAttemptFailed,
            MakeStrong(this),
            requestIndex,
            resultOrError)
            .AsyncVia(Invoker_)
            .Run();
    }
}

TFuture<TSharedRef> TLookupSession::OnTabletLookupAttemptFailed(
    int requestIndex,
    const TError& error)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_VERIFY(!error.IsOK());

    auto& request = TabletRequests_[requestIndex];

    if (NQueryAgent::IsRetriableQueryError(error)) {
        request.InnerErrors.push_back(error);
        if (std::ssize(request.InnerErrors) < MaxRetryCount_) {
            YT_LOG_INFO(error, "Tablet lookup request failed, retrying "
                "(Iteration: %v, MaxRetryCount: %v, TabletId: %v)",
                std::ssize(request.InnerErrors),
                MaxRetryCount_,
                request.TabletId);

            RetryCount_.fetch_add(1, std::memory_order::relaxed);

            return RunTabletRequest(requestIndex);
        } else {
            return OnTabletLookupFailed(
                request.TabletId,
                TError("Request failed after %v retries",
                    MaxRetryCount_)
                    << request.InnerErrors);
        }
    } else {
        YT_LOG_DEBUG(error, "Tablet lookup request failed (TabletId: %v)",
            request.TabletId);

        return OnTabletLookupFailed(request.TabletId, error);
    }
}

TFuture<TSharedRef> TLookupSession::OnTabletLookupFailed(TTabletId tabletId, TError error)
{
    if (auto tabletSnapshot = SnapshotStore_->FindLatestTabletSnapshot(tabletId)) {
        tabletSnapshot->PerformanceCounters->LookupError.Counter.fetch_add(1, std::memory_order::relaxed);

        return MakeFuture<TSharedRef>(EnrichErrorForErrorManager(std::move(error),tabletSnapshot));
    }

    return MakeFuture<TSharedRef>(std::move(error));
}

std::vector<TSharedRef> TLookupSession::ProcessResults(
    std::vector<TErrorOr<TSharedRef>>&& resultOrErrors)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    // NB: No trace context is available in dtor so we have to fetch cpu time here.
    if (const auto* traceContext = NTracing::TryGetCurrentTraceContext()) {
        NTracing::FlushCurrentTraceContextElapsedTime();
        CpuTime_ = traceContext->GetElapsedTime();
    }

    std::vector<TSharedRef> results;
    results.reserve(resultOrErrors.size());

    int skippedTabletResultCount = 0;
    for (auto& resultOrError : resultOrErrors) {
        if (!resultOrError.IsOK()) {
            if (EnablePartialResult_) {
                ++skippedTabletResultCount;
                results.emplace_back();
                continue;
            } else {
                YT_LOG_DEBUG(resultOrError, "Lookup session failed");
                resultOrError.ThrowOnError();
            }
        }

        results.push_back(std::move(resultOrError.Value()));
    }

    FinishedSuccessfully_ = true;

    YT_LOG_DEBUG("Lookup session finished successfully "
        "(CpuTime: %v, RemoteCpuTime: %v, WallTime: %v, SkippedTabletResultCount: %v)",
        CpuTime_,
        ChunkReadOptions_.ChunkReaderStatistics->RemoteCpuTime.load(std::memory_order::relaxed),
        WallTimer_.GetElapsedTime(),
        skippedTabletResultCount);

    return results;
}

TLookupSession::~TLookupSession()
{
    if (!ProfilerGuard_) {
        return;
    }

    const auto& tabletSnapshot = ProfilerGuard_->first;

    auto* counters = tabletSnapshot->TableProfiler->GetLookupCounters(ProfilingUser_);

    counters->RowCount.Increment(FoundRowCount_.load(std::memory_order::relaxed));
    counters->MissingRowCount.Increment(MissingRowCount_.load(std::memory_order::relaxed));
    counters->DataWeight.Increment(FoundDataWeight_.load(std::memory_order::relaxed));
    counters->UnmergedRowCount.Increment(UnmergedRowCount_.load(std::memory_order::relaxed));
    counters->UnmergedMissingRowCount.Increment(UnmergedMissingRowCount_.load(std::memory_order::relaxed));
    counters->UnmergedDataWeight.Increment(UnmergedDataWeight_.load(std::memory_order::relaxed));
    if (!FinishedSuccessfully_) {
        counters->WastedUnmergedDataWeight.Increment(UnmergedDataWeight_.load(std::memory_order::relaxed));
    }

    counters->DecompressionCpuTime.Add(
        TDuration::MicroSeconds(DecompressionCpuTime_.load(std::memory_order::relaxed)));
    if (CpuTime_) {
        counters->CpuTime.Add(*CpuTime_);
        tabletSnapshot->PerformanceCounters->LookupCpuTime.Counter.fetch_add(
            CpuTime_->MicroSeconds(),
            std::memory_order::relaxed);
    }

    counters->RetryCount.Increment(RetryCount_.load(std::memory_order::relaxed));

    counters->ChunkReaderStatisticsCounters.Increment(
        ChunkReadOptions_.ChunkReaderStatistics,
        !FinishedSuccessfully_);
    counters->HunkChunkReaderCounters.Increment(
        ChunkReadOptions_.HunkChunkReaderStatistics,
        !FinishedSuccessfully_);

    if (FinishedSuccessfully_ && tabletSnapshot->Settings.MountConfig->EnableDetailedProfiling) {
        counters->LookupDuration.Record(WallTimer_.GetElapsedTime());
    }

    if (const auto& keyFilterStatistics = ChunkReadOptions_.KeyFilterStatistics) {
        counters->KeyFilterCounters.InputKeyCount.Increment(
            keyFilterStatistics->InputEntryCount.load(std::memory_order::relaxed));
        counters->KeyFilterCounters.FilteredOutKeyCount.Increment(
            keyFilterStatistics->FilteredOutEntryCount.load(std::memory_order::relaxed));
        counters->KeyFilterCounters.FalsePositiveKeyCount.Increment(
            keyFilterStatistics->FalsePositiveEntryCount.load(std::memory_order::relaxed));
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TRowAdapter>
TFuture<TSharedRef> DoRunTabletLookupSession(
    TRowAdapter&& adapter,
    bool useLookupCache,
    TTabletSnapshotPtr tabletSnapshot,
    bool produceAllVersions,
    TColumnFilter columnFilter,
    TSharedRange<TUnversionedRow> lookupKeys,
    TLookupSessionPtr lookupSession,
    TRowBufferPtr rowBuffer)
{
    if (useLookupCache) {
        if (tabletSnapshot->PhysicalSchema->HasHunkColumns()) {
            using TWholePipeline = TTabletLookupSession<THunkDecodingPipeline<TRowCachePipeline<TRowAdapter>>>;
            return New<TWholePipeline>(
                std::move(adapter),
                std::move(tabletSnapshot),
                /*produceAllVersions*/ true,
                std::move(columnFilter),
                std::move(lookupKeys),
                std::move(lookupSession),
                std::move(rowBuffer))
                ->Run();
        } else {
            using TWholePipeline = TTabletLookupSession<TRowCachePipeline<TRowAdapter>>;
            return New<TWholePipeline>(
                std::move(adapter),
                std::move(tabletSnapshot),
                /*produceAllVersions*/ true,
                std::move(columnFilter),
                std::move(lookupKeys),
                std::move(lookupSession),
                std::move(rowBuffer))
                ->Run();
        }
    } else {
        if (tabletSnapshot->PhysicalSchema->HasHunkColumns()) {
            using TWholePipeline = TTabletLookupSession<THunkDecodingPipeline<TSimplePipeline<TRowAdapter>>>;
            return New<TWholePipeline>(
                std::move(adapter),
                std::move(tabletSnapshot),
                produceAllVersions,
                std::move(columnFilter),
                std::move(lookupKeys),
                std::move(lookupSession),
                std::move(rowBuffer))
                ->Run();
        } else {
            using TWholePipeline = TTabletLookupSession<TSimplePipeline<TRowAdapter>>;
            return New<TWholePipeline>(
                std::move(adapter),
                std::move(tabletSnapshot),
                produceAllVersions,
                std::move(columnFilter),
                std::move(lookupKeys),
                std::move(lookupSession),
                std::move(rowBuffer))
                ->Run();
        }
    }
}

TFuture<TSharedRef> TTabletLookupRequest::RunTabletLookupSession(
    const TLookupSessionPtr& lookupSession)
{
    YT_ASSERT_INVOKER_AFFINITY(lookupSession->Invoker_);

    auto tabletSnapshot = lookupSession->SnapshotStore_->GetTabletSnapshotOrThrow(
        TabletId,
        CellId,
        MountRevision);

    auto timestamp = lookupSession->TimestampRange_.Timestamp;

    lookupSession->SnapshotStore_->ValidateTabletAccess(
        tabletSnapshot,
        timestamp);

    lookupSession->SnapshotStore_->ValidateBundleNotBanned(tabletSnapshot);

    ThrowUponDistributedThrottlerOverdraft(
        ETabletDistributedThrottlerKind::Lookup,
        tabletSnapshot,
        lookupSession->ChunkReadOptions_);

    ValidateReadTimestamp(timestamp);
    ValidateTabletRetainedTimestamp(tabletSnapshot, timestamp);

    tabletSnapshot->TabletRuntimeData->AccessTime = NProfiling::GetInstant();

    tabletSnapshot->WaitOnLocks(timestamp);

    auto rowBuffer = New<TRowBuffer>(
        TLookupRowsBufferTag(),
        TChunkedMemoryPool::DefaultStartChunkSize,
        lookupSession->ChunkReadOptions_.MemoryUsageTracker);

    auto reader = CreateWireProtocolReader(RequestData, rowBuffer);

    auto command = reader->ReadCommand();

    std::unique_ptr<NTableClient::NProto::TColumnFilter> columnFilterProto;
    switch (command) {
        case EWireProtocolCommand::LookupRows: {
            NTableClient::NProto::TReqLookupRows req;
            reader->ReadMessage(&req);
            columnFilterProto.reset(req.release_column_filter());
            break;
        }

        case EWireProtocolCommand::VersionedLookupRows: {
            NTableClient::NProto::TReqVersionedLookupRows req;
            reader->ReadMessage(&req);
            columnFilterProto.reset(req.release_column_filter());
            break;
        }

        default:
            THROW_ERROR_EXCEPTION("Unknown read command %v",
                command);
    }

    const auto& physicalSchema = tabletSnapshot->PhysicalSchema;

    bool isTimestampedLookup = lookupSession->VersionedReadOptions_.ReadMode == EVersionedIOMode::LatestTimestamp;

    auto columnFilter = DecodeColumnFilter(
        std::move(columnFilterProto),
        physicalSchema->GetColumnCount() + (isTimestampedLookup
            ? physicalSchema->GetValueColumnCount()
            : 0));
    auto lookupKeys = reader->ReadSchemafulRowset(
        IWireProtocolReader::GetSchemaData(*physicalSchema->ToKeys()),
        /*captureValues*/ false);
    lookupKeys = MakeSharedRange(lookupKeys, lookupKeys, RequestData);

    const auto& Logger = lookupSession->Logger;
    YT_LOG_DEBUG("Creating tablet lookup session (TabletId: %v, CellId: %v, KeyCount: %v)",
        TabletId,
        CellId,
        lookupKeys.Size());

    bool useLookupCache = GetUseLookupCache(tabletSnapshot, lookupSession->UseLookupCache_);

    switch (command) {
        case EWireProtocolCommand::LookupRows: {
            if (!reader->IsFinished()) {
                THROW_ERROR_EXCEPTION("Lookup command message is malformed");
            }

            TColumnFilter::TIndexes indexes;
            TTimestampReadOptions timestampReadOptions;
            if (isTimestampedLookup) {
                if (columnFilter.IsUniversal()) {
                    for (int columnIndex = physicalSchema->GetKeyColumnCount();
                        columnIndex < physicalSchema->GetColumnCount();
                        ++columnIndex)
                    {
                        timestampReadOptions.TimestampColumnMapping.push_back({
                            .ColumnIndex = columnIndex,
                            .TimestampColumnIndex = columnIndex + physicalSchema->GetValueColumnCount(),
                        });
                    }
                } else {
                    for (int columnIndex : columnFilter.GetIndexes()) {
                        if (columnIndex >= physicalSchema->GetColumnCount()) {
                            int originalColumnIndex = columnIndex - physicalSchema->GetValueColumnCount();

                            timestampReadOptions.TimestampColumnMapping.push_back({
                                .ColumnIndex = originalColumnIndex,
                                .TimestampColumnIndex = columnIndex,
                            });

                            // TODO(dave11ar): Optimize?
                            if (!columnFilter.ContainsIndex(originalColumnIndex)) {
                                timestampReadOptions.TimestampOnlyColumns.push_back(originalColumnIndex);
                                indexes.push_back(originalColumnIndex);
                            }
                        } else {
                            indexes.push_back(columnIndex);
                        }
                    }
                }

                isTimestampedLookup &= !timestampReadOptions.TimestampColumnMapping.empty();
            }

            return DoRunTabletLookupSession<TUnversionedAdapter>(
                TUnversionedAdapter(
                    tabletSnapshot,
                    columnFilter,
                    lookupSession->TimestampRange_,
                    lookupSession->ResponseCodec_,
                    rowBuffer,
                    lookupSession->ChunkReadOptions_.MemoryUsageTracker,
                    timestampReadOptions.TimestampColumnMapping),
                useLookupCache,
                std::move(tabletSnapshot),
                /*produceAllVersions*/ false,
                columnFilter.IsUniversal() || !isTimestampedLookup
                    ? std::move(columnFilter)
                    : TColumnFilter(std::move(indexes)),
                std::move(lookupKeys),
                lookupSession,
                rowBuffer);
        }

        case EWireProtocolCommand::VersionedLookupRows: {
            if (!reader->IsFinished()) {
                THROW_ERROR_EXCEPTION("Versioned lookup command message is malformed");
            }

            if (lookupSession->TimestampRange_.RetentionTimestamp != NullTimestamp) {
                THROW_ERROR_EXCEPTION("Versioned lookup does not support retention timestamp");
            }

            return DoRunTabletLookupSession<TVersionedAdapter>(
                TVersionedAdapter(
                    tabletSnapshot,
                    columnFilter,
                    lookupSession->RetentionConfig_,
                    lookupSession->TimestampRange_,
                    lookupSession->ResponseCodec_,
                    rowBuffer,
                    lookupSession->ChunkReadOptions_.MemoryUsageTracker),
                useLookupCache,
                std::move(tabletSnapshot),
                /*produceAllVersions*/ true,
                std::move(columnFilter),
                std::move(lookupKeys),
                lookupSession,
                rowBuffer);
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TPipeline>
TTabletLookupSession<TPipeline>::TTabletLookupSession(
    TPipeline::TAdapter&& adapter,
    TTabletSnapshotPtr tabletSnapshot,
    bool produceAllVersions,
    TColumnFilter columnFilter,
    TSharedRange<TUnversionedRow> lookupKeys,
    TLookupSessionPtr lookupSession,
    TRowBufferPtr rowBuffer)
    : TPipeline(
        std::move(adapter),
        tabletSnapshot,
        columnFilter,
        lookupSession->TimestampRange_,
        lookupSession->ChunkReadOptions_,
        lookupSession->ProfilingUser_,
        std::move(rowBuffer),
        lookupSession->Logger)
    , Invoker_(lookupSession->Invoker_)
    , TabletSnapshot_(std::move(tabletSnapshot))
    , Timestamp_(lookupSession->TimestampRange_.Timestamp)
    , ProduceAllVersions_(produceAllVersions)
    , ChunkReadOptions_(lookupSession->ChunkReadOptions_)
    , ColumnFilter_(std::move(columnFilter))
    , LookupKeys_(std::move(lookupKeys))
    , ChunkLookupKeys_(TPipeline::Initialize(LookupKeys_))
    , OnDestruction_([lookupSession, this] {
        lookupSession->FoundRowCount_.fetch_add(FoundRowCount_, std::memory_order::relaxed);
        lookupSession->FoundDataWeight_.fetch_add(FoundDataWeight_, std::memory_order::relaxed);
        lookupSession->MissingRowCount_.fetch_add(
            LookupKeys_.size() - FoundRowCount_,
            std::memory_order::relaxed);
        lookupSession->UnmergedRowCount_.fetch_add(
            DataStatistics_.row_count(),
            std::memory_order::relaxed);
        lookupSession->UnmergedMissingRowCount_.fetch_add(
            RequestedUnmergedRowCount_ - DataStatistics_.row_count(),
            std::memory_order::relaxed);
        lookupSession->UnmergedDataWeight_.fetch_add(
            DataStatistics_.data_weight(),
            std::memory_order::relaxed);
        lookupSession->DecompressionCpuTime_.fetch_add(
            DecompressionStatistics_.GetTotalDuration().MicroSeconds(),
            std::memory_order::relaxed);
    })
    , Logger(lookupSession->Logger().WithTag("TabletId: %v", TabletSnapshot_->TabletId))
{
    YT_VERIFY(TPipeline::TAdapter::QueryKind == EInitialQueryKind::LookupRows);
}

template <class TPipeline>
TTabletLookupSession<TPipeline>::TTabletLookupSession(
    TPipeline::TAdapter&& adapter,
    TTabletSnapshotPtr tabletSnapshot,
    TSharedRange<TUnversionedRow> lookupKeys,
    bool produceAllVersions,
    TColumnFilter columnFilter,
    const TReadTimestampRange& readTimestampRange,
    const TClientChunkReadOptions& chunkReadOptions,
    IInvokerPtr invoker,
    const std::optional<std::string>& profilingUser,
    TRowBufferPtr rowBuffer,
    NLogging::TLogger logger)
    : TPipeline(
        std::move(adapter),
        tabletSnapshot,
        columnFilter,
        readTimestampRange,
        chunkReadOptions,
        profilingUser,
        std::move(rowBuffer),
        logger)
    , Invoker_(std::move(invoker))
    , TabletSnapshot_(std::move(tabletSnapshot))
    , Timestamp_(readTimestampRange.Timestamp)
    , ProduceAllVersions_(produceAllVersions)
    , ChunkReadOptions_(chunkReadOptions)
    , ColumnFilter_(std::move(columnFilter))
    , LookupKeys_(std::move(lookupKeys))
    , ChunkLookupKeys_(TPipeline::Initialize(LookupKeys_))
    , OnDestruction_([this, profilingUser] {
        auto* counters = TabletSnapshot_->TableProfiler->GetSelectRowsCounters(profilingUser);

        // The following counters are normally accounted for in TProfilingReaderWrapper,
        // except for MissingRowCount and UnmergedMissingRowCount which are lookup-specific.

        // TODO(sabdenovch): support SelectDuration if detailed profiling is enabled.

        counters->RowCount.Increment(FoundRowCount_);
        counters->MissingRowCount.Increment(std::ssize(LookupKeys_) - FoundRowCount_);
        counters->DataWeight.Increment(FoundDataWeight_);
        counters->UnmergedRowCount.Increment(DataStatistics_.row_count());
        counters->UnmergedMissingRowCount.Increment(RequestedUnmergedRowCount_ - DataStatistics_.row_count());
        counters->UnmergedDataWeight.Increment(DataStatistics_.data_weight());
        counters->DecompressionCpuTime.Add(DecompressionStatistics_.GetTotalDuration());
        if (auto errorOrOK = ResultPromise_.TryGet(); !errorOrOK || !errorOrOK->IsOK()) {
            counters->WastedUnmergedDataWeight.Increment(DataStatistics_.data_weight());
        }
    })
    , Logger(logger.WithTag("TabletId: %v", TabletSnapshot_->TabletId))
{
    YT_VERIFY(TPipeline::TAdapter::QueryKind == EInitialQueryKind::SelectRows);
}

template <class TPipeline>
auto TTabletLookupSession<TPipeline>::Run() -> TFuture<typename decltype(TPipeline::TAdapter::ResultPromise_)::TValueType>
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    // Synchronously fetch store meta and create store readers.
    // However, may impose a WaitFor call during waiting on locks and during slow path obtaining chunk meta for ext-memory.
    // TODO(akozhikhov): Proper memory management: make this slow path for ext-mem asynchronous.

    std::vector<ISortedStorePtr> dynamicEdenStores;
    std::vector<ISortedStorePtr> chunkEdenStores;

    for (auto store : TabletSnapshot_->GetEdenStores()) {
        if (store->IsDynamic()) {
            // Can not check store state via GetStoreState.
            if (TabletSnapshot_->ActiveStore == store) {
                YT_VERIFY(ActiveStoreIndex_ == -1);
                ActiveStoreIndex_ = dynamicEdenStores.size();
            }

            dynamicEdenStores.push_back(std::move(store));
        } else {
            chunkEdenStores.push_back(std::move(store));
        }
    }

    YT_LOG_DEBUG("Creating store sessions (ActiveStoreIndex: %v)", ActiveStoreIndex_);

    DynamicEdenSessions_ = CreateStoreSessions(
        dynamicEdenStores,
        LookupKeys_);

    ChunkEdenSessions_ = CreateStoreSessions(
        chunkEdenStores,
        ChunkLookupKeys_);

    auto currentIt = LookupKeys_.Begin();
    int startChunkKeyIndex = 0;
    while (currentIt != LookupKeys_.End()) {
        PartitionSessions_.push_back(CreatePartitionSession(&currentIt, &startChunkKeyIndex));
    }

    InitializationDuration_ = Timer_.GetElapsedTime();

    // Lookup session is synchronous for in-memory tables.
    // However, for compressed in-memory tables is executed asynchronously due to potential block decompression.
    // TODO(akozhikhov): Proper memory management: make fast path for ext-mem (row cache or uncompressed block cache) synchronous.

    Timer_.Restart();

    std::vector<TFuture<void>> openFutures;
    auto openStoreSessions = [&] (const auto& sessions) {
        auto moreOpenFutures = OpenStoreSessions(sessions);
        openFutures.reserve(openFutures.size() + moreOpenFutures.size());
        std::move(moreOpenFutures.begin(), moreOpenFutures.end(), std::back_inserter(openFutures));
    };

    openStoreSessions(DynamicEdenSessions_);
    openStoreSessions(ChunkEdenSessions_);

    YT_VERIFY(!PartitionSessions_.empty());
    PartitionSessions_[0].SessionStarted = true;
    PartitionSessions_[0].StoreSessions = CreateStoreSessions(
        PartitionSessions_[0].PartitionSnapshot->Stores,
        PartitionSessions_[0].ChunkLookupKeys);
    openStoreSessions(PartitionSessions_[0].StoreSessions);

    if (openFutures.empty()) {
        LookupInPartitions(TError{});
    } else {
        auto sessionFuture = AllSucceeded(std::move(openFutures));
        sessionFuture.Subscribe(BIND(
            &TTabletLookupSession::LookupInPartitions,
            MakeStrong(this))
            .Via(Invoker_));
        SetSessionFuture(std::move(sessionFuture));
    }

    ResultPromise_.OnCanceled(BIND(&TTabletLookupSession::OnCanceled, MakeWeak(this)));
    return ResultPromise_;
}

template <class TPipeline>
TStoreSessionList TTabletLookupSession<TPipeline>::CreateStoreSessions(
    const std::vector<ISortedStorePtr>& stores,
    const TSharedRange<TLegacyKey>& keys)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    TStoreSessionList sessions;
    sessions.reserve(stores.size());

    for (const auto& store : stores) {
        ui32 storeFlushIndex = 0;

        if (store->IsSorted() && store->IsDynamic()) {
            storeFlushIndex = store->AsSortedDynamic()->GetFlushIndex();
        }

        YT_LOG_DEBUG("Creating reader (Store: %v, KeyCount: %v, StoreFlushIndex: %v)",
            store->GetId(),
            keys.Size(),
            storeFlushIndex);

        RequestedUnmergedRowCount_ += keys.Size();

        sessions.emplace_back(store->CreateReader(
            TabletSnapshot_,
            keys,
            TPipeline::GetReadTimestamp(),
            ProduceAllVersions_,
            ProduceAllVersions_ ? TColumnFilter::MakeUniversal() : ColumnFilter_,
            ChunkReadOptions_,
            ChunkReadOptions_.WorkloadDescriptor.Category));
    }

    return sessions;
}

template <class TPipeline>
std::vector<TFuture<void>> TTabletLookupSession<TPipeline>::OpenStoreSessions(
    const TStoreSessionList& sessions)
{
    // NB: Will remain empty for in-memory tables.
    std::vector<TFuture<void>> futures;
    for (const auto& session : sessions) {
        auto future = session.Open();
        if (auto maybeError = future.TryGet()) {
            if (!maybeError->IsOK()) {
                return {future};
            }
        } else {
            futures.push_back(std::move(future));
        }
    }

    return futures;
}

template <class TPipeline>
TPartitionSession TTabletLookupSession<TPipeline>::CreatePartitionSession(
    decltype(LookupKeys_)::iterator* currentIt,
    int* startChunkKeyIndex)
{
    auto nextPartitionIt = std::upper_bound(
        TabletSnapshot_->PartitionList.begin(),
        TabletSnapshot_->PartitionList.end(),
        **currentIt,
        [] (TLegacyKey lhs, const TPartitionSnapshotPtr& rhs) {
            return lhs < rhs->PivotKey;
        });
    YT_VERIFY(nextPartitionIt != TabletSnapshot_->PartitionList.begin());
    const auto& partitionSnapshot = *(nextPartitionIt - 1);

    auto nextIt = nextPartitionIt == TabletSnapshot_->PartitionList.end()
        ? LookupKeys_.End()
        : std::lower_bound(*currentIt, LookupKeys_.End(), (*nextPartitionIt)->PivotKey);
    int startKeyIndex = *currentIt - LookupKeys_.Begin();
    int endKeyIndex = nextIt - LookupKeys_.Begin();
    int endChunkKeyIndex = *startChunkKeyIndex;
    for (int index = startKeyIndex; index < endKeyIndex; ++index) {
        endChunkKeyIndex += static_cast<int>(TPipeline::IsLookupInChunkNeeded(index));
    }

    TPartitionSession partitionSession{
        .CurrentKeyIndex = startKeyIndex,
        .EndKeyIndex = endKeyIndex,
        .PartitionSnapshot = partitionSnapshot,
        .ChunkLookupKeys = ChunkLookupKeys_.Slice(*startChunkKeyIndex, endChunkKeyIndex),
    };

    *startChunkKeyIndex = endChunkKeyIndex;
    *currentIt = nextIt;

    return partitionSession;
}

template <class TPipeline>
void TTabletLookupSession<TPipeline>::LookupInPartitions(const TError& error)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    if (ResultPromise_.IsSet()) {
        return;
    }

    if (!error.IsOK()) {
        ResultPromise_.TrySet(error);
        return;
    }

    try {
        while (CurrentPartitionSessionIndex_ < std::ssize(PartitionSessions_)) {
            if (LookupInCurrentPartition()) {
                return;
            }
        }
    } catch (const std::exception& ex) {
        ResultPromise_.TrySet(TError(ex));
        return;
    }

    UpdateUnmergedStatistics(DynamicEdenSessions_);
    UpdateUnmergedStatistics(ChunkEdenSessions_);

    PartitionsLookupDuration_ = Timer_.GetElapsedTime();
    Timer_.Restart();

    auto rowsetFuture = TPipeline::PostprocessTabletLookup(this, &Timer_);
    if (const auto& error = rowsetFuture.TryGet()) {
        FinishSession(*error);
        return;
    }

    rowsetFuture.Subscribe(BIND(
        &TTabletLookupSession::FinishSession,
        MakeStrong(this))
        .Via(Invoker_));
    SetSessionFuture(std::move(rowsetFuture).AsVoid());
}

template <class TPipeline>
bool TTabletLookupSession<TPipeline>::LookupInCurrentPartition()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    auto& partitionSession = PartitionSessions_[CurrentPartitionSessionIndex_];
    if (!partitionSession.SessionStarted) {
        partitionSession.SessionStarted = true;
        partitionSession.StoreSessions = CreateStoreSessions(
            partitionSession.PartitionSnapshot->Stores,
            partitionSession.ChunkLookupKeys);
        auto openFutures = OpenStoreSessions(partitionSession.StoreSessions);
        if (!openFutures.empty()) {
            auto sessionFuture = AllSucceeded(std::move(openFutures));
            sessionFuture.Subscribe(BIND(
                &TTabletLookupSession::LookupInPartitions,
                MakeStrong(this))
                .Via(Invoker_));
            SetSessionFuture(std::move(sessionFuture));
            return true;
        }
    }

    return DoLookupInCurrentPartition();
}

template <class TPipeline>
bool TTabletLookupSession<TPipeline>::DoLookupInCurrentPartition()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    auto& partitionSession = PartitionSessions_[CurrentPartitionSessionIndex_];

    if (partitionSession.StoreSessionsPrepared) {
        OnStoreSessionsPrepared();
    }

    while (partitionSession.CurrentKeyIndex < partitionSession.EndKeyIndex) {
        // Need to insert rows into cache even from active dynamic store.
        // Otherwise, cache misses will occur.
        // Process dynamic store rows firstly.
        LookupFromStoreSessions(&DynamicEdenSessions_, ActiveStoreIndex_);

        if (TPipeline::IsLookupInChunkNeeded(partitionSession.CurrentKeyIndex++)) {
            std::vector<TFuture<void>> futures;
            auto getUnpreparedSessions = [&] (auto* sessions) {
                for (auto& session : *sessions) {
                    if (!session.PrepareBatch()) {
                        auto future = session.GetReadyEvent();
                        // TODO(akozhikhov): Proper block fetcher: make scenario of empty batch and set future here impossible.
                        if (!future.IsSet() || !future.Get().IsOK()) {
                            // NB: In case of error AllSucceeded below will terminate this session
                            // and cancel its other block fetchers.
                            futures.push_back(std::move(future));
                        }
                    }
                }
            };

            getUnpreparedSessions(&partitionSession.StoreSessions);
            getUnpreparedSessions(&ChunkEdenSessions_);

            if (futures.empty()) {
                OnStoreSessionsPrepared();
            } else {
                // NB: When sessions become prepared we check the StoreSessionsPrepared flag and
                // read row in OnStoreSessionsPrepared. Then move to the next key with the while loop.
                partitionSession.StoreSessionsPrepared = true;
                auto sessionFuture = AllSucceeded(std::move(futures));
                sessionFuture.Subscribe(BIND(
                    &TTabletLookupSession::LookupInPartitions,
                    MakeStrong(this))
                    .Via(Invoker_));
                SetSessionFuture(std::move(sessionFuture));

                return true;
            }
        } else {
            TPipeline::FinishRow();
        }
    }

    UpdateUnmergedStatistics(partitionSession.StoreSessions);

    ++CurrentPartitionSessionIndex_;

    return false;
}

template <class TPipeline>
void TTabletLookupSession<TPipeline>::OnStoreSessionsPrepared()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    auto& partitionSession = PartitionSessions_[CurrentPartitionSessionIndex_];

    LookupFromStoreSessions(&partitionSession.StoreSessions, -1);
    LookupFromStoreSessions(&ChunkEdenSessions_, -1);

    TPipeline::FinishRow();

    partitionSession.StoreSessionsPrepared = false;
}

template <class TPipeline>
void TTabletLookupSession<TPipeline>::LookupFromStoreSessions(
    TStoreSessionList* sessions,
    int activeStoreIndex)
{
    for (int sessionIndex = 0; sessionIndex < std::ssize(*sessions); ++sessionIndex) {
        auto& session = (*sessions)[sessionIndex];
        // TODO(akozhikhov): Proper block fetcher: make scenario of empty batch here impossible.
        if (!session.PrepareBatch()) {
            auto readyEvent = session.GetReadyEvent();
            YT_VERIFY(readyEvent.IsSet());
            readyEvent.Get().ThrowOnError();
            YT_VERIFY(session.PrepareBatch());
        }
        auto row = session.FetchRow();
        TPipeline::AddPartialRow(row, Timestamp_ + 1, activeStoreIndex == sessionIndex);
    }
}

template <class TPipeline>
void TTabletLookupSession<TPipeline>::FinishSession(const TError& error)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    if (!error.IsOK()) {
        ResultPromise_.TrySet(error);
        return;
    }

    auto throttlerKind = GetThrottlerKindFromQueryKind(TPipeline::QueryKind);
    if (const auto& throttler = TabletSnapshot_->DistributedThrottlers[throttlerKind]) {
        throttler->Acquire(FoundDataWeight_);
    }

    YT_LOG_DEBUG(
        "Tablet lookup completed "
        "(TabletId: %v, CellId: %v, EnableDetailedProfiling: %v, "
        "FoundRowCount: %v, FoundDataWeight: %v, DecompressionCpuTime: %v, "
        "InitializationTime: %v, PartitionsLookupTime: %v, HunksDecodingTime: %v, ResponseCompressionTime: %v)",
        TabletSnapshot_->TabletId,
        TabletSnapshot_->CellId,
        TabletSnapshot_->Settings.MountConfig->EnableDetailedProfiling,
        FoundRowCount_,
        FoundDataWeight_,
        DecompressionStatistics_.GetTotalDuration(),
        InitializationDuration_,
        PartitionsLookupDuration_,
        HunksDecodingTime_,
        ResponseCompressionTime_);

    YT_VERIFY(ResultPromise_.IsSet());
}

template <class TPipeline>
void TTabletLookupSession<TPipeline>::UpdateUnmergedStatistics(const TStoreSessionList& sessions)
{
    for (const auto& session : sessions) {
        DataStatistics_ += session.GetDataStatistics();
        DecompressionStatistics_ += session.GetDecompressionStatistics();
    }
}

template <class TPipeline>
TTabletLookupSession<TPipeline>::~TTabletLookupSession()
{
    if (OnDestruction_) {
        OnDestruction_();
    }
}

template <class TPipeline>
void TTabletLookupSession<TPipeline>::OnCanceled(const TError& error)
{
    TFuture<void> sessionFuture;
    {
        auto guard = Guard(CancelationSpinLock_);

        if (CancelationError_) {
            return;
        }

        CancelationError_ = error;

        sessionFuture = SessionFuture_;
    }

    sessionFuture.Cancel(error);

    ResultPromise_.TrySet(error);
}

template <class TPipeline>
void TTabletLookupSession<TPipeline>::SetSessionFuture(TFuture<void> sessionFuture)
{
    auto guard = Guard(CancelationSpinLock_);

    if (CancelationError_) {
        guard.Release();

        sessionFuture.Cancel(*CancelationError_);
        return;
    }

    SessionFuture_ = std::move(sessionFuture);
}

////////////////////////////////////////////////////////////////////////////////

ILookupSessionPtr CreateLookupSession(
    EInMemoryMode inMemoryMode,
    int tabletRequestCount,
    ICodec* responseCodec,
    int maxRetryCount,
    int maxConcurrentSubqueries,
    TReadTimestampRange timestampRange,
    std::optional<bool> useLookupCache,
    NChunkClient::TClientChunkReadOptions chunkReadOptions,
    TRetentionConfigPtr retentionConfig,
    bool enablePartialResult,
    TVersionedReadOptions versionedReadOptions,
    const ITabletSnapshotStorePtr& snapshotStore,
    const std::optional<std::string>& profilingUser,
    IInvokerPtr invoker)
{
    return New<TLookupSession>(
        inMemoryMode,
        tabletRequestCount,
        responseCodec,
        maxRetryCount,
        maxConcurrentSubqueries,
        timestampRange,
        useLookupCache,
        std::move(chunkReadOptions),
        std::move(retentionConfig),
        enablePartialResult,
        std::move(versionedReadOptions),
        snapshotStore,
        profilingUser,
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

class TSchemafulPipeAdapter
    : public TAdapterBase
{
public:
    TSchemafulPipeAdapter(
        TSchemafulPipePtr pipe,
        const TTabletSnapshotPtr& tabletSnapshot,
        const TReadTimestampRange& timestampRange,
        const TTimestampReadOptions& timestampReadOptions,
        const TColumnFilter& columnFilter,
        TRowBufferPtr rowBuffer)
        : Writer_(pipe->GetWriter())
        , Pipe_(std::move(pipe))
        , Merger_(CreateQueryLatestTimestampRowMerger(
            std::move(rowBuffer),
            tabletSnapshot,
            columnFilter,
            timestampRange.RetentionTimestamp,
            timestampReadOptions))
    { }

    TSchemafulPipeAdapter(TSchemafulPipeAdapter&& other) = default;
    TSchemafulPipeAdapter& operator=(TSchemafulPipeAdapter&& other) = default;

protected:
    using TMutableRow = TMutableUnversionedRow;

    static constexpr auto QueryKind = EInitialQueryKind::SelectRows;

    const IUnversionedRowsetWriterPtr Writer_;
    const TSchemafulPipePtr Pipe_;
    std::unique_ptr<TSchemafulRowMerger> Merger_;

    const TPromise<void> ResultPromise_ = NewPromise<void>();

    void WriteRow(TUnversionedRow row)
    {
        FoundRowCount_ += static_cast<bool>(row);
        FoundDataWeight_ += GetDataWeight(row);
        if (!Writer_->Write(TRange<TUnversionedRow>(&row, size_t(1)))) {
            WaitFor(Writer_->GetReadyEvent())
                .ThrowOnError();
        }
    }

    void Finish(TWallTimer* timer)
    {
        HunksDecodingTime_ = timer->GetElapsedTime();
        timer->Restart();
        auto error = WaitFor(Writer_->Close());
        if (error.IsOK()) {
            Pipe_->SetDataStatistics(std::move(DataStatistics_));
        }
        ResultPromise_.TrySet();
    }
};

ISchemafulUnversionedReaderPtr CreateLookupSessionReader(
    IMemoryChunkProviderPtr memoryChunkProvider,
    TTabletSnapshotPtr tabletSnapshot,
    TColumnFilter columnFilter,
    TSharedRange<TUnversionedRow> lookupKeys,
    const TReadTimestampRange& timestampRange,
    std::optional<bool> useLookupCache,
    const TClientChunkReadOptions& chunkReadOptions,
    const TTimestampReadOptions& timestampReadOptions,
    IInvokerPtr invoker,
    const std::optional<std::string>& profilingUser,
    NLogging::TLogger Logger)
{
    ValidateTabletRetainedTimestamp(tabletSnapshot, timestampRange.Timestamp);
    tabletSnapshot->WaitOnLocks(timestampRange.Timestamp);

    ThrowUponDistributedThrottlerOverdraft(
        ETabletDistributedThrottlerKind::Select,
        tabletSnapshot,
        chunkReadOptions);

    YT_LOG_DEBUG("Creating lookup session reader (ColumnFilter: %v, LookupKeyCount: %v, UseLookupCache: %v)",
        columnFilter,
        lookupKeys.Size(),
        useLookupCache);

    auto rowBuffer = New<TRowBuffer>(TLookupRowsBufferTag(), memoryChunkProvider);

    auto pipe = New<TSchemafulPipe>(memoryChunkProvider);
    auto reader = pipe->GetReader();

    auto adapter = TSchemafulPipeAdapter(
        pipe,
        tabletSnapshot,
        timestampRange,
        timestampReadOptions,
        columnFilter,
        rowBuffer);

    auto pipeWatcher = BIND([pipe = std::move(pipe)] (const TError& error) {
        if (!error.IsOK()) {
            pipe->Fail(error);
        }
    });

    if (GetUseLookupCache(tabletSnapshot, useLookupCache)) {
        if (tabletSnapshot->PhysicalSchema->HasHunkColumns()) {
            New<TTabletLookupSession<THunkDecodingPipeline<TRowCachePipeline<TSchemafulPipeAdapter>>>>(
                std::move(adapter),
                std::move(tabletSnapshot),
                std::move(lookupKeys),
                /*produceAllVersions*/ true,
                std::move(columnFilter),
                timestampRange,
                chunkReadOptions,
                std::move(invoker),
                profilingUser,
                std::move(rowBuffer),
                std::move(Logger))
                ->Run()
                .Subscribe(pipeWatcher);
        } else {
            New<TTabletLookupSession<TRowCachePipeline<TSchemafulPipeAdapter>>>(
                std::move(adapter),
                std::move(tabletSnapshot),
                std::move(lookupKeys),
                /*produceAllVersions*/ true,
                std::move(columnFilter),
                timestampRange,
                chunkReadOptions,
                std::move(invoker),
                profilingUser,
                std::move(rowBuffer),
                std::move(Logger))
                ->Run()
                .Subscribe(pipeWatcher);
        }
    } else {
        if (tabletSnapshot->PhysicalSchema->HasHunkColumns()) {
            New<TTabletLookupSession<THunkDecodingPipeline<TSimplePipeline<TSchemafulPipeAdapter>>>>(
                std::move(adapter),
                std::move(tabletSnapshot),
                std::move(lookupKeys),
                /*produceAllVersions*/ false,
                std::move(columnFilter),
                timestampRange,
                chunkReadOptions,
                std::move(invoker),
                profilingUser,
                std::move(rowBuffer),
                std::move(Logger))
                ->Run()
                .Subscribe(pipeWatcher);
        } else {
            New<TTabletLookupSession<TSimplePipeline<TSchemafulPipeAdapter>>>(
                std::move(adapter),
                std::move(tabletSnapshot),
                std::move(lookupKeys),
                /*produceAllVersions*/ false,
                std::move(columnFilter),
                timestampRange,
                chunkReadOptions,
                std::move(invoker),
                profilingUser,
                std::move(rowBuffer),
                std::move(Logger))
                ->Run()
                .Subscribe(pipeWatcher);
        }
    }

    return reader;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
