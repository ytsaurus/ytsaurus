#include "query_executor.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/node/tablet_node/bootstrap.h>
#include <yt/yt/server/node/tablet_node/error_manager.h>
#include <yt/yt/server/node/tablet_node/lookup.h>
#include <yt/yt/server/node/tablet_node/security_manager.h>
#include <yt/yt/server/node/tablet_node/slot_manager.h>
#include <yt/yt/server/node/tablet_node/store.h>
#include <yt/yt/server/node/tablet_node/tablet.h>
#include <yt/yt/server/node/tablet_node/tablet_manager.h>
#include <yt/yt/server/node/tablet_node/tablet_profiling.h>
#include <yt/yt/server/node/tablet_node/tablet_reader.h>
#include <yt/yt/server/node/tablet_node/tablet_slot.h>
#include <yt/yt/server/node/tablet_node/tablet_snapshot_store.h>


#include <yt/yt/server/lib/hydra/hydra_manager.h>

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/query_client/executor.h>
#include <yt/yt/ytlib/query_client/functions_cache.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/key_filter.h>
#include <yt/yt/ytlib/table_client/row_merger.h>
#include <yt/yt/ytlib/table_client/schemaful_chunk_reader.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/table_client/pipe.h>
#include <yt/yt/client/table_client/timestamped_schema_helpers.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/unordered_schemaful_reader.h>
#include <yt/yt/client/table_client/versioned_io_options.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/range_formatters.h>
#include <yt/yt/core/misc/tls_cache.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>
#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/coordination_helpers.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/coordinator.h>
#include <yt/yt/library/query/engine_api/evaluator.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

namespace NYT::NQueryClient {

using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

template <>
TRow GetPivotKey(const NTabletNode::TPartitionSnapshotPtr& shard)
{
    return shard->PivotKey;
}

template <>
TRow GetNextPivotKey(const NTabletNode::TPartitionSnapshotPtr& shard)
{
    return shard->NextPivotKey;
}

template <>
TRange<TRow> GetSampleKeys(const NTabletNode::TPartitionSnapshotPtr& shard)
{
    return shard->SampleKeys->Keys;
}

std::pair<std::vector<TKeyRef>, std::vector<int>> GetSampleKeysForPrefix(TRange<TLegacyKey> keys, int prefixSize, TLegacyKey startKey)
{
    std::vector<TKeyRef> samplePrefixes;
    std::vector<int> weights;
    weights.push_back(1);

    // Filter out prefixes outside partition's pivot keys.
    auto previousKey = ToKeyRef(startKey, std::min<int>(startKey.GetCount(), prefixSize));

    for (auto key : keys) {
        auto current = ToKeyRef(key, prefixSize);

        if (CompareValueRanges(previousKey, current) != 0) {
            samplePrefixes.push_back(current);
            weights.push_back(1);
            previousKey = current;
        } else {
            ++weights.back();
        }
    }

    YT_VERIFY(weights.size() == samplePrefixes.size() + 1);

    return {samplePrefixes, weights};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

namespace NYT::NQueryAgent {

using namespace NClusterNode;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDataNode;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletNode;
using namespace NYTree;

using NTabletNode::TTabletSnapshotPtr;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::pair<NTableClient::TColumnFilter, TTimestampReadOptions> GetColumnFilter(
    const NTableClient::TTableSchema& desiredSchema,
    const NTableClient::TTableSchema& tabletSchema)
{
    // Infer column filter.
    TColumnFilter::TIndexes indexes;
    TTimestampReadOptions timestampReadOptions;

    for (const auto& column : desiredSchema.Columns()) {
        if (auto columnWithTimestamp = GetTimestampColumnOriginalNameOrNull(column.Name())) {
            bool isTimestampOnlyColumn = !desiredSchema.FindColumn(columnWithTimestamp.value());
            int columnIndex = tabletSchema.GetColumnIndexOrThrow(columnWithTimestamp.value());

            timestampReadOptions.TimestampColumnMapping.push_back({
                .ColumnIndex = columnIndex,
                .TimestampColumnIndex = columnIndex + tabletSchema.GetValueColumnCount(),
            });

            if (isTimestampOnlyColumn) {
                indexes.push_back(columnIndex);
                timestampReadOptions.TimestampOnlyColumns.push_back(columnIndex);
            }
        } else {
            const auto& tabletColumn = tabletSchema.GetColumnOrThrow(column.Name());
            if (tabletColumn.GetWireType() != column.GetWireType()) {
                THROW_ERROR_EXCEPTION("Mismatched type of column %v in schema: expected %Qlv, found %Qlv",
                    column.GetDiagnosticNameString(),
                    tabletColumn.GetWireType(),
                    column.GetWireType());
            }
            indexes.push_back(tabletSchema.GetColumnIndex(tabletColumn));
        }
    }

    return {TColumnFilter(std::move(indexes)), std::move(timestampReadOptions)};
}

class TProfilingReaderWrapper
    : public ISchemafulUnversionedReader
{
private:
    const ISchemafulUnversionedReaderPtr Underlying_;
    const TSelectRowsCounters Counters_;

    std::optional<TWallTimer> Timer_;

public:
    TProfilingReaderWrapper(
        ISchemafulUnversionedReaderPtr underlying,
        TSelectRowsCounters counters,
        bool enableDetailedProfiling)
        : Underlying_(std::move(underlying))
        , Counters_(std::move(counters))
    {
        if (enableDetailedProfiling) {
            Timer_.emplace();
        }
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return Underlying_->Read(options);
    }

    TFuture<void> GetReadyEvent() const override
    {
        return Underlying_->GetReadyEvent();
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return Underlying_->GetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

    ~TProfilingReaderWrapper()
    {
        auto statistics = GetDataStatistics();
        auto decompressionCpuTime = GetDecompressionStatistics().GetTotalDuration();

        Counters_.RowCount.Increment(statistics.row_count());
        Counters_.DataWeight.Increment(statistics.data_weight());
        Counters_.UnmergedRowCount.Increment(statistics.unmerged_row_count());
        Counters_.UnmergedDataWeight.Increment(statistics.unmerged_data_weight());
        Counters_.DecompressionCpuTime.Add(decompressionCpuTime);

        if (Timer_) {
            Counters_.SelectDuration.Record(Timer_->GetElapsedTime());
        }
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

struct TQuerySubexecutorBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

class TTabletSnapshotCache
{
public:
    TTabletSnapshotCache(
        ITabletSnapshotStorePtr snapshotStore,
        const NLogging::TLogger& logger)
        : SnapshotStore_(std::move(snapshotStore))
        , Logger(std::move(logger))
    { }

    void ValidateAndRegisterTabletSnapshot(
        TTabletId tabletId,
        TCellId cellId,
        NHydra::TRevision mountRevision,
        TTimestamp timestamp,
        bool suppressAccessTracking)
    {
        auto tabletSnapshot = SnapshotStore_->GetTabletSnapshotOrThrow(tabletId, cellId, mountRevision);

        SnapshotStore_->ValidateTabletAccess(tabletSnapshot, timestamp);
        SnapshotStore_->ValidateBundleNotBanned(tabletSnapshot);

        Map_.emplace(tabletId, tabletSnapshot);

        if (!MultipleTables_) {
            if (TableId_ && tabletSnapshot->TableId != TableId_) {
                YT_LOG_ERROR("Found different tables in query, profiling will be incorrect (TableId1: %v, TableId2: %v)",
                    TableId_,
                    tabletSnapshot->TableId);
                MultipleTables_ = true;
            }

            TableId_ = tabletSnapshot->TableId;
            TableProfiler_ = tabletSnapshot->TableProfiler;
        }

        if (!suppressAccessTracking) {
            tabletSnapshot->TabletRuntimeData->AccessTime = NProfiling::GetInstant();
        }
    }

    TTableProfilerPtr GetTableProfiler()
    {
        if (MultipleTables_ || !TableProfiler_) {
            return TTableProfiler::GetDisabled();
        }

        return TableProfiler_;
    }

    TTabletSnapshotPtr GetCachedTabletSnapshot(TTabletId tabletId)
    {
        return GetOrCrash(Map_, tabletId);
    }

    IHunkChunkReaderStatisticsPtr CreateHunkChunkReaderStatistics()
    {
        auto tabletSnapshot = GetAnyTabletSnapshot();
        return tabletSnapshot
            ? NTableClient::CreateHunkChunkReaderStatistics(
                tabletSnapshot->Settings.MountConfig->EnableHunkColumnarProfiling,
                tabletSnapshot->PhysicalSchema)
            : nullptr;
    }

    TKeyFilterStatisticsPtr CreateKeyFilterStatistics()
    {
        auto tabletSnapshot = GetAnyTabletSnapshot();
        return tabletSnapshot && tabletSnapshot->Settings.MountConfig->EnableKeyFilterForLookup
            ? New<TKeyFilterStatistics>()
            : nullptr;
    }

private:
    const ITabletSnapshotStorePtr SnapshotStore_;
    const NLogging::TLogger Logger;

    THashMap<TTabletId, TTabletSnapshotPtr> Map_;
    TTableId TableId_;

    NProfiling::TTagIdList ProfilerTags_;
    bool MultipleTables_ = false;
    TTableProfilerPtr TableProfiler_;

    TTabletSnapshotPtr GetAnyTabletSnapshot()
    {
        return MultipleTables_ || Map_.empty() ? nullptr : Map_.begin()->second;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRowsetSubrangeReader
    : public ISchemafulUnversionedReader
{
public:
    TRowsetSubrangeReader(
        TFuture<TSharedRange<TUnversionedRow>> asyncRows,
        std::optional<std::pair<TKeyBoundRef, TKeyBoundRef>> readRange)
        : AsyncRows_(std::move(asyncRows))
        , ReadRange_(std::move(readRange))
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (!ReadRange_) {
            return nullptr;
        }
        auto readRange = *ReadRange_;

        if (!AsyncRows_.IsSet() || !AsyncRows_.Get().IsOK()) {
            return CreateEmptyUnversionedRowBatch();
        }

        const auto& rows = AsyncRows_.Get().Value();

        CurrentRowIndex_ = BinarySearch(CurrentRowIndex_, std::ssize(rows), [&] (i64 index) {
            return !TestKeyWithWidening(
                ToKeyRef(rows[index]),
                readRange.first);
        });

        auto startIndex = CurrentRowIndex_;

        CurrentRowIndex_ = std::min(CurrentRowIndex_ + options.MaxRowsPerRead, std::ssize(rows));

        CurrentRowIndex_ = BinarySearch(startIndex, CurrentRowIndex_, [&] (i64 index) {
            return TestKeyWithWidening(
                ToKeyRef(rows[index]),
                readRange.second);
        });

        if (startIndex == CurrentRowIndex_) {
            return nullptr;
        }

        return CreateBatchFromUnversionedRows(MakeSharedRange(rows.Slice(startIndex, CurrentRowIndex_), rows));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return AsyncRows_.AsVoid();
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return NChunkClient::NProto::TDataStatistics();
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return NChunkClient::TCodecStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

private:
    TFuture<TSharedRange<TUnversionedRow>> AsyncRows_;
    i64 CurrentRowIndex_ = 0;
    std::optional<std::pair<TKeyBoundRef, TKeyBoundRef>> ReadRange_;
};

////////////////////////////////////////////////////////////////////////////////

struct TabletReadItems
{
    TTabletId TabletId;

    std::vector<TPartitionBounds> PartitionBounds;
    // Ranges for ordered tables.
    TSharedRange<TRowRange> Ranges;
    TSharedRange<TRow> Keys;
};

////////////////////////////////////////////////////////////////////////////////

class TQueryExecution
    : public TRefCounted
{
public:
    TQueryExecution(
        TQueryAgentConfigPtr config,
        TFunctionImplCachePtr functionImplCache,
        NTabletNode::IBootstrap* bootstrap,
        IEvaluatorPtr evaluator,
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        std::vector<TDataSource> dataSources,
        IUnversionedRowsetWriterPtr writer,
        IMemoryChunkProviderPtr memoryChunkProvider,
        IInvokerPtr invoker,
        TQueryOptions queryOptions,
        TFeatureFlags requestFeatureFlags)
        : Config_(std::move(config))
        , FunctionImplCache_(std::move(functionImplCache))
        , Bootstrap_(bootstrap)
        , ColumnEvaluatorCache_(Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetColumnEvaluatorCache())
        , Evaluator_(std::move(evaluator))
        , Query_(std::move(query))
        , ExternalCGInfo_(std::move(externalCGInfo))
        , DataSources_(std::move(dataSources))
        , Writer_(std::move(writer))
        , MemoryChunkProvider_(std::move(memoryChunkProvider))
        , Invoker_(std::move(invoker))
        , QueryOptions_(std::move(queryOptions))
        , RequestFeatureFlags_(std::move(requestFeatureFlags))
        , Logger(MakeQueryLogger(Query_))
        , Identity_(NRpc::GetCurrentAuthenticationIdentity())
        , TabletSnapshots_(Bootstrap_->GetTabletSnapshotStore(), Logger)
        , ChunkReadOptions_{
            .WorkloadDescriptor = QueryOptions_.WorkloadDescriptor,
            .ReadSessionId = QueryOptions_.ReadSessionId,
            .MemoryUsageTracker = Bootstrap_->GetNodeMemoryUsageTracker()->WithCategory(EMemoryCategory::Query),
        }
    { }

    TQueryStatistics Execute(TServiceProfilerGuard& profilerGuard)
    {
        for (const auto& source : DataSources_) {
            auto type = TypeFromId(source.ObjectId);
            switch (type) {
                case EObjectType::Tablet:
                    TabletSnapshots_.ValidateAndRegisterTabletSnapshot(
                        source.ObjectId,
                        source.CellId,
                        source.MountRevision,
                        QueryOptions_.TimestampRange.Timestamp,
                        QueryOptions_.SuppressAccessTracking);
                    break;
                default:
                    THROW_ERROR_EXCEPTION("Unsupported data source type %Qlv",
                        type);
            }
        }

        profilerGuard.Start(TabletSnapshots_.GetTableProfiler()->GetQueryServiceCounters(GetCurrentProfilingUser())->Execute);

        auto counters = TabletSnapshots_.GetTableProfiler()->GetSelectRowsCounters(GetProfilingUser(Identity_));

        ChunkReadOptions_.HunkChunkReaderStatistics = TabletSnapshots_.CreateHunkChunkReaderStatistics();
        ChunkReadOptions_.KeyFilterStatistics = TabletSnapshots_.CreateKeyFilterStatistics();

        auto updateProfiling = Finally([&] {
            auto failed = std::uncaught_exceptions() != 0;
            counters->ChunkReaderStatisticsCounters.Increment(ChunkReadOptions_.ChunkReaderStatistics, failed);
            counters->HunkChunkReaderCounters.Increment(ChunkReadOptions_.HunkChunkReaderStatistics, failed);

            if (const auto& keyFilterStatistics = ChunkReadOptions_.KeyFilterStatistics) {
                const auto& rangeFilterCounters = counters->RangeFilterCounters;
                rangeFilterCounters.InputRangeCount.Increment(
                    keyFilterStatistics->InputEntryCount.load(std::memory_order::relaxed));
                rangeFilterCounters.FilteredOutRangeCount.Increment(
                    keyFilterStatistics->FilteredOutEntryCount.load(std::memory_order::relaxed));
                rangeFilterCounters.FalsePositiveRangeCount.Increment(
                    keyFilterStatistics->FalsePositiveEntryCount.load(std::memory_order::relaxed));
            }
        });

        auto rowBuffer = New<TRowBuffer>(TQuerySubexecutorBufferTag(), MemoryChunkProvider_);
        auto classifiedDataSources = GetClassifiedDataSources(rowBuffer);
        auto minKeyWidth = GetMinKeyWidth(classifiedDataSources);

        auto groupedDataSplits = QueryOptions_.MergeVersionedRows
            ? CoordinateDataSourcesOld(std::move(classifiedDataSources), rowBuffer)
            : CoordinateDataSourcesNew(std::move(classifiedDataSources), rowBuffer);

        auto statistics = DoCoordinateAndExecute(
            std::move(groupedDataSplits),
            rowBuffer,
            minKeyWidth);

        auto cpuTime = statistics.SyncTime;
        for (const auto& innerStatistics : statistics.InnerStatistics) {
            cpuTime += innerStatistics.SyncTime;
        }
        counters->CpuTime.Add(cpuTime);

        return statistics;
    }

private:
    const TQueryAgentConfigPtr Config_;
    const TFunctionImplCachePtr FunctionImplCache_;
    NTabletNode::IBootstrap* const Bootstrap_;
    const IColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    const IEvaluatorPtr Evaluator_;

    const TConstQueryPtr Query_;

    const TConstExternalCGInfoPtr ExternalCGInfo_;
    const std::vector<TDataSource> DataSources_;
    const IUnversionedRowsetWriterPtr Writer_;
    const IMemoryChunkProviderPtr MemoryChunkProvider_;

    const IInvokerPtr Invoker_;
    const TQueryOptions QueryOptions_;
    const TFeatureFlags RequestFeatureFlags_;

    const NLogging::TLogger Logger;

    const NRpc::TAuthenticationIdentity Identity_;

    TTabletSnapshotCache TabletSnapshots_;

    TClientChunkReadOptions ChunkReadOptions_;

    using TSubreaderCreator = std::function<ISchemafulUnversionedReaderPtr()>;

    TDataSource GetPrefixReadItems(TRange<TabletReadItems> dataSplits, size_t keyPrefix)
    {
        auto rowBuffer = New<TRowBuffer>(TQuerySubexecutorBufferTag(), MemoryChunkProvider_);
        std::vector<TRowRange> prefixRanges;
        std::vector<TRow> prefixKeys;
        bool isRanges = false;
        bool isKeys = false;

        for (const auto& split : dataSplits) {
            auto tabletSnapshot = TabletSnapshots_.GetCachedTabletSnapshot(split.TabletId);

            auto partitions = TRange(tabletSnapshot->PartitionList);
            YT_VERIFY(!partitions.empty());

            for (const auto& partitionBound : split.PartitionBounds) {
                isRanges = true;
                YT_VERIFY(!isKeys);
                for (const auto& bound : partitionBound.Bounds) {
                    TRowRange range{
                        std::max<TRow>(partitions[partitionBound.PartitionIndex]->PivotKey, bound.first),
                        std::min<TRow>(partitions[partitionBound.PartitionIndex]->NextPivotKey, bound.second),
                    };

                    int lowerBoundWidth = std::min(
                        GetSignificantWidth(range.first),
                        keyPrefix);

                    auto lowerBound = rowBuffer->AllocateUnversioned(lowerBoundWidth);
                    for (int column = 0; column < lowerBoundWidth; ++column) {
                        lowerBound[column] = rowBuffer->CaptureValue(range.first[column]);
                    }

                    int upperBoundWidth = std::min(
                        GetSignificantWidth(range.second),
                        keyPrefix);

                    auto upperBound = WidenKeySuccessor(
                        range.second,
                        upperBoundWidth,
                        rowBuffer,
                        true);

                    prefixRanges.emplace_back(lowerBound, upperBound);

                    YT_LOG_DEBUG_IF(QueryOptions_.VerboseLogging, "Transforming range [%v .. %v] -> [%v .. %v]",
                        range.first,
                        range.second,
                        lowerBound,
                        upperBound);
                }
            }

            for (const auto& range : split.Ranges) {
                isRanges = true;
                YT_VERIFY(!isKeys);
                int lowerBoundWidth = std::min(
                    GetSignificantWidth(range.first),
                    keyPrefix);

                auto lowerBound = rowBuffer->AllocateUnversioned(lowerBoundWidth);
                for (int column = 0; column < lowerBoundWidth; ++column) {
                    lowerBound[column] = rowBuffer->CaptureValue(range.first[column]);
                }

                int upperBoundWidth = std::min(
                    GetSignificantWidth(range.second),
                    keyPrefix);

                auto upperBound = WidenKeySuccessor(
                    range.second,
                    upperBoundWidth,
                    rowBuffer,
                    true);

                prefixRanges.emplace_back(lowerBound, upperBound);

                YT_LOG_DEBUG_IF(QueryOptions_.VerboseLogging, "Transforming range [%v .. %v] -> [%v .. %v]",
                    range.first,
                    range.second,
                    lowerBound,
                    upperBound);
            }

            for (const auto& key : split.Keys) {
                isKeys = true;
                YT_VERIFY(!isRanges);

                int keyWidth = std::min(
                    size_t(key.GetCount()),
                    keyPrefix);

                auto prefixKey = rowBuffer->AllocateUnversioned(keyWidth);
                for (int column = 0; column < keyWidth; ++column) {
                    prefixKey[column] = rowBuffer->CaptureValue(key[column]);
                }
                prefixKeys.emplace_back(prefixKey);
            }
        }

        TDataSource dataSource;

        if (isRanges) {
            prefixRanges.erase(
                MergeOverlappingRanges(prefixRanges.begin(), prefixRanges.end()),
                prefixRanges.end());
            dataSource.Ranges = MakeSharedRange(prefixRanges, rowBuffer);
        }

        if (isKeys) {
            prefixKeys.erase(std::unique(prefixKeys.begin(), prefixKeys.end()), prefixKeys.end());
            dataSource.Keys = MakeSharedRange(prefixKeys, rowBuffer);
        }

        return dataSource;
    }

    static size_t GetMinKeyWidth(TRange<TDataSource> dataSources)
    {
        size_t minKeyWidth = std::numeric_limits<size_t>::max();
        for (const auto& split : dataSources) {
            for (const auto& range : split.Ranges) {
                minKeyWidth = std::min({
                    minKeyWidth,
                    GetSignificantWidth(range.first),
                    GetSignificantWidth(range.second)});
            }

            for (const auto& key : split.Keys) {
                minKeyWidth = std::min(
                    minKeyWidth,
                    static_cast<size_t>(key.GetCount()));
            }
        }

        return minKeyWidth;
    }

    TQueryStatistics DoCoordinateAndExecute(
        std::vector<std::vector<TabletReadItems>> groupedDataSplits,
        const TRowBufferPtr& rowBuffer,
        size_t minKeyWidth)
    {
        auto clientOptions = NApi::TClientOptions::FromAuthenticationIdentity(Identity_);
        auto client = Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->CreateNativeClient(clientOptions);

        auto remoteExecutor = CreateQueryExecutor(
            MemoryChunkProvider_,
            client->GetNativeConnection(),
            ColumnEvaluatorCache_,
            Evaluator_,
            client->GetChannelFactory(),
            FunctionImplCache_);

        auto functionGenerators = New<TFunctionProfilerMap>();
        auto aggregateGenerators = New<TAggregateProfilerMap>();
        MergeFrom(functionGenerators.Get(), *GetBuiltinFunctionProfilers());
        MergeFrom(aggregateGenerators.Get(), *GetBuiltinAggregateProfilers());

        FetchFunctionImplementationsFromCypress(
            functionGenerators,
            aggregateGenerators,
            ExternalCGInfo_,
            FunctionImplCache_,
            ChunkReadOptions_);

        auto [frontQuery, bottomQueryPattern] = GetDistributedQueryPattern(Query_);

        int splitCount = std::ssize(groupedDataSplits);

        return CoordinateAndExecute(
            Query_->IsOrdered(RequestFeatureFlags_),
            Query_->IsPrefetching(),
            splitCount,
            [
                &,
                bottomQueryPattern = bottomQueryPattern,
                groupedDataSplits = std::move(groupedDataSplits),
                rowBuffer,
                subqueryIndex = 0
            ] () mutable -> TEvaluateResult {
                if (subqueryIndex >= std::ssize(groupedDataSplits)) {
                    return {};
                }

                auto dataSplits = groupedDataSplits[subqueryIndex++];

                auto asyncSubqueryResults = std::make_shared<std::vector<TFuture<TQueryStatistics>>>();

                // Copy query to generate new id.
                auto bottomQuery = New<TQuery>(*bottomQueryPattern);

                bool orderedExecution = bottomQuery->IsOrdered(RequestFeatureFlags_);

                auto foreignProfileCallback = [
                    asyncSubqueryResults,
                    remoteExecutor,
                    dataSplits,
                    orderedExecution,
                    minKeyWidth,
                    this,
                    this_ = MakeStrong(this)
                ] (int joinIndex) -> TJoinSubqueryEvaluator {
                    auto joinClause = Query_->JoinClauses[joinIndex];

                    auto remoteOptions = QueryOptions_;
                    remoteOptions.MaxSubqueries = 1;
                    remoteOptions.MergeVersionedRows = true;

                    auto remoteFeatureFlags = RequestFeatureFlags_;

                    auto definedKeyColumns = Query_->GetRenamedSchema()->Columns();
                    definedKeyColumns.resize(minKeyWidth);

                    auto lhsTableWhereClause = SplitPredicateByColumnSubset(Query_->WhereClause, *Query_->GetRenamedSchema()).first;
                    bool lhsQueryCanBeSelective = SplitPredicateByColumnSubset(lhsTableWhereClause, TTableSchema(definedKeyColumns))
                        .second->As<TLiteralExpression>() == nullptr;
                    bool inferredRangesCompletelyDefineRhsRanges = joinClause->CommonKeyPrefix >= minKeyWidth && minKeyWidth > 0;
                    bool canUseMergeJoin = inferredRangesCompletelyDefineRhsRanges && !orderedExecution && !lhsQueryCanBeSelective;

                    YT_LOG_DEBUG("Profiling query (CommonKeyPrefix: %v, MinKeyWidth: %v, LhsQueryCanBeSelective: %v)",
                        joinClause->CommonKeyPrefix,
                        minKeyWidth,
                        lhsQueryCanBeSelective);

                    if (canUseMergeJoin) {
                        auto dataSource = GetPrefixReadItems(dataSplits, joinClause->CommonKeyPrefix);
                        dataSource.ObjectId = joinClause->ForeignObjectId;
                        dataSource.CellId = joinClause->ForeignCellId;
                        auto joinSubquery = joinClause->GetJoinSubquery();

                        joinSubquery->InferRanges = false;

                        // COMPAT(lukyan): Use ordered read without modification of protocol
                        joinSubquery->Limit = OrderedReadWithPrefetchHint;

                        YT_LOG_DEBUG("Evaluating remote subquery (SubqueryId: %v)", joinSubquery->Id);

                        auto writer = New<TSimpleRowsetWriter>(MemoryChunkProvider_);

                        auto asyncResult = BIND(
                            &IExecutor::Execute,
                            remoteExecutor,
                            TPlanFragment{
                                .Query = std::move(joinSubquery),
                                .DataSource = std::move(dataSource),
                            },
                            ExternalCGInfo_,
                            writer,
                            remoteOptions,
                            remoteFeatureFlags)
                            .AsyncVia(Invoker_)
                            .Run();

                        asyncResult.Subscribe(BIND([writer] (const TErrorOr<TQueryStatistics>& error) {
                            if (!error.IsOK()) {
                                writer->Fail(error);
                            }
                        }));

                        asyncSubqueryResults->push_back(asyncResult);

                        auto asyncRows = writer->GetResult();

                        return [
                            asyncRows,
                            foreignKeyPrefix = joinClause->ForeignKeyPrefix
                        ] (std::vector<TRow> keys, TRowBufferPtr /*permanentBuffer*/) {
                            std::optional<std::pair<TKeyBoundRef, TKeyBoundRef>> readRange;
                            if (!keys.empty()) {
                                readRange = {
                                    TKeyBoundRef(keys.front().FirstNElements(foreignKeyPrefix), /*inclusive*/ true, /*upper*/ false),
                                    TKeyBoundRef(keys.back().FirstNElements(foreignKeyPrefix), /*inclusive*/ true, /*upper*/ true)};
                            }

                            return New<TRowsetSubrangeReader>(asyncRows, readRange);
                        };
                    } else {
                        return [
                            asyncSubqueryResults,
                            remoteExecutor,
                            joinClause,
                            remoteOptions,
                            remoteFeatureFlags,
                            this,
                            this_ = MakeStrong(this)
                        ] (std::vector<TRow> keys, TRowBufferPtr permanentBuffer) {
                            if (keys.empty()) {
                                return ISchemafulUnversionedReaderPtr{};
                            }
                            TDataSource dataSource;
                            TQueryPtr foreignQuery;
                            std::tie(foreignQuery, dataSource) = GetForeignQuery(
                                std::move(keys),
                                std::move(permanentBuffer),
                                *joinClause);

                            YT_LOG_DEBUG("Evaluating remote subquery (SubqueryId: %v)", foreignQuery->Id);

                            auto pipe = New<NTableClient::TSchemafulPipe>(MemoryChunkProvider_);

                            auto asyncResult = BIND(
                                &IExecutor::Execute,
                                remoteExecutor,
                                TPlanFragment{
                                    .Query = std::move(foreignQuery),
                                    .DataSource = std::move(dataSource),
                                },
                                ExternalCGInfo_,
                                pipe->GetWriter(),
                                remoteOptions,
                                remoteFeatureFlags)
                                .AsyncVia(Invoker_)
                                .Run();

                            asyncResult.Subscribe(BIND([pipe] (const TErrorOr<TQueryStatistics>& error) {
                                if (!error.IsOK()) {
                                    pipe->Fail(error);
                                }
                            }));

                            asyncSubqueryResults->push_back(asyncResult);

                            return pipe->GetReader();
                        };
                    }
                };

                auto mergingReader = CreateReaderForDataSources(std::move(dataSplits), rowBuffer);

                YT_LOG_DEBUG("Evaluating bottom query (BottomQueryId: %v)", bottomQuery->Id);

                auto pipe = New<TSchemafulPipe>(MemoryChunkProvider_);

                // This refers to the Node execution level.
                // There is only NodeThread execution level below,
                // so we can set the most recent feature flags.
                auto responseFeatureFlags = MakeFuture(MostFreshFeatureFlags());

                auto asyncStatistics = BIND(&IEvaluator::Run, Evaluator_)
                    .AsyncVia(Invoker_)
                    .Run(
                        bottomQuery,
                        mergingReader,
                        pipe->GetWriter(),
                        foreignProfileCallback,
                        functionGenerators,
                        aggregateGenerators,
                        MemoryChunkProvider_,
                        QueryOptions_,
                        RequestFeatureFlags_,
                        responseFeatureFlags);

                asyncStatistics = asyncStatistics.Apply(BIND([
                    =,
                    this,
                    this_ = MakeStrong(this)
                ] (const TErrorOr<TQueryStatistics>& result) -> TFuture<TQueryStatistics> {
                    if (!result.IsOK()) {
                        pipe->Fail(result);
                        YT_LOG_DEBUG(result, "Bottom query failed (SubqueryId: %v)", bottomQuery->Id);
                        return MakeFuture(result);
                    } else {
                        TQueryStatistics statistics = result.Value();

                        YT_LOG_DEBUG("Bottom query finished (SubqueryId: %v, Statistics: %v)",
                            bottomQuery->Id,
                            statistics);

                        return AllSucceeded(*asyncSubqueryResults)
                        .Apply(BIND([
                            =,
                            this,
                            this_ = MakeStrong(this)
                        ] (const std::vector<TQueryStatistics>& subqueryResults) mutable {
                            for (const auto& subqueryResult : subqueryResults) {
                                YT_LOG_DEBUG("Remote subquery statistics %v", subqueryResult);
                                statistics.AddInnerStatistics(subqueryResult);
                            }
                            return statistics;
                        }));
                    }
                }));

                return {pipe->GetReader(), asyncStatistics, responseFeatureFlags};
            },
            [&, frontQuery = frontQuery] (
                const ISchemafulUnversionedReaderPtr& reader,
                TFuture<TFeatureFlags> responseFeatureFlags
            ) -> TQueryStatistics {
                YT_LOG_DEBUG("Evaluating front query (FrontQueryId: %v)", frontQuery->Id);
                auto result = Evaluator_->Run(
                    frontQuery,
                    std::move(reader),
                    Writer_,
                    nullptr,
                    functionGenerators,
                    aggregateGenerators,
                    MemoryChunkProvider_,
                    QueryOptions_,
                    RequestFeatureFlags_,
                    responseFeatureFlags);
                YT_LOG_DEBUG("Finished evaluating front query (FrontQueryId: %v)", frontQuery->Id);
                return result;
            });
    }

    std::vector<std::vector<TabletReadItems>> CoordinateDataSourcesOld(
        std::vector<TDataSource> dataSourcesByTablet,
        const TRowBufferPtr& rowBuffer)
    {
        std::vector<TabletReadItems> splits;

        bool sortedDataSource = false;

        for (const auto& tabletIdRange : dataSourcesByTablet) {
            auto tabletId = tabletIdRange.ObjectId;
            const auto& ranges = tabletIdRange.Ranges;
            const auto& keys = tabletIdRange.Keys;

            auto tabletSnapshot = TabletSnapshots_.GetCachedTabletSnapshot(tabletId);

            YT_VERIFY(tabletIdRange.Keys.Empty() != ranges.Empty());

            if (!tabletSnapshot->TableSchema->IsSorted() || ranges.Empty()) {
                splits.push_back(TabletReadItems{
                    .TabletId = tabletId,
                    .Ranges = ranges,
                    .Keys = keys,
                });
                continue;
            }

            sortedDataSource = true;

            for (auto it = ranges.begin(), itEnd = ranges.end(); it + 1 < itEnd; ++it) {
                YT_QL_CHECK(it->second <= (it + 1)->first);
            }

            const auto& partitions = tabletSnapshot->PartitionList;
            YT_VERIFY(!partitions.empty());

            auto tabletSplits = SplitTablet(
                TRange(partitions),
                ranges,
                rowBuffer,
                Config_->MaxSubsplitsPerTablet,
                QueryOptions_.VerboseLogging,
                Logger);

            for (const auto& split : tabletSplits) {
                splits.push_back(TabletReadItems{
                    .TabletId = tabletId,
                    .Ranges = split,
                });
            }
        }

        if (sortedDataSource) {
            for (const auto& split : splits) {
                for (auto it = split.Ranges.begin(), itEnd = split.Ranges.end(); it + 1 < itEnd; ++it) {
                    YT_QL_CHECK(it->second <= (it + 1)->first);
                }
            }

            for (auto it = splits.begin(), itEnd = splits.end(); it + 1 < itEnd; ++it) {
                const auto& lhs = *it;
                const auto& rhs = *(it + 1);

                const auto& lhsValue = lhs.Ranges ? lhs.Ranges.Back().second : lhs.Keys.Back();
                const auto& rhsValue = rhs.Ranges ? rhs.Ranges.Front().first : rhs.Keys.Front();

                YT_QL_CHECK(lhsValue <= rhsValue);
            }
        }

        bool regroupByTablets = Query_->GroupClause && Query_->GroupClause->CommonPrefixWithPrimaryKey > 0;

        std::vector<std::vector<TabletReadItems>> groupedDataSplits;

        auto processSplitsRanges = [&] (int beginIndex, int endIndex) {
            if (beginIndex == endIndex) {
                return;
            }

            groupedDataSplits.emplace_back(splits.begin() + beginIndex, splits.begin() + endIndex);
        };

        auto regroupAndProcessSplitsRanges = [&] (int beginIndex, int endIndex) {
            if (!regroupByTablets) {
                processSplitsRanges(beginIndex, endIndex);
                return;
            }
            // Tablets must be read individually because they can be interleaved with tablets at other nodes.
            ssize_t lastOffset = beginIndex;
            for (ssize_t index = beginIndex; index < endIndex; ++index) {
                if (index > lastOffset && splits[index].TabletId != splits[lastOffset].TabletId) {
                    processSplitsRanges(lastOffset, index);
                    lastOffset = index;
                }
            }
            processSplitsRanges(lastOffset, endIndex);
        };

        auto processSplitKeys = [&] (int index) {
            groupedDataSplits.push_back({splits[index]});
        };

        int splitCount = splits.size();
        auto maxSubqueries = std::min({QueryOptions_.MaxSubqueries, Config_->MaxSubqueries, splitCount});
        int splitOffset = 0;
        int queryIndex = 1;
        int nextSplitOffset = queryIndex * splitCount / maxSubqueries;
        for (ssize_t splitIndex = 0; splitIndex < splitCount;) {
            if (splits[splitIndex].Keys) {
                regroupAndProcessSplitsRanges(splitOffset, splitIndex);
                processSplitKeys(splitIndex);
                splitOffset = ++splitIndex;
            } else {
                ++splitIndex;
            }

            if (splitIndex == nextSplitOffset) {
                regroupAndProcessSplitsRanges(splitOffset, splitIndex);
                splitOffset = splitIndex;
                ++queryIndex;
                nextSplitOffset = queryIndex * splitCount / maxSubqueries;
            }
        }

        YT_VERIFY(splitOffset == splitCount);

        return groupedDataSplits;
    }

    std::vector<TDataSource> GetClassifiedDataSources(const TRowBufferPtr& rowBuffer)
    {
        YT_LOG_DEBUG("Classifying data sources into ranges and lookup keys");

        std::vector<TDataSource> classifiedDataSources;

        auto keySize = Query_->Schema.Original->GetKeyColumnCount();

        bool lookupSupported;

        bool hasRanges = false;
        for (const auto& source : DataSources_) {
            auto tabletSnapshot = TabletSnapshots_.GetCachedTabletSnapshot(source.ObjectId);

            lookupSupported = tabletSnapshot->TableSchema->IsSorted();

            for (const auto& range : source.Ranges) {
                auto lowerBound = range.first;
                auto upperBound = range.second;

                if (lookupSupported &&
                    keySize == static_cast<int>(lowerBound.GetCount()) &&
                    keySize + 1 == static_cast<int>(upperBound.GetCount()) &&
                    upperBound[keySize].Type == EValueType::Max &&
                    CompareValueRanges(lowerBound.Elements(), upperBound.FirstNElements(keySize)) == 0)
                {
                    continue;
                }

                hasRanges = true;
                break;
            }
        }

        size_t rangeCount = 0;
        size_t keyCount = 0;
        for (const auto& source : DataSources_) {
            TRowRanges rowRanges;
            std::vector<TRow> keys;

            auto pushRanges = [&] {
                if (!rowRanges.empty()) {
                    rangeCount += rowRanges.size();
                    classifiedDataSources.push_back(TDataSource{
                        .ObjectId = source.ObjectId,
                        .CellId = source.CellId,
                        .Ranges = MakeSharedRange(std::move(rowRanges), source.Ranges.GetHolder(), rowBuffer),
                    });
                }
            };

            auto pushKeys = [&] {
                if (!keys.empty()) {
                    keyCount += keys.size();
                    classifiedDataSources.push_back(TDataSource{
                        .ObjectId = source.ObjectId,
                        .CellId = source.CellId,
                        .Keys = MakeSharedRange(std::move(keys), source.Ranges.GetHolder()),
                    });
                }
            };

            for (const auto& range : source.Ranges) {
                auto lowerBound = range.first;
                auto upperBound = range.second;

                if (lookupSupported &&
                    !hasRanges &&
                    keySize == static_cast<int>(lowerBound.GetCount()) &&
                    keySize + 1 == static_cast<int>(upperBound.GetCount()) &&
                    upperBound[keySize].Type == EValueType::Max &&
                    CompareValueRanges(lowerBound.Elements(), upperBound.FirstNElements(keySize)) == 0)
                {
                    pushRanges();
                    keys.push_back(lowerBound);
                } else {
                    pushKeys();
                    rowRanges.push_back(range);
                }
            }

            for (const auto& key : source.Keys) {
                auto rowSize = key.GetCount();
                if (lookupSupported &&
                    !hasRanges &&
                    keySize == static_cast<int>(key.GetCount()))
                {
                    pushRanges();
                    keys.push_back(key);
                } else {
                    pushKeys();
                    rowRanges.emplace_back(key, WidenKeySuccessor(key, rowSize, rowBuffer, false));
                }
            }
            pushRanges();
            pushKeys();
        }

        YT_LOG_DEBUG("Splitting ranges (RangeCount: %v, KeyCount: %v)",
            rangeCount,
            keyCount);

        return classifiedDataSources;
    }

    std::vector<std::vector<TabletReadItems>> CoordinateDataSourcesNew(
        std::vector<TDataSource> dataSourcesByTablet,
        const TRowBufferPtr& rowBuffer)
    {
        struct TSampleRange
        {
            TKeyRef LowerSampleKey;
            TKeyRef UpperSampleKey;

            TRange<TRowRange> Ranges;
            ui64 Weight = 0;
        };

        struct TPartitionRanges
        {
            std::vector<TSampleRange> SampleRanges;

            // PartitionIndex is used to determine which partiton to read if range intersects partition's bound.
            int PartitionIndex;
        };

        struct TTabletRanges
        {
            std::vector<TPartitionRanges> PartitionRanges;
            TTabletId TabletId;

            TSharedRange<TRowRange> Ranges;
            TSharedRange<TRow> Keys;
        };

        std::vector<TTabletRanges> tabletRanges;

        ui64 totalWeight = 0;
        ui64 maxWeight = 0;

        for (const auto& dataSource : dataSourcesByTablet) {
            auto tabletId = dataSource.ObjectId;
            const auto& ranges = dataSource.Ranges;
            const auto& keys = dataSource.Keys;

            YT_VERIFY(keys.Empty() != ranges.Empty());
            for (auto it = ranges.begin(), itEnd = ranges.end(); it + 1 < itEnd; ++it) {
                YT_QL_CHECK(it->second <= (it + 1)->first);
            }

            auto tabletSnapshot = TabletSnapshots_.GetCachedTabletSnapshot(tabletId);

            if (!tabletSnapshot->TableSchema->IsSorted() || ranges.Empty()) {
                tabletRanges.push_back({.TabletId = tabletId, .Ranges = ranges, .Keys = keys});
                continue;
            }

            auto partitions = TRange(tabletSnapshot->PartitionList);
            YT_VERIFY(!partitions.empty());

            using TPartitionIt = typename TRange<TPartitionSnapshotPtr>::iterator;
            using TRangesIt = TRange<TRowRange>::iterator;

            struct TPredicate
            {
                // itemIt PRECEDES partitionIt
                bool operator() (TRangeIt itemIt, TPartitionIt partitionIt) const
                {
                    return itemIt->second <= (*partitionIt)->NextPivotKey;
                }

                // itemIt FOLLOWS partitionIt
                bool operator() (TPartitionIt partitionIt, TRangeIt itemIt) const
                {
                    return (*partitionIt)->NextPivotKey <= itemIt->first;
                }
            };

            std::vector<TPartitionRanges> partitionRanges;

            GroupItemsByShards(ranges, partitions, TPredicate{}, [&] (TPartitionIt partitionIt, TRangesIt rangesIt, TRangesIt rangesItEnd) {
                YT_VERIFY(rangesIt != rangesItEnd);

                if (partitionIt == partitions.end()) {
                    YT_VERIFY(rangesIt + 1 == ranges.end());
                    return;
                }

                struct TPredicate
                {
                    // itemIt PRECEDES shardIt
                    bool operator() (const TRowRange* itemIt, const TKeyRef* shardIt) const
                    {
                        return CompareValueRanges(ToKeyRef(itemIt->second), *shardIt) <= 0;
                    }

                    // itemIt FOLLOWS shardIt
                    bool operator() (const TKeyRef* shardIt, const TRowRange* itemIt) const
                    {
                        return CompareValueRanges(*shardIt, ToKeyRef(itemIt->first)) <= 0;
                    }
                };

                int partitionIndex = static_cast<int>(partitionIt - partitions.begin());

                TRange<TLegacyKey> paritionSampleKeys = (*partitionIt)->SampleKeys->Keys;

                // Alternatively can group items by original sample ranges with weight 1 and concat them afterwise.
                // It does not make difference because of original ranges width.
                std::vector<TKeyRef> sampleKeyPrefixes;
                std::vector<int> weights;
                if (QueryOptions_.MergeVersionedRows) {
                    weights.push_back(1);

                    for (auto key : paritionSampleKeys) {
                        sampleKeyPrefixes.push_back(ToKeyRef(key));
                        weights.push_back(1);
                    }
                } else {
                    size_t keyWidth = std::numeric_limits<size_t>::min();
                    for (const auto& range : ranges) {
                        keyWidth = std::max({
                            keyWidth,
                            GetSignificantWidth(range.first),
                            GetSignificantWidth(range.second)});
                    }

                    if (QueryOptions_.VerboseLogging) {
                        YT_LOG_DEBUG("Preparing sample key prefixes (PartitionIndex: %v, KeyWidth: %v, SamplesInPartition: %v)",
                            partitionIndex,
                            keyWidth,
                            std::ssize(paritionSampleKeys));
                    }

                    auto maxKeyWidth = paritionSampleKeys.Empty() ? keyWidth : paritionSampleKeys.Front().GetCount();

                    auto optimalKeyWidth = ExponentialSearch(keyWidth, maxKeyWidth, [&] (size_t keyWidth) {
                        std::tie(sampleKeyPrefixes, weights) = GetSampleKeysForPrefix(
                            paritionSampleKeys,
                            keyWidth,
                            (*partitionIt)->PivotKey);

                        int maxWeight = 0;
                        for (auto weight : weights) {
                            maxWeight = std::max(maxWeight, weight);
                        }

                        if (QueryOptions_.VerboseLogging) {
                            YT_LOG_DEBUG("Iteration (KeyWidth: %v, MaxWeight: %v, Weights: %v, SamplePrefixes: %v)",
                                keyWidth,
                                maxWeight,
                                weights,
                                MakeFormattableView(sampleKeyPrefixes, TKeyFormatter()));
                        }

                        // Stop when maxWeight is less than square root of sample key count per parittion.
                        return maxWeight * maxWeight > std::ssize(paritionSampleKeys);
                    });

                    std::tie(sampleKeyPrefixes, weights) = GetSampleKeysForPrefix(
                        paritionSampleKeys,
                        optimalKeyWidth,
                        (*partitionIt)->PivotKey);

                    if (QueryOptions_.VerboseLogging) {
                        YT_LOG_DEBUG("Prepared sample key prefixes (KeyWidth: %v, OptimalKeyWidth: %v)", keyWidth, optimalKeyWidth);
                    }

                    keyWidth = optimalKeyWidth;
                }

                ui64 rowCountInPartition = 0;

                for (const auto& store : (*partitionIt)->Stores) {
                    rowCountInPartition += store->GetRowCount();
                }

                ui64 rowCountPerSampleRange = std::max<ui64>(rowCountInPartition / (paritionSampleKeys.size() + 1), 1);
                YT_VERIFY(rowCountPerSampleRange > 0);

                if (QueryOptions_.VerboseLogging) {
                    YT_LOG_DEBUG("Processing partition (PartitionIndex: %v, InitialRanges: %v, SamplePrefixes: %v, Weights: %v, RowCountPerSampleRange: %v)",
                        partitionIndex,
                        MakeFormattableView(TRange(rangesIt, rangesItEnd), TRangeFormatter()),
                        MakeFormattableView(sampleKeyPrefixes, TKeyFormatter()),
                        weights,
                        rowCountPerSampleRange);
                }

                std::vector<TSampleRange> sampleRanges;
                GroupItemsByShards(
                    TRange(rangesIt, rangesItEnd),
                    TRange(sampleKeyPrefixes),
                    TPredicate{},
                    [&] (const TKeyRef* sampleIt, TRangeIt rangesIt, TRangeIt rangesItEnd) {
                        TKeyRef lowerSampleBound = sampleIt == sampleKeyPrefixes.begin() ? ToKeyRef(MinKey()) : *(sampleIt - 1);
                        TKeyRef upperSampleBound = sampleIt == sampleKeyPrefixes.end() ? ToKeyRef(MaxKey()) : *sampleIt;

                        auto weight = weights[sampleIt - sampleKeyPrefixes.begin()] * rowCountPerSampleRange;

                        YT_VERIFY(weight > 0);
                        totalWeight += weight;
                        if (maxWeight < weight) {
                            maxWeight = weight;
                        }

                        sampleRanges.push_back(TSampleRange{
                            lowerSampleBound,
                            upperSampleBound,
                            ranges.Slice(rangesIt, rangesItEnd),
                            weight});
                    });

                if (QueryOptions_.VerboseLogging) {
                    YT_LOG_DEBUG("Got grouped by samples ranges (PartitionIndex: %v, SampleRanges: %v)",
                        partitionIndex,
                        MakeFormattableView(sampleRanges, [] (TStringBuilderBase* builder, const TSampleRange& item) {
                            builder->AppendFormat("Sample: %kv .. %kv, Ranges: %v",
                                item.LowerSampleKey,
                                item.UpperSampleKey,
                                MakeFormattableView(item.Ranges, TRangeFormatter()));
                        }));
                }
                partitionRanges.push_back({std::move(sampleRanges), partitionIndex});
            });

            tabletRanges.push_back({.PartitionRanges = std::move(partitionRanges), .TabletId = tabletId});
        }

        ui64 minWeightPerSubquery = QueryOptions_.MinRowCountPerSubquery;
        auto maxGroups = std::min(QueryOptions_.MaxSubqueries, Config_->MaxSubqueries);
        YT_VERIFY(maxGroups > 0);

        auto weightPerSubquery = std::max(maxWeight, std::min(minWeightPerSubquery, totalWeight));
        int targetGroupCount = std::min<int>(totalWeight == 0 ? 1 : totalWeight / weightPerSubquery, maxGroups);
        YT_VERIFY(targetGroupCount > 0);

        YT_LOG_DEBUG("Regrouping by weight (TotalWeight: %v, MaxWeight %v, MinWeightPerSubquery: %v, MaxGroups: %v, TargetGroupCount: %v)",
            totalWeight,
            maxWeight,
            minWeightPerSubquery,
            maxGroups,
            targetGroupCount);

        ui64 currentSummaryWeight = 0;
        int groupId = 0;
        ui64 nextWeight = (groupId + 1) * totalWeight / targetGroupCount;

        std::vector<std::vector<TabletReadItems>> groupedReadRanges;
        std::vector<TabletReadItems> tabletBoundsGroup;

        bool firstSampleInGroup = true;

        TRow lowerBound;
        const TRowRange* lastRowRange;

        for (const auto& [partitionRanges, tabletId, ranges, keys] : tabletRanges) {
            if (!ranges.empty() || !keys.empty()) {
                YT_VERIFY(tabletBoundsGroup.empty());
                groupedReadRanges.push_back({{.TabletId = tabletId, .Ranges = ranges, .Keys = keys}});
                continue;
            }

            for (const auto& [sampleRanges, partitionIndex] : partitionRanges) {
                lastRowRange = nullptr;

                for (const auto& sampleRange : sampleRanges) {
                    currentSummaryWeight += sampleRange.Weight;

                    if (tabletBoundsGroup.empty() || tabletBoundsGroup.back().TabletId != tabletId) {
                        tabletBoundsGroup.push_back({.TabletId = tabletId});
                    }

                    if (tabletBoundsGroup.back().PartitionBounds.empty() ||
                        tabletBoundsGroup.back().PartitionBounds.back().PartitionIndex != partitionIndex)
                    {
                        tabletBoundsGroup.back().PartitionBounds.push_back({.PartitionIndex = partitionIndex});
                    }

                    auto& partitionBounds = tabletBoundsGroup.back().PartitionBounds.back();

                    if (firstSampleInGroup) {
                        lowerBound = sampleRange.Ranges.Front().first;
                        if (CompareValueRanges(ToKeyRef(lowerBound), sampleRange.LowerSampleKey) < 0) {
                            lowerBound = rowBuffer->CaptureRow(sampleRange.LowerSampleKey);
                        }

                        firstSampleInGroup = false;
                    }

                    for (auto range : TRange(std::max(lastRowRange, sampleRange.Ranges.Begin()), sampleRange.Ranges.End())) {
                        YT_VERIFY(partitionBounds.Bounds.empty() || partitionBounds.Bounds.back().second <= range.first);
                        partitionBounds.Bounds.push_back(range);
                    }

                    lastRowRange = sampleRange.Ranges.End();

                    if (currentSummaryWeight >= nextWeight) {
                        // Flush group.
                        YT_VERIFY(!firstSampleInGroup);

                        auto upperBound = tabletBoundsGroup.back().PartitionBounds.back().Bounds.back().second;
                        if (CompareValueRanges(sampleRange.UpperSampleKey, ToKeyRef(upperBound)) < 0) {
                            upperBound = rowBuffer->CaptureRow(sampleRange.UpperSampleKey);
                        }

                        tabletBoundsGroup.front().PartitionBounds.front().Bounds.front().first = lowerBound;
                        tabletBoundsGroup.back().PartitionBounds.back().Bounds.back().second = upperBound;

                        for (const auto& boundsGroup : tabletBoundsGroup) {
                            for (const auto& partitionBounds : boundsGroup.PartitionBounds) {
                                for (const auto& bound : partitionBounds.Bounds) {
                                    YT_QL_CHECK(bound.first <= bound.second);
                                }
                            }
                        }

                        groupedReadRanges.push_back(std::move(tabletBoundsGroup));

                        // Initialize new group.
                        lastRowRange = nullptr;
                        firstSampleInGroup = true;

                        ++groupId;
                        nextWeight = (groupId + 1) * totalWeight / targetGroupCount;
                    }
                }
            }
        }

        // Last group must be flushed and empty here.
        YT_VERIFY(
            tabletBoundsGroup.empty() ||
            tabletBoundsGroup.back().PartitionBounds.empty() ||
            tabletBoundsGroup.back().PartitionBounds.back().Bounds.empty());
        YT_VERIFY(currentSummaryWeight == totalWeight);

        bool regroupByTablets = Query_->GroupClause && Query_->GroupClause->CommonPrefixWithPrimaryKey > 0;

        if (regroupByTablets) {
            std::vector<std::vector<TabletReadItems>> regroupedReadRanges;
            std::vector<TabletReadItems> tabletBoundsGroup;

            for (const auto& group : groupedReadRanges) {
                for (const auto& tabletReadItems : group) {
                    if (tabletBoundsGroup.empty() || tabletBoundsGroup.back().TabletId != tabletReadItems.TabletId) {
                        tabletBoundsGroup.push_back(tabletReadItems);
                    } else {
                        regroupedReadRanges.push_back(std::move(tabletBoundsGroup));
                    }
                }
                regroupedReadRanges.push_back(std::move(tabletBoundsGroup));
            }

            return regroupedReadRanges;
        }

        return groupedReadRanges;
    }

    ISchemafulUnversionedReaderPtr CreateReaderForDataSources(
        std::vector<TabletReadItems> dataSplits,
        TRowBufferPtr rowBuffer)
    {
        size_t partitionBounds = 0;
        size_t sortedRangeCount = 0;
        size_t orderedRangeCount = 0;
        size_t keyCount = 0;

        for (const auto& dataSplit : dataSplits) {
            partitionBounds += dataSplit.PartitionBounds.size();
            for (const auto& bounds : dataSplit.PartitionBounds) {
                sortedRangeCount += bounds.Bounds.size();
            }

            orderedRangeCount += dataSplit.Ranges.size();
            keyCount += dataSplit.Keys.size();
        }

        YT_LOG_DEBUG("Generating reader (SplitCount: %v, PartitionBounds: %v, SortedRanges: %v, OrderedRanges: %v, Keys: %v",
            dataSplits.size(),
            partitionBounds,
            sortedRangeCount,
            orderedRangeCount,
            keyCount);

        if (QueryOptions_.VerboseLogging) {
            for (const auto& dataSplit : dataSplits) {
                YT_LOG_DEBUG("Read items in split (TabletId: %v, Partitions: %v, Ranges: %v, Keys: %v)",
                    dataSplit.TabletId,
                    MakeFormattableView(
                        dataSplit.PartitionBounds,
                        [] (TStringBuilderBase* builder, const TPartitionBounds& source) {
                            builder->AppendFormat("%v: %v",
                                source.PartitionIndex,
                                MakeFormattableView(source.Bounds, TRangeFormatter()));
                        }),
                    MakeFormattableView(dataSplit.Ranges, TRangeFormatter()),
                    MakeFormattableView(dataSplit.Keys, TKeyFormatter()));
            }
        }

        auto bottomSplitReaderGenerator = [
            =,
            this,
            this_ = MakeStrong(this),
            dataSplits = std::move(dataSplits),
            dataSplitIndex = 0
        ] () mutable -> ISchemafulUnversionedReaderPtr {
            if (dataSplitIndex == std::ssize(dataSplits)) {
                return nullptr;
            }

            const auto& dataSplit = dataSplits[dataSplitIndex++];

            auto tabletSnapshot = TabletSnapshots_.GetCachedTabletSnapshot(dataSplit.TabletId);
            auto [columnFilter, timestampReadOptions] = GetColumnFilter(*Query_->GetReadSchema(), *tabletSnapshot->QuerySchema);

            try {
                ISchemafulUnversionedReaderPtr reader;

                if (dataSplit.Ranges) {
                    if (tabletSnapshot->TableSchema->IsSorted()) {
                        reader = CreateSchemafulSortedTabletReader(
                            tabletSnapshot,
                            columnFilter,
                            dataSplit.Ranges,
                            QueryOptions_.TimestampRange,
                            ChunkReadOptions_,
                            ETabletDistributedThrottlerKind::Select,
                            ChunkReadOptions_.WorkloadDescriptor.Category,
                            std::move(timestampReadOptions),
                            QueryOptions_.MergeVersionedRows);
                    } else {
                        auto orderedRangeReaderGenerator = [
                            =,
                            this,
                            this_ = MakeStrong(this),
                            rangeIndex = 0
                        ] () mutable -> ISchemafulUnversionedReaderPtr {
                            if (rangeIndex == std::ssize(dataSplit.Ranges)) {
                                return nullptr;
                            }

                            const auto& range = dataSplit.Ranges[rangeIndex++];

                            return CreateSchemafulOrderedTabletReader(
                                tabletSnapshot,
                                columnFilter,
                                TLegacyOwningKey(range.first),
                                TLegacyOwningKey(range.second),
                                QueryOptions_.TimestampRange,
                                ChunkReadOptions_,
                                ETabletDistributedThrottlerKind::Select,
                                ChunkReadOptions_.WorkloadDescriptor.Category);
                        };

                        reader = CreateUnorderedSchemafulReader(std::move(orderedRangeReaderGenerator), 1);
                    }
                } else if (!dataSplit.PartitionBounds.empty()) {
                    reader = CreatePartitionScanReader(
                        tabletSnapshot,
                        columnFilter,
                        MakeSharedRange(dataSplit.PartitionBounds, rowBuffer),
                        QueryOptions_.TimestampRange,
                        ChunkReadOptions_,
                        ETabletDistributedThrottlerKind::Select,
                        ChunkReadOptions_.WorkloadDescriptor.Category,
                        std::move(timestampReadOptions),
                        QueryOptions_.MergeVersionedRows);
                } else if (dataSplit.Keys) {
                    THROW_ERROR_EXCEPTION_IF(!QueryOptions_.MergeVersionedRows,
                        "Read on full key is incompatible with not merging versioned rows");

                    return CreateLookupSessionReader(
                        MemoryChunkProvider_,
                        tabletSnapshot,
                        columnFilter,
                        dataSplit.Keys,
                        QueryOptions_.TimestampRange,
                        QueryOptions_.UseLookupCache,
                        ChunkReadOptions_,
                        std::move(timestampReadOptions),
                        Invoker_,
                        GetProfilingUser(Identity_),
                        Logger);
                }

                return New<TProfilingReaderWrapper>(
                    reader,
                    *tabletSnapshot->TableProfiler->GetSelectRowsCounters(GetProfilingUser(Identity_)),
                    tabletSnapshot->Settings.MountConfig->EnableDetailedProfiling);
            } catch (const std::exception& ex) {
                THROW_ERROR EnrichErrorForErrorManager(TError(ex), tabletSnapshot);
            }
        };

        return CreatePrefetchingOrderedSchemafulReader(std::move(bottomSplitReaderGenerator));
    }
};

////////////////////////////////////////////////////////////////////////////////

TQueryStatistics ExecuteSubquery(
    TQueryAgentConfigPtr config,
    TFunctionImplCachePtr functionImplCache,
    NTabletNode::IBootstrap* const bootstrap,
    IEvaluatorPtr evaluator,
    TConstQueryPtr query,
    TConstExternalCGInfoPtr externalCGInfo,
    std::vector<TDataSource> dataSources,
    IUnversionedRowsetWriterPtr writer,
    IMemoryChunkProviderPtr memoryChunkProvider,
    IInvokerPtr invoker,
    TQueryOptions queryOptions,
    TFeatureFlags requestFeatureFlags,
    TServiceProfilerGuard& profilerGuard)
{
    ValidateReadTimestamp(queryOptions.TimestampRange.Timestamp);

    auto execution = New<TQueryExecution>(
        config,
        functionImplCache,
        bootstrap,
        evaluator,
        std::move(query),
        std::move(externalCGInfo),
        std::move(dataSources),
        std::move(writer),
        std::move(memoryChunkProvider),
        invoker,
        std::move(queryOptions),
        std::move(requestFeatureFlags));

    return execution->Execute(profilerGuard);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
