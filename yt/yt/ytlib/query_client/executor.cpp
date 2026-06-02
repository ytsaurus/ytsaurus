#include "executor.h"
#include "functions_cache.h"
#include "query_service_proxy.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>
#include <yt/yt/ytlib/api/native/tablet_helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/library/query/base/coordination_helpers.h>
#include <yt/yt/library/query/base/helpers.h>
#include <yt/yt/library/query/base/join_profiler.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>
#include <yt/yt/library/query/base/query_visitors.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/coordinator.h>
#include <yt/yt/library/query/engine_api/evaluator.h>
#include <yt/yt/library/query/engine_api/new_range_inferrer.h>

#include <yt/yt/library/query/engine_api/query_engine_config.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>
#include <yt/yt/client/tablet_client/table_mount_cache_detail.h>

#include <yt/yt/client/table_client/pipe.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/versioned_io_options.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/range_formatters.h>

#include <yt/yt/core/rpc/helpers.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NHydra;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NChunkClient;
using namespace NApi;
using namespace NApi::NNative;
using namespace NProfiling;

using NYT::FromProto;
using NYT::ToProto;

using NChunkClient::NProto::TDataStatistics;

using NNodeTrackerClient::INodeChannelFactoryPtr;

using NObjectClient::TObjectId;
using NObjectClient::FromObjectId;

using NHiveClient::TConstCellDescriptorPtr;

////////////////////////////////////////////////////////////////////////////////

namespace {

void MarkMountCacheInvalidationExhausted(TError* error)
{
    auto isMountCacheRetryableCode = [] (TErrorCode code) {
        return std::find(
            TableMountCacheRetryableCodes.begin(),
            TableMountCacheRetryableCodes.end(),
            code) != TableMountCacheRetryableCodes.end();
    };

    if (error->Attributes().Contains("tablet_id") && isMountCacheRetryableCode(error->GetCode())) {
        (*error) <<= TErrorAttribute("mount_cache_invalidation_exhausted", true);
    }

    for (auto& innerError : *error->MutableInnerErrors()) {
        MarkMountCacheInvalidationExhausted(&innerError);
    }
}

bool ValidateSchema(const TTableSchema& original, const TTableSchema& query)
{
    if (original.IsStrict() != query.IsStrict() ||
        original.IsUniqueKeys() != query.IsUniqueKeys() ||
        original.GetSchemaModification() != query.GetSchemaModification() ||
        original.DeletedColumns() != query.DeletedColumns())
    {
        return false;
    }

    auto queryColumnIt = query.Columns().begin();
    for (const auto& column : original.Columns()) {
        if (!GetTimestampColumnOriginalNameOrNull(column.Name()) &&
            (queryColumnIt == query.Columns().end() || *queryColumnIt++ != column))
        {
            return false;
        }
    }

    if (queryColumnIt != query.Columns().end()) {
        return false;
    }

    return true;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

using TShardIt = const TTabletInfoPtr*;
using THolder = TRefCountedPtr;

template <>
TRow GetPivotKey(const TTabletInfoPtr& shard)
{
    return shard->PivotKey;
}

std::vector<std::pair<TDataSource, std::string>> CoordinateDataSources(
    const NHiveClient::ICellDirectoryPtr& cellDirectory,
    const NNodeTrackerClient::TNetworkPreferenceList& networks,
    const TTableMountInfoPtr& tableInfo,
    const TDataSource& dataSource,
    TRowBufferPtr rowBuffer,
    EPeerKind readFrom)
{
    auto ranges = dataSource.Ranges;
    auto keys = dataSource.Keys;

    THashMap<NTabletClient::TTabletCellId, TConstCellDescriptorPtr> tabletCellReplicas;

    auto getAddress = [&] (const TTabletInfoPtr& tabletInfo) {
        ValidateTabletMountedOrFrozen(tableInfo, tabletInfo);

        auto insertResult = tabletCellReplicas.emplace(tabletInfo->CellId, TConstCellDescriptorPtr());
        auto& descriptor = insertResult.first->second;

        if (insertResult.second) {
            descriptor = cellDirectory->GetDescriptorByCellIdOrThrow(tabletInfo->CellId);
        }

        const auto& peerDescriptor = GetPrimaryTabletPeerDescriptor(*descriptor, readFrom);
        return peerDescriptor.GetAddressOrThrow(networks);
    };

    std::vector<std::pair<TDataSource, std::string>> subsources;
    auto makeSubsource = [&] (TShardIt it) {
        // `.Slice(1, tablets.size())`, `*(it - 1)` and `(*shardIt)->PivotKey` are related.
        // If there would be (*shardIt)->NextPivotKey then no .Slice(1, tablets.size()) and *(it - 1) are needed.

        YT_VERIFY(it != tableInfo->Tablets.begin()); // But can be equal to end.

        const auto& tabletInfo = *(it - 1);

        TDataSource dataSubsource;

        dataSubsource.ObjectId = tabletInfo->TabletId;
        dataSubsource.CellId = tabletInfo->CellId;
        dataSubsource.MountRevision = tabletInfo->MountRevision;

        const auto& address = getAddress(tabletInfo);

        return &subsources.emplace_back(std::move(dataSubsource), address).first;
    };

    if (ranges) {
        YT_VERIFY(!keys);

        SplitRangesByTablets(
            ranges,
            TRange(tableInfo->Tablets),
            tableInfo->LowerCapBound,
            tableInfo->UpperCapBound,
            [&] (auto shardIt, auto rangesIt, auto rangesItEnd) {
                if (tableInfo->IsOrdered()) {
                    // Crop ranges for ordered table.
                    auto nextPivotKey = shardIt != TRange(tableInfo->Tablets).end()
                        ? GetPivotKey(*shardIt)
                        : tableInfo->UpperCapBound;

                    auto lower = std::max(rangesIt->first, GetPivotKey(*(shardIt - 1)));
                    auto upper = std::min((rangesItEnd - 1)->second, nextPivotKey);

                    std::vector<TRowRange> result;
                    ForEachRange(TRange(rangesIt, rangesItEnd), TRowRange(lower, upper), [&] (auto item) {
                        auto [lower, upper] = item;
                        result.emplace_back(rowBuffer->CaptureRow(lower), rowBuffer->CaptureRow(upper));
                    });

                    makeSubsource(shardIt)->Ranges = MakeSharedRange(
                        result,
                        ranges.GetHolder());
                } else {
                    makeSubsource(shardIt)->Ranges = MakeSharedRange(
                        TRange(rangesIt, rangesItEnd),
                        ranges.GetHolder());
                }
            });
    } else if (keys) {
        YT_VERIFY(!ranges);
        size_t keyWidth = keys.Front().GetCount();

        auto fullKeySize = tableInfo->Schemas[ETableSchemaKind::Query]->GetKeyColumnCount();

        SplitKeysByTablets(
            keys,
            keyWidth,
            fullKeySize,
            TRange(tableInfo->Tablets),
            tableInfo->LowerCapBound,
            tableInfo->UpperCapBound,
            [&] (auto shardIt, auto keysIt, auto keysItEnd) {
                makeSubsource(shardIt)->Keys = MakeSharedRange(
                    TRange(keysIt, keysItEnd),
                    keys.GetHolder());
            });
    }

    return subsources;
}

void ValidateTableInfo(const TTableMountInfoPtr& tableInfo, TTableSchemaPtr expectedQuerySchema)
{
    if (!ValidateSchema(*expectedQuerySchema, *tableInfo->Schemas[ETableSchemaKind::Query])) {
        THROW_ERROR_EXCEPTION(
            NTabletClient::EErrorCode::InvalidMountRevision,
            "Invalid revision for table info; schema has changed")
            << TErrorAttribute("path", tableInfo->Path)
            << TErrorAttribute("original_schema", *expectedQuerySchema)
            << TErrorAttribute("query_schema", *tableInfo->Schemas[ETableSchemaKind::Query]);
    }

    tableInfo->ValidateDynamic();
    tableInfo->ValidateNotPhysicallyLog();
}

void LogInitialDataSources(
    const TTableMountInfoPtr& tableInfo,
    const TDataSource& dataSource,
    bool verboseLogging,
    const NLogging::TLogger& Logger)
{
    auto ranges = dataSource.Ranges;
    auto keys = dataSource.Keys;

    YT_LOG_DEBUG("Splitting %v (RangeCount: %v, TabletsCount: %v, LowerCapBound: %v, UpperCapBound: %v)",
        ranges ? "ranges" : "keys",
        ranges ? ranges.size() : keys.size(),
        tableInfo->Tablets.size(),
        tableInfo->LowerCapBound,
        tableInfo->UpperCapBound);

    if (verboseLogging) {
        YT_LOG_DEBUG("Splitting %v tablet pivot keys (Pivots: %v)",
            ranges ? "ranges" : "keys",
            MakeFormattableView(tableInfo->Tablets, [] (auto* builder, const auto& tablet) {
                builder->AppendFormat("%v", tablet->PivotKey.Get());
            }));
    }
}

////////////////////////////////////////////////////////////////////////////////

class TQueryResponseReader
    : public ISchemafulUnversionedReader
{
public:
    TQueryResponseReader(
        TGuid id,
        TTableSchemaPtr schema,
        NCompression::ECodec codecId,
        IMemoryChunkProviderPtr memoryChunkProvider,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        NLogging::TLogger logger)
        : Id_(id)
        , Schema_(std::move(schema))
        , CodecId_(codecId)
        , Logger(std::move(logger))
        , MemoryChunkProvider_(std::move(memoryChunkProvider))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
    { }

    void Initialize(TFuture<TQueryServiceProxy::TRspExecutePtr> asyncResponse)
    {
        // NB: Don't move this assignment to initializer list as
        // OnResponse will access "this", which is not fully constructed yet.
        QueryResult_ = asyncResponse.Apply(BIND(
            &TQueryResponseReader::OnResponse,
            MakeStrong(this)));

        ResponseFeatureFlags_ = asyncResponse.Apply(BIND([this_ = MakeStrong(this)] (const TQueryServiceProxy::TRspExecutePtr& response) {
            auto flags = MostArchaicFeatureFlags();
            if (response->has_feature_flags()) {
                FromProto(&flags, response->feature_flags());
                const auto& Logger = this_->Logger;
                YT_LOG_DEBUG("Got response feature flags (Flags: %v)",
                    ToString(flags));
            }
            return flags;
        }));
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        auto reader = RowsetReader_.Acquire();
        return reader
            ? reader->Read(options)
            : CreateEmptyUnversionedRowBatch();
    }

    TFuture<void> GetReadyEvent() const override
    {
        auto reader = RowsetReader_.Acquire();
        return reader
            ? reader->GetReadyEvent()
            : QueryResult_.As<void>();
    }

    TFuture<TQueryStatistics> GetQueryResult() const
    {
        return QueryResult_;
    }

    TFuture<TFeatureFlags> GetResponseFeatureFlags() const
    {
        return ResponseFeatureFlags_;
    }

    TDataStatistics GetDataStatistics() const override
    {
        return {};
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return {};
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

private:
    const TGuid Id_;
    const TTableSchemaPtr Schema_;
    const NCompression::ECodec CodecId_;
    const NLogging::TLogger Logger;

    TFuture<TQueryStatistics> QueryResult_;
    TFuture<TFeatureFlags> ResponseFeatureFlags_;
    TAtomicIntrusivePtr<ISchemafulUnversionedReader> RowsetReader_;
    IMemoryChunkProviderPtr MemoryChunkProvider_;
    IMemoryUsageTrackerPtr MemoryUsageTracker_;

    TErrorOr<TQueryStatistics> OnResponse(const TErrorOr<TQueryServiceProxy::TRspExecutePtr>& responseOrError)
    {
        if (responseOrError.IsOK()) {
            const auto& response = responseOrError.Value();
            for (auto& attachment : response->Attachments()) {
                attachment = TryTrackMemory(MemoryUsageTracker_, std::move(attachment))
                    .ValueOrThrow();
            }
            RowsetReader_.Store(CreateWireProtocolRowsetReader(
                std::move(response->Attachments()),
                CodecId_,
                Schema_,
                /*schemaful*/ false,
                MemoryChunkProvider_,
                Logger));
            auto statistics = FromProto<TQueryStatistics>(response->query_statistics());



            size_t bottomRowsRead = 0;
            for (const auto& inner : statistics.InnerStatistics) {
                bottomRowsRead += inner.RowsRead.GetTotal();
            }

            YT_LOG_DEBUG("Subquery finished (SubqueryId: %v, Statistics: %v, BottomRowsRead: %v)",
                Id_,
                statistics,
                bottomRowsRead);

            return statistics;
        } else {
            YT_LOG_DEBUG(responseOrError, "Subquery failed (SubqueryId: %v)",
                Id_);

            return TError(responseOrError);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryResponseReader)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueryExecutor)

class TQueryExecutor
    : public IExecutor
{
public:
    TQueryExecutor(
        IMemoryChunkProviderPtr memoryChunkProvider,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        NNative::IConnectionPtr connection,
        IColumnEvaluatorCachePtr columnEvaluatorCache,
        IEvaluatorPtr evaluator,
        INodeChannelFactoryPtr nodeChannelFactory,
        TFunctionImplCachePtr functionImplCache,
        bool retryOnMetadataCacheInconsistency)
        : MemoryChunkProvider_(std::move(memoryChunkProvider))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
        , Connection_(std::move(connection))
        , ColumnEvaluatorCache_(std::move(columnEvaluatorCache))
        , Evaluator_(std::move(evaluator))
        , NodeChannelFactory_(std::move(nodeChannelFactory))
        , FunctionImplCache_(std::move(functionImplCache))
        , RetryOnMetadataCacheInconsistency_(retryOnMetadataCacheInconsistency)
    { }

    TQueryStatistics Execute(
        const TPlanFragment& planFragment,
        const TConstExternalCGInfoPtr& externalCGInfo,
        const IUnversionedRowsetWriterPtr& writer,
        const TQueryOptions& options,
        const TFeatureFlags& requestFeatureFlags) override
    {
        if (planFragment.DataSource.ObjectId) {
            YT_VERIFY(!planFragment.SubqueryFragment);

            return Execute(
                planFragment.Query,
                externalCGInfo,
                planFragment.DataSource,
                writer,
                options,
                requestFeatureFlags);
        }

        if (planFragment.SubqueryFragment) {
            auto pipe = CreateSchemafulPipe(MemoryChunkProvider_);

            auto subqueryStatistics = Execute(
                *planFragment.SubqueryFragment,
                externalCGInfo,
                pipe->GetWriter(),
                options,
                requestFeatureFlags);

            auto queryStatistics = Execute(
                planFragment.Query,
                externalCGInfo,
                pipe->GetReader(),
                writer,
                options,
                requestFeatureFlags);

            queryStatistics.AddInnerStatistics(std::move(subqueryStatistics));

            return queryStatistics;
        }

        YT_ABORT();
    }

private:
    const IMemoryChunkProviderPtr MemoryChunkProvider_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;
    const NNative::IConnectionPtr Connection_;
    const IColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    const IEvaluatorPtr Evaluator_;
    const INodeChannelFactoryPtr NodeChannelFactory_;
    const TFunctionImplCachePtr FunctionImplCache_;
    const bool RetryOnMetadataCacheInconsistency_;

    TQueryStatistics Execute(
        const TConstQueryPtr& query,
        const TConstExternalCGInfoPtr& externalCGInfo,
        const TDataSource& dataSource,
        const IUnversionedRowsetWriterPtr& writer,
        const TQueryOptions& options,
        const TFeatureFlags& requestFeatureFlags)
    {
        if (RetryOnMetadataCacheInconsistency_) {
            try {
                return NApi::NNative::CallAndRetryIfMetadataCacheIsInconsistent(
                    Connection_,
                    /*profilingInfo*/ nullptr,
                    MakeQueryLogger(query),
                    [&] {
                        return DoExecute(query, externalCGInfo, dataSource, writer, options, requestFeatureFlags);
                    });
            } catch (const TErrorException& ex) {
                auto error = ex.Error();
                // Avoid cascading retries on upper levels of execution.
                MarkMountCacheInvalidationExhausted(&error);
                THROW_ERROR error;
            }
        }
        return DoExecute(query, externalCGInfo, dataSource, writer, options, requestFeatureFlags);
    }

    TQueryStatistics DoExecute(
        const TConstQueryPtr& query,
        const TConstExternalCGInfoPtr& externalCGInfo,
        const TDataSource& dataSource,
        const IUnversionedRowsetWriterPtr& writer,
        const TQueryOptions& options,
        const TFeatureFlags& requestFeatureFlags)
    {
        NTracing::TChildTraceContextGuard guard("QueryClient.Execute");

        auto Logger = MakeQueryLogger(query);

        auto rowBuffer = New<TRowBuffer>(
            TQueryExecutorRowBufferTag{},
            MemoryChunkProvider_);

        auto [inferredDataSource, coordinatedQuery] = InferRanges(
            Connection_->GetColumnEvaluatorCache(),
            query,
            dataSource,
            options,
            rowBuffer,
            MemoryChunkProvider_,
            Logger);

        auto tableId = dataSource.ObjectId;

        auto tableInfo = WaitFor(Connection_->GetTableMountCache()->GetTableInfo(FromObjectId(tableId)))
            .ValueOrThrow();

        ValidateTableInfo(tableInfo, query->Schema.Original);

        LogInitialDataSources(tableInfo, dataSource, options.VerboseLogging, Logger);

        auto allSplits = CoordinateDataSources(
            Connection_->GetCellDirectory(),
            Connection_->GetNetworks(),
            tableInfo,
            inferredDataSource,
            rowBuffer,
            options.ReadFrom);

        for (const auto& dataSource : allSplits) {
            VerifyIdsInRanges(dataSource.first.Ranges);
            VerifyIdsInKeys(dataSource.first.Keys);
        }

        if (coordinatedQuery->GetScanOrder(options.AllowUnorderedGroupByWithLimit) == EScanOrder::Reversed) {
            std::reverse(allSplits.begin(), allSplits.end());
            YT_LOG_DEBUG("Reversed tablet order for DESC scan (SplitCount: %v)", allSplits.size());
        }

        std::vector<std::pair<std::vector<TDataSource>, std::string>> groupedDataSplits;

        auto scanOrder = coordinatedQuery->GetScanOrder(options.AllowUnorderedGroupByWithLimit);
        if (scanOrder == EScanOrder::Ordered || scanOrder == EScanOrder::Reversed) {
            // Splits are ordered by tablet bounds (or reversed for DESC scans).
            YT_LOG_DEBUG("Got ordered splits (SplitCount: %v)", allSplits.size());

            for (const auto& [dataSource, address] : allSplits) {
                groupedDataSplits.emplace_back(std::vector<TDataSource>{dataSource}, address);
            }
        } else {
            size_t minKeyWidth = std::numeric_limits<size_t>::max();
            for (const auto& [split, address] : allSplits) {
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

            if (minKeyWidth == 0) {
                if (!options.AllowFullScan) {
                    THROW_ERROR_EXCEPTION("Primary table key is not used in the where clause (full scan); "
                        "the query is inefficient, consider rewriting it");
                } else {
                    YT_LOG_DEBUG("Executing query with full scan");
                }
            }

            YT_LOG_DEBUG("Regrouping %v splits into groups",
                allSplits.size());

            THashMap<std::string, std::vector<TDataSource>> groupsByAddress;
            for (const auto& split : allSplits) {
                const auto& address = split.second;
                groupsByAddress[address].push_back(split.first);
            }

            for (const auto& group : groupsByAddress) {
                groupedDataSplits.emplace_back(group.second, group.first);
            }

            YT_LOG_DEBUG("Regrouped %v splits into %v groups",
                allSplits.size(),
                groupsByAddress.size());
        }

        auto [queryWithPrefetch, prefetchedJoinRowsets, prefetchQueryStatistics] = MaybePrefetchJoinRowsets(
            std::move(coordinatedQuery),
            query->WhereClause,
            externalCGInfo,
            options,
            requestFeatureFlags,
            Logger);

        auto statistics = DoCoordinateAndExecute(
            queryWithPrefetch,
            externalCGInfo,
            options,
            requestFeatureFlags,
            writer,
            std::move(groupedDataSplits),
            std::move(prefetchedJoinRowsets));

        for (auto& subqueryStatistics : prefetchQueryStatistics) {
            statistics.AddInnerStatistics(std::move(subqueryStatistics));
        }

        return statistics;
    }

    TQueryStatistics Execute(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        ISchemafulUnversionedReaderPtr reader,
        IUnversionedRowsetWriterPtr writer,
        TQueryOptions options,
        TFeatureFlags requestFeatureFlags)
    {
        NTracing::TChildTraceContextGuard guard("QueryClient.Execute");

        auto Logger = MakeQueryLogger(query);

        auto rowBuffer = New<TRowBuffer>(
            TQueryExecutorRowBufferTag{},
            MemoryChunkProvider_);

        auto functionGenerators = New<TFunctionProfilerMap>();
        auto aggregateGenerators = New<TAggregateProfilerMap>();
        MergeFrom(functionGenerators.Get(), *GetBuiltinFunctionProfilers());
        MergeFrom(aggregateGenerators.Get(), *GetBuiltinAggregateProfilers());
        auto sdk = NWebAssembly::TModuleBytecode{NWebAssembly::EBytecodeFormat::Binary};

        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = options.WorkloadDescriptor,
            .ReadSessionId = options.ReadSessionId,
        };

        FetchFunctionImplementationsFromCypress(
            functionGenerators,
            aggregateGenerators,
            externalCGInfo,
            FunctionImplCache_,
            chunkReadOptions,
            &sdk,
            options.ExecutionBackend);

        auto executePlanCallback = GetExecutePlanCallback(
            externalCGInfo,
            options,
            requestFeatureFlags);

        auto subqueryResults = New<TMpscStack<TQueryStatistics>>();

        auto singletonsConfig = TSingletonManager::GetDynamicConfig();
        auto queryEngineConfig = singletonsConfig
            ? singletonsConfig->GetSingletonConfig<TQueryEngineDynamicConfig>()
            : nullptr;
        auto allowHeavyRangeInferenceInJoins = queryEngineConfig
            ? queryEngineConfig->AllowHeavyRangeInferenceInJoins.value_or(false)
            : false;

        TJoinProfilerRegistry joinProfilerRegistry;
        for (int joinIndex = 0; joinIndex < std::ssize(query->JoinClauses); ++joinIndex) {
            const auto& joinClause = query->JoinClauses[joinIndex];
            joinProfilerRegistry.InsertJoinProfilerOrThrow(joinIndex, CreateJoinSubqueryProfiler(
                joinClause,
                executePlanCallback,
                [subqueryResults] (TQueryStatistics statistics) mutable {
                    subqueryResults->Enqueue(std::move(statistics));
                },
                [] { return std::nullopt; },
                MemoryChunkProvider_,
                options.UseOrderByInJoinSubqueries,
                allowHeavyRangeInferenceInJoins,
                options.JoinCacheSize,
                Logger));
        }

        auto statistics = Evaluator_->Run(
            query,
            reader,
            writer,
            joinProfilerRegistry,
            functionGenerators,
            aggregateGenerators,
            sdk,
            MemoryChunkProvider_,
            options,
            requestFeatureFlags,
            MakeFuture(MostFreshFeatureFlags()));

        TQueryStatistics dequeuedStatistics;
        while (subqueryResults->TryDequeue(&dequeuedStatistics)) {
            statistics.AddInnerStatistics(std::move(dequeuedStatistics));
        }

        return statistics;
    }

    TQueryStatistics DoCoordinateAndExecute(
        const TConstQueryPtr& query,
        const TConstExternalCGInfoPtr& externalCGInfo,
        const TQueryOptions& options,
        const TFeatureFlags& requestFeatureFlags,
        const IUnversionedRowsetWriterPtr& writer,
        std::vector<std::pair<std::vector<TDataSource>, std::string>> groupedDataSplits,
        std::vector<TSharedRef> prefetchedJoinRowsets)
    {
        auto Logger = MakeQueryLogger(query);

        auto functionGenerators = New<TFunctionProfilerMap>();
        auto aggregateGenerators = New<TAggregateProfilerMap>();
        MergeFrom(functionGenerators.Get(), *GetBuiltinFunctionProfilers());
        MergeFrom(aggregateGenerators.Get(), *GetBuiltinAggregateProfilers());
        auto sdk = NWebAssembly::TModuleBytecode{NWebAssembly::EBytecodeFormat::Binary};

        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = options.WorkloadDescriptor,
            .ReadSessionId = options.ReadSessionId,
        };

        FetchFunctionImplementationsFromCypress(
            functionGenerators,
            aggregateGenerators,
            externalCGInfo,
            FunctionImplCache_,
            chunkReadOptions,
            &sdk,
            options.ExecutionBackend);

        auto [frontQuery, bottomQueryPattern] = GetDistributedQueryPattern(query);

        int splitCount = std::ssize(groupedDataSplits);

        // Track all subquery statistics futures so we can collect all
        // TabletServantIsNotActive errors and group them into one.
        std::vector<TFuture<void>> subqueryStatisticsFutures;

        // Keep holders so that the query is not terminated prematurely
        // and we can collect errors from all subqueries.
        auto subplanHolders = New<TSubplanFutureHolders>();

        auto evaluateBottom = [
            &,
            bottomQueryPattern = bottomQueryPattern,
            groupedDataSplits = std::move(groupedDataSplits),
            prefetchedJoinRowsets = std::move(prefetchedJoinRowsets),
            subqueryIndex = 0] () mutable -> TEvaluateResult
        {
            if (subqueryIndex >= std::ssize(groupedDataSplits)) {
                return {};
            }

            auto& [dataSources, address] = groupedDataSplits[subqueryIndex++];

            // Copy query to generate new id.
            auto bottomQuery = New<TQuery>(*bottomQueryPattern);

            YT_LOG_DEBUG("Delegating subquery (SubQueryId: %v, Address: %v, MaxSubqueries: %v, MinRowCountPerSubquery: %v)",
                bottomQuery->Id,
                address,
                options.MaxSubqueries,
                options.MinRowCountPerSubquery);

            auto result = Delegate(
                bottomQuery,
                externalCGInfo,
                options,
                requestFeatureFlags,
                std::move(dataSources),
                prefetchedJoinRowsets,
                address);

            subqueryStatisticsFutures.push_back(result.Statistics.As<void>());

            return result;
        };

        auto getEvaluateTop = [&] (TConstFrontQueryPtr frontQuery) {
            return [=, this, this_ = MakeStrong(this), frontQuery = std::move(frontQuery)] (
                const ISchemafulUnversionedReaderPtr& reader,
                TFuture<TFeatureFlags> responseFeatureFlags) -> TQueryStatistics
            {
                YT_LOG_DEBUG("Evaluating top query (TopQueryId: %v)", frontQuery->Id);
                return Evaluator_->Run(
                    frontQuery,
                    reader,
                    writer,
                    /*joinProfilerRegistry*/ {},
                    functionGenerators,
                    aggregateGenerators,
                    sdk,
                    MemoryChunkProvider_,
                    options,
                    requestFeatureFlags,
                    responseFeatureFlags);
            };
        };

        try {
            TQueryStatistics statistics;

            auto scanOrder = query->GetScanOrder(options.AllowUnorderedGroupByWithLimit);
            if (options.EnableParallelizeUnorderedGroupBy &&
                scanOrder == EScanOrder::Unordered &&
                query->GroupClause &&
                query->GroupClause->CommonPrefixWithPrimaryKey == 0 &&
                !query->UseDisjointGroupBy)
            {
                if (splitCount == 0) {
                    WaitForFast(writer->Close())
                        .ThrowOnError();
                    return {};
                }
                auto [middleQuery, newFrontQuery] = GetGroupingQueriesForParallelization(frontQuery);
                auto evaluateMiddle = [&, middleQuery] (
                    const ISchemafulUnversionedReaderPtr& reader,
                    TFuture<TFeatureFlags> responseFeatureFlags)
                {
                    YT_LOG_DEBUG("Evaluating middle query (MiddleQueryId: %v)", middleQuery->Id);

                    TEvaluateResult middleResult;
                    auto pipe = CreateSchemafulPipe(MemoryChunkProvider_);
                    middleResult.Reader = pipe->GetReader();

                    middleResult.Statistics = BIND(&IEvaluator::Run, Evaluator_)
                        .AsyncVia(GetCurrentInvoker())
                        .Run(
                            middleQuery,
                            reader,
                            pipe->GetWriter(),
                            /*joinProfilerRegistry*/ {},
                            functionGenerators,
                            aggregateGenerators,
                            sdk,
                            MemoryChunkProvider_,
                            options,
                            requestFeatureFlags,
                            std::move(responseFeatureFlags))
                        .AsUnique()
                        .Apply(BIND([pipe] (TErrorOr<TQueryStatistics>&& errorOrStatistics) {
                            if (!errorOrStatistics.IsOK()) {
                                pipe->Fail(errorOrStatistics);
                            }
                            return errorOrStatistics;
                        }));

                    return middleResult;
                };
                statistics = CoordinateAndExecuteWithShuffle(
                    splitCount,
                    query->GroupClause->GroupItems.size(),
                    evaluateBottom,
                    evaluateMiddle,
                    getEvaluateTop(newFrontQuery),
                    MemoryChunkProvider_);
            } else {
                statistics = CoordinateAndExecute(
                    scanOrder,
                    query->IsPrefetching(),
                    splitCount,
                    query->Offset,
                    query->Limit,
                    options.AdaptiveOrderedSchemafulReader,
                    evaluateBottom,
                    getEvaluateTop(frontQuery),
                    subplanHolders);
            }

            if (TQueryStatistics::IsDepthAggregate(options.StatisticsAggregation)) {
                auto aggregated = TQueryStatistics();
                aggregated.Merge(statistics);
                return aggregated;
            }

            return statistics;
        } catch (const TErrorException& ex) {
            if (!ex.Error().FindMatching(NTabletClient::EErrorCode::TabletServantIsNotActive)) {
                throw;
            }

            // Wait for all subquery futures to settle and collect all
            // TabletServantIsNotActive errors to throw as a single combined error.
            // However, try not to introduce excessive delays.
            Y_UNUSED(WaitFor(AllSet(subqueryStatisticsFutures)
                .WithTimeout(Connection_->GetConfig()->CumulativeSelectRowsFailedResponseWaitTime)));

            std::vector<TError> servantNotActiveErrors;
            for (auto& future : subqueryStatisticsFutures) {
                if (!future.IsSet()) {
                    continue;
                }

                const auto& result = future.GetOrCrash();
                if (!result.IsOK() &&
                    result.FindMatching(NTabletClient::EErrorCode::TabletServantIsNotActive))
                {
                    servantNotActiveErrors.push_back(result);
                }
            }

            if (!servantNotActiveErrors.empty()) {
                if (servantNotActiveErrors.size() == 1) {
                    servantNotActiveErrors[0].ThrowOnError();
                } else {
                    THROW_ERROR_EXCEPTION("Some select subrequests failed because tablet servants are not active")
                        << servantNotActiveErrors;
                }
            }

            throw;
        }
    }

    TEvaluateResult Delegate(
        TConstQueryPtr query,
        const TConstExternalCGInfoPtr& externalCGInfo,
        const TQueryOptions& options,
        const TFeatureFlags& requestFeatureFlags,
        std::vector<TDataSource> dataSources,
        const std::vector<TSharedRef>& prefetchedJoinRowsets,
        const std::string& address)
    {
        auto Logger = MakeQueryLogger(query);

        NTracing::TChildTraceContextGuard guard("QueryClient.Delegate");
        auto channel = NodeChannelFactory_->CreateChannel(address);
        auto config = Connection_->GetConfig();

        TQueryServiceProxy proxy(channel);

        auto req = proxy.Execute();
        req->SetRequestCodec(NCompression::ECodec::Lz4);
        req->SetMultiplexingBand(NRpc::EMultiplexingBand::Interactive);
        if (options.Deadline != TInstant::Max()) {
            req->SetTimeout(options.Deadline - Now());
        }

        {
            auto* ext = req->Header().MutableExtension(NQueryClient::NProto::TReqExecuteExt::req_execute_ext);
            YT_OPTIONAL_TO_PROTO(ext, execution_pool, options.ExecutionPool);
            ext->set_execution_tag(ToString(options.ReadSessionId));
        }

        TDuration serializationTime;
        {
            TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&serializationTime);

            ToProto(req->mutable_query(), query);
            req->mutable_query()->set_input_row_limit(options.InputRowLimit);
            req->mutable_query()->set_output_row_limit(options.OutputRowLimit);
            ToProto(req->mutable_external_functions(), externalCGInfo->Functions);
            externalCGInfo->NodeDirectory->DumpTo(req->mutable_node_directory());
            ToProto(req->mutable_sdk(), externalCGInfo->Sdk);
            ToProto(req->mutable_options(), options);

            std::vector<NTableClient::TLogicalTypePtr> schema;
            for (int index = 0; index < query->Schema.Original->GetKeyColumnCount(); ++index) {
                schema.push_back(query->Schema.Original->Columns()[index].LogicalType());
            }

            ToProto(req->mutable_data_sources(), dataSources, schema);
            req->set_response_codec(ToProto(config->SelectRowsResponseCodec));
        }

        ToProto(req->mutable_feature_flags(), requestFeatureFlags);

        req->Attachments() = prefetchedJoinRowsets;

        auto queryFingerprint = InferName(query, {.OmitValues = true});
        YT_LOG_DEBUG("Sending subquery (Fingerprint: %v, ReadSchema: %v, ResultSchema: %v, SerializationTime: %v, "
            "RequestSize: %v)",
            queryFingerprint,
            *query->GetReadSchema(),
            *query->GetTableSchema(),
            serializationTime,
            req->ByteSize());

        auto resultReader = New<TQueryResponseReader>(
            query->Id,
            query->GetTableSchema(),
            config->SelectRowsResponseCodec,
            MemoryChunkProvider_,
            MemoryUsageTracker_,
            Logger);

        resultReader->Initialize(req->Invoke());

        return {resultReader, resultReader->GetQueryResult(), resultReader->GetResponseFeatureFlags()};
    }

    TExecutePlan GetExecutePlanCallback(
        const TConstExternalCGInfoPtr& externalCGInfo,
        const TQueryOptions& options,
        const TFeatureFlags& requestFeatureFlags)
    {
        return [=, this_ = MakeStrong(this), invoker = GetCurrentInvoker()] (
            TPlanFragment planFragment,
            IUnversionedRowsetWriterPtr writer
        ) {
            return BIND(
                &IExecutor::Execute,
                this_,
                planFragment,
                externalCGInfo,
                writer,
                options,
                requestFeatureFlags)
                .AsyncVia(invoker)
                .Run();
        };
    }

    std::tuple<TConstQueryPtr, std::vector<TSharedRef>, std::vector<TQueryStatistics>> MaybePrefetchJoinRowsets(
        TConstQueryPtr query,
        const TConstExpressionPtr& initialPredicate,
        const TConstExternalCGInfoPtr& externalCGInfo,
        const TQueryOptions& options,
        const TFeatureFlags& requestFeatureFlags,
        const NLogging::TLogger& Logger)
    {
        // TODO(sabdenovch): Add this to explain-query.
        std::vector<TSharedRef> prefetchedRowsets;
        std::vector<TQueryStatistics> subqueryStatistics;

        if (!options.PrefetchJoinTables) {
            return {std::move(query), std::move(prefetchedRowsets), std::move(subqueryStatistics)};
        }

        auto prefetchOptions = options;
        prefetchOptions.MergeVersionedRows = true;
        prefetchOptions.InputRowLimit = 300'000;
        prefetchOptions.OutputRowLimit = 100'000;

        std::vector<IWireProtocolRowsetWriterPtr> blockWriters;
        std::vector<TFuture<TQueryStatistics>> futures;
        std::vector<bool> prefetched(query->JoinClauses.size(), false);

        for (int joinIndex = 0; joinIndex < std::ssize(query->JoinClauses); ++joinIndex) {
            const auto& joinClause = query->JoinClauses[joinIndex];
            if (!joinClause->ArrayExpressions.empty()) {
                continue;
            }

            auto mainPredicate = initialPredicate;
            auto joinPredicate = joinClause->Predicate;

            for (int index = 0; index < static_cast<int>(joinClause->ForeignKeyPrefix); ++index) {
                const auto& selfJoinKey = joinClause->SelfEquations[index];
                const auto* foreignReference = joinClause->ForeignEquations[index]
                    ->As<TReferenceExpression>();
                if (const auto* reference = selfJoinKey->As<TReferenceExpression>(); reference && mainPredicate) {
                    mainPredicate = TReferenceReplacer(reference->ColumnName, foreignReference)
                        .Visit(mainPredicate);
                } else if (const auto* literal = selfJoinKey->As<TLiteralExpression>()) {
                    joinPredicate = MakeAndExpression(
                        std::move(joinPredicate),
                        New<TBinaryOpExpression>(
                            EValueType::Boolean,
                            EBinaryOp::Equal,
                            literal,
                            foreignReference));
                }
            }

            auto totalPredicate = MakeAndExpression(
                std::move(mainPredicate),
                std::move(joinPredicate));

            YT_LOG_DEBUG("Inferring ranges to consider prefetching joined tables (JoinIndex: %v, Predicate: %v, TableId: %v)",
                joinIndex,
                InferName(totalPredicate),
                joinClause->ForeignObjectId);

            auto ranges = CreateNewRangeInferrer(
                std::move(totalPredicate),
                joinClause->Schema.Original,
                joinClause->GetKeyColumns(),
                Connection_->GetColumnEvaluatorCache(),
                GetBuiltinConstraintExtractors(),
                options,
                MemoryChunkProvider_,
                false);

            if (options.VerboseLogging) {
                YT_LOG_DEBUG("Ranges are inferred (Ranges: %v)", ranges);
            } else {
                YT_LOG_DEBUG("Ranges are inferred (RangeCount: %v)", ranges.size());
            }

            ui32 keyColumnCount = joinClause->Schema.Original->GetKeyColumnCount();

            bool allRangesAreKeys = std::all_of(
                ranges.begin(),
                ranges.end(),
                [&] (const TRowRange& rowRange) {
                    return rowRange.first.GetCount() == keyColumnCount &&
                        rowRange.second.GetCount() == keyColumnCount + 1 &&
                        rowRange.second[keyColumnCount].Type == EValueType::Max &&
                        CompareValueRanges(
                            rowRange.first.Elements(),
                            rowRange.second.FirstNElements(keyColumnCount)) == 0;
                });

            // TODO(sabdenovch): Implement proper read_data_weight prediction
            // algorithm based on inferred ranges.
            auto checkSmallTableAndStrictEnoughRanges = [&] {
                int firstNonEvaluated = 0;
                for (; firstNonEvaluated < static_cast<int>(keyColumnCount); ++firstNonEvaluated) {
                    if (!joinClause->Schema.Original->Columns()[firstNonEvaluated].Expression()) {
                        break;
                    }
                }

                bool rangesAreStrict = std::all_of(
                    ranges.begin(),
                    ranges.end(),
                    [&] (const TRowRange& rowRange) {
                        return static_cast<int>(rowRange.first.GetCount()) > firstNonEvaluated &&
                            static_cast<int>(rowRange.second.GetCount()) > firstNonEvaluated &&
                            CompareValueRanges(
                                rowRange.first.FirstNElements(firstNonEvaluated + 1),
                                rowRange.second.FirstNElements(firstNonEvaluated + 1)) == 0;
                    });

                if (!rangesAreStrict) {
                    YT_LOG_DEBUG("Will not prefetch from table due to loose constraints");
                    return false;
                }

                auto mountInfo = WaitFor(Connection_
                    ->GetTableMountCache()
                    ->GetTableInfo(FromObjectId(joinClause->ForeignObjectId)))
                    .ValueOrThrow();

                if (mountInfo->Tablets.size() > 1) {
                    YT_LOG_DEBUG("Will not prefetch from large table (TabletCount: %v)", mountInfo->Tablets.size());
                    return false;
                }

                return true;
            };

            if (allRangesAreKeys || checkSmallTableAndStrictEnoughRanges()) {
                YT_LOG_DEBUG("Prefetching from joined table (TableId: %v)",
                    joinClause->ForeignObjectId);
                prefetched[joinIndex] = true;
            } else {
                continue;
            }

            auto foreignQuery = joinClause->GetJoinSubquery();
            foreignQuery->InferRanges = false;
            if (joinClause->ForeignKeyPrefix > 0) {
                foreignQuery->Limit = OrderedReadWithPrefetchHint;
            }

            // TODO(sabdenovch): Use SplitPredicateByColumnSubset to filter rows from joined table even further.
            // Could be useful if some restrictions are applied from which ranges can't be inferred.

            auto writer = CreateWireProtocolRowsetWriter(
                NCompression::ECodec::Lz4,
                16_MB,
                foreignQuery->GetTableSchema(),
                false,
                Logger);

            auto dataSource = TDataSource{
                .ObjectId = joinClause->ForeignObjectId,
                .CellId = joinClause->ForeignCellId,
                .Ranges = std::move(ranges),
            };

            futures.push_back(BIND(
                &IExecutor::Execute,
                MakeStrong(this),
                TPlanFragment{
                    .Query = std::move(foreignQuery),
                    .DataSource = std::move(dataSource),
                },
                externalCGInfo,
                writer,
                prefetchOptions,
                requestFeatureFlags)
                .AsyncVia(GetCurrentInvoker())
                .Run());

            blockWriters.push_back(std::move(writer));
        }

        if (blockWriters.empty()) {
            return {std::move(query), std::move(prefetchedRowsets), std::move(subqueryStatistics)};
        }

        WaitFor(AllSet(futures))
            .ThrowOnError();

        auto mutableQuery = New<TQuery>(*query);

        int writerIndex = 0;
        for (int joinIndex = 0; joinIndex < std::ssize(query->JoinClauses); ++joinIndex) {
            if (!prefetched[joinIndex]) {
                continue;
            }

            const auto& joinClause = query->JoinClauses[joinIndex];
            const auto& blockWriter = blockWriters[writerIndex];
            auto statistics = WaitForFast(futures[writerIndex++])
                .ValueOrThrow();

            if (statistics.IncompleteInput || statistics.IncompleteOutput) {
                YT_LOG_DEBUG("Join table prefetch failed, fallback (JoinIndex: %v, TableId: %v)",
                    joinIndex,
                    joinClause->ForeignObjectId);
                continue;
            }
            int firstBlock = prefetchedRowsets.size();
            auto blocks = blockWriter->GetCompressedBlocks();
            for (auto& block : blocks) {
                prefetchedRowsets.push_back(std::move(block));
            }
            int lastBlock = prefetchedRowsets.size() - 1;

            auto newJoinClause = New<TJoinClause>(*joinClause);
            newJoinClause->PrefetchedBlockRange = {firstBlock, lastBlock};

            mutableQuery->JoinClauses[joinIndex] = newJoinClause;

            subqueryStatistics.push_back(statistics);
        }

        return {std::move(mutableQuery), std::move(prefetchedRowsets), std::move(subqueryStatistics)};
    }

    std::pair<TConstFrontQueryPtr, TConstFrontQueryPtr> GetGroupingQueriesForParallelization(
        const TConstFrontQueryPtr& frontQuery)
    {
        auto middleQuery = New<TFrontQuery>();
        middleQuery->GroupClause = frontQuery->GroupClause;
        middleQuery->HavingClause = frontQuery->HavingClause;
        middleQuery->Schema = frontQuery->Schema;
        middleQuery->OrderClause = frontQuery->OrderClause;
        middleQuery->Limit = frontQuery->Limit;
        middleQuery->Offset = frontQuery->Offset;
        middleQuery->IsFinal = false;
        middleQuery->HasExclusiveGroupKeyView = true;
        // Why does this field even exist for front query. This code makes no gosh darn sense.
        middleQuery->InferRanges = false;

        auto newFrontQuery = New<TFrontQuery>(*frontQuery);
        newFrontQuery->EnableCombineGroupOpWithOrderOp = false;

        return {std::move(middleQuery), std::move(newFrontQuery)};
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryExecutor)

IExecutorPtr CreateQueryExecutor(
    IMemoryChunkProviderPtr memoryChunkProvider,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    NNative::IConnectionPtr connection,
    IColumnEvaluatorCachePtr columnEvaluatorCache,
    IEvaluatorPtr evaluator,
    INodeChannelFactoryPtr nodeChannelFactory,
    TFunctionImplCachePtr functionImplCache,
    bool retryOnMetadataCacheInconsistency)
{
    return New<TQueryExecutor>(
        std::move(memoryChunkProvider),
        std::move(memoryUsageTracker),
        std::move(connection),
        std::move(columnEvaluatorCache),
        std::move(evaluator),
        std::move(nodeChannelFactory),
        std::move(functionImplCache),
        retryOnMetadataCacheInconsistency);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
