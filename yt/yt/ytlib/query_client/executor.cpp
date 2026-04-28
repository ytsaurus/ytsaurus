#include "executor.h"
#include "functions_cache.h"
#include "query_service_proxy.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/tablet_helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/library/query/base/helpers.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>
#include <yt/yt/library/query/base/coordination_helpers.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/coordinator.h>
#include <yt/yt/library/query/engine_api/evaluator.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

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
        TFunctionImplCachePtr functionImplCache)
        : MemoryChunkProvider_(std::move(memoryChunkProvider))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
        , Connection_(std::move(connection))
        , ColumnEvaluatorCache_(std::move(columnEvaluatorCache))
        , Evaluator_(std::move(evaluator))
        , NodeChannelFactory_(std::move(nodeChannelFactory))
        , FunctionImplCache_(std::move(functionImplCache))
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

    TQueryStatistics Execute(
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

        std::vector<std::pair<std::vector<TDataSource>, std::string>> groupedDataSplits;

        if (coordinatedQuery->IsOrdered(options.AllowUnorderedGroupByWithLimit)) {
            // Splits are ordered by tablet bounds.
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

        return DoCoordinateAndExecute(
            coordinatedQuery,
            externalCGInfo,
            options,
            requestFeatureFlags,
            writer,
            std::move(groupedDataSplits));
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

        std::vector<IJoinProfilerPtr> joinProfilers;
        for (int joinIndex = 0; joinIndex < std::ssize(query->JoinClauses); ++joinIndex) {
            joinProfilers.push_back(CreateJoinSubqueryProfiler(
                query->JoinClauses[joinIndex],
                executePlanCallback,
                [subqueryResults] (TQueryStatistics statistics) mutable {
                    subqueryResults->Enqueue(std::move(statistics));
                },
                [] { return std::nullopt; },
                MemoryChunkProvider_,
                options.UseOrderByInJoinSubqueries,
                Logger));
        }

        auto statistics = Evaluator_->Run(
            query,
            reader,
            writer,
            std::move(joinProfilers),
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
        std::vector<std::pair<std::vector<TDataSource>, std::string>> groupedDataSplits)
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

        auto statistics = CoordinateAndExecute(
            query->IsOrdered(options.AllowUnorderedGroupByWithLimit),
            query->IsPrefetching(),
            splitCount,
            query->Offset,
            query->Limit,
            options.AdaptiveOrderedSchemafulReader,
            [
                &,
                bottomQueryPattern = bottomQueryPattern,
                groupedDataSplits = std::move(groupedDataSplits),
                subqueryIndex = 0
            ] () mutable -> TEvaluateResult {
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

                return Delegate(
                    bottomQuery,
                    externalCGInfo,
                    options,
                    requestFeatureFlags,
                    std::move(dataSources),
                    address);
            },
            [&, frontQuery = frontQuery] (
                const ISchemafulUnversionedReaderPtr& reader,
                TFuture<TFeatureFlags> responseFeatureFlags
            ) -> TQueryStatistics {
                YT_LOG_DEBUG("Evaluating top query (TopQueryId: %v)", frontQuery->Id);
                return Evaluator_->Run(
                    std::move(frontQuery),
                    std::move(reader),
                    writer,
                    /*joinProfilers*/ {},
                    functionGenerators,
                    aggregateGenerators,
                    sdk,
                    MemoryChunkProvider_,
                    options,
                    requestFeatureFlags,
                    responseFeatureFlags);
            });

        if (TQueryStatistics::IsDepthAggregate(options.StatisticsAggregation)) {
            auto aggregated = TQueryStatistics();
            aggregated.Merge(statistics);
            return aggregated;
        }

        return statistics;
    }

    TEvaluateResult Delegate(
        TConstQueryPtr query,
        const TConstExternalCGInfoPtr& externalCGInfo,
        const TQueryOptions& options,
        const TFeatureFlags& requestFeatureFlags,
        std::vector<TDataSource> dataSources,
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
};

DEFINE_REFCOUNTED_TYPE(TQueryExecutor)

IExecutorPtr CreateQueryExecutor(
    IMemoryChunkProviderPtr memoryChunkProvider,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    NNative::IConnectionPtr connection,
    IColumnEvaluatorCachePtr columnEvaluatorCache,
    IEvaluatorPtr evaluator,
    INodeChannelFactoryPtr nodeChannelFactory,
    TFunctionImplCachePtr functionImplCache)
{
    return New<TQueryExecutor>(
        std::move(memoryChunkProvider),
        std::move(memoryUsageTracker),
        std::move(connection),
        std::move(columnEvaluatorCache),
        std::move(evaluator),
        std::move(nodeChannelFactory),
        std::move(functionImplCache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
