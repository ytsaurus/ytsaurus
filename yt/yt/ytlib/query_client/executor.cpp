#include "executor.h"
#include "functions_cache.h"
#include "query_service_proxy.h"

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>
#include <yt/yt/library/query/base/coordination_helpers.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/coordinator.h>
#include <yt/yt/library/query/engine_api/evaluator.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/tablet_helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/helpers.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NQueryClient {

using namespace NConcurrency;
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

using NHiveClient::TCellDescriptorPtr;

////////////////////////////////////////////////////////////////////////////////

using TShardIt = const TTabletInfoPtr*;
using THolder = TRefCountedPtr;

template <>
TRow GetPivotKey(const TTabletInfoPtr& shard)
{
    return shard->PivotKey;
}

std::vector<std::pair<TDataSource, TString>> DoInferRanges(
    const NHiveClient::ICellDirectoryPtr& cellDirectory,
    const NNodeTrackerClient::TNetworkPreferenceList& networks,
    const IColumnEvaluatorCachePtr& columnEvaluatorCache,
    const TTableMountInfoPtr& tableInfo,
    TConstQueryPtr query,
    const TDataSource& dataSource,
    const TQueryOptions& options,
    TRowBufferPtr rowBuffer,
    const NLogging::TLogger& Logger)
{
    auto tableId = dataSource.ObjectId;
    auto ranges = dataSource.Ranges;
    auto keys = dataSource.Keys;

    // TODO(lukyan): Infer ranges if no initial ranges or keys?
    if (!keys && query->InferRanges) {
        ranges = GetPrunedRanges(
            query,
            tableId,
            ranges,
            rowBuffer,
            columnEvaluatorCache,
            GetBuiltinRangeExtractors(),
            options);

        YT_LOG_DEBUG("Ranges are inferred (RangeCount: %v, TableId: %v)",
            ranges.Size(),
            tableId);
    }

    if (*query->Schema.Original != *tableInfo->Schemas[ETableSchemaKind::Query]) {
        THROW_ERROR_EXCEPTION(
            NTabletClient::EErrorCode::InvalidMountRevision,
            "Invalid revision for table info; schema has changed")
            << TErrorAttribute("path", tableInfo->Path)
            << TErrorAttribute("original_schema", *query->Schema.Original)
            << TErrorAttribute("query_schema", *tableInfo->Schemas[ETableSchemaKind::Query]);
    }

    tableInfo->ValidateDynamic();
    tableInfo->ValidateNotPhysicallyLog();

    THashMap<NTabletClient::TTabletCellId, TCellDescriptorPtr> tabletCellReplicas;

    auto getAddress = [&] (const TTabletInfoPtr& tabletInfo) {
        ValidateTabletMountedOrFrozen(tableInfo, tabletInfo);

        auto insertResult = tabletCellReplicas.emplace(tabletInfo->CellId, TCellDescriptorPtr());
        auto& descriptor = insertResult.first->second;

        if (insertResult.second) {
            descriptor = cellDirectory->GetDescriptorByCellIdOrThrow(tabletInfo->CellId);
        }

        // TODO(babenko): pass proper read options
        const auto& peerDescriptor = GetPrimaryTabletPeerDescriptor(*descriptor);
        return peerDescriptor.GetAddressOrThrow(networks);
    };

    std::vector<std::pair<TDataSource, TString>> subsources;
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

    YT_LOG_DEBUG("Splitting %v (RangeCount: %v, TabletsCount: %v, LowerCapBound: %v, UpperCapBound: %v)",
        ranges ? "ranges" : "keys",
        ranges ? ranges.size() : keys.size(),
        tableInfo->Tablets.size(),
        tableInfo->LowerCapBound,
        tableInfo->UpperCapBound);

    if (options.VerboseLogging) {
        YT_LOG_DEBUG("Splitting %v tablet pivot keys (Pivots: %v)",
            ranges ? "ranges" : "keys",
            MakeFormattableView(tableInfo->Tablets, [] (auto* builder, const auto& tablet) {
                builder->AppendFormat("%v", tablet->PivotKey.Get());
            }));
    }

    if (ranges) {
        YT_VERIFY(!keys);

        SplitRangesByTablets(
            ranges,
            MakeRange(tableInfo->Tablets),
            tableInfo->LowerCapBound,
            tableInfo->UpperCapBound,
            [&] (auto shardIt, auto rangesIt, auto rangesItEnd) {
                if (tableInfo->IsOrdered()) {
                    // Crop ranges for ordered table.
                    auto nextPivotKey = shardIt != MakeRange(tableInfo->Tablets).end()
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
                        MakeRange(rangesIt, rangesItEnd),
                        ranges.GetHolder());
                }
            });
    } else if (keys) {
        YT_VERIFY(tableInfo->IsSorted());
        YT_VERIFY(!ranges);
        size_t keyWidth = keys.Front().GetCount();

        auto fullKeySize = tableInfo->Schemas[ETableSchemaKind::Query]->GetKeyColumnCount();

        SplitKeysByTablets(
            keys,
            keyWidth,
            fullKeySize,
            MakeRange(tableInfo->Tablets),
            tableInfo->LowerCapBound,
            tableInfo->UpperCapBound,
            [&] (auto shardIt, auto keysIt, auto keysItEnd) {
                makeSubsource(shardIt)->Keys = MakeSharedRange(
                    MakeRange(keysIt, keysItEnd),
                    keys.GetHolder());
            });
    }

    return subsources;
}

std::vector<std::pair<TDataSource, TString>> InferRanges(
    NApi::NNative::IConnectionPtr connection,
    TConstQueryPtr query,
    const TDataSource& dataSource,
    const TQueryOptions& options,
    TRowBufferPtr rowBuffer,
    const NLogging::TLogger& Logger)
{
    auto tableInfo = WaitFor(connection->GetTableMountCache()->GetTableInfo(FromObjectId(dataSource.ObjectId)))
        .ValueOrThrow();

    return DoInferRanges(
        connection->GetCellDirectory(),
        connection->GetNetworks(),
        connection->GetColumnEvaluatorCache(),
        tableInfo,
        query,
        dataSource,
        options,
        rowBuffer,
        Logger);
}

////////////////////////////////////////////////////////////////////////////////

class TQueryResponseReader
    : public ISchemafulUnversionedReader
{
public:
    TQueryResponseReader(
        TFuture<TQueryServiceProxy::TRspExecutePtr> asyncResponse,
        TTableSchemaPtr schema,
        NCompression::ECodec codecId,
        NLogging::TLogger logger)
        : Schema_(std::move(schema))
        , CodecId_(codecId)
        , Logger(std::move(logger))
    {
        // NB: Don't move this assignment to initializer list as
        // OnResponse will access "this", which is not fully constructed yet.
        QueryResult_ = asyncResponse.Apply(BIND(
            &TQueryResponseReader::OnResponse,
            MakeStrong(this)));
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
    const TTableSchemaPtr Schema_;
    const NCompression::ECodec CodecId_;
    const NLogging::TLogger Logger;

    TFuture<TQueryStatistics> QueryResult_;
    TAtomicIntrusivePtr<ISchemafulUnversionedReader> RowsetReader_;

    TQueryStatistics OnResponse(const TQueryServiceProxy::TRspExecutePtr& response)
    {
        RowsetReader_.Store(CreateWireProtocolRowsetReader(
            response->Attachments(),
            CodecId_,
            Schema_,
            false,
            Logger));
        return FromProto<TQueryStatistics>(response->query_statistics());
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
        NNative::IConnectionPtr connection,
        IInvokerPtr invoker,
        IColumnEvaluatorCachePtr columnEvaluatorCache,
        IEvaluatorPtr evaluator,
        INodeChannelFactoryPtr nodeChannelFactory,
        TFunctionImplCachePtr functionImplCache)
        : MemoryChunkProvider_(std::move(memoryChunkProvider))
        , Connection_(std::move(connection))
        , Invoker_(std::move(invoker))
        , ColumnEvaluatorCache_(std::move(columnEvaluatorCache))
        , Evaluator_(std::move(evaluator))
        , NodeChannelFactory_(std::move(nodeChannelFactory))
        , FunctionImplCache_(std::move(functionImplCache))
    { }

    TFuture<TQueryStatistics> Execute(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        TDataSource dataSource,
        IUnversionedRowsetWriterPtr writer,
        const TQueryOptions& options) override
    {
        NTracing::TChildTraceContextGuard guard("QueryClient.Execute");
        auto execute = query->IsOrdered()
            ? &TQueryExecutor::DoExecuteOrdered
            : &TQueryExecutor::DoExecute;

        return BIND(execute, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run(
                std::move(query),
                std::move(externalCGInfo),
                std::move(dataSource),
                options,
                std::move(writer));
    }

private:
    const IMemoryChunkProviderPtr MemoryChunkProvider_;
    const NNative::IConnectionPtr Connection_;
    const IInvokerPtr Invoker_;
    const IColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    const IEvaluatorPtr Evaluator_;
    const INodeChannelFactoryPtr NodeChannelFactory_;
    const TFunctionImplCachePtr FunctionImplCache_;

    TQueryStatistics DoCoordinateAndExecute(
        const TConstQueryPtr& query,
        const TConstExternalCGInfoPtr& externalCGInfo,
        const TQueryOptions& options,
        const IUnversionedRowsetWriterPtr& writer,
        bool sortedDataSource,
        int subrangesCount,
        std::function<std::pair<std::vector<TDataSource>, TString>(int)> getSubsources)
    {
        auto Logger = MakeQueryLogger(query);

        std::vector<TRefiner> refiners(subrangesCount, [] (
            TConstExpressionPtr expr,
            const TKeyColumns& /*keyColumns*/) {
                return expr;
            });

        auto functionGenerators = New<TFunctionProfilerMap>();
        auto aggregateGenerators = New<TAggregateProfilerMap>();
        MergeFrom(functionGenerators.Get(), *GetBuiltinFunctionProfilers());
        MergeFrom(aggregateGenerators.Get(), *GetBuiltinAggregateProfilers());

        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = options.WorkloadDescriptor,
            .ReadSessionId = options.ReadSessionId
        };

        FetchFunctionImplementationsFromCypress(
            functionGenerators,
            aggregateGenerators,
            externalCGInfo,
            FunctionImplCache_,
            chunkReadOptions);

        return CoordinateAndExecute(
            query,
            writer,
            refiners,
            [&] (const TConstQueryPtr& subquery, int index) {
                std::vector<TDataSource> dataSources;
                TString address;
                std::tie(dataSources, address) = getSubsources(index);

                YT_LOG_DEBUG("Delegating subquery (SubQueryId: %v, Address: %v, MaxSubqueries: %v)",
                    subquery->Id,
                    address,
                    options.MaxSubqueries);

                return Delegate(
                    std::move(subquery),
                    externalCGInfo,
                    options,
                    std::move(dataSources),
                    sortedDataSource,
                    address);
            },
            [&] (const TConstFrontQueryPtr& topQuery, const ISchemafulUnversionedReaderPtr& reader, const IUnversionedRowsetWriterPtr& writer) {
                YT_LOG_DEBUG("Evaluating top query (TopQueryId: %v)", topQuery->Id);
                return Evaluator_->Run(
                    std::move(topQuery),
                    std::move(reader),
                    std::move(writer),
                    nullptr,
                    functionGenerators,
                    aggregateGenerators,
                    MemoryChunkProvider_,
                    options);
            });
    }

    TQueryStatistics DoExecute(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        TDataSource dataSource,
        const TQueryOptions& options,
        IUnversionedRowsetWriterPtr writer)
    {
        auto Logger = MakeQueryLogger(query);

        auto rowBuffer = New<TRowBuffer>(
            TQueryExecutorRowBufferTag{},
            MemoryChunkProvider_);

        auto tableInfo = WaitFor(Connection_->GetTableMountCache()->GetTableInfo(FromObjectId(dataSource.ObjectId)))
            .ValueOrThrow();

        const auto& cellDirectory = Connection_->GetCellDirectory();
        const auto& networks = Connection_->GetNetworks();

        auto allSplits = DoInferRanges(
            cellDirectory,
            networks,
            Connection_->GetColumnEvaluatorCache(),
            tableInfo,
            query,
            dataSource,
            options,
            rowBuffer,
            Logger);

        if (!query->IsOrdered()) {
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
        }

        YT_LOG_DEBUG("Regrouping %v splits into groups",
            allSplits.size());

        THashMap<TString, std::vector<TDataSource>> groupsByAddress;
        for (const auto& split : allSplits) {
            const auto& address = split.second;
            groupsByAddress[address].push_back(split.first);
        }

        std::vector<std::pair<std::vector<TDataSource>, TString>> groupedSplits;
        for (const auto& group : groupsByAddress) {
            groupedSplits.emplace_back(group.second, group.first);
        }

        YT_LOG_DEBUG("Regrouped %v splits into %v groups",
            allSplits.size(),
            groupsByAddress.size());

        return DoCoordinateAndExecute(
            query,
            externalCGInfo,
            options,
            writer,
            tableInfo->IsSorted(),
            groupedSplits.size(),
            [&] (int index) {
                return groupedSplits[index];
            });
    }

    TQueryStatistics DoExecuteOrdered(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        TDataSource dataSource,
        const TQueryOptions& options,
        IUnversionedRowsetWriterPtr writer)
    {
        auto Logger = MakeQueryLogger(query);

        auto rowBuffer = New<TRowBuffer>(
            TQueryExecutorRowBufferTag(),
            MemoryChunkProvider_);

        auto tableInfo = WaitFor(Connection_->GetTableMountCache()->GetTableInfo(FromObjectId(dataSource.ObjectId)))
            .ValueOrThrow();

        const auto& cellDirectory = Connection_->GetCellDirectory();
        const auto& networks = Connection_->GetNetworks();

        auto allSplits = DoInferRanges(
            cellDirectory,
            networks,
            Connection_->GetColumnEvaluatorCache(),
            tableInfo,
            query,
            dataSource,
            options,
            rowBuffer,
            Logger);

        // Splits are ordered by tablet bounds.
        YT_LOG_DEBUG("Got splits (SplitCount: %v)", allSplits.size());

        return DoCoordinateAndExecute(
            query,
            externalCGInfo,
            options,
            writer,
            tableInfo->IsSorted(),
            allSplits.size(),
            [&] (int index) {
                const auto& split = allSplits[index];
                const auto& [dataSource, address] = split;

                YT_LOG_DEBUG("Delegating request to tablet (TabletId: %v, CellId: %v, Address: %v)",
                    dataSource.ObjectId,
                    dataSource.CellId,
                    address);

                return std::pair(std::vector<TDataSource>{dataSource}, address);
            });
    }

    std::pair<ISchemafulUnversionedReaderPtr, TFuture<TQueryStatistics>> Delegate(
        TConstQueryPtr query,
        const TConstExternalCGInfoPtr& externalCGInfo,
        const TQueryOptions& options,
        std::vector<TDataSource> dataSources,
        bool sortedDataSource,
        const TString& address)
    {
        auto Logger = MakeQueryLogger(query);

        NTracing::TChildTraceContextGuard guard("QueryClient.Delegate");
        auto channel = NodeChannelFactory_->CreateChannel(address);
        auto config = Connection_->GetConfig();

        TQueryServiceProxy proxy(channel);

        auto req = proxy.Execute();
        // TODO(babenko): set proper band
        if (options.Deadline != TInstant::Max()) {
            req->SetTimeout(options.Deadline - Now());
        }

        {
            auto* ext = req->Header().MutableExtension(NQueryClient::NProto::TReqExecuteExt::req_execute_ext);
            if (options.ExecutionPool) {
                ext->set_execution_pool(*options.ExecutionPool);
            }
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
            ToProto(req->mutable_options(), options);

            std::vector<NTableClient::TLogicalTypePtr> schema;
            for (int index = 0; index < query->Schema.Original->GetKeyColumnCount(); ++index) {
                schema.push_back(query->Schema.Original->Columns()[index].LogicalType());
            }

            auto lookupSupported = sortedDataSource;
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

            ToProto(req->mutable_data_sources(), dataSources, schema, lookupSupported, minKeyWidth);
            req->set_response_codec(static_cast<int>(config->SelectRowsResponseCodec));
        }

        auto queryFingerprint = InferName(query, {.OmitValues = true});
        YT_LOG_DEBUG("Sending subquery (Fingerprint: %v, ReadSchema: %v, ResultSchema: %v, SerializationTime: %v, "
            "RequestSize: %v)",
            queryFingerprint,
            *query->GetReadSchema(),
            *query->GetTableSchema(),
            serializationTime,
            req->ByteSize());

        // TODO(prime): put these into the trace log
        // TRACE_ANNOTATION("serialization_time", serializationTime);
        // TRACE_ANNOTATION("request_size", req->ByteSize());

        auto resultReader = New<TQueryResponseReader>(
            req->Invoke(),
            query->GetTableSchema(),
            config->SelectRowsResponseCodec,
            Logger);
        return std::pair(resultReader, resultReader->GetQueryResult());
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryExecutor)

IExecutorPtr CreateQueryExecutor(
    IMemoryChunkProviderPtr memoryChunkProvider,
    NNative::IConnectionPtr connection,
    IInvokerPtr invoker,
    IColumnEvaluatorCachePtr columnEvaluatorCache,
    IEvaluatorPtr evaluator,
    INodeChannelFactoryPtr nodeChannelFactory,
    TFunctionImplCachePtr functionImplCache)
{
    return New<TQueryExecutor>(
        std::move(memoryChunkProvider),
        std::move(connection),
        std::move(invoker),
        std::move(columnEvaluatorCache),
        std::move(evaluator),
        std::move(nodeChannelFactory),
        std::move(functionImplCache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
