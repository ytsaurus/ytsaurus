#include "executor.h"
#include "column_evaluator.h"
#include "coordinator.h"
#include "evaluator.h"
#include "helpers.h"
#include "query.h"
#include "query_helpers.h"
#include "query_service_proxy.h"
#include "functions_cache.h"
#include "coordination_helpers.h"

#include <yt/client/query_client/query_statistics.h>

#include <yt/ytlib/api/native/config.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/tablet_helpers.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/tablet_client/table_mount_cache.h>

#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/wire_protocol.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/algorithm_helpers.h>

#include <yt/core/rpc/helpers.h>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NChunkClient;
using namespace NApi;
using namespace NApi::NNative;
using namespace NProfiling;

using NYT::ToProto;

using NChunkClient::NProto::TDataStatistics;

using NNodeTrackerClient::INodeChannelFactoryPtr;

using NObjectClient::TObjectId;
using NObjectClient::FromObjectId;

using NHiveClient::TCellDescriptor;

////////////////////////////////////////////////////////////////////////////////

using TShardIt = const TTabletInfoPtr*;
using THolder = TIntrusivePtr<TIntrinsicRefCounted>;

template <>
TRow GetPivotKey(const TTabletInfoPtr& shard)
{
    return shard->PivotKey;
}

////////////////////////////////////////////////////////////////////////////////

class TQueryResponseReader
    : public ISchemafulReader
{
public:
    TQueryResponseReader(
        TFuture<TQueryServiceProxy::TRspExecutePtr> asyncResponse,
        const TTableSchema& schema,
        NCompression::ECodec codecId,
        const NLogging::TLogger& logger)
        : Schema_(schema)
        , CodecId_(codecId)
        , Logger(logger)
    {
        // NB: Don't move this assignment to initializer list as
        // OnResponse will access "this", which is not fully constructed yet.
        QueryResult_ = asyncResponse.Apply(BIND(
            &TQueryResponseReader::OnResponse,
            MakeStrong(this)));
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        auto reader = GetRowsetReader();
        return !reader || reader->Read(rows);
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        auto reader = GetRowsetReader();
        return reader
            ? reader->GetReadyEvent()
            : QueryResult_.As<void>();
    }

    TFuture<TQueryStatistics> GetQueryResult() const
    {
        return QueryResult_;
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return TDataStatistics();
    }

    virtual NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return NChunkClient::TCodecStatistics();
    }

private:
    const TTableSchema Schema_;
    const NCompression::ECodec CodecId_;
    const NLogging::TLogger Logger;

    TFuture<TQueryStatistics> QueryResult_;
    ISchemafulReaderPtr RowsetReader_;
    TSpinLock SpinLock_;

    ISchemafulReaderPtr GetRowsetReader()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        return RowsetReader_;
    }

    TQueryStatistics OnResponse(const TQueryServiceProxy::TRspExecutePtr& response)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        YT_VERIFY(!RowsetReader_);
        RowsetReader_ = CreateWireProtocolRowsetReader(
            response->Attachments(),
            CodecId_,
            Schema_,
            false,
            Logger);
        TQueryStatistics statistics;
        FromProto(&statistics, response->query_statistics());
        return statistics;
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryResponseReader)

////////////////////////////////////////////////////////////////////////////////

struct TQueryExecutorRowBufferTag
{ };

DECLARE_REFCOUNTED_CLASS(TQueryExecutor)

class TQueryExecutor
    : public IExecutor
{
public:
    TQueryExecutor(
        NNative::IConnectionPtr connection,
        IInvokerPtr invoker,
        TColumnEvaluatorCachePtr columnEvaluatorCache,
        TEvaluatorPtr evaluator,
        INodeChannelFactoryPtr nodeChannelFactory,
        TFunctionImplCachePtr functionImplCache)
        : Connection_(std::move(connection))
        , Invoker_(std::move(invoker))
        , ColumnEvaluatorCache(std::move(columnEvaluatorCache))
        , Evaluator_(std::move(evaluator))
        , NodeChannelFactory_(std::move(nodeChannelFactory))
        , FunctionImplCache_(std::move(functionImplCache))
    { }

    virtual TFuture<TQueryStatistics> Execute(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        TDataRanges dataSource,
        IUnversionedRowsetWriterPtr writer,
        const TClientBlockReadOptions& blockReadOptions,
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
                blockReadOptions,
                std::move(writer));
    }

private:
    const NNative::IConnectionPtr Connection_;
    const IInvokerPtr Invoker_;
    const TColumnEvaluatorCachePtr ColumnEvaluatorCache;
    const TEvaluatorPtr Evaluator_;
    const INodeChannelFactoryPtr NodeChannelFactory_;
    const TFunctionImplCachePtr FunctionImplCache_;

    std::vector<std::pair<TDataRanges, TString>> InferRanges(
        TConstQueryPtr query,
        const TDataRanges& dataSource,
        const TQueryOptions& options,
        TRowBufferPtr rowBuffer,
        const NLogging::TLogger& Logger)
    {
        auto tableId = dataSource.Id;

        auto tableMountCache = Connection_->GetTableMountCache();
        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(FromObjectId(tableId)))
            .ValueOrThrow();

        if (query->Schema.Original != tableInfo->Schemas[ETableSchemaKind::Query]) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::InvalidMountRevision,
                "Invalid revision for table info; schema changed")
                << TErrorAttribute("path", tableInfo->Path);
        }

        tableInfo->ValidateDynamic();
        tableInfo->ValidateNotReplicated();

        THashMap<NTabletClient::TTabletCellId, TCellDescriptor> tabletCellReplicas;

        auto getAddress = [&] (const TTabletInfoPtr& tabletInfo) -> TString
        {
            const auto& cellDirectory = Connection_->GetCellDirectory();
            const auto& networks = Connection_->GetNetworks();

            ValidateTabletMountedOrFrozen(tabletInfo);

            auto insertResult = tabletCellReplicas.insert(std::make_pair(tabletInfo->CellId, TCellDescriptor()));
            auto& descriptor = insertResult.first->second;

            if (insertResult.second) {
                descriptor = cellDirectory->GetDescriptorOrThrow(tabletInfo->CellId);
            }

            // TODO(babenko): pass proper read options
            const auto& peerDescriptor = GetPrimaryTabletPeerDescriptor(descriptor);
            return peerDescriptor.GetAddressOrThrow(networks);
        };

        std::vector<std::pair<TDataRanges, TString>> subsources;
        auto getSubsource = [&] (TShardIt it, size_t keyWidth) -> TDataRanges*
        {
            // `.Slice(1, tablets.size())`, `*(it - 1)` and `(*shardIt)->PivotKey` are related.
            // If there would be (*shardIt)->NextPivotKey then no .Slice(1, tablets.size()) and *(it - 1) are needed.

            YT_VERIFY(it != tableInfo->Tablets.begin()); // But can be equal to end.

            const TTabletInfoPtr& tabletInfo = *(it - 1);
            TDataRanges dataSubsource;

            dataSubsource.Schema = dataSource.Schema;
            dataSubsource.LookupSupported = tableInfo->IsSorted();

            dataSubsource.Id = tabletInfo->TabletId;
            dataSubsource.MountRevision = tabletInfo->MountRevision;
            dataSubsource.KeyWidth = keyWidth;

            const auto& address = getAddress(tabletInfo);

            return &subsources.emplace_back(std::move(dataSubsource), address).first;
        };

        auto ranges = dataSource.Ranges;
        auto keys = dataSource.Keys;

        if (ranges) {
            auto prunedRanges = GetPrunedRanges(
                query,
                tableId,
                ranges,
                rowBuffer,
                ColumnEvaluatorCache,
                BuiltinRangeExtractorMap,
                options);

            YT_LOG_DEBUG("Ranges are refined (PrunedRangeCount: %v, OriginalRangeCount: %v, TableId: %v)",
                prunedRanges.size(),
                ranges.Size(),
                tableId);

            ranges = MakeSharedRange(std::move(prunedRanges), rowBuffer);
        }

        YT_LOG_DEBUG("Splitting %v (RangeCount: %v, TabletsCount: %v, LowerCapBound: %v, UpperCapBound: %v, Pivots: %v)",
            ranges ? "ranges" : "keys",
            ranges ? ranges.size() : keys.size(),
            tableInfo->Tablets.size(),
            tableInfo->LowerCapBound,
            tableInfo->UpperCapBound,
            MakeFormattableView(tableInfo->Tablets, [] (auto* builder, const auto& tablet) {
                builder->AppendFormat("%v", tablet->PivotKey.Get());
            }));

        if (dataSource.Ranges) {
            YT_VERIFY(!dataSource.Keys);

            size_t keyWidth = std::numeric_limits<size_t>::max();
            for (const auto& range : ranges) {
                keyWidth = std::min({keyWidth, GetSignificantWidth(range.first), GetSignificantWidth(range.second)});
            }

            SplitRangesByTablets(
                ranges,
                MakeRange(tableInfo->Tablets),
                tableInfo->LowerCapBound,
                tableInfo->UpperCapBound,
                [&] (auto shardIt, auto rangesIt, auto rangesItEnd) {
                    if (tableInfo->IsOrdered()) {
                        // Crop ranges For ordered table.
                        auto nextPivotKey = shardIt != MakeRange(tableInfo->Tablets).end()
                            ? GetPivotKey(*shardIt)
                            : tableInfo->UpperCapBound;

                        auto lower = std::max(rangesIt->first, GetPivotKey(*(shardIt - 1)));
                        auto upper = std::min((rangesItEnd - 1)->second, nextPivotKey);

                        std::vector<TRowRange> result;
                        ForEachRange(TRange(rangesIt, rangesItEnd), TRowRange(lower, upper), [&] (auto item) {
                            auto [lower, upper] = item;
                            result.emplace_back(rowBuffer->Capture(lower), rowBuffer->Capture(upper));
                        });

                        getSubsource(shardIt, keyWidth)->Ranges = MakeSharedRange(
                            result,
                            ranges.GetHolder());
                    } else {
                        getSubsource(shardIt, keyWidth)->Ranges = MakeSharedRange(
                            MakeRange(rangesIt, rangesItEnd),
                            ranges.GetHolder());
                    }
                });
        } else {
            YT_VERIFY(tableInfo->IsSorted());
            YT_VERIFY(!dataSource.Ranges);
            YT_VERIFY(!dataSource.Schema.empty());
            size_t keyWidth = dataSource.Schema.size();

            auto fullKeySize = tableInfo->Schemas[ETableSchemaKind::Query].GetKeyColumnCount();

            SplitKeysByTablets(
                keys,
                keyWidth,
                fullKeySize,
                MakeRange(tableInfo->Tablets),
                tableInfo->LowerCapBound,
                tableInfo->UpperCapBound,
                [&] (auto shardIt, auto keysIt, auto keysItEnd) {
                    getSubsource(shardIt, fullKeySize)->Keys = MakeSharedRange(
                        MakeRange(keysIt, keysItEnd),
                        keys.GetHolder());
                });
        }

        return subsources;
    }

    TQueryStatistics DoCoordinateAndExecute(
        const TConstQueryPtr& query,
        const TConstExternalCGInfoPtr& externalCGInfo,
        const TQueryOptions& options,
        const TClientBlockReadOptions& blockReadOptions,
        const IUnversionedRowsetWriterPtr& writer,
        int subrangesCount,
        std::function<std::pair<std::vector<TDataRanges>, TString>(int)> getSubsources)
    {
        auto Logger = MakeQueryLogger(query);

        std::vector<TRefiner> refiners(subrangesCount, [] (
            TConstExpressionPtr expr,
            const TKeyColumns& keyColumns) {
                return expr;
            });

        auto functionGenerators = New<TFunctionProfilerMap>();
        auto aggregateGenerators = New<TAggregateProfilerMap>();
        MergeFrom(functionGenerators.Get(), *BuiltinFunctionProfilers);
        MergeFrom(aggregateGenerators.Get(), *BuiltinAggregateProfilers);
        FetchFunctionImplementationsFromCypress(
            functionGenerators,
            aggregateGenerators,
            externalCGInfo,
            FunctionImplCache_,
            blockReadOptions);

        return CoordinateAndExecute(
            query,
            writer,
            refiners,
            [&] (const TConstQueryPtr& subquery, int index) {
                std::vector<TDataRanges> dataSources;
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
                    address);
            },
            [&] (const TConstFrontQueryPtr& topQuery, const ISchemafulReaderPtr& reader, const IUnversionedRowsetWriterPtr& writer) {
                YT_LOG_DEBUG("Evaluating top query (TopQueryId: %v)", topQuery->Id);
                return Evaluator_->Run(
                    std::move(topQuery),
                    std::move(reader),
                    std::move(writer),
                    nullptr,
                    functionGenerators,
                    aggregateGenerators,
                    options);
            });
    }

    TQueryStatistics DoExecute(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        TDataRanges dataSource,
        const TQueryOptions& options,
        const TClientBlockReadOptions& blockReadOptions,
        IUnversionedRowsetWriterPtr writer)
    {
        auto Logger = MakeQueryLogger(query);

        auto rowBuffer = New<TRowBuffer>(TQueryExecutorRowBufferTag{});
        auto allSplits = InferRanges(
            query,
            dataSource,
            options,
            rowBuffer,
            Logger);

        for (const auto& split : allSplits) {
            if (split.first.KeyWidth == 0 && !query->IsOrdered()) {
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

        THashMap<TString, std::vector<TDataRanges>> groupsByAddress;
        for (const auto& split : allSplits) {
            const auto& address = split.second;
            groupsByAddress[address].push_back(split.first);
        }

        std::vector<std::pair<std::vector<TDataRanges>, TString>> groupedSplits;
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
            blockReadOptions,
            writer,
            groupedSplits.size(),
            [&] (int index) {
                return groupedSplits[index];
            });
    }

    TQueryStatistics DoExecuteOrdered(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        TDataRanges dataSource,
        const TQueryOptions& options,
        const TClientBlockReadOptions& blockReadOptions,
        IUnversionedRowsetWriterPtr writer)
    {
        auto Logger = MakeQueryLogger(query);

        auto rowBuffer = New<TRowBuffer>(TQueryExecutorRowBufferTag());
        auto allSplits = InferRanges(
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
            blockReadOptions,
            writer,
            allSplits.size(),
            [&] (int index) {
                const auto& split = allSplits[index];

                YT_LOG_DEBUG("Delegating request to tablet (TabletId: %v, Address: %v)",
                    split.first.Id,
                    split.second);

                return std::make_pair(std::vector<TDataRanges>{split.first}, split.second);
            });
    }

    std::pair<ISchemafulReaderPtr, TFuture<TQueryStatistics>> Delegate(
        TConstQueryPtr query,
        const TConstExternalCGInfoPtr& externalCGInfo,
        const TQueryOptions& options,
        std::vector<TDataRanges> dataSources,
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

        TDuration serializationTime;
        {
            TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&serializationTime);

            ToProto(req->mutable_query(), query);
            req->mutable_query()->set_input_row_limit(options.InputRowLimit);
            req->mutable_query()->set_output_row_limit(options.OutputRowLimit);
            ToProto(req->mutable_external_functions(), externalCGInfo->Functions);
            externalCGInfo->NodeDirectory->DumpTo(req->mutable_node_directory());
            ToProto(req->mutable_options(), options);
            ToProto(req->mutable_data_sources(), dataSources);
            req->set_response_codec(static_cast<int>(config->SelectRowsResponseCodec));
        }

        auto queryFingerprint = InferName(query, true);
        YT_LOG_DEBUG("Sending subquery (Fingerprint: %v, ReadSchema: %v, ResultSchema: %v, SerializationTime: %v, "
            "RequestSize: %v)",
            queryFingerprint,
            query->GetReadSchema(),
            query->GetTableSchema(),
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
        return std::make_pair(resultReader, resultReader->GetQueryResult());
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryExecutor)

IExecutorPtr CreateQueryExecutor(
    NNative::IConnectionPtr connection,
    IInvokerPtr invoker,
    TColumnEvaluatorCachePtr columnEvaluatorCache,
    TEvaluatorPtr evaluator,
    INodeChannelFactoryPtr nodeChannelFactory,
    TFunctionImplCachePtr functionImplCache)
{
    return New<TQueryExecutor>(
        std::move(connection),
        std::move(invoker),
        std::move(columnEvaluatorCache),
        std::move(evaluator),
        std::move(nodeChannelFactory),
        std::move(functionImplCache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
