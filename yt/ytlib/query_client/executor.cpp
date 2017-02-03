#include "executor.h"
#include "column_evaluator.h"
#include "coordinator.h"
#include "evaluator.h"
#include "helpers.h"
#include "query.h"
#include "query_helpers.h"
#include "query_service_proxy.h"
#include "query_statistics.h"
#include "functions_cache.h"

#include <yt/ytlib/api/config.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/tablet_helpers.h>

#include <yt/ytlib/node_tracker_client/channel.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/tablet_client/table_mount_cache.h>
#include <yt/ytlib/table_client/schemaful_reader.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/core/profiling/scoped_timer.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/helpers.h>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NTabletClient;

using NYT::ToProto;

using NApi::INativeConnectionPtr;
using NApi::ValidateTabletMountedOrFrozen;
using NApi::GetPrimaryTabletPeerDescriptor;

using NNodeTrackerClient::INodeChannelFactoryPtr;

using NObjectClient::TObjectId;
using NObjectClient::FromObjectId;

using NHiveClient::TCellDescriptor;

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
        YCHECK(!RowsetReader_);
        RowsetReader_ = CreateWireProtocolRowsetReader(
            response->Attachments(),
            CodecId_,
            Schema_,
            Logger);
        return FromProto(response->query_statistics());
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryResponseReader)

////////////////////////////////////////////////////////////////////////////////

struct TQueryHelperRowBufferTag
{ };

DECLARE_REFCOUNTED_CLASS(TQueryExecutor)

class TQueryExecutor
    : public IExecutor
{
public:
    TQueryExecutor(
        INativeConnectionPtr connection,
        INodeChannelFactoryPtr nodeChannelFactory,
        const TFunctionImplCachePtr& functionImplCache)
        : Connection_(std::move(connection))
        , NodeChannelFactory_(std::move(nodeChannelFactory))
        , FunctionImplCache_(functionImplCache)
    { }

    virtual TFuture<TQueryStatistics> Execute(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        TDataRanges dataSource,
        ISchemafulWriterPtr writer,
        const TQueryOptions& options) override
    {
        TRACE_CHILD("QueryClient", "Execute") {

            auto execute = query->IsOrdered()
                ? &TQueryExecutor::DoExecuteOrdered
                : &TQueryExecutor::DoExecute;

            return BIND(execute, MakeStrong(this))
                .AsyncVia(Connection_->GetHeavyInvoker())
                .Run(
                    std::move(query),
                    std::move(externalCGInfo),
                    std::move(dataSource),
                    options,
                    std::move(writer));
        }
    }

private:
    const INativeConnectionPtr Connection_;
    const INodeChannelFactoryPtr NodeChannelFactory_;
    const TFunctionImplCachePtr FunctionImplCache_;

    std::vector<std::pair<TDataRanges, Stroka>> SplitTable(
        const TObjectId& tableId,
        const TSharedRange<TRowRange>& ranges,
        const TRowBufferPtr& rowBuffer,
        const TQueryOptions& options,
        const NLogging::TLogger& Logger)
    {
        LOG_DEBUG_IF(options.VerboseLogging, "Started splitting table ranges (TableId: %v, RangeCount: %v)",
            tableId,
            ranges.Size());

        auto tableMountCache = Connection_->GetTableMountCache();
        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(FromObjectId(tableId)))
            .ValueOrThrow();

        tableInfo->ValidateDynamic();
        tableInfo->ValidateNotReplicated();

        const auto& cellDirectory = Connection_->GetCellDirectory();
        const auto& networks = Connection_->GetNetworks();

        yhash_map<NTabletClient::TTabletCellId, TCellDescriptor> tabletCellReplicas;

        auto getAddress = [&] (const TTabletInfoPtr& tabletInfo) mutable {
            ValidateTabletMountedOrFrozen(tableInfo, tabletInfo);

            auto insertResult = tabletCellReplicas.insert(std::make_pair(tabletInfo->CellId, TCellDescriptor()));
            auto& descriptor = insertResult.first->second;

            if (insertResult.second) {
                descriptor = cellDirectory->GetDescriptorOrThrow(tabletInfo->CellId);
            }

            // TODO(babenko): pass proper read options
            const auto& peerDescriptor = GetPrimaryTabletPeerDescriptor(descriptor);
            return peerDescriptor.GetAddress(networks);
        };

        std::vector<std::pair<TDataRanges, Stroka>> subsources;
        for (auto rangesIt = begin(ranges); rangesIt != end(ranges);) {
            auto lowerBound = rangesIt->first;
            auto upperBound = rangesIt->second;

            if (lowerBound < tableInfo->LowerCapBound) {
                lowerBound = rowBuffer->Capture(tableInfo->LowerCapBound.Get());
            }
            if (upperBound > tableInfo->UpperCapBound) {
                upperBound = rowBuffer->Capture(tableInfo->UpperCapBound.Get());
            }

            if (lowerBound >= upperBound) {
                ++rangesIt;
                continue;
            }

            // Run binary search to find the relevant tablets.
            auto startIt = std::upper_bound(
                tableInfo->Tablets.begin(),
                tableInfo->Tablets.end(),
                lowerBound,
                [] (TKey key, const TTabletInfoPtr& tabletInfo) {
                    return key < tabletInfo->PivotKey;
                }) - 1;

            auto tabletInfo = *startIt;
            auto nextPivotKey = (startIt + 1 == tableInfo->Tablets.end())
                ? tableInfo->UpperCapBound
                : (*(startIt + 1))->PivotKey;

            if (upperBound < nextPivotKey) {
                auto rangesItEnd = std::upper_bound(
                    rangesIt,
                    end(ranges),
                    nextPivotKey.Get(),
                    [] (TKey key, const TRowRange& rowRange) {
                        return key < rowRange.second;
                    });

                const auto& address = getAddress(tabletInfo);

                TDataRanges dataSource;
                dataSource.Id = tabletInfo->TabletId;
                dataSource.MountRevision = tabletInfo->MountRevision;
                dataSource.Ranges = MakeSharedRange(
                    MakeRange<TRowRange>(rangesIt, rangesItEnd),
                    rowBuffer,
                    ranges.GetHolder());
                dataSource.LookupSupported = tableInfo->IsSorted();

                subsources.emplace_back(std::move(dataSource), address);
                rangesIt = rangesItEnd;
            } else {
                for (auto it = startIt; it != tableInfo->Tablets.end(); ++it) {
                    const auto& tabletInfo = *it;
                    Y_ASSERT(upperBound > tabletInfo->PivotKey);

                    const auto& address = getAddress(tabletInfo);

                    auto pivotKey = tabletInfo->PivotKey;
                    auto nextPivotKey = (it + 1 == tableInfo->Tablets.end())
                        ? tableInfo->UpperCapBound
                        : (*(it + 1))->PivotKey;

                    bool isLast = (upperBound <= nextPivotKey);

                    TRowRange subrange;
                    subrange.first = it == startIt ? lowerBound : rowBuffer->Capture(pivotKey.Get());
                    subrange.second = isLast ? upperBound : rowBuffer->Capture(nextPivotKey.Get());

                    TDataRanges dataSource;
                    dataSource.Id = tabletInfo->TabletId;
                    dataSource.MountRevision = tabletInfo->MountRevision;
                    dataSource.Ranges = MakeSharedRange(
                        SmallVector<TRowRange, 1>{subrange},
                        rowBuffer,
                        ranges.GetHolder());
                    dataSource.LookupSupported = tableInfo->IsSorted();

                    subsources.emplace_back(std::move(dataSource), address);

                    if (isLast) {
                        break;
                    }
                }
                ++rangesIt;
            }
        }

        LOG_DEBUG_IF(options.VerboseLogging, "Finished splitting table ranges (TableId: %v, SubsourceCount: %v)",
            tableId,
            subsources.size());

        return subsources;
    }

    std::vector<std::pair<TDataRanges, Stroka>> InferRanges(
        TConstQueryPtr query,
        const TDataRanges& dataSource,
        const TQueryOptions& options,
        TRowBufferPtr rowBuffer,
        const NLogging::TLogger& Logger)
    {
        const auto& tableId = dataSource.Id;
        auto ranges = dataSource.Ranges;

        auto prunedRanges = GetPrunedRanges(
            query,
            tableId,
            ranges,
            rowBuffer,
            Connection_->GetColumnEvaluatorCache(),
            BuiltinRangeExtractorMap,
            options);

        LOG_DEBUG("Splitting %v pruned splits", prunedRanges.size());

        return SplitTable(
            tableId,
            MakeSharedRange(std::move(prunedRanges), rowBuffer),
            std::move(rowBuffer),
            options,
            Logger);
    }

    TQueryStatistics DoCoordinateAndExecute(
        TConstQueryPtr query,
        const TConstExternalCGInfoPtr& externalCGInfo,
        const TQueryOptions& options,
        ISchemafulWriterPtr writer,
        int subrangesCount,
        std::function<std::pair<std::vector<TDataRanges>, Stroka>(int)> getSubsources)
    {
        auto Logger = MakeQueryLogger(query);

        std::vector<TRefiner> refiners(subrangesCount, [] (
            TConstExpressionPtr expr,
            const TKeyColumns& keyColumns) {
                return expr;
            });

        auto functionGenerators = New<TFunctionProfilerMap>();
        auto aggregateGenerators = New<TAggregateProfilerMap>();
        MergeFrom(functionGenerators.Get(), *BuiltinFunctionCG);
        MergeFrom(aggregateGenerators.Get(), *BuiltinAggregateCG);
        FetchImplementations(
            functionGenerators,
            aggregateGenerators,
            externalCGInfo,
            FunctionImplCache_);

        return CoordinateAndExecute(
            query,
            writer,
            refiners,
            [&] (TConstQueryPtr subquery, int index) {
                std::vector<TDataRanges> dataSources;
                Stroka address;
                std::tie(dataSources, address) = getSubsources(index);

                LOG_DEBUG("Delegating subquery (SubQueryId: %v, Address: %v, MaxSubqueries: %v)",
                    subquery->Id,
                    address,
                    options.MaxSubqueries);

                return Delegate(std::move(subquery), externalCGInfo, options, std::move(dataSources), address);
            },
            [&] (TConstQueryPtr topQuery, ISchemafulReaderPtr reader, ISchemafulWriterPtr writer) {
                LOG_DEBUG("Evaluating top query (TopQueryId: %v)", topQuery->Id);
                auto evaluator = Connection_->GetQueryEvaluator();
                return evaluator->Run(
                    std::move(topQuery),
                    std::move(reader),
                    std::move(writer),
                    functionGenerators,
                    aggregateGenerators,
                    options.EnableCodeCache);
            });
    }

    TQueryStatistics DoExecute(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        TDataRanges dataSource,
        const TQueryOptions& options,
        ISchemafulWriterPtr writer)
    {
        auto Logger = MakeQueryLogger(query);

        auto rowBuffer = New<TRowBuffer>(TQueryHelperRowBufferTag{});
        auto allSplits = InferRanges(
            query,
            dataSource,
            options,
            rowBuffer,
            Logger);

        LOG_DEBUG("Regrouping %v splits into groups",
            allSplits.size());

        yhash_map<Stroka, std::vector<TDataRanges>> groupsByAddress;
        for (const auto& split : allSplits) {
            const auto& address = split.second;
            groupsByAddress[address].push_back(split.first);
        }

        std::vector<std::pair<std::vector<TDataRanges>, Stroka>> groupedSplits;
        for (const auto& group : groupsByAddress) {
            groupedSplits.emplace_back(group.second, group.first);
        }

        LOG_DEBUG("Regrouped %v splits into %v groups",
            allSplits.size(),
            groupsByAddress.size());

        return DoCoordinateAndExecute(
            query,
            externalCGInfo,
            options,
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
        ISchemafulWriterPtr writer)
    {
        auto Logger = MakeQueryLogger(query);

        auto rowBuffer = New<TRowBuffer>(TQueryHelperRowBufferTag());
        auto allSplits = InferRanges(
            query,
            dataSource,
            options,
            rowBuffer,
            Logger);

        // Should be already sorted
        LOG_DEBUG("Sorting %v splits", allSplits.size());

        std::sort(
            allSplits.begin(),
            allSplits.end(),
            [] (const std::pair<TDataRanges, Stroka>& lhs, const std::pair<TDataRanges, Stroka>& rhs) {
                return lhs.first.Ranges.Begin()->first < rhs.first.Ranges.Begin()->first;
            });

        return DoCoordinateAndExecute(
            query,
            externalCGInfo,
            options,
            writer,
            allSplits.size(),
            [&] (int index) {
                const auto& split = allSplits[index];

                LOG_DEBUG("Delegating to tablet %v at %v",
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
        const Stroka& address)
    {
        auto Logger = MakeQueryLogger(query);

        TRACE_CHILD("QueryClient", "Delegate") {
            auto channel = NodeChannelFactory_->CreateChannel(address);
            auto config = Connection_->GetConfig();

            TQueryServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(config->QueryTimeout);

            auto req = proxy.Execute();

            TDuration serializationTime;
            {
                NProfiling::TAggregatingTimingGuard timingGuard(&serializationTime);
                ToProto(req->mutable_query(), query);
                ToProto(req->mutable_external_functions(), externalCGInfo->Functions);
                externalCGInfo->NodeDirectory->DumpTo(req->mutable_node_directory());
                ToProto(req->mutable_options(), options);
                ToProto(req->mutable_data_sources(), dataSources);
                req->set_response_codec(static_cast<int>(config->QueryResponseCodec));
            }

            auto queryFingerprint = InferName(query, true);
            LOG_DEBUG("Sending subquery (Fingerprint: %v, ReadSchema: %v, ResultSchema: %v, SerializationTime: %v, "
                "RequestSize: %v)",
                queryFingerprint,
                query->GetReadSchema(),
                query->GetTableSchema(),
                serializationTime,
                req->ByteSize());

            TRACE_ANNOTATION("serialization_time", serializationTime);
            TRACE_ANNOTATION("request_size", req->ByteSize());

            auto resultReader = New<TQueryResponseReader>(
                req->Invoke(),
                query->GetTableSchema(),
                config->QueryResponseCodec,
                Logger);
            return std::make_pair(resultReader, resultReader->GetQueryResult());
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryExecutor)

IExecutorPtr CreateQueryExecutor(
    INativeConnectionPtr connection,
    INodeChannelFactoryPtr nodeChannelFactory,
    const TFunctionImplCachePtr& functionImplCache)
{
    return New<TQueryExecutor>(connection, nodeChannelFactory, functionImplCache);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
