#include "stdafx.h"
#include "connection.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/scheduler.h>

#include <core/rpc/bus_channel.h>
#include <core/rpc/retrying_channel.h>
#include <core/rpc/caching_channel_factory.h>

#include <core/compression/helpers.h>

#include <ytlib/hydra/peer_channel.h>
#include <ytlib/hydra/config.h>

#include <ytlib/scheduler/scheduler_channel.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/chunk_replica.h>
#include <ytlib/chunk_client/read_limit.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/tablet_client/table_mount_cache.h>
#include <ytlib/tablet_client/wire_protocol.h>

#include <ytlib/new_table_client/schemaful_merging_reader.h>
#include <ytlib/new_table_client/schemaful_ordered_reader.h>

#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/evaluator.h>
#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/plan_helpers.h>
#include <ytlib/query_client/coordinator.h>
#include <ytlib/query_client/private.h>
#include <ytlib/query_client/helpers.h>
#include <ytlib/query_client/query_statistics.h>
#include <ytlib/query_client/query_service_proxy.h>

#include <ytlib/transaction_client/timestamp_provider.h>
#include <ytlib/transaction_client/remote_timestamp_provider.h>
#include <ytlib/transaction_client/config.h>

#include <ytlib/driver/dispatcher.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <util/random/random.h>

// TODO(babenko): consider removing
#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/table_client/table_ypath_proxy.h>
#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/schemaful_reader.h>

namespace NYT {
namespace NApi {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYPath;
using namespace NHive;
using namespace NHydra;
using namespace NChunkClient;
using namespace NTabletClient;
using namespace NQueryClient;
using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NTableClient;  // TODO(babenko): consider removing
using namespace NTableClient::NProto;
using namespace NVersionedTableClient::NProto;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TClientOptions GetRootClientOptions()
{
    TClientOptions options;
    options.User = NSecurityClient::RootUserName;
    return options;
}

////////////////////////////////////////////////////////////////////////////////

class TQueryResponseReader
    : public ISchemafulReader
{
public:
    explicit TQueryResponseReader(TQueryServiceProxy::TInvExecute asyncResponse)
        : AsyncResponse_(std::move(asyncResponse))
        , QueryResult_(NewPromise<TErrorOr<TQueryStatistics>>())
    { }

    virtual TAsyncError Open(const TTableSchema& schema) override
    {
        return AsyncResponse_.Apply(BIND(
            &TQueryResponseReader::OnResponse,
            MakeStrong(this),
            schema));
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        return RowsetReader_->Read(rows);
    }

    virtual TAsyncError GetReadyEvent() override
    {
        return RowsetReader_->GetReadyEvent();
    }

    TFuture<TErrorOr<TQueryStatistics>> GetQueryResult() const
    {
        return QueryResult_.ToFuture();
    }    

private:
    TQueryServiceProxy::TInvExecute AsyncResponse_;

    std::unique_ptr<TWireProtocolReader> ProtocolReader_;
    ISchemafulReaderPtr RowsetReader_;

    TPromise<TErrorOr<TQueryStatistics>> QueryResult_;

    
    TError OnResponse(
        const TTableSchema& schema,
        TQueryServiceProxy::TRspExecutePtr response)
    {
        if (!response->IsOK()) {
            return response->GetError();
        }

        QueryResult_.Set(FromProto(response->query_statistics()));

        YCHECK(!ProtocolReader_);
        auto data  = NCompression::DecompressWithEnvelope(response->Attachments());
        ProtocolReader_.reset(new TWireProtocolReader(data));

        YCHECK(!RowsetReader_);
        RowsetReader_ = ProtocolReader_->CreateSchemafulRowsetReader();

        auto asyncResult = RowsetReader_->Open(schema);
        YCHECK(asyncResult.IsSet()); // this reader is sync
        return asyncResult.Get();
    }
};

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(IConnectionPtr connection, const TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueryClient)

class TQueryClient
    : public IExecutor
    , public IPrepareCallbacks
{
public:
    explicit TQueryClient(
        NQueryClient::TExecutorConfigPtr executorConfig,
        TDuration queryTimeout,
        NCompression::ECodec responseCodec,
        TTableMountCachePtr tableMountCache,
        IChannelPtr masterChannel,
        IChannelFactoryPtr nodeChannelFactory)
        : Evaluator_(New<TEvaluator>(std::move(executorConfig)))
        , QueryTimeout_(queryTimeout)
        , ResponseCodec_(responseCodec)
        , TableMountCache_(std::move(tableMountCache))
        , MasterChannel_(std::move(masterChannel))
        , NodeChannelFactory_(std::move(nodeChannelFactory))
    { }

    // IPrepareCallbacks implementation.

    virtual TFuture<TErrorOr<TDataSplit>> GetInitialSplit(
        const TYPath& path,
        TTimestamp timestamp) override
    {
        return BIND(&TQueryClient::DoGetInitialSplit, MakeStrong(this))
            .Guarded()
            .AsyncVia(NDriver::TDispatcher::Get()->GetLightInvoker())
            .Run(path, timestamp);
    }

    TDataSplits DoSplit(
        const TDataSplits& splits,
        TNodeDirectoryPtr nodeDirectory,
        const NLog::TLogger& Logger)
    {
        TDataSplits allSplits;
        for (const auto& split : splits) {
            auto objectId = GetObjectIdFromDataSplit(split);
            auto type = TypeFromId(objectId);

            if (type != EObjectType::Table) {
                allSplits.push_back(split);
                continue;
            }

            auto newSplits = DoSplitFurther(split, nodeDirectory);

            LOG_DEBUG("Got %v splits for input %v", newSplits.size(), objectId);

            allSplits.insert(allSplits.end(), newSplits.begin(), newSplits.end());
        }

        return allSplits;
    }

    std::pair<ISchemafulReaderPtr, TFuture<TErrorOr<TQueryStatistics>>> Delegate(
        const TPlanFragmentPtr& fragment,
        const Stroka& address)
    {
        auto channel = NodeChannelFactory_->CreateChannel(address);

        TQueryServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(QueryTimeout_);
        auto req = proxy.Execute();
        fragment->NodeDirectory->DumpTo(req->mutable_node_directory());
        ToProto(req->mutable_plan_fragment(), fragment);
        req->set_response_codec(ResponseCodec_);

        auto resultReader = New<TQueryResponseReader>(req->Invoke());
        return std::make_pair(resultReader, resultReader->GetQueryResult());
    }

    // IExecutor implementation.

    TQueryStatistics DoExecute(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer)
    {
        auto nodeDirectory = fragment->NodeDirectory;
        auto query = fragment->Query;
        auto Logger = BuildLogger(query);

        auto splits = DoSplit(GetPrunedSplits(query, fragment->DataSplits), nodeDirectory, Logger);

        std::map<Stroka, TDataSplits> groupes;
        
        LOG_DEBUG("Regrouping %v splits", splits.size());

        for (const auto& split : splits) {
            auto replicas = FromProto<TChunkReplica, TChunkReplicaList>(split.replicas());
            if (replicas.empty()) {
                auto objectId = GetObjectIdFromDataSplit(split);
                THROW_ERROR_EXCEPTION("No alive replicas for split %v",
                    objectId);
            }
            auto replica = replicas[RandomNumber(replicas.size())];
            auto descriptor = nodeDirectory->GetDescriptor(replica);

            groupes[descriptor.GetDefaultAddress()].push_back(split);
        }

        std::vector<TKeyRange> ranges;
        std::vector<std::pair<TDataSplits, Stroka>> groupedSplits;

        for (const auto& group : groupes) {
            groupedSplits.emplace_back(group.second, group.first);
            ranges.push_back(GetRange(group.second));
        }

        TConstQueryPtr topQuery;
        std::vector<TConstQueryPtr> subqueries;

        std::tie(topQuery, subqueries) = CoordinateQuery(query, ranges);

        std::vector<ISchemafulReaderPtr> splitReaders;
        std::vector<TFuture<TErrorOr<TQueryStatistics>>> subqueriesStatistics;

        for (size_t index = 0; index < groupedSplits.size(); ++index) {
            if (!groupedSplits[index].first.empty()) {
                auto subquery = subqueries[index];
                LOG_DEBUG("Delegating subfragment (SubfragmentId: %v)",
                    subquery->GetId());

                auto subfragment = New<TPlanFragment>(fragment->GetSource());
                subfragment->NodeDirectory = nodeDirectory;
                subfragment->DataSplits = groupedSplits[index].first;
                subfragment->Query = subquery;

                ISchemafulReaderPtr reader;
                TFuture<TErrorOr<TQueryStatistics>> statistics;

                std::tie(reader, statistics) = Delegate(
                    subfragment,
                    groupedSplits[index].second);

                splitReaders.push_back(reader);
                subqueriesStatistics.push_back(statistics);
            }
        }

        auto mergingReader = CreateSchemafulMergingReader(splitReaders);

        auto queryStatistics = Evaluator_->Run(topQuery, std::move(mergingReader), std::move(writer));

        for (auto const& subqueryStatistics : subqueriesStatistics) {
            if (subqueryStatistics.IsSet()) {
                queryStatistics += subqueryStatistics.Get().ValueOrThrow();
            }
        }
        
        return queryStatistics;
    }

    TQueryStatistics DoExecuteOrdered(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer)
    {
        auto nodeDirectory = fragment->NodeDirectory;
        auto query = fragment->Query;
        auto Logger = BuildLogger(query);

        auto splits = DoSplit(GetPrunedSplits(query, fragment->DataSplits), nodeDirectory, Logger);

        LOG_DEBUG("Sorting %v splits", splits.size());

        std::sort(splits.begin(), splits.end(), [] (const TDataSplit& lhs, const TDataSplit& rhs) {
            return GetLowerBoundFromDataSplit(lhs) < GetLowerBoundFromDataSplit(rhs);
        });

        std::vector<TKeyRange> ranges;
        for (auto const& split : splits) {
            ranges.push_back(GetBothBoundsFromDataSplit(split));
        }

        TConstQueryPtr topquery;
        std::vector<TConstQueryPtr> subqueries;

        std::tie(topquery, subqueries) = CoordinateQuery(query, ranges);

        std::vector<TFuture<TErrorOr<TQueryStatistics>>> subqueriesStatistics;
        size_t index = 0;

        auto mergingReader = CreateSchemafulOrderedReader([&] () -> ISchemafulReaderPtr {
            if (index >= splits.size()) {
                return nullptr;
            }

            auto replicas = FromProto<TChunkReplica, TChunkReplicaList>(splits[index].replicas());
            if (replicas.empty()) {
                auto objectId = GetObjectIdFromDataSplit(splits[index]);
                THROW_ERROR_EXCEPTION("No alive replicas for split %v", objectId);
            }
            auto replica = replicas[RandomNumber(replicas.size())];
            auto descriptor = nodeDirectory->GetDescriptor(replica);

            auto subquery = subqueries[index];

            LOG_DEBUG("Delegating subquery (SubqueryId: %v)", subquery->GetId());

            auto subfragment = New<TPlanFragment>(fragment->GetSource());
            subfragment->NodeDirectory = nodeDirectory;
            subfragment->DataSplits.push_back(splits[index]);
            subfragment->Query = subquery;

            ISchemafulReaderPtr reader;
            TFuture<TErrorOr<TQueryStatistics>> statistics;

            std::tie(reader, statistics) = Delegate(
                subfragment,
                descriptor.GetDefaultAddress());

            subqueriesStatistics.push_back(statistics);

            ++index;

            return reader;
        });

        auto queryStatistics = Evaluator_->Run(topquery, std::move(mergingReader), std::move(writer));

        for (auto const& subqueryStatistics : subqueriesStatistics) {
            YCHECK(subqueryStatistics.IsSet());
            queryStatistics += subqueryStatistics.Get().ValueOrThrow();
        }
        
        return queryStatistics;
    }

    virtual TFuture<TErrorOr<TQueryStatistics>> Execute(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer) override
    {
        auto execute = fragment->Ordered
            ? &TQueryClient::DoExecuteOrdered
            : &TQueryClient::DoExecute;

        return BIND(execute, MakeStrong(this))
            .Guarded()
            .AsyncVia(NDriver::TDispatcher::Get()->GetHeavyInvoker())
            .Run(fragment, std::move(writer));
    }

private:
    TEvaluatorPtr Evaluator_;
    TDuration QueryTimeout_;
    NCompression::ECodec ResponseCodec_;
    TTableMountCachePtr TableMountCache_;
    IChannelPtr MasterChannel_;
    IChannelFactoryPtr NodeChannelFactory_;

    TDataSplit DoGetInitialSplit(
        const TYPath& path,
        TTimestamp timestamp)
    {
        auto asyncInfoOrError = TableMountCache_->GetTableInfo(path);
        auto infoOrError = WaitFor(asyncInfoOrError);
        THROW_ERROR_EXCEPTION_IF_FAILED(infoOrError);
        const auto& info = infoOrError.Value();

        TDataSplit result;
        SetObjectId(&result, info->TableId);
        SetTableSchema(&result, info->Schema);
        SetKeyColumns(&result, info->KeyColumns);
        SetTimestamp(&result, timestamp);
        return result;
    }

    std::vector<TDataSplit> DoSplitFurther(
        const TDataSplit& split,
        TNodeDirectoryPtr nodeDirectory)
    {
        auto objectId = GetObjectIdFromDataSplit(split);

        std::vector<TDataSplit> subsplits;
        switch (TypeFromId(objectId)) {
            case EObjectType::Table:
                subsplits = DoSplitTableFurther(split, std::move(nodeDirectory));
                break;

            default:
                YUNREACHABLE();
        }

        return subsplits;
    }

    std::vector<TDataSplit> DoSplitTableFurther(
        const TDataSplit& split,
        TNodeDirectoryPtr nodeDirectory)
    {
        auto tableId = GetObjectIdFromDataSplit(split);
        auto tableInfoOrError = WaitFor(TableMountCache_->GetTableInfo(FromObjectId(tableId)));
        THROW_ERROR_EXCEPTION_IF_FAILED(tableInfoOrError);
        const auto& tableInfo = tableInfoOrError.Value();

        return tableInfo->Sorted
            ? DoSplitSortedTableFurther(split, std::move(nodeDirectory))
            : DoSplitUnsortedTableFurther(split, std::move(nodeDirectory), std::move(tableInfo));
    }

    std::vector<TDataSplit> DoSplitSortedTableFurther(
        const TDataSplit& split,
        TNodeDirectoryPtr nodeDirectory)
    {
        auto tableId = GetObjectIdFromDataSplit(split);

        // TODO(babenko): refactor and optimize
        TObjectServiceProxy proxy(MasterChannel_);

        auto req = TTableYPathProxy::Fetch(FromObjectId(tableId));
        ToProto(req->mutable_ranges(), std::vector<TReadRange>({TReadRange()}));
        req->set_fetch_all_meta_extensions(true);

        auto rsp = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        nodeDirectory->MergeFrom(rsp->node_directory());

        auto chunkSpecs = FromProto<NChunkClient::NProto::TChunkSpec>(rsp->chunks());
        auto keyColumns = GetKeyColumnsFromDataSplit(split);
        auto schema = GetTableSchemaFromDataSplit(split);

        for (auto& chunkSpec : chunkSpecs) {
            auto chunkKeyColumns = FindProtoExtension<TKeyColumnsExt>(chunkSpec.chunk_meta().extensions());
            auto chunkSchema = FindProtoExtension<TTableSchemaExt>(chunkSpec.chunk_meta().extensions());

            // TODO(sandello): One day we should validate consistency.
            // Now we just check we do _not_ have any of these.
            YCHECK(!chunkKeyColumns);
            YCHECK(!chunkSchema);

            SetKeyColumns(&chunkSpec, keyColumns);
            SetTableSchema(&chunkSpec, schema);

            auto boundaryKeys = FindProtoExtension<TOldBoundaryKeysExt>(chunkSpec.chunk_meta().extensions());
            if (boundaryKeys) {
                auto chunkLowerBound = NYT::FromProto<TOwningKey>(boundaryKeys->start());
                auto chunkUpperBound = NYT::FromProto<TOwningKey>(boundaryKeys->end());
                // Boundary keys are exact, so advance right bound to its successor.
                chunkUpperBound = GetKeySuccessor(chunkUpperBound.Get());
                SetLowerBound(&chunkSpec, chunkLowerBound);
                SetUpperBound(&chunkSpec, chunkUpperBound);
            }
        }

        return chunkSpecs;
    }

    std::vector<TDataSplit> DoSplitUnsortedTableFurther(
        const TDataSplit& split,
        TNodeDirectoryPtr nodeDirectory,
        TTableMountInfoPtr tableInfo)
    {
        auto tableId = GetObjectIdFromDataSplit(split);

        if (tableInfo->Tablets.empty()) {
            THROW_ERROR_EXCEPTION("Table %v is neither sorted nor has tablets",
                tableId);
        }

        auto lowerBound = GetLowerBoundFromDataSplit(split);
        auto upperBound = GetUpperBoundFromDataSplit(split);
        auto keyColumns = GetKeyColumnsFromDataSplit(split);
        auto schema = GetTableSchemaFromDataSplit(split);
        auto timestamp = GetTimestampFromDataSplit(split);

        // Run binary search to find the relevant tablets.
        auto startIt = std::upper_bound(
            tableInfo->Tablets.begin(),
            tableInfo->Tablets.end(),
            lowerBound,
            [] (const TOwningKey& key, const TTabletInfoPtr& tabletInfo) {
                return key < tabletInfo->PivotKey;
            }) - 1;

        std::vector<TDataSplit> subsplits;
        for (auto it = startIt; it != tableInfo->Tablets.end(); ++it) {
            const auto& tabletInfo = *it;
            if (upperBound <= tabletInfo->PivotKey)
                break;

            if (tabletInfo->State != ETabletState::Mounted) {
                // TODO(babenko): learn to work with unmounted tablets
                THROW_ERROR_EXCEPTION("Tablet %v is not mounted",
                    tabletInfo->TabletId);
            }

            TDataSplit subsplit;
            SetObjectId(&subsplit, tabletInfo->TabletId);   
            SetKeyColumns(&subsplit, keyColumns);
            SetTableSchema(&subsplit, schema);
            
            auto pivotKey = tabletInfo->PivotKey;
            auto nextPivotKey = (it + 1 == tableInfo->Tablets.end()) ? MaxKey() : (*(it + 1))->PivotKey;

            SetLowerBound(&subsplit, std::max(lowerBound, pivotKey));
            SetUpperBound(&subsplit, std::min(upperBound, nextPivotKey));
            SetTimestamp(&subsplit, timestamp); 

            for (const auto& tabletReplica : tabletInfo->Replicas) {
                nodeDirectory->AddDescriptor(tabletReplica.Id, tabletReplica.Descriptor);
                TChunkReplica chunkReplica(tabletReplica.Id, 0);
                subsplit.add_replicas(ToProto<ui32>(chunkReplica));
            }

            subsplits.push_back(std::move(subsplit));
        }
        return subsplits;
    }

};

DEFINE_REFCOUNTED_TYPE(TQueryClient)

class TConnection
    : public IConnection
{
public:
    explicit TConnection(
        TConnectionConfigPtr config,
        TCallback<bool(const TError&)> isRetriableError)
        : Config_(config)
    {
        MasterChannel_ = CreateMasterChannel(Config_->Master, isRetriableError);

        auto timestampProviderConfig = Config_->TimestampProvider;
        if (!timestampProviderConfig) {
            // Use masters for timestamp generation.
            timestampProviderConfig = New<TRemoteTimestampProviderConfig>();
            timestampProviderConfig->Addresses = Config_->Master->Addresses;
            timestampProviderConfig->RpcTimeout = Config_->Master->RpcTimeout;
        }
        TimestampProvider_ = CreateRemoteTimestampProvider(
            timestampProviderConfig,
            GetBusChannelFactory());

        auto masterCacheConfig = Config_->MasterCache;
        if (!masterCacheConfig) {
            // Disable cache.
            masterCacheConfig = Config_->Master;
        }
        MasterCacheChannel_ = CreateMasterChannel(masterCacheConfig, isRetriableError);

        SchedulerChannel_ = CreateSchedulerChannel(
            Config_->Scheduler,
            GetBusChannelFactory(),
            MasterChannel_);

        NodeChannelFactory_ = CreateCachingChannelFactory(GetBusChannelFactory());

        CellDirectory_ = New<TCellDirectory>(
            Config_->CellDirectory,
            GetBusChannelFactory());
        CellDirectory_->RegisterCell(config->Master);

        CompressedBlockCache_ = CreateClientBlockCache(
            Config_->CompressedBlockCache);

        UncompressedBlockCache_ = CreateClientBlockCache(
            Config_->UncompressedBlockCache);

        TableMountCache_ = New<TTableMountCache>(
            Config_->TableMountCache,
            MasterCacheChannel_,
            CellDirectory_);

        QueryClient_ = New<TQueryClient>(
            Config_->QueryExecutor,
            Config_->QueryTimeout,
            Config_->SelectResponseCodec,
            TableMountCache_,
            MasterChannel_,
            NodeChannelFactory_);
    }

    // IConnection implementation.

    virtual TConnectionConfigPtr GetConfig() override
    {
        return Config_;
    }

    virtual IChannelPtr GetMasterChannel() override
    {
        return MasterChannel_;
    }

    virtual IChannelPtr GetMasterCacheChannel() override
    {
        return MasterCacheChannel_;
    }

    virtual IChannelPtr GetSchedulerChannel() override
    {
        return SchedulerChannel_;
    }

    virtual IChannelFactoryPtr GetNodeChannelFactory() override
    {
        return NodeChannelFactory_;
    }

    virtual IBlockCachePtr GetCompressedBlockCache() override
    {
        return CompressedBlockCache_;
    }

    virtual IBlockCachePtr GetUncompressedBlockCache() override
    {
        return UncompressedBlockCache_;
    }

    virtual TTableMountCachePtr GetTableMountCache() override
    {
        return TableMountCache_;
    }

    virtual ITimestampProviderPtr GetTimestampProvider() override
    {
        return TimestampProvider_;
    }

    virtual TCellDirectoryPtr GetCellDirectory() override
    {
        return CellDirectory_;
    }

    virtual IPrepareCallbacks* GetQueryPrepareCallbacks() override
    {
        return QueryClient_.Get();
    }

    virtual IExecutorPtr GetQueryExecutor() override
    {
        return QueryClient_.Get();
    }

    virtual IClientPtr CreateClient(const TClientOptions& options) override
    {
        return NApi::CreateClient(this, options);
    }

    virtual void ClearMetadataCaches() override
    {
        TableMountCache_->Clear();
    }


private:
    TConnectionConfigPtr Config_;

    IChannelPtr MasterChannel_;
    IChannelPtr MasterCacheChannel_;
    IChannelPtr SchedulerChannel_;
    IChannelFactoryPtr NodeChannelFactory_;
    IBlockCachePtr CompressedBlockCache_;
    IBlockCachePtr UncompressedBlockCache_;
    TTableMountCachePtr TableMountCache_;
    ITimestampProviderPtr TimestampProvider_;
    TCellDirectoryPtr CellDirectory_;
    TQueryClientPtr QueryClient_;
    
    static IChannelPtr CreateMasterChannel(
        TMasterConnectionConfigPtr config,
        TCallback<bool(const TError&)> isRetriableError)
    {
        auto leaderChannel = CreateLeaderChannel(
            config,
            GetBusChannelFactory());
        auto masterChannel = CreateRetryingChannel(
            config,
            leaderChannel,
            isRetriableError);
        masterChannel->SetDefaultTimeout(config->RpcTimeout);
        return masterChannel;
    }

};

IConnectionPtr CreateConnection(
    TConnectionConfigPtr config,
    TCallback<bool(const TError&)> isRetriableError)
{
    return New<TConnection>(config, isRetriableError);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
