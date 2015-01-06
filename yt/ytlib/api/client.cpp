#include "stdafx.h"
#include "client.h"
#include "transaction.h"
#include "connection.h"
#include "file_reader.h"
#include "file_writer.h"
#include "journal_reader.h"
#include "journal_writer.h"
#include "rowset.h"
#include "config.h"

#include <core/concurrency/scheduler.h>
#include <core/concurrency/parallel_collector.h>
#include <core/concurrency/parallel_awaiter.h>

#include <core/ytree/attribute_helpers.h>
#include <core/ytree/ypath_proxy.h>

#include <core/rpc/helpers.h>
#include <core/rpc/scoped_channel.h>

#include <core/compression/helpers.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/timestamp_provider.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/tablet_client/wire_protocol.h>
#include <ytlib/tablet_client/table_mount_cache.h>
#include <ytlib/tablet_client/tablet_service_proxy.h>
#include <ytlib/tablet_client/wire_protocol.pb.h>

#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/security_client/group_ypath_proxy.h>

#include <ytlib/driver/dispatcher.h>

#include <ytlib/new_table_client/name_table.h>

#include <ytlib/hive/config.h>
#include <ytlib/hive/cell_directory.h>

#include <ytlib/new_table_client/schemaful_writer.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/plan_helpers.h>
#include <ytlib/query_client/coordinator.h>
#include <ytlib/query_client/helpers.h>
#include <ytlib/query_client/query_statistics.h>
#include <ytlib/query_client/query_service_proxy.h>
#include <ytlib/query_client/query_statistics.h>
#include <ytlib/query_client/evaluator.h>
#include <ytlib/query_client/private.h> // XXX(sandello): refactor BuildLogger

#include <ytlib/chunk_client/chunk_replica.h>
#include <ytlib/chunk_client/read_limit.h>

// TODO(babenko): refactor this
#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/table_client/table_ypath_proxy.h>
#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {
namespace NApi {

using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NRpc;
using namespace NVersionedTableClient;
using namespace NVersionedTableClient::NProto;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NSecurityClient;
using namespace NQueryClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueryResponseReader)
DECLARE_REFCOUNTED_CLASS(TQueryHelper)
DECLARE_REFCOUNTED_CLASS(TClient)
DECLARE_REFCOUNTED_CLASS(TTransaction)

////////////////////////////////////////////////////////////////////////////////

namespace {

TNameTableToSchemaIdMapping BuildColumnIdMapping(
    const TTableMountInfoPtr& tableInfo,
    const TNameTablePtr& nameTable)
{
    for (const auto& name : tableInfo->KeyColumns) {
        if (!nameTable->FindId(name)) {
            THROW_ERROR_EXCEPTION("Missing key column %Qv in name table",
                name);
        }
    }

    TNameTableToSchemaIdMapping mapping;
    mapping.resize(nameTable->GetSize());
    for (int nameTableId = 0; nameTableId < nameTable->GetSize(); ++nameTableId) {
        const auto& name = nameTable->GetName(nameTableId);
        int schemaId = tableInfo->Schema.GetColumnIndexOrThrow(name);
        mapping[nameTableId] = schemaId;
    }
    return mapping;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TQueryResponseReader
    : public ISchemafulReader
{
public:
    explicit TQueryResponseReader(TFuture<TQueryServiceProxy::TRspExecutePtr> asyncResponse)
        : AsyncResponse_(std::move(asyncResponse))
    { }

    virtual TFuture<void> Open(const TTableSchema& schema) override
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

    virtual TFuture<void> GetReadyEvent() override
    {
        return RowsetReader_->GetReadyEvent();
    }

    TFuture<TQueryStatistics> GetQueryResult() const
    {
        return QueryResult_.ToFuture();
    }    

private:
    TFuture<TQueryServiceProxy::TRspExecutePtr> AsyncResponse_;

    std::unique_ptr<TWireProtocolReader> ProtocolReader_;
    ISchemafulReaderPtr RowsetReader_;

    TPromise<TQueryStatistics> QueryResult_ = NewPromise<TQueryStatistics>();

    
    void OnResponse(
        const TTableSchema& schema,
        TQueryServiceProxy::TRspExecutePtr response)
    {
        QueryResult_.Set(FromProto(response->query_statistics()));

        YCHECK(!ProtocolReader_);
        auto data  = NCompression::DecompressWithEnvelope(response->Attachments());
        ProtocolReader_.reset(new TWireProtocolReader(data));

        YCHECK(!RowsetReader_);
        RowsetReader_ = ProtocolReader_->CreateSchemafulRowsetReader();

        auto openResult = RowsetReader_->Open(schema);
        YCHECK(openResult.IsSet()); // this reader is sync
        openResult.Get().ThrowOnError();
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryResponseReader)

////////////////////////////////////////////////////////////////////////////////

class TQueryHelper
    : public IExecutor
    , public IPrepareCallbacks
{
public:
    TQueryHelper(
        IConnectionPtr connection,
        IChannelPtr masterChannel,
        IChannelFactoryPtr nodeChannelFactory)
        : Connection_(std::move(connection))
        , MasterChannel_(std::move(masterChannel))
        , NodeChannelFactory_(std::move(nodeChannelFactory))
    { }

    // IPrepareCallbacks implementation.

    virtual TFuture<TDataSplit> GetInitialSplit(
        const TYPath& path,
        TTimestamp timestamp) override
    {
        return BIND(&TQueryHelper::DoGetInitialSplit, MakeStrong(this))
            .AsyncVia(NDriver::TDispatcher::Get()->GetLightInvoker())
            .Run(path, timestamp);
    }

    // IExecutor implementation.

    virtual TFuture<TQueryStatistics> Execute(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer) override
    {
        auto execute = fragment->Ordered
            ? &TQueryHelper::DoExecuteOrdered
            : &TQueryHelper::DoExecute;

        return BIND(execute, MakeStrong(this))
            .AsyncVia(NDriver::TDispatcher::Get()->GetHeavyInvoker())
            .Run(fragment, std::move(writer));
    }

private:
    IConnectionPtr Connection_;
    IChannelPtr MasterChannel_;
    IChannelFactoryPtr NodeChannelFactory_;


    TDataSplit DoGetInitialSplit(
        const TYPath& path,
        TTimestamp timestamp)
    {
        auto tableMountCache = Connection_->GetTableMountCache();
        auto info = WaitFor(tableMountCache->GetTableInfo(path)).ValueOrThrow();

        TDataSplit result;
        SetObjectId(&result, info->TableId);
        SetTableSchema(&result, info->Schema);
        SetKeyColumns(&result, info->KeyColumns);
        SetTimestamp(&result, timestamp);
        return result;
    }


    TDataSplits Split(
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

            auto newSplits = SplitFurther(split, nodeDirectory);

            LOG_DEBUG("Got %v splits for input %v", newSplits.size(), objectId);

            allSplits.insert(allSplits.end(), newSplits.begin(), newSplits.end());
        }

        return allSplits;
    }

    std::vector<TDataSplit> SplitFurther(
        const TDataSplit& split,
        TNodeDirectoryPtr nodeDirectory)
    {
        auto objectId = GetObjectIdFromDataSplit(split);

        std::vector<TDataSplit> subsplits;
        switch (TypeFromId(objectId)) {
            case EObjectType::Table:
                subsplits = SplitTableFurther(split, std::move(nodeDirectory));
                break;

            default:
                YUNREACHABLE();
        }

        return subsplits;
    }

    std::vector<TDataSplit> SplitTableFurther(
        const TDataSplit& split,
        TNodeDirectoryPtr nodeDirectory)
    {
        auto tableId = GetObjectIdFromDataSplit(split);
        auto tableMountCache = Connection_->GetTableMountCache();
        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(FromObjectId(tableId))).ValueOrThrow();
        return tableInfo->Sorted
            ? SplitSortedTableFurther(split, std::move(nodeDirectory))
            : SplitUnsortedTableFurther(split, std::move(nodeDirectory), std::move(tableInfo));
    }

    std::vector<TDataSplit> SplitSortedTableFurther(
        const TDataSplit& split,
        TNodeDirectoryPtr nodeDirectory)
    {
        auto tableId = GetObjectIdFromDataSplit(split);

        // TODO(babenko): refactor and optimize
        TObjectServiceProxy proxy(MasterChannel_);

        auto req = TTableYPathProxy::Fetch(FromObjectId(tableId));
        ToProto(req->mutable_ranges(), std::vector<TReadRange>({TReadRange()}));
        req->set_fetch_all_meta_extensions(true);

        auto rsp = WaitFor(proxy.Execute(req)).ValueOrThrow();

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

    std::vector<TDataSplit> SplitUnsortedTableFurther(
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


    TQueryStatistics DoExecute(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer)
    {
        auto nodeDirectory = fragment->NodeDirectory;
        auto Logger = BuildLogger(fragment->Query);
        
        std::vector<std::pair<TDataSplits, Stroka>> groupedSplits;
        return CoordinateAndExecute(fragment, writer, false, [&] (const TDataSplits& prunedSplits) {
                auto splits = Split(prunedSplits, nodeDirectory, Logger);

                LOG_DEBUG("Regrouping %v splits", splits.size());

                std::map<Stroka, TDataSplits> groupes;
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
                for (const auto& group : groupes) {
                    if (group.second.empty()) {
                        continue;
                    }

                    groupedSplits.emplace_back(group.second, group.first);
                    ranges.push_back(GetRange(group.second));
                }
                return ranges;
            }, [&] (const TConstQueryPtr& subquery, size_t index) {
                auto subfragment = New<TPlanFragment>(fragment->GetSource());
                subfragment->NodeDirectory = nodeDirectory;
                subfragment->DataSplits = groupedSplits[index].first;
                subfragment->Query = subquery;
                return Delegate(subfragment, groupedSplits[index].second);
            }, [&] (const TConstQueryPtr& topQuery, ISchemafulReaderPtr reader, ISchemafulWriterPtr writer) {
                auto evaluator = Connection_->GetQueryEvaluator();
                return evaluator->Run(topQuery, std::move(reader), std::move(writer));
            });
    }

    TQueryStatistics DoExecuteOrdered(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer)
    {
        auto nodeDirectory = fragment->NodeDirectory;
        auto Logger = BuildLogger(fragment->Query);
        
        TDataSplits splits;
        return CoordinateAndExecute(fragment, writer, true, [&] (const TDataSplits& prunedSplits) {
                splits = Split(prunedSplits, nodeDirectory, Logger);

                LOG_DEBUG("Sorting %v splits", splits.size());

                std::sort(splits.begin(), splits.end(), [] (const TDataSplit& lhs, const TDataSplit& rhs) {
                    return GetLowerBoundFromDataSplit(lhs) < GetLowerBoundFromDataSplit(rhs);
                });

                std::vector<TKeyRange> ranges;
                for (auto const& split : splits) {
                    ranges.push_back(GetBothBoundsFromDataSplit(split));
                }
                return ranges;
            }, [&] (const TConstQueryPtr& subquery, size_t index) {
                auto replicas = FromProto<TChunkReplica, TChunkReplicaList>(splits[index].replicas());
                if (replicas.empty()) {
                    auto objectId = GetObjectIdFromDataSplit(splits[index]);
                    THROW_ERROR_EXCEPTION("No alive replicas for split %v", objectId);
                }
                auto replica = replicas[RandomNumber(replicas.size())];
                auto descriptor = nodeDirectory->GetDescriptor(replica);

                LOG_DEBUG("Delegating subquery (SubqueryId: %v)", subquery->GetId());

                auto subfragment = New<TPlanFragment>(fragment->GetSource());
                subfragment->NodeDirectory = nodeDirectory;
                subfragment->DataSplits.push_back(splits[index]);
                subfragment->Query = subquery;

                return Delegate(subfragment, descriptor.GetDefaultAddress());
            }, [&] (const TConstQueryPtr& topQuery, ISchemafulReaderPtr reader, ISchemafulWriterPtr writer) {
                auto evaluator = Connection_->GetQueryEvaluator();
                return evaluator->Run(topQuery, std::move(reader), std::move(writer));
            });
    }

   std::pair<ISchemafulReaderPtr, TFuture<TQueryStatistics>> Delegate(
        const TPlanFragmentPtr& fragment,
        const Stroka& address)
    {
        auto channel = NodeChannelFactory_->CreateChannel(address);
        auto config = Connection_->GetConfig();

        TQueryServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(config->QueryTimeout);
        auto req = proxy.Execute();
        fragment->NodeDirectory->DumpTo(req->mutable_node_directory());
        ToProto(req->mutable_plan_fragment(), fragment);
        req->set_response_codec(static_cast<int>(config->SelectResponseCodec));

        auto resultReader = New<TQueryResponseReader>(req->Invoke());
        return std::make_pair(resultReader, resultReader->GetQueryResult());
    }

};

DEFINE_REFCOUNTED_TYPE(TQueryHelper)

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
{
public:
    TClient(
        IConnectionPtr connection,
        const TClientOptions& options)
        : Connection_(std::move(connection))
        , Options_(options)
        , Invoker_(NDriver::TDispatcher::Get()->GetLightInvoker())
    {
        MasterChannel_ = Connection_->GetMasterChannel();
        SchedulerChannel_ = Connection_->GetSchedulerChannel();
        NodeChannelFactory_ = Connection_->GetNodeChannelFactory();

        if (options.User != NSecurityClient::RootUserName) {
            MasterChannel_ = CreateAuthenticatedChannel(MasterChannel_, options.User);
            SchedulerChannel_ = CreateAuthenticatedChannel(SchedulerChannel_, options.User);
            NodeChannelFactory_ = CreateAuthenticatedChannelFactory(NodeChannelFactory_, options.User);
        }

        MasterChannel_ = CreateScopedChannel(MasterChannel_);
        SchedulerChannel_ = CreateScopedChannel(SchedulerChannel_);
            
        TransactionManager_ = New<TTransactionManager>(
            Connection_->GetConfig()->TransactionManager,
            Connection_->GetConfig()->Master->CellTag,
            Connection_->GetConfig()->Master->CellId,
            MasterChannel_,
            Connection_->GetTimestampProvider(),
            Connection_->GetCellDirectory());

        QueryHelper_ = New<TQueryHelper>(Connection_, MasterChannel_, NodeChannelFactory_);

        ObjectProxy_.reset(new TObjectServiceProxy(MasterChannel_));
    }


    virtual IConnectionPtr GetConnection() override
    {
        return Connection_;
    }

    virtual IChannelPtr GetMasterChannel() override
    {
        return MasterChannel_;
    }

    virtual IChannelPtr GetSchedulerChannel() override
    {
        return SchedulerChannel_;
    }

    virtual IChannelFactoryPtr GetNodeChannelFactory() override
    {
        return NodeChannelFactory_;
    }

    virtual TTransactionManagerPtr GetTransactionManager() override
    {
        return TransactionManager_;
    }


    virtual TFuture<void> Terminate() override
    {
        TransactionManager_->AbortAll();

        auto error = TError("Client terminated");
        auto awaiter = New<TParallelAwaiter>(GetSyncInvoker());
        awaiter->Await(MasterChannel_->Terminate(error));
        awaiter->Await(SchedulerChannel_->Terminate(error));
        return awaiter->Complete();
    }


    virtual TFuture<ITransactionPtr> StartTransaction(
        ETransactionType type,
        const TTransactionStartOptions& options) override;

#define DROP_BRACES(...) __VA_ARGS__
#define IMPLEMENT_METHOD(returnType, method, signature, args) \
    virtual TFuture<returnType> method signature override \
    { \
        return Execute<returnType>(BIND( \
            &TClient::Do ## method, \
            MakeStrong(this), \
            DROP_BRACES args)); \
    }

    virtual TFuture<IRowsetPtr> LookupRow(
        const TYPath& path,
        TNameTablePtr nameTable,
        NVersionedTableClient::TKey key,
        const TLookupRowsOptions& options) override
    {
        return LookupRows(
            path,
            std::move(nameTable),
            std::vector<NVersionedTableClient::TKey>(1, key),
            options);
    }
    
    IMPLEMENT_METHOD(IRowsetPtr, LookupRows, (
        const TYPath& path,
        TNameTablePtr nameTable,
        const std::vector<NVersionedTableClient::TKey>& keys,
        const TLookupRowsOptions& options),
        (path, nameTable, keys, options))
    IMPLEMENT_METHOD(TQueryStatistics, SelectRows, (
        const Stroka& query,
        ISchemafulWriterPtr writer,
        const TSelectRowsOptions& options),
        (query, writer, options))

    virtual TFuture<std::pair<IRowsetPtr, TQueryStatistics>> SelectRows(
        const Stroka& query,
        const TSelectRowsOptions& options) override
    {
        auto result = NewPromise<std::pair<IRowsetPtr, TQueryStatistics>>();

        ISchemafulWriterPtr writer;
        TPromise<IRowsetPtr> rowset;
        std::tie(writer, rowset) = CreateSchemafulRowsetWriter();

        SelectRows(query, writer, options).Subscribe(BIND([=] (const TErrorOr<TQueryStatistics>& error) mutable {
            if (!error.IsOK()) {
                // It's uncommon to have the promise set here but let's be sloppy about it.
                result.Set(TError(error));
            } else {
                result.Set(std::make_pair(rowset.Get().Value(), error.Value()));
            }
        }));

        return result;
    }


    IMPLEMENT_METHOD(void, MountTable, (
        const TYPath& path,
        const TMountTableOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, UnmountTable, (
        const TYPath& path,
        const TUnmountTableOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, RemountTable, (
        const TYPath& path,
        const TRemountTableOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, ReshardTable, (
        const TYPath& path,
        const std::vector<NVersionedTableClient::TKey>& pivotKeys,
        const TReshardTableOptions& options),
        (path, pivotKeys, options))


    IMPLEMENT_METHOD(TYsonString, GetNode, (
        const TYPath& path,
        const TGetNodeOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, SetNode, (
        const TYPath& path,
        const TYsonString& value,
        const TSetNodeOptions& options),
        (path, value, options))
    IMPLEMENT_METHOD(void, RemoveNode, (
        const TYPath& path,
        const TRemoveNodeOptions& options),
        (path, options))
    IMPLEMENT_METHOD(TYsonString, ListNodes, (
        const TYPath& path,
        const TListNodesOptions& options),
        (path, options))
    IMPLEMENT_METHOD(TNodeId, CreateNode, (
        const TYPath& path,
        EObjectType type,
        const TCreateNodeOptions& options),
        (path, type, options))
    IMPLEMENT_METHOD(TLockId, LockNode, (
        const TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options),
        (path, mode, options))
    IMPLEMENT_METHOD(TNodeId, CopyNode, (
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TCopyNodeOptions& options),
        (srcPath, dstPath, options))
    IMPLEMENT_METHOD(TNodeId, MoveNode, (
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TMoveNodeOptions& options),
        (srcPath, dstPath, options))
    IMPLEMENT_METHOD(TNodeId, LinkNode, (
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TLinkNodeOptions& options),
        (srcPath, dstPath, options))
    IMPLEMENT_METHOD(bool, NodeExists, (
        const TYPath& path,
        const TNodeExistsOptions& options),
        (path, options))


    IMPLEMENT_METHOD(TObjectId, CreateObject, (
        EObjectType type,
        const TCreateObjectOptions& options),
        (type, options))


    virtual IFileReaderPtr CreateFileReader(
        const TYPath& path,
        const TFileReaderOptions& options,
        TFileReaderConfigPtr config) override
    {
        return NApi::CreateFileReader(
            this,
            path,
            options,
            config);
    }

    virtual IFileWriterPtr CreateFileWriter(
        const TYPath& path,
        const TFileWriterOptions& options,
        TFileWriterConfigPtr config) override
    {
        return NApi::CreateFileWriter(
            this,
            path,
            options,
            config);
    }


    virtual IJournalReaderPtr CreateJournalReader(
        const TYPath& path,
        const TJournalReaderOptions& options,
        TJournalReaderConfigPtr config) override
    {
        return NApi::CreateJournalReader(
            this,
            path,
            options,
            config);
    }

    virtual IJournalWriterPtr CreateJournalWriter(
        const TYPath& path,
        const TJournalWriterOptions& options,
        TJournalWriterConfigPtr config) override
    {
        return NApi::CreateJournalWriter(
            this,
            path,
            options,
            config);
    }


    IMPLEMENT_METHOD(void, AddMember, (
        const Stroka& group,
        const Stroka& member,
        const TAddMemberOptions& options),
        (group, member, options))
    IMPLEMENT_METHOD(void, RemoveMember, (
        const Stroka& group,
        const Stroka& member,
        const TRemoveMemberOptions& options),
        (group, member, options))
    IMPLEMENT_METHOD(TCheckPermissionResult, CheckPermission, (
        const Stroka& user,
        const TYPath& path,
        EPermission permission,
        const TCheckPermissionOptions& options),
        (user, path, permission, options))

#undef DROP_BRACES
#undef IMPLEMENT_METHOD

    IChannelPtr GetTabletChannel(const TTabletCellId& cellId)
    {
        const auto& cellDirectory = Connection_->GetCellDirectory();
        auto channel = cellDirectory->GetChannelOrThrow(cellId);
        if (Options_.User != NSecurityClient::RootUserName) {
            channel = CreateAuthenticatedChannel(std::move(channel), Options_.User);
        }
        return channel;
    }

private:
    friend class TTransaction;

    IConnectionPtr Connection_;
    TClientOptions Options_;

    IChannelPtr MasterChannel_;
    IChannelPtr SchedulerChannel_;
    IChannelFactoryPtr NodeChannelFactory_;
    TTransactionManagerPtr TransactionManager_;
    TQueryHelperPtr QueryHelper_;
    std::unique_ptr<TObjectServiceProxy> ObjectProxy_;
    IInvokerPtr Invoker_;
    

    template <class TResult, class TSignature>
    TFuture<TResult> Execute(TCallback<TSignature> callback)
    {
        return callback
            .AsyncVia(Invoker_)
            .Run();
    }


    TTableMountInfoPtr SyncGetTableInfo(const TYPath& path)
    {
        const auto& tableMountCache = Connection_->GetTableMountCache();
        return WaitFor(tableMountCache->GetTableInfo(path)).ValueOrThrow();
    }

    static TTabletInfoPtr SyncGetTabletInfo(
        TTableMountInfoPtr tableInfo,
        NVersionedTableClient::TKey key)
    {
        auto tabletInfo = tableInfo->GetTablet(key);
        if (tabletInfo->State != ETabletState::Mounted) {
            THROW_ERROR_EXCEPTION("Tablet %v of table %v is in %Qlv state",
                tabletInfo->TabletId,
                tableInfo->Path,
                tabletInfo->State);
        }
        return tabletInfo;
    }


    static void GenerateMutationId(IClientRequestPtr request, TMutatingOptions& commandOptions)
    {
        SetMutationId(request, GenerateMutationId(commandOptions));
    }

    static TMutationId GenerateMutationId(TMutatingOptions& commandOptions)
    {
        if (commandOptions.MutationId == NullMutationId) {
            commandOptions.MutationId = NRpc::GenerateMutationId();
        }
        auto result = commandOptions.MutationId;
        ++commandOptions.MutationId.Parts32[0];
        return result;
    }
    

    TTransactionId GetTransactionId(const TTransactionalOptions& commandOptions, bool allowNullTransaction)
    {
        auto transaction = GetTransaction(commandOptions, allowNullTransaction, true);
        return transaction ? transaction->GetId() : NullTransactionId;
    }

    NTransactionClient::TTransactionPtr GetTransaction(
        const TTransactionalOptions& commandOptions,
        bool allowNullTransaction,
        bool pingTransaction)
    {
        if (commandOptions.TransactionId == NullTransactionId) {
            if (!allowNullTransaction) {
                THROW_ERROR_EXCEPTION("A valid master transaction is required");
            }
            return nullptr;
        }

        if (TypeFromId(commandOptions.TransactionId) != EObjectType::Transaction) {
            THROW_ERROR_EXCEPTION("A valid master transaction is required");
        }

        TTransactionAttachOptions attachOptions(commandOptions.TransactionId);
        attachOptions.AutoAbort = false;
        attachOptions.Ping = pingTransaction;
        attachOptions.PingAncestors = commandOptions.PingAncestors;
        return TransactionManager_->Attach(attachOptions);
    }

    void SetTransactionId(
        IClientRequestPtr request,
        const TTransactionalOptions& commandOptions,
        bool allowNullTransaction)
    {
        NCypressClient::SetTransactionId(request, GetTransactionId(commandOptions, allowNullTransaction));
    }

    void SetPrerequisites(
        TObjectServiceProxy::TReqExecuteBatchPtr batchReq,
        const TPrerequisiteOptions& options)
    {
        for (const auto& id : options.PrerequisiteTransactionIds) {
            batchReq->PrerequisiteTransactions().push_back(TObjectServiceProxy::TPrerequisiteTransaction(id));
        }
    }


    static void SetSuppressAccessTracking(
        IClientRequestPtr request,
        const TSuppressableAccessTrackingOptions& commandOptions)
    {
        NCypressClient::SetSuppressAccessTracking(
            &request->Header(),
            commandOptions.SuppressAccessTracking);
    }


    class TTabletLookupSession
        : public TIntrinsicRefCounted
    {
    public:
        TTabletLookupSession(
            TClient* owner,
            TTabletInfoPtr tabletInfo,
            const TLookupRowsOptions& options,
            const TNameTableToSchemaIdMapping& idMapping)
            : Config_(owner->Connection_->GetConfig())
            , TabletId_(tabletInfo->TabletId)
            , Options_(options)
            , IdMapping_(idMapping)
        { }

        void AddKey(int index, NVersionedTableClient::TKey key)
        {
            if (Batches_.empty() || Batches_.back()->Indexes.size() >= Config_->MaxRowsPerReadRequest) {
                Batches_.emplace_back(new TBatch());
            }

            auto& batch = Batches_.back();
            batch->Indexes.push_back(index);
            batch->Keys.push_back(key);
        }

        TFuture<void> Invoke(IChannelPtr channel)
        {
            // Do all the heavy lifting here.
            for (auto& batch : Batches_) {
                TReqLookupRows req;
                if (!Options_.ColumnFilter.All) {
                    ToProto(req.mutable_column_filter()->mutable_indexes(), Options_.ColumnFilter.Indexes);
                }

                TWireProtocolWriter writer;
                writer.WriteCommand(EWireProtocolCommand::LookupRows);
                writer.WriteMessage(req);
                writer.WriteUnversionedRowset(batch->Keys, &IdMapping_);

                batch->RequestData = NCompression::CompressWithEnvelope(
                    writer.Flush(),
                    Config_->LookupRequestCodec);
            }

            InvokeChannel_ = channel;
            InvokeNextBatch();
            return InvokePromise_;
        }

        void ParseResponse(
            std::vector<TUnversionedRow>* resultRows,
            std::vector<std::unique_ptr<TWireProtocolReader>>* readers)
        {
            for (const auto& batch : Batches_) {
                auto data = NCompression::DecompressWithEnvelope(batch->Response->Attachments());
                auto reader = std::make_unique<TWireProtocolReader>(data);
                for (int index = 0; index < batch->Keys.size(); ++index) {
                    auto row = reader->ReadUnversionedRow();
                    (*resultRows)[batch->Indexes[index]] = row;
                }
                readers->push_back(std::move(reader));
            }
        }

    private:
        TConnectionConfigPtr Config_;
        TTabletId TabletId_;
        TLookupRowsOptions Options_;
        TNameTableToSchemaIdMapping IdMapping_;

        struct TBatch
        {
            std::vector<int> Indexes;
            std::vector<NVersionedTableClient::TKey> Keys;
            std::vector<TSharedRef> RequestData;
            TTabletServiceProxy::TRspReadPtr Response;
        };

        std::vector<std::unique_ptr<TBatch>> Batches_;           

        IChannelPtr InvokeChannel_;
        int InvokeBatchIndex_ = 0;
        TPromise<void> InvokePromise_ = NewPromise<void>();


        void InvokeNextBatch()
        {
            if (InvokeBatchIndex_ >= Batches_.size()) {
                InvokePromise_.Set(TError());
                return;
            }

            const auto& batch = Batches_[InvokeBatchIndex_];

            TTabletServiceProxy proxy(InvokeChannel_);
            auto req = proxy.Read()
                ->SetRequestAck(false);
            ToProto(req->mutable_tablet_id(), TabletId_);
            req->set_timestamp(Options_.Timestamp);
            req->set_response_codec(static_cast<int>(Config_->LookupResponseCodec));
            req->Attachments() = std::move(batch->RequestData);

            req->Invoke().Subscribe(
                BIND(&TTabletLookupSession::OnResponse, MakeStrong(this)));
        }

        void OnResponse(const TTabletServiceProxy::TErrorOrRspReadPtr& rspOrError)
        {
            if (rspOrError.IsOK()) {
                Batches_[InvokeBatchIndex_]->Response = rspOrError.Value();
                ++InvokeBatchIndex_;
                InvokeNextBatch();
            } else {
                InvokePromise_.Set(rspOrError);
            }
        }

    };

    typedef TIntrusivePtr<TTabletLookupSession> TLookupTabletSessionPtr;

    IRowsetPtr DoLookupRows(
        const TYPath& path,
        TNameTablePtr nameTable,
        const std::vector<NVersionedTableClient::TKey>& keys,
        const TLookupRowsOptions& options)
    {
        auto tableInfo = SyncGetTableInfo(path);

        int schemaColumnCount = static_cast<int>(tableInfo->Schema.Columns().size());
        int keyColumnCount = static_cast<int>(tableInfo->KeyColumns.size());

        ValidateColumnFilter(options.ColumnFilter, schemaColumnCount);

        auto resultSchema = tableInfo->Schema.Filter(options.ColumnFilter);
        auto idMapping = BuildColumnIdMapping(tableInfo, nameTable);

        // Server-side is specifically optimized for handling long runs of keys
        // from the same partition. Let's sort the keys to facilitate this.
        typedef std::pair<int, NVersionedTableClient::TKey> TIndexedKey;
        std::vector<TIndexedKey> sortedKeys;
        sortedKeys.reserve(keys.size());
        for (int index = 0; index < static_cast<int>(keys.size()); ++index) {
            sortedKeys.push_back(std::make_pair(index, keys[index]));
        }
        std::sort(
            sortedKeys.begin(),
            sortedKeys.end(),
            [] (const TIndexedKey& lhs, const TIndexedKey& rhs) {
                return lhs.second < rhs.second;
            });

        yhash_map<TTabletInfoPtr, TLookupTabletSessionPtr> tabletToSession;

        for (const auto& pair : sortedKeys) {
            int index = pair.first;
            auto key = pair.second;
            ValidateClientKey(key, keyColumnCount, tableInfo->Schema);
            auto tabletInfo = SyncGetTabletInfo(tableInfo, key);
            auto it = tabletToSession.find(tabletInfo);
            if (it == tabletToSession.end()) {
                it = tabletToSession.insert(std::make_pair(
                    tabletInfo,
                    New<TTabletLookupSession>(this, tabletInfo, options, idMapping))).first;
            }
            const auto& session = it->second;
            session->AddKey(index, key);
        }

        auto collector = New<TParallelCollector<void>>();
        for (const auto& pair : tabletToSession) {
            const auto& tabletInfo = pair.first;
            const auto& session = pair.second;
            auto channel = GetTabletChannel(tabletInfo->CellId);
            collector->Collect(session->Invoke(std::move(channel)));
        }

        WaitFor(collector->Complete()).ThrowOnError();

        std::vector<TUnversionedRow> resultRows;
        resultRows.resize(keys.size());
        
        std::vector<std::unique_ptr<TWireProtocolReader>> readers;

        for (const auto& pair : tabletToSession) {
            const auto& session = pair.second;
            session->ParseResponse(&resultRows, &readers);
        }

        if (!options.KeepMissingRows) {
            resultRows.erase(
                std::remove_if(
                    resultRows.begin(),
                    resultRows.end(),
                    [] (TUnversionedRow row) {
                        return !static_cast<bool>(row);
                    }),
                resultRows.end());
        }

        return CreateRowset(
            std::move(readers),
            resultSchema,
            std::move(resultRows));
    }

    TQueryStatistics DoSelectRows(
        const Stroka& query,
        ISchemafulWriterPtr writer,
        TSelectRowsOptions options)
    {
        auto fragment = PreparePlanFragment(
            QueryHelper_.Get(),
            query,
            options.InputRowLimit.Get(Connection_->GetConfig()->DefaultInputRowLimit),
            options.OutputRowLimit.Get(Connection_->GetConfig()->DefaultOutputRowLimit),
            options.Timestamp);
        return WaitFor(QueryHelper_->Execute(fragment, writer)).ValueOrThrow();
    }


    void DoMountTable(
        const TYPath& path,
        const TMountTableOptions& options)
    {
        auto req = TTableYPathProxy::Mount(path);
        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_last_tablet_index(*options.LastTabletIndex);
        }
        if (options.CellId != NullTabletCellId) {
            ToProto(req->mutable_cell_id(), options.CellId);
        }

        WaitFor(ObjectProxy_->Execute(req)).ThrowOnError();
    }

    void DoUnmountTable(
        const TYPath& path,
        const TUnmountTableOptions& options)
    {
        auto req = TTableYPathProxy::Unmount(path);
        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_first_tablet_index(*options.LastTabletIndex);
        }
        req->set_force(options.Force);

        WaitFor(ObjectProxy_->Execute(req)).ThrowOnError();
    }

    void DoRemountTable(
        const TYPath& path,
        const TRemountTableOptions& options)
    {
        auto req = TTableYPathProxy::Remount(path);
        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_first_tablet_index(*options.LastTabletIndex);
        }

        WaitFor(ObjectProxy_->Execute(req)).ThrowOnError();
    }

    void DoReshardTable(
        const TYPath& path,
        const std::vector<NVersionedTableClient::TKey>& pivotKeys,
        const TReshardTableOptions& options)
    {
        auto req = TTableYPathProxy::Reshard(path);
        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_last_tablet_index(*options.LastTabletIndex);
        }
        ToProto(req->mutable_pivot_keys(), pivotKeys);

        WaitFor(ObjectProxy_->Execute(req)).ThrowOnError();
    }


    TYsonString DoGetNode(
        const TYPath& path,
        TGetNodeOptions options)
    {
        auto req = TYPathProxy::Get(path);
        SetTransactionId(req, options, true);
        SetSuppressAccessTracking(req, options);

        ToProto(req->mutable_attribute_filter(), options.AttributeFilter);
        if (options.MaxSize) {
            req->set_max_size(*options.MaxSize);
        }
        req->set_ignore_opaque(options.IgnoreOpaque);
        if (options.Options) {
            ToProto(req->mutable_options(), *options.Options);
        }

        auto rsp = WaitFor(ObjectProxy_->Execute(req)).ValueOrThrow();

        return TYsonString(rsp->value());
    }

    void DoSetNode(
        const TYPath& path,
        const TYsonString& value,
        TSetNodeOptions options)
    {
        auto batchReq = ObjectProxy_->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TYPathProxy::Set(path);
        SetTransactionId(req, options, true);
        GenerateMutationId(req, options);
        req->set_value(value.Data());
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke()).ValueOrThrow();
        batchRsp->GetResponse<TYPathProxy::TRspSet>(0).ThrowOnError();
    }

    void DoRemoveNode(
        const TYPath& path,
        TRemoveNodeOptions options)
    {
        auto batchReq = ObjectProxy_->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TYPathProxy::Remove(path);
        SetTransactionId(req, options, true);
        GenerateMutationId(req, options);
        req->set_recursive(options.Recursive);
        req->set_force(options.Force);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke()).ValueOrThrow();
        batchRsp->GetResponse<TYPathProxy::TRspRemove>(0).ThrowOnError();
    }

    TYsonString DoListNodes(
        const TYPath& path,
        TListNodesOptions options)
    {
        auto req = TYPathProxy::List(path);
        SetTransactionId(req, options, true);
        SetSuppressAccessTracking(req, options);

        ToProto(req->mutable_attribute_filter(), options.AttributeFilter);
        if (options.MaxSize) {
            req->set_max_size(*options.MaxSize);
        }

        auto rsp = WaitFor(ObjectProxy_->Execute(req)).ValueOrThrow();

        return TYsonString(rsp->keys());
    }

    TNodeId DoCreateNode(
        const TYPath& path,
        EObjectType type,
        TCreateNodeOptions options)
    {
        auto batchReq = ObjectProxy_->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TCypressYPathProxy::Create(path);
        SetTransactionId(req, options, true);
        GenerateMutationId(req, options);
        req->set_type(static_cast<int>(type));
        req->set_recursive(options.Recursive);
        req->set_ignore_existing(options.IgnoreExisting);
        if (options.Attributes) {
            ToProto(req->mutable_node_attributes(), *options.Attributes);
        }
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke()).ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCreate>(0).ValueOrThrow();
        return FromProto<TNodeId>(rsp->node_id());
    }

    TLockId DoLockNode(
        const TYPath& path,
        NCypressClient::ELockMode mode,
        TLockNodeOptions options)
    {
        auto batchReq = ObjectProxy_->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TCypressYPathProxy::Lock(path);
        SetTransactionId(req, options, false);
        GenerateMutationId(req, options);
        req->set_mode(static_cast<int>(mode));
        req->set_waitable(options.Waitable);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke()).ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspLock>(0).ValueOrThrow();
        return FromProto<TLockId>(rsp->lock_id());
    }

    TNodeId DoCopyNode(
        const TYPath& srcPath,
        const TYPath& dstPath,
        TCopyNodeOptions options)
    {
        auto batchReq = ObjectProxy_->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TCypressYPathProxy::Copy(dstPath);
        SetTransactionId(req, options, true);
        GenerateMutationId(req, options);
        req->set_source_path(srcPath);
        req->set_preserve_account(options.PreserveAccount);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke()).ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCopy>(0).ValueOrThrow();
        return FromProto<TNodeId>(rsp->object_id());
    }

    TNodeId DoMoveNode(
        const TYPath& srcPath,
        const TYPath& dstPath,
        TMoveNodeOptions options)
    {
        auto batchReq = ObjectProxy_->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TCypressYPathProxy::Copy(dstPath);
        SetTransactionId(req, options, true);
        GenerateMutationId(req, options);
        req->set_source_path(srcPath);
        req->set_preserve_account(options.PreserveAccount);
        req->set_remove_source(true);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke()).ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCopy>(0).ValueOrThrow();
        return FromProto<TNodeId>(rsp->object_id());
    }

    TNodeId DoLinkNode(
        const TYPath& srcPath,
        const TYPath& dstPath,
        TLinkNodeOptions options)
    {
        auto batchReq = ObjectProxy_->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TCypressYPathProxy::Create(dstPath);
        req->set_type(static_cast<int>(EObjectType::Link));
        req->set_recursive(options.Recursive);
        req->set_ignore_existing(options.IgnoreExisting);
        SetTransactionId(req, options, true);
        GenerateMutationId(req, options);
        auto attributes = options.Attributes ? ConvertToAttributes(options.Attributes) : CreateEphemeralAttributes();
        attributes->Set("target_path", srcPath);
        ToProto(req->mutable_node_attributes(), *attributes);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke()).ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCreate>(0).ValueOrThrow();
        return FromProto<TNodeId>(rsp->node_id());
    }

    bool DoNodeExists(
        const TYPath& path,
        const TNodeExistsOptions& options)
    {
        auto req = TYPathProxy::Exists(path);
        SetTransactionId(req, options, true);

        auto rsp = WaitFor(ObjectProxy_->Execute(req)).ValueOrThrow();
        return rsp->value();
    }


    TObjectId DoCreateObject(
        EObjectType type,
        TCreateObjectOptions options)
    {
        auto batchReq = ObjectProxy_->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TMasterYPathProxy::CreateObjects();
        GenerateMutationId(req, options);
        if (options.TransactionId != NullTransactionId) {
            ToProto(req->mutable_transaction_id(), options.TransactionId);
        }
        req->set_type(static_cast<int>(type));
        if (options.Attributes) {
            ToProto(req->mutable_object_attributes(), *options.Attributes);
        }
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke()).ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObjects>(0).ValueOrThrow();
        return FromProto<TObjectId>(rsp->object_ids(0));
    }


    static Stroka GetGroupPath(const Stroka& name)
    {
        return "//sys/groups/" + ToYPathLiteral(name);
    }

    void DoAddMember(
        const Stroka& group,
        const Stroka& member,
        TAddMemberOptions options)
    {
        auto req = TGroupYPathProxy::AddMember(GetGroupPath(group));
        req->set_name(member);
        GenerateMutationId(req, options);

        WaitFor(ObjectProxy_->Execute(req)).ThrowOnError();
    }

    void DoRemoveMember(
        const Stroka& group,
        const Stroka& member,
        TRemoveMemberOptions options)
    {
        auto req = TGroupYPathProxy::RemoveMember(GetGroupPath(group));
        req->set_name(member);
        GenerateMutationId(req, options);

        WaitFor(ObjectProxy_->Execute(req)).ThrowOnError();
    }

    TCheckPermissionResult DoCheckPermission(
        const Stroka& user,
        const TYPath& path,
        EPermission permission,
        TCheckPermissionOptions options)
    {
        auto req = TObjectYPathProxy::CheckPermission(path);
        req->set_user(user);
        req->set_permission(static_cast<int>(permission));
        SetTransactionId(req, options, true);

        auto rsp = WaitFor(ObjectProxy_->Execute(req)).ValueOrThrow();

        TCheckPermissionResult result;
        result.Action = ESecurityAction(rsp->action());
        result.ObjectId = rsp->has_object_id() ? FromProto<TObjectId>(rsp->object_id()) : NullObjectId;
        result.Subject = rsp->has_subject() ? MakeNullable(rsp->subject()) : Null;
        return result;
    }

};

DEFINE_REFCOUNTED_TYPE(TClient)

IClientPtr CreateClient(IConnectionPtr connection, const TClientOptions& options)
{
    YCHECK(connection);

    return New<TClient>(std::move(connection), options);
}

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public ITransaction
{
public:
    TTransaction(
        TClientPtr client,
        NTransactionClient::TTransactionPtr transaction)
        : Client_(std::move(client))
        , Transaction_(std::move(transaction))
        , TransactionStartCollector_(New<TParallelCollector<void>>())
    { }


    virtual IConnectionPtr GetConnection() override
    {
        return Client_->GetConnection();
    }

    virtual IClientPtr GetClient() const override
    {
        return Client_;
    }

    virtual NTransactionClient::ETransactionType GetType() const override
    {
        return Transaction_->GetType();
    }

    virtual const TTransactionId& GetId() const override
    {
        return Transaction_->GetId();
    }

    virtual TTimestamp GetStartTimestamp() const override
    {
        return Transaction_->GetStartTimestamp();
    }


    virtual TFuture<void> Commit(const TTransactionCommitOptions& options) override
    {
        return BIND(&TTransaction::DoCommit, MakeStrong(this))
            .AsyncVia(Client_->Invoker_)
            .Run(options);
    }

    virtual TFuture<void> Abort(const TTransactionAbortOptions& options) override
    {
        return Transaction_->Abort(options);
    }


    virtual TFuture<ITransactionPtr> StartTransaction(
        ETransactionType type,
        const TTransactionStartOptions& options) override
    {
        auto adjustedOptions = options;
        adjustedOptions.ParentId = GetId();
        return Client_->StartTransaction(
            type,
            adjustedOptions);
    }

        
    virtual void WriteRow(
        const TYPath& path,
        TNameTablePtr nameTable,
        TUnversionedRow row,
        const TWriteRowsOptions& options) override
    {
        WriteRows(
            path,
            std::move(nameTable),
            std::vector<TUnversionedRow>(1, row),
            options);
    }

    virtual void WriteRows(
        const TYPath& path,
        TNameTablePtr nameTable,
        std::vector<TUnversionedRow> rows,
        const TWriteRowsOptions& options) override
    {
        Requests_.push_back(std::unique_ptr<TRequestBase>(new TWriteRequest(
            this,
            path,
            std::move(nameTable),
            std::move(rows),
            options)));
    }


    virtual void DeleteRow(
        const TYPath& path,
        TNameTablePtr nameTable,
        NVersionedTableClient::TKey key,
        const TDeleteRowsOptions& options) override
    {
        DeleteRows(
            path,
            std::move(nameTable),
            std::vector<NVersionedTableClient::TKey>(1, key),
            options);
    }

    virtual void DeleteRows(
        const TYPath& path,
        TNameTablePtr nameTable,
        std::vector<NVersionedTableClient::TKey> keys,
        const TDeleteRowsOptions& options) override
    {
        Requests_.push_back(std::unique_ptr<TRequestBase>(new TDeleteRequest(
            this,
            path,
            std::move(nameTable),
            std::move(keys),
            options)));
    }


#define DELEGATE_TRANSACTIONAL_METHOD(returnType, method, signature, args) \
    virtual returnType method signature override \
    { \
        auto& originalOptions = options; \
        { \
            auto options = originalOptions; \
            options.TransactionId = GetId(); \
            return Client_->method args; \
        } \
    }

#define DELEGATE_TIMESTAMPTED_METHOD(returnType, method, signature, args) \
    virtual returnType method signature override \
    { \
        auto& originalOptions = options; \
        { \
            auto options = originalOptions; \
            options.Timestamp = GetStartTimestamp(); \
            return Client_->method args; \
        } \
    }

    DELEGATE_TIMESTAMPTED_METHOD(TFuture<IRowsetPtr>, LookupRow, (
        const TYPath& path,
        TNameTablePtr nameTable,
        NVersionedTableClient::TKey key,
        const TLookupRowsOptions& options),
        (path, nameTable, key, options))
    DELEGATE_TIMESTAMPTED_METHOD(TFuture<IRowsetPtr>, LookupRows, (
        const TYPath& path,
        TNameTablePtr nameTable,
        const std::vector<NVersionedTableClient::TKey>& keys,
        const TLookupRowsOptions& options),
        (path, nameTable, keys, options))
    DELEGATE_TIMESTAMPTED_METHOD(TFuture<NQueryClient::TQueryStatistics>, SelectRows, (
        const Stroka& query,
        ISchemafulWriterPtr writer,
        const TSelectRowsOptions& options),
        (query, writer, options))

    typedef std::pair<IRowsetPtr, NQueryClient::TQueryStatistics> TSelectRowsResult;
    DELEGATE_TIMESTAMPTED_METHOD(TFuture<TSelectRowsResult>, SelectRows, (
        const Stroka& query,
        const TSelectRowsOptions& options),
        (query, options))


    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TYsonString>, GetNode, (
        const TYPath& path,
        const TGetNodeOptions& options),
        (path, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<void>, SetNode, (
        const TYPath& path,
        const TYsonString& value,
        const TSetNodeOptions& options),
        (path, value, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<void>, RemoveNode, (
        const TYPath& path,
        const TRemoveNodeOptions& options),
        (path, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TYsonString>, ListNodes, (
        const TYPath& path,
        const TListNodesOptions& options),
        (path, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TNodeId>, CreateNode, (
        const TYPath& path,
        EObjectType type,
        const TCreateNodeOptions& options),
        (path, type, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TLockId>, LockNode, (
        const TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options),
        (path, mode, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TNodeId>, CopyNode, (
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TCopyNodeOptions& options),
        (srcPath, dstPath, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TNodeId>, MoveNode, (
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TMoveNodeOptions& options),
        (srcPath, dstPath, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TNodeId>, LinkNode, (
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TLinkNodeOptions& options),
        (srcPath, dstPath, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<bool>, NodeExists, (
        const TYPath& path,
        const TNodeExistsOptions& options),
        (path, options))


    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TObjectId>, CreateObject, (
        EObjectType type,
        const TCreateObjectOptions& options),
        (type, options))


    DELEGATE_TRANSACTIONAL_METHOD(IFileReaderPtr, CreateFileReader, (
        const TYPath& path,
        const TFileReaderOptions& options,
        TFileReaderConfigPtr config),
        (path, options, config))
    DELEGATE_TRANSACTIONAL_METHOD(IFileWriterPtr, CreateFileWriter, (
        const TYPath& path,
        const TFileWriterOptions& options,
        TFileWriterConfigPtr config),
        (path, options, config))

    DELEGATE_TRANSACTIONAL_METHOD(IJournalReaderPtr, CreateJournalReader, (
        const TYPath& path,
        const TJournalReaderOptions& options,
        TJournalReaderConfigPtr config),
        (path, options, config))
    DELEGATE_TRANSACTIONAL_METHOD(IJournalWriterPtr, CreateJournalWriter, (
        const TYPath& path,
        const TJournalWriterOptions& options,
        TJournalWriterConfigPtr config),
        (path, options, config))

#undef DELEGATE_TRANSACTIONAL_METHOD
#undef DELEGATE_TIMESTAMPTED_METHOD

private:
    TClientPtr Client_;
    NTransactionClient::TTransactionPtr Transaction_;

    class TRequestBase
    {
    public:
        virtual void Run()
        {
            TableInfo_ = Transaction_->Client_->SyncGetTableInfo(Path_);
        }

    protected:
        explicit TRequestBase(
            TTransaction* transaction,
            const TYPath& path,
            TNameTablePtr nameTable)
            : Transaction_(transaction)
            , Path_(path)
            , NameTable_(std::move(nameTable))
        { }

        TTransaction* Transaction_;
        TYPath Path_;
        TNameTablePtr NameTable_;

        TTableMountInfoPtr TableInfo_;

    };

    class TWriteRequest
        : public TRequestBase
    {
    public:
        TWriteRequest(
            TTransaction* transaction,
            const TYPath& path,
            TNameTablePtr nameTable,
            std::vector<TUnversionedRow> rows,
            const TWriteRowsOptions& options)
            : TRequestBase(transaction, path, std::move(nameTable))
            , Rows_(std::move(rows))
            , Options_(options)
        { }

        virtual void Run() override
        {
            TRequestBase::Run();

            const auto& idMapping = Transaction_->GetColumnIdMapping(TableInfo_, NameTable_);
            int keyColumnCount = static_cast<int>(TableInfo_->KeyColumns.size());

            TReqWriteRow req;
            req.set_lock_mode(static_cast<int>(Options_.LockMode));

            for (auto row : Rows_) {
                ValidateClientDataRow(row, keyColumnCount, idMapping, TableInfo_->Schema);
                auto tabletInfo = Transaction_->Client_->SyncGetTabletInfo(TableInfo_, row);
                auto* writer = Transaction_->GetTabletWriter(tabletInfo);
                writer->WriteCommand(EWireProtocolCommand::WriteRow);
                writer->WriteMessage(req);
                writer->WriteUnversionedRow(row, &idMapping);
            }
        }

    private:
        std::vector<TUnversionedRow> Rows_;
        TWriteRowsOptions Options_;

    };

    class TDeleteRequest
        : public TRequestBase
    {
    public:
        TDeleteRequest(
            TTransaction* transaction,
            const TYPath& path,
            TNameTablePtr nameTable,
            std::vector<NVersionedTableClient::TKey> keys,
            const TDeleteRowsOptions& options)
            : TRequestBase(transaction, path, std::move(nameTable))
            , Keys_(std::move(keys))
            , Options_(options)
        { }

        virtual void Run() override
        {
            TRequestBase::Run();

            const auto& idMapping = Transaction_->GetColumnIdMapping(TableInfo_, NameTable_);
            int keyColumnCount = static_cast<int>(TableInfo_->KeyColumns.size());
            for (auto key : Keys_) {
                ValidateClientKey(key, keyColumnCount, TableInfo_->Schema);
                
                TReqDeleteRow req;

                auto tabletInfo = Transaction_->Client_->SyncGetTabletInfo(TableInfo_, key);
                auto* writer = Transaction_->GetTabletWriter(tabletInfo);
                writer->WriteCommand(EWireProtocolCommand::DeleteRow);
                writer->WriteMessage(req);
                writer->WriteUnversionedRow(key, &idMapping);
            }
        }

    private:
        std::vector<TUnversionedRow> Keys_;
        TDeleteRowsOptions Options_;

    };

    std::vector<std::unique_ptr<TRequestBase>> Requests_;

    class TTabletCommitSession
        : public TIntrinsicRefCounted
    {
    public:
        TTabletCommitSession(
            TTransactionPtr owner,
            TTabletInfoPtr tabletInfo)
            : TransactionId_(owner->Transaction_->GetId())
            , TabletId_(tabletInfo->TabletId)
            , Config_(owner->Client_->Connection_->GetConfig())
        { }
        
        TWireProtocolWriter* GetWriter()
        {
            if (Batches_.empty() || CurrentRowCount_ >= Config_->MaxRowsPerWriteRequest) {
                Batches_.emplace_back(new TBatch());
                CurrentRowCount_ = 0;
            }
            ++CurrentRowCount_;
            return &Batches_.back()->Writer;
        }

        TFuture<void> Invoke(IChannelPtr channel)
        {
            // Do all the heavy lifting here.
            for (auto& batch : Batches_) {
                batch->RequestData = NCompression::CompressWithEnvelope(
                    batch->Writer.Flush(),
                    Config_->WriteRequestCodec);;
            }

            InvokeChannel_ = channel;
            InvokeNextBatch();
            return InvokePromise_;
        }

    private:
        TTransactionId TransactionId_;
        TTabletId TabletId_;
        TConnectionConfigPtr Config_;

        struct TBatch
        {
            TWireProtocolWriter Writer;
            std::vector<TSharedRef> RequestData;
        };

        std::vector<std::unique_ptr<TBatch>> Batches_;
        int CurrentRowCount_ = 0; // in the current batch
        
        IChannelPtr InvokeChannel_;
        int InvokeBatchIndex_ = 0;
        TPromise<void> InvokePromise_ = NewPromise<void>();


        void InvokeNextBatch()
        {
            if (InvokeBatchIndex_ >= Batches_.size()) {
                InvokePromise_.Set(TError());
                return;
            }

            const auto& batch = Batches_[InvokeBatchIndex_];

            TTabletServiceProxy proxy(InvokeChannel_);
            auto req = proxy.Write()
                ->SetRequestAck(false);
            ToProto(req->mutable_transaction_id(), TransactionId_);
            ToProto(req->mutable_tablet_id(), TabletId_);
            req->Attachments() = std::move(batch->RequestData);

            req->Invoke().Subscribe(
                BIND(&TTabletCommitSession::OnResponse, MakeStrong(this)));
        }

        void OnResponse(const TTabletServiceProxy::TErrorOrRspWritePtr& rspOrError)
        {
            if (rspOrError.IsOK()) {
                ++InvokeBatchIndex_;
                InvokeNextBatch();
            } else {
                InvokePromise_.Set(rspOrError);
            }
        }

    };

    typedef TIntrusivePtr<TTabletCommitSession> TTabletSessionPtr;

    yhash_map<TTabletInfoPtr, TTabletSessionPtr> TabletToSession_;
    
    TIntrusivePtr<TParallelCollector<void>> TransactionStartCollector_;

    // Maps ids from name table to schema, for each involved name table.
    yhash_map<TNameTablePtr, TNameTableToSchemaIdMapping> NameTableToIdMapping_;


    const TNameTableToSchemaIdMapping& GetColumnIdMapping(const TTableMountInfoPtr& tableInfo, const TNameTablePtr& nameTable)
    {
        auto it = NameTableToIdMapping_.find(nameTable);
        if (it == NameTableToIdMapping_.end()) {
            auto mapping = BuildColumnIdMapping(tableInfo, nameTable);
            it = NameTableToIdMapping_.insert(std::make_pair(nameTable, std::move(mapping))).first;
        }
        return it->second;
    }

    TWireProtocolWriter* GetTabletWriter(const TTabletInfoPtr& tabletInfo)
    {
        auto it = TabletToSession_.find(tabletInfo);
        if (it == TabletToSession_.end()) {
            TransactionStartCollector_->Collect(Transaction_->AddTabletParticipant(tabletInfo->CellId));
            it = TabletToSession_.insert(std::make_pair(
                tabletInfo,
                New<TTabletCommitSession>(this, tabletInfo))).first;
        }
        return it->second->GetWriter();
    }

    void DoCommit(const TTransactionCommitOptions& options)
    {
        try {
            for (const auto& request : Requests_) {
                request->Run();
            }

            WaitFor(TransactionStartCollector_->Complete()).ThrowOnError();


            auto writeCollector = New<TParallelCollector<void>>();
            for (const auto& pair : TabletToSession_) {
                const auto& tabletInfo = pair.first;
                const auto& session = pair.second;
                auto channel = Client_->GetTabletChannel(tabletInfo->CellId);
                writeCollector->Collect(session->Invoke(std::move(channel)));
            }

            WaitFor(writeCollector->Complete()).ThrowOnError();
        } catch (const std::exception& ex) {
            // Fire and forget.
            Transaction_->Abort();
            throw;
        }

        WaitFor(Transaction_->Commit(options)).ThrowOnError();
    }

};

DEFINE_REFCOUNTED_TYPE(TTransaction)

TFuture<ITransactionPtr> TClient::StartTransaction(
    ETransactionType type,
    const TTransactionStartOptions& options)
{
    auto this_ = MakeStrong(this);
    return TransactionManager_->Start(type, options).Apply(
        BIND([=] (NTransactionClient::TTransactionPtr transaction) -> ITransactionPtr {
            return New<TTransaction>(this_, transaction);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

