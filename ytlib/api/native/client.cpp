#include "client.h"
#include "box.h"
#include "config.h"
#include "connection.h"
#include "transaction.h"
#include "file_reader.h"
#include "file_writer.h"
#include "journal_reader.h"
#include "journal_writer.h"
#include "table_reader.h"
#include "table_writer.h"
#include "skynet.h"
#include "private.h"

#include <yt/client/api/rowset.h>
#include <yt/client/api/operation_archive_schema.h>
#include <yt/ytlib/api/native/tablet_helpers.h>
#include <yt/client/api/file_reader.h>
#include <yt/client/api/file_writer.h>
#include <yt/client/api/journal_reader.h>
#include <yt/client/api/journal_writer.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/client/chunk_client/chunk_replica.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/chunk_teleporter.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/medium_directory.pb.h>
#include <yt/client/chunk_client/read_limit.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/file_client/file_chunk_writer.h>
#include <yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/cluster_directory.h>
#include <yt/ytlib/hive/cluster_directory_synchronizer.h>
#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/job_proxy/job_spec_helper.h>
#include <yt/ytlib/job_proxy/user_job_read_controller.h>

#include <yt/ytlib/job_prober_client/public.h>
#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/client/object_client/helpers.h>
#include <yt/ytlib/object_client/master_ypath_proxy.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/query_client/executor.h>
#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/functions_cache.h>
#include <yt/ytlib/query_client/helpers.h>
#include <yt/ytlib/query_client/query_service_proxy.h>
#include <yt/ytlib/query_client/column_evaluator.h>
#include <yt/ytlib/query_client/ast.h>
#include <yt/ytlib/query_client/query_service.pb.h>

#include <yt/ytlib/scheduler/helpers.h>
#include <yt/ytlib/scheduler/proto/job.pb.h>
#include <yt/ytlib/scheduler/job_prober_service_proxy.h>
#include <yt/ytlib/scheduler/scheduler_service_proxy.h>

#include <yt/ytlib/security_client/group_ypath_proxy.h>
#include <yt/client/security_client/helpers.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schema.h>
#include <yt/ytlib/table_client/schema_inferer.h>
#include <yt/ytlib/table_client/table_ypath_proxy.h>
#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/row_merger.h>
#include <yt/ytlib/table_client/columnar_statistics_fetcher.h>

#include <yt/client/tablet_client/table_mount_cache.h>
#include <yt/ytlib/tablet_client/tablet_service_proxy.h>
#include <yt/client/table_client/wire_protocol.h>
#include <yt/client/table_client/proto/wire_protocol.pb.h>
#include <yt/ytlib/tablet_client/table_replica_ypath.h>
#include <yt/ytlib/tablet_client/master_tablet_service.h>

#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/ytlib/transaction_client/action.h>
#include <yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/async_stream_pipe.h>
#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/crypto/crypto.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/rpc/helpers.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_proxy.h>

#include <yt/core/misc/collection_helpers.h>

#include <util/string/join.h>

namespace NYT {
namespace NApi {
namespace NNative {

using namespace NCrypto;
using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NCypressClient;
using namespace NFileClient;
using namespace NTransactionClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NSecurityClient;
using namespace NQueryClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NScheduler;
using namespace NHiveClient;
using namespace NHydra;
using namespace NJobTrackerClient;

using NChunkClient::TChunkReaderStatistics;
using NChunkClient::TReadLimit;
using NChunkClient::TReadRange;
using NNodeTrackerClient::CreateNodeChannelFactory;
using NNodeTrackerClient::INodeChannelFactoryPtr;
using NNodeTrackerClient::TNetworkPreferenceList;
using NNodeTrackerClient::TNodeDescriptor;
using NTableClient::TColumnSchema;

using TTableReplicaInfoPtrList = SmallVector<TTableReplicaInfoPtr, TypicalReplicaCount>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJobInputReader)
DECLARE_REFCOUNTED_CLASS(TClient)
DECLARE_REFCOUNTED_CLASS(TTransaction)

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

namespace {

TUnversionedOwningRow CreateJobKey(const TJobId& jobId, const TNameTablePtr& nameTable)
{
    TOwningRowBuilder keyBuilder(2);

    keyBuilder.AddValue(MakeUnversionedUint64Value(jobId.Parts64[0], nameTable->GetIdOrRegisterName("job_id_hi")));
    keyBuilder.AddValue(MakeUnversionedUint64Value(jobId.Parts64[1], nameTable->GetIdOrRegisterName("job_id_lo")));

    return keyBuilder.FinishRow();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr int FileCacheHashDigitCount = 2;

NYPath::TYPath GetFilePathInCache(const TString& md5, const NYPath::TYPath cachePath)
{
    auto lastDigits = md5.substr(md5.size() - FileCacheHashDigitCount);
    return cachePath + "/" + lastDigits + "/" + md5;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TReq>
void SetDynamicTableCypressRequestFullPath(TReq* req, TYPath* fullPath)
{ }

template <>
void SetDynamicTableCypressRequestFullPath<NTabletClient::NProto::TReqMount>(
    NTabletClient::NProto::TReqMount* req,
    TYPath* fullPath)
{
    req->set_path(*fullPath);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TJobInputReader
    : public NConcurrency::IAsyncZeroCopyInputStream
{
public:
    TJobInputReader(NJobProxy::IUserJobReadControllerPtr userJobReadController, IInvokerPtr invoker)
        : Invoker_(std::move(invoker))
        , UserJobReadController_(std::move(userJobReadController))
        , AsyncStreamPipe_(New<TAsyncStreamPipe>())
    { }

    ~TJobInputReader()
    {
        if (TransferResultFuture_) {
            TransferResultFuture_.Cancel();
        }
    }

    void Open()
    {
        auto transferClosure = UserJobReadController_->PrepareJobInputTransfer(AsyncStreamPipe_);
        TransferResultFuture_ = transferClosure
            .AsyncVia(Invoker_)
            .Run();
    }

    virtual TFuture<TSharedRef> Read() override
    {
        return AsyncStreamPipe_->Read();
    }

private:
    const IInvokerPtr Invoker_;
    const NJobProxy::IUserJobReadControllerPtr UserJobReadController_;
    const NConcurrency::TAsyncStreamPipePtr AsyncStreamPipe_;

    TFuture<void> TransferResultFuture_;
};

DEFINE_REFCOUNTED_TYPE(TJobInputReader)

////////////////////////////////////////////////////////////////////////////////

class TQueryPreparer
    : public virtual TRefCounted
    , public IPrepareCallbacks
{
public:
    TQueryPreparer(
        NTabletClient::ITableMountCachePtr mountTableCache,
        IInvokerPtr invoker)
        : MountTableCache_(std::move(mountTableCache))
        , Invoker_(std::move(invoker))
    { }

    // IPrepareCallbacks implementation.

    virtual TFuture<TDataSplit> GetInitialSplit(
        const TYPath& path,
        TTimestamp timestamp) override
    {
        return BIND(&TQueryPreparer::DoGetInitialSplit, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run(path, timestamp);
    }

private:
    const NTabletClient::ITableMountCachePtr MountTableCache_;
    const IInvokerPtr Invoker_;

    TTableSchema GetTableSchema(
        const TRichYPath& path,
        const TTableMountInfoPtr& tableInfo)
    {
        if (auto maybePathSchema = path.GetSchema()) {
            if (tableInfo->Dynamic) {
                THROW_ERROR_EXCEPTION("Explicit YPath \"schema\" specification is only allowed for static tables");
            }
            return *maybePathSchema;
        }

        return tableInfo->Schemas[ETableSchemaKind::Query];
    }

    TDataSplit DoGetInitialSplit(
        const TRichYPath& path,
        TTimestamp timestamp)
    {
        auto tableInfo = WaitFor(MountTableCache_->GetTableInfo(path.GetPath()))
            .ValueOrThrow();

        tableInfo->ValidateNotReplicated();

        TDataSplit result;
        SetObjectId(&result, tableInfo->TableId);
        SetTableSchema(&result, GetTableSchema(path, tableInfo));
        SetTimestamp(&result, timestamp);
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TLookupRowsInputBufferTag
{ };

struct TLookupRowsOutputBufferTag
{ };

struct TWriteRowsBufferTag
{ };

struct TDeleteRowsBufferTag
{ };

struct TGetInSyncReplicasTag
{ };

class TClient
    : public IClient
{
public:
    TClient(
        IConnectionPtr connection,
        const TClientOptions& options)
        : Connection_(std::move(connection))
        , Options_(options)
        , ConcurrentRequestsSemaphore_(New<TAsyncSemaphore>(Connection_->GetConfig()->MaxConcurrentRequests))
    {
        auto wrapChannel = [&] (IChannelPtr channel) {
            channel = CreateAuthenticatedChannel(channel, options.GetUser());
            return channel;
        };
        auto wrapChannelFactory = [&] (IChannelFactoryPtr factory) {
            factory = CreateAuthenticatedChannelFactory(factory, options.GetUser());
            return factory;
        };

        auto initMasterChannel = [&] (EMasterChannelKind kind, TCellTag cellTag) {
            // NB: Caching is only possible for the primary master.
            if (kind == EMasterChannelKind::Cache && cellTag != Connection_->GetPrimaryMasterCellTag()) {
                return;
            }
            MasterChannels_[kind][cellTag] = wrapChannel(Connection_->GetMasterChannelOrThrow(kind, cellTag));
        };
        for (auto kind : TEnumTraits<EMasterChannelKind>::GetDomainValues()) {
            initMasterChannel(kind, Connection_->GetPrimaryMasterCellTag());
            for (auto cellTag : Connection_->GetSecondaryMasterCellTags()) {
                initMasterChannel(kind, cellTag);
            }
        }

        SchedulerChannel_ = wrapChannel(Connection_->GetSchedulerChannel());

        for (auto kind : TEnumTraits<EMasterChannelKind>::GetDomainValues()) {
            // NOTE(asaitgalin): Cache is tied to user so to utilize cache properly all Cypress
            // requests for operations archive should be performed under the same user.
            OperationsArchiveChannels_[kind] = CreateAuthenticatedChannel(
                Connection_->GetMasterChannelOrThrow(kind, PrimaryMasterCellTag),
                "application_operations");
        }

        ChannelFactory_ = CreateNodeChannelFactory(
            wrapChannelFactory(Connection_->GetChannelFactory()),
            Connection_->GetNetworks());

        SchedulerProxy_.reset(new TSchedulerServiceProxy(GetSchedulerChannel()));
        JobProberProxy_.reset(new TJobProberServiceProxy(GetSchedulerChannel()));

        TransactionManager_ = New<TTransactionManager>(
            Connection_->GetConfig()->TransactionManager,
            Connection_->GetConfig()->PrimaryMaster->CellId,
            Connection_,
            Options_.GetUser(),
            Connection_->GetTimestampProvider(),
            Connection_->GetCellDirectory());

        FunctionImplCache_ = CreateFunctionImplCache(
            Connection_->GetConfig()->FunctionImplCache,
            MakeWeak(this));

        FunctionRegistry_ = CreateFunctionRegistryCache(
            Connection_->GetConfig()->UdfRegistryPath,
            Connection_->GetConfig()->FunctionRegistryCache,
            MakeWeak(this),
            Connection_->GetInvoker());

        Logger.AddTag("ClientId: %v", TGuid::Create());
    }


    virtual NApi::IConnectionPtr GetConnection() override
    {
        return Connection_;
    }

    virtual const ITableMountCachePtr& GetTableMountCache() override
    {
        return Connection_->GetTableMountCache();
    }

    virtual const ITimestampProviderPtr& GetTimestampProvider() override
    {
        return Connection_->GetTimestampProvider();
    }

    virtual const IConnectionPtr& GetNativeConnection() override
    {
        return Connection_;
    }

    virtual IFunctionRegistryPtr GetFunctionRegistry() override
    {
        return FunctionRegistry_;
    }

    virtual TFunctionImplCachePtr GetFunctionImplCache() override
    {
        return FunctionImplCache_;
    }

    virtual const TClientOptions& GetOptions() override
    {
        return Options_;
    }

    virtual IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        TCellTag cellTag = PrimaryMasterCellTag) override
    {
        const auto& channels = MasterChannels_[kind];
        auto it = channels.find(cellTag == PrimaryMasterCellTag ? Connection_->GetPrimaryMasterCellTag() : cellTag);
        if (it == channels.end()) {
            THROW_ERROR_EXCEPTION("Unknown master cell tag %v",
                cellTag);
        }
        return it->second;
    }

    virtual IChannelPtr GetCellChannelOrThrow(const TTabletCellId& cellId) override
    {
        const auto& cellDirectory = Connection_->GetCellDirectory();
        auto channel = cellDirectory->GetChannelOrThrow(cellId);
        return CreateAuthenticatedChannel(std::move(channel), Options_.GetUser());
    }

    virtual IChannelPtr GetSchedulerChannel() override
    {
        return SchedulerChannel_;
    }

    virtual const INodeChannelFactoryPtr& GetChannelFactory() override
    {
        return ChannelFactory_;
    }

    virtual TFuture<void> Terminate() override
    {
        TransactionManager_->AbortAll();

        auto error = TError("Client terminated");
        std::vector<TFuture<void>> asyncResults;

        for (auto kind : TEnumTraits<EMasterChannelKind>::GetDomainValues()) {
            for (const auto& pair : MasterChannels_[kind]) {
                auto channel = pair.second;
                asyncResults.push_back(channel->Terminate(error));
            }
        }
        asyncResults.push_back(SchedulerChannel_->Terminate(error));

        return Combine(asyncResults);
    }


    virtual TFuture<ITransactionPtr> StartNativeTransaction(
        ETransactionType type,
        const TTransactionStartOptions& options) override
    {
        return TransactionManager_->Start(type, options).Apply(
            BIND([=, this_ = MakeStrong(this)] (const NTransactionClient::TTransactionPtr& transaction) {
                auto wrappedTransaction = CreateTransaction(this_, transaction, Logger);
                if (options.Sticky) {
                    Connection_->RegisterStickyTransaction(wrappedTransaction);
                }
                return wrappedTransaction;
            }));
    }

    virtual ITransactionPtr AttachNativeTransaction(
        const TTransactionId& transactionId,
        const TTransactionAttachOptions& options) override
    {
        if (options.Sticky) {
            return Connection_->GetStickyTransaction(transactionId);
        } else {
            auto wrappedTransaction = TransactionManager_->Attach(transactionId, options);
            return CreateTransaction(this, std::move(wrappedTransaction), Logger);
        }
    }

    virtual TFuture<NApi::ITransactionPtr> StartTransaction(
        ETransactionType type,
        const TTransactionStartOptions& options) override
    {
        return StartNativeTransaction(type, options).As<NApi::ITransactionPtr>();
    }

    virtual NApi::ITransactionPtr AttachTransaction(
        const TTransactionId& transactionId,
        const TTransactionAttachOptions& options) override
    {
        return AttachNativeTransaction(transactionId, options);
    }

#define DROP_BRACES(...) __VA_ARGS__
#define IMPLEMENT_OVERLOADED_METHOD(returnType, method, doMethod, signature, args) \
    virtual TFuture<returnType> method signature override \
    { \
        return Execute( \
            #method, \
            options, \
            BIND( \
                &TClient::doMethod, \
                Unretained(this), \
                DROP_BRACES args)); \
    }

#define IMPLEMENT_METHOD(returnType, method, signature, args) \
    IMPLEMENT_OVERLOADED_METHOD(returnType, method, Do##method, signature, args)

    IMPLEMENT_METHOD(IUnversionedRowsetPtr, LookupRows, (
        const TYPath& path,
        TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TLookupRowsOptions& options),
        (path, std::move(nameTable), std::move(keys), options))
    IMPLEMENT_METHOD(IVersionedRowsetPtr, VersionedLookupRows, (
        const TYPath& path,
        TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TVersionedLookupRowsOptions& options),
        (path, std::move(nameTable), std::move(keys), options))
    IMPLEMENT_METHOD(TSelectRowsResult, SelectRows, (
        const TString& query,
        const TSelectRowsOptions& options),
        (query, options))
    IMPLEMENT_METHOD(std::vector<NTabletClient::TTableReplicaId>, GetInSyncReplicas, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TGetInSyncReplicasOptions& options),
        (path, nameTable, keys, options))
    IMPLEMENT_METHOD(std::vector<TTabletInfo>, GetTabletInfos, (
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const TGetTabletsInfoOptions& options),
        (path, tabletIndexes, options))
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
    IMPLEMENT_METHOD(void, FreezeTable, (
        const TYPath& path,
        const TFreezeTableOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, UnfreezeTable, (
        const TYPath& path,
        const TUnfreezeTableOptions& options),
        (path, options))
    IMPLEMENT_OVERLOADED_METHOD(void, ReshardTable, DoReshardTableWithPivotKeys, (
        const TYPath& path,
        const std::vector<NTableClient::TOwningKey>& pivotKeys,
        const TReshardTableOptions& options),
        (path, pivotKeys, options))
    IMPLEMENT_OVERLOADED_METHOD(void, ReshardTable, DoReshardTableWithTabletCount, (
        const TYPath& path,
        int tabletCount,
        const TReshardTableOptions& options),
        (path, tabletCount, options))
    IMPLEMENT_METHOD(void, AlterTable, (
        const TYPath& path,
        const TAlterTableOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, TrimTable, (
        const TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const TTrimTableOptions& options),
        (path, tabletIndex, trimmedRowCount, options))
    IMPLEMENT_METHOD(void, AlterTableReplica, (
        const TTableReplicaId& replicaId,
        const TAlterTableReplicaOptions& options),
        (replicaId, options))


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
    IMPLEMENT_METHOD(TYsonString, ListNode, (
        const TYPath& path,
        const TListNodeOptions& options),
        (path, options))
    IMPLEMENT_METHOD(TNodeId, CreateNode, (
        const TYPath& path,
        EObjectType type,
        const TCreateNodeOptions& options),
        (path, type, options))
    IMPLEMENT_METHOD(TLockNodeResult, LockNode, (
        const TYPath& path,
        ELockMode mode,
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
    IMPLEMENT_METHOD(void, ConcatenateNodes, (
        const std::vector<TYPath>& srcPaths,
        const TYPath& dstPath,
        const TConcatenateNodesOptions& options),
        (srcPaths, dstPath, options))
    IMPLEMENT_METHOD(bool, NodeExists, (
        const TYPath& path,
        const TNodeExistsOptions& options),
        (path, options))


    IMPLEMENT_METHOD(TObjectId, CreateObject, (
        EObjectType type,
        const TCreateObjectOptions& options),
        (type, options))


    virtual TFuture<IFileReaderPtr> CreateFileReader(
        const TYPath& path,
        const TFileReaderOptions& options) override
    {
        return NNative::CreateFileReader(this, path, options);
    }

    virtual IFileWriterPtr CreateFileWriter(
        const TYPath& path,
        const TFileWriterOptions& options) override
    {
        return NNative::CreateFileWriter(this, path, options);
    }


    virtual IJournalReaderPtr CreateJournalReader(
        const TYPath& path,
        const TJournalReaderOptions& options) override
    {
        return NNative::CreateJournalReader(this, path, options);
    }

    virtual IJournalWriterPtr CreateJournalWriter(
        const TYPath& path,
        const TJournalWriterOptions& options) override
    {
        return NNative::CreateJournalWriter(this, path, options);
    }

    virtual TFuture<ITableReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options) override
    {
        return NNative::CreateTableReader(this, path, options, New<TNameTable>());
    }

    virtual TFuture<TSkynetSharePartsLocationsPtr> LocateSkynetShare(
        const NYPath::TRichYPath& path,
        const TLocateSkynetShareOptions& options) override
    {
        return NNative::LocateSkynetShare(this, path, options);
    }

    virtual TFuture<ITableWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const NApi::TTableWriterOptions& options) override
    {
        return NNative::CreateTableWriter(this, path, options);
    }

    IMPLEMENT_METHOD(std::vector<TColumnarStatistics>, GetColumnarStatistics, (
        const std::vector<TRichYPath>& paths,
        const TGetColumnarStatisticsOptions& options),
        (paths, options))

    IMPLEMENT_METHOD(TGetFileFromCacheResult, GetFileFromCache, (
        const TString& md5,
        const TGetFileFromCacheOptions& options),
        (md5, options))

    IMPLEMENT_METHOD(TPutFileToCacheResult, PutFileToCache, (
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options),
        (path, expectedMD5, options))

    IMPLEMENT_METHOD(void, AddMember, (
        const TString& group,
        const TString& member,
        const TAddMemberOptions& options),
        (group, member, options))
    IMPLEMENT_METHOD(void, RemoveMember, (
        const TString& group,
        const TString& member,
        const TRemoveMemberOptions& options),
        (group, member, options))
    IMPLEMENT_METHOD(TCheckPermissionResult, CheckPermission, (
        const TString& user,
        const TYPath& path,
        EPermission permission,
        const TCheckPermissionOptions& options),
        (user, path, permission, options))
    IMPLEMENT_METHOD(TCheckPermissionByAclResult, CheckPermissionByAcl, (
        const TNullable<TString>& user,
        EPermission permission,
        INodePtr acl,
        const TCheckPermissionByAclOptions& options),
        (user, permission, acl, options))

    IMPLEMENT_METHOD(TOperationId, StartOperation, (
        EOperationType type,
        const TYsonString& spec,
        const TStartOperationOptions& options),
        (type, spec, options))
    IMPLEMENT_METHOD(void, AbortOperation, (
        const TOperationId& operationId,
        const TAbortOperationOptions& options),
        (operationId, options))
    IMPLEMENT_METHOD(void, SuspendOperation, (
        const TOperationId& operationId,
        const TSuspendOperationOptions& options),
        (operationId, options))
    IMPLEMENT_METHOD(void, ResumeOperation, (
        const TOperationId& operationId,
        const TResumeOperationOptions& options),
        (operationId, options))
    IMPLEMENT_METHOD(void, CompleteOperation, (
        const TOperationId& operationId,
        const TCompleteOperationOptions& options),
        (operationId, options))
    IMPLEMENT_METHOD(void, UpdateOperationParameters, (
        const TOperationId& operationId,
        const TYsonString& parameters,
        const TUpdateOperationParametersOptions& options),
        (operationId, parameters, options))
    IMPLEMENT_METHOD(TYsonString, GetOperation, (
        const NScheduler::TOperationId& operationId,
        const TGetOperationOptions& options),
        (operationId, options))
    IMPLEMENT_METHOD(void, DumpJobContext, (
        const TJobId& jobId,
        const TYPath& path,
        const TDumpJobContextOptions& options),
        (jobId, path, options))
    IMPLEMENT_METHOD(IAsyncZeroCopyInputStreamPtr, GetJobInput, (
        const TJobId& jobId,
        const TGetJobInputOptions& options),
        (jobId, options))
    IMPLEMENT_METHOD(TSharedRef, GetJobStderr, (
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobStderrOptions& options),
        (operationId, jobId, options))
    IMPLEMENT_METHOD(TSharedRef, GetJobFailContext, (
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobFailContextOptions& options),
        (operationId, jobId, options))
    IMPLEMENT_METHOD(TListOperationsResult, ListOperations, (
        const TListOperationsOptions& options),
        (options))
    IMPLEMENT_METHOD(TListJobsResult, ListJobs, (
        const TOperationId& operationId,
        const TListJobsOptions& options),
        (operationId, options))
    IMPLEMENT_METHOD(TYsonString, GetJob, (
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobOptions& options),
        (operationId, jobId, options))
    IMPLEMENT_METHOD(TYsonString, StraceJob, (
        const TJobId& jobId,
        const TStraceJobOptions& options),
        (jobId, options))
    IMPLEMENT_METHOD(void, SignalJob, (
        const TJobId& jobId,
        const TString& signalName,
        const TSignalJobOptions& options),
        (jobId, signalName, options))
    IMPLEMENT_METHOD(void, AbandonJob, (
        const TJobId& jobId,
        const TAbandonJobOptions& options),
        (jobId, options))
    IMPLEMENT_METHOD(TYsonString, PollJobShell, (
        const TJobId& jobId,
        const TYsonString& parameters,
        const TPollJobShellOptions& options),
        (jobId, parameters, options))
    IMPLEMENT_METHOD(void, AbortJob, (
        const TJobId& jobId,
        const TAbortJobOptions& options),
        (jobId, options))


    IMPLEMENT_METHOD(TClusterMeta, GetClusterMeta, (
        const TGetClusterMetaOptions& options),
        (options))

#undef DROP_BRACES
#undef IMPLEMENT_METHOD

private:
    friend class TTransaction;

    const IConnectionPtr Connection_;
    const TClientOptions Options_;

    TEnumIndexedVector<THashMap<TCellTag, IChannelPtr>, EMasterChannelKind> MasterChannels_;
    IChannelPtr SchedulerChannel_;
    TEnumIndexedVector<IChannelPtr, EMasterChannelKind> OperationsArchiveChannels_;
    INodeChannelFactoryPtr ChannelFactory_;
    TTransactionManagerPtr TransactionManager_;
    TFunctionImplCachePtr FunctionImplCache_;
    IFunctionRegistryPtr FunctionRegistry_;
    std::unique_ptr<TSchedulerServiceProxy> SchedulerProxy_;
    std::unique_ptr<TJobProberServiceProxy> JobProberProxy_;

    TAsyncSemaphorePtr ConcurrentRequestsSemaphore_;

    NLogging::TLogger Logger = ApiLogger;


    template <class T>
    TFuture<T> Execute(
        const TString& commandName,
        const TTimeoutOptions& options,
        TCallback<T()> callback)
    {
        auto guard = TAsyncSemaphoreGuard::TryAcquire(ConcurrentRequestsSemaphore_);
        if (!guard) {
            return MakeFuture<T>(TError(EErrorCode::TooManyConcurrentRequests, "Too many concurrent requests"));
        }

        return
            BIND([commandName, callback = std::move(callback), this_ = MakeWeak(this), guard = std::move(guard)] () {
                auto client = this_.Lock();
                if (!client) {
                    THROW_ERROR_EXCEPTION("Client was abandoned");
                }
                const auto& Logger = client->Logger;
                try {
                    LOG_DEBUG("Command started (Command: %v)", commandName);
                    TBox<T> result(callback);
                    LOG_DEBUG("Command completed (Command: %v)", commandName);
                    return result.Unwrap();
                } catch (const std::exception& ex) {
                    LOG_DEBUG(ex, "Command failed (Command: %v)", commandName);
                    throw;
                }
            })
            .AsyncVia(Connection_->GetInvoker())
            .Run()
            .WithTimeout(options.Timeout);
    }

    template <class T>
    auto CallAndRetryIfMetadataCacheIsInconsistent(T&& callback) -> decltype(callback())
    {
        int retryCount = 0;
        while (true) {
            TError error;

            try {
                return callback();
            } catch (const NYT::TErrorException& ex) {
                error = ex.Error();
            }

            const auto& config = Connection_->GetConfig();
            const auto& tableMountCache = Connection_->GetTableMountCache();
            bool retry;
            TTabletInfoPtr tabletInfo;
            std::tie(retry, tabletInfo) = tableMountCache->InvalidateOnError(error);

            if (retry && ++retryCount <= config->TableMountCache->OnErrorRetryCount) {
                LOG_DEBUG(error, "Got error, will retry (attempt %v of %v)",
                    retryCount,
                    config->TableMountCache->OnErrorRetryCount);
                auto now = Now();
                auto retryTime = (tabletInfo ? tabletInfo->UpdateTime : now) +
                    config->TableMountCache->OnErrorSlackPeriod;
                if (retryTime > now) {
                    WaitFor(TDelayedExecutor::MakeDelayed(retryTime - now))
                        .ThrowOnError();
                }
                continue;
            }

            THROW_ERROR error;
        }
    }


    static void SetMutationId(const IClientRequestPtr& request, const TMutatingOptions& options)
    {
        NRpc::SetMutationId(request, options.GetOrGenerateMutationId(), options.Retry);
    }


    TTransactionId GetTransactionId(const TTransactionalOptions& options, bool allowNullTransaction)
    {
        auto transaction = GetTransaction(options, allowNullTransaction, true);
        return transaction ? transaction->GetId() : NullTransactionId;
    }

    NTransactionClient::TTransactionPtr GetTransaction(
        const TTransactionalOptions& options,
        bool allowNullTransaction,
        bool pingTransaction)
    {
        if (!options.TransactionId) {
            if (!allowNullTransaction) {
                THROW_ERROR_EXCEPTION("A valid master transaction is required");
            }
            return nullptr;
        }

        TTransactionAttachOptions attachOptions;
        attachOptions.Ping = pingTransaction;
        attachOptions.PingAncestors = options.PingAncestors;
        return TransactionManager_->Attach(options.TransactionId, attachOptions);
    }

    void SetTransactionId(
        const IClientRequestPtr& request,
        const TTransactionalOptions& options,
        bool allowNullTransaction)
    {
        NCypressClient::SetTransactionId(request, GetTransactionId(options, allowNullTransaction));
    }


    void SetPrerequisites(
        const IClientRequestPtr& request,
        const TPrerequisiteOptions& options)
    {
        if (options.PrerequisiteTransactionIds.empty() && options.PrerequisiteRevisions.empty()) {
            return;
        }

        auto* prerequisitesExt = request->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
        for (const auto& id : options.PrerequisiteTransactionIds) {
            auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
            ToProto(prerequisiteTransaction->mutable_transaction_id(), id);
        }
        for (const auto& revision : options.PrerequisiteRevisions) {
            auto* prerequisiteRevision = prerequisitesExt->add_revisions();
            prerequisiteRevision->set_path(revision->Path);
            ToProto(prerequisiteRevision->mutable_transaction_id(), revision->TransactionId);
            prerequisiteRevision->set_revision(revision->Revision);
        }
    }


    static void SetSuppressAccessTracking(
        const IClientRequestPtr& request,
        const TSuppressableAccessTrackingOptions& commandOptions)
    {
        if (commandOptions.SuppressAccessTracking) {
            NCypressClient::SetSuppressAccessTracking(request, true);
        }
        if (commandOptions.SuppressModificationTracking) {
            NCypressClient::SetSuppressModificationTracking(request, true);
        }
    }

    static void SetCachingHeader(
        const IClientRequestPtr& request,
        const TMasterReadOptions& options)
    {
        if (options.ReadFrom == EMasterChannelKind::Cache) {
            auto* cachingHeaderExt = request->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
            cachingHeaderExt->set_success_expiration_time(ToProto<i64>(options.ExpireAfterSuccessfulUpdateTime));
            cachingHeaderExt->set_failure_expiration_time(ToProto<i64>(options.ExpireAfterFailedUpdateTime));
        }
    }

    static void SetBalancingHeader(
        const IClientRequestPtr& request,
        const TMasterReadOptions& options)
    {
        if (options.ReadFrom == EMasterChannelKind::Cache) {
            auto* balancingHeaderExt = request->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
            balancingHeaderExt->set_enable_stickness(true);
            balancingHeaderExt->set_sticky_group_size(options.CacheStickyGroupSize);
        }
    }

    template <class TProxy>
    std::unique_ptr<TProxy> CreateReadProxy(
        const TMasterReadOptions& options,
        TCellTag cellTag = PrimaryMasterCellTag)
    {
        auto channel = GetMasterChannelOrThrow(options.ReadFrom, cellTag);
        return std::make_unique<TProxy>(channel);
    }

    template <class TProxy>
    std::unique_ptr<TProxy> CreateWriteProxy(
        TCellTag cellTag = PrimaryMasterCellTag)
    {
        auto channel = GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
        return std::make_unique<TProxy>(channel);
    }

    IChannelPtr GetReadCellChannelOrThrow(const TTabletCellId& cellId)
    {
        const auto& cellDirectory = Connection_->GetCellDirectory();
        const auto& cellDescriptor = cellDirectory->GetDescriptorOrThrow(cellId);
        const auto& primaryPeerDescriptor = GetPrimaryTabletPeerDescriptor(cellDescriptor, EPeerKind::Leader);
        return ChannelFactory_->CreateChannel(primaryPeerDescriptor.GetAddressOrThrow(Connection_->GetNetworks()));
    }


    class TTabletCellLookupSession
        : public TIntrinsicRefCounted
    {
    public:
        using TEncoder = std::function<std::vector<TSharedRef>(const std::vector<TUnversionedRow>&)>;
        using TDecoder = std::function<TTypeErasedRow(TWireProtocolReader*)>;

        TTabletCellLookupSession(
            TConnectionConfigPtr config,
            const TNetworkPreferenceList& networks,
            const TCellId& cellId,
            const TLookupRowsOptionsBase& options,
            TTableMountInfoPtr tableInfo,
            const TNullable<TString>& retentionConfig,
            TEncoder encoder,
            TDecoder decoder)
            : Config_(std::move(config))
            , Networks_(networks)
            , CellId_(cellId)
            , Options_(options)
            , TableInfo_(std::move(tableInfo))
            , RetentionConfig_(retentionConfig)
            , Encoder_(std::move(encoder))
            , Decoder_(std::move(decoder))
        { }

        void AddKey(int index, TTabletInfoPtr tabletInfo, NTableClient::TKey key)
        {
            if (Batches_.empty() ||
                Batches_.back()->TabletInfo->TabletId != tabletInfo->TabletId ||
                Batches_.back()->Indexes.size() >= Config_->MaxRowsPerLookupRequest)
            {
                Batches_.emplace_back(new TBatch(std::move(tabletInfo)));
            }

            auto& batch = Batches_.back();
            batch->Indexes.push_back(index);
            batch->Keys.push_back(key);
        }

        TFuture<void> Invoke(IChannelFactoryPtr channelFactory, TCellDirectoryPtr cellDirectory)
        {
            auto* codec = NCompression::GetCodec(Config_->LookupRowsRequestCodec);

            // Do all the heavy lifting here.
            for (auto& batch : Batches_) {
                batch->RequestData = codec->Compress(Encoder_(batch->Keys));
            }

            const auto& cellDescriptor = cellDirectory->GetDescriptorOrThrow(CellId_);
            auto channel = CreateTabletReadChannel(
                channelFactory,
                cellDescriptor,
                Options_,
                Networks_);

            InvokeProxy_ = std::make_unique<TQueryServiceProxy>(std::move(channel));
            InvokeProxy_->SetDefaultTimeout(Options_.Timeout.Get(Config_->DefaultLookupRowsTimeout));
            InvokeProxy_->SetDefaultRequestAck(false);

            InvokeNextBatch();
            return InvokePromise_;
        }

        void ParseResponse(
            const TRowBufferPtr& rowBuffer,
            std::vector<TTypeErasedRow>* resultRows)
        {
            auto* responseCodec = NCompression::GetCodec(Config_->LookupRowsResponseCodec);
            for (const auto& batch : Batches_) {
                auto responseData = responseCodec->Decompress(batch->Response->Attachments()[0]);
                TWireProtocolReader reader(responseData, rowBuffer);
                auto batchSize = batch->Keys.size();
                for (int index = 0; index < batchSize; ++index) {
                    (*resultRows)[batch->Indexes[index]] = Decoder_(&reader);
                }
            }
        }

    private:
        const TConnectionConfigPtr Config_;
        const TNetworkPreferenceList Networks_;
        const TCellId CellId_;
        const TLookupRowsOptionsBase Options_;
        const TTableMountInfoPtr TableInfo_;
        const TNullable<TString> RetentionConfig_;
        const TEncoder Encoder_;
        const TDecoder Decoder_;

        struct TBatch
        {
            explicit TBatch(TTabletInfoPtr tabletInfo)
                : TabletInfo(std::move(tabletInfo))
            { }

            TTabletInfoPtr TabletInfo;
            std::vector<int> Indexes;
            std::vector<NTableClient::TKey> Keys;
            TSharedRef RequestData;
            TQueryServiceProxy::TRspReadPtr Response;
        };

        std::vector<std::unique_ptr<TBatch>> Batches_;
        std::unique_ptr<TQueryServiceProxy> InvokeProxy_;
        int InvokeBatchIndex_ = 0;
        TPromise<void> InvokePromise_ = NewPromise<void>();


        void InvokeNextBatch()
        {
            if (InvokeBatchIndex_ >= Batches_.size()) {
                InvokePromise_.Set(TError());
                return;
            }

            const auto& batch = Batches_[InvokeBatchIndex_];

            auto req = InvokeProxy_->Read();
            req->SetMultiplexingBand(NRpc::EMultiplexingBand::Heavy);
            ToProto(req->mutable_tablet_id(), batch->TabletInfo->TabletId);
            req->set_mount_revision(batch->TabletInfo->MountRevision);
            req->set_timestamp(Options_.Timestamp);
            req->set_request_codec(static_cast<int>(Config_->LookupRowsRequestCodec));
            req->set_response_codec(static_cast<int>(Config_->LookupRowsResponseCodec));
            req->Attachments().push_back(batch->RequestData);
            if (batch->TabletInfo->IsInMemory()) {
                req->Header().set_uncancelable(true);
            }
            if (RetentionConfig_) {
                req->set_retention_config(*RetentionConfig_);
            }

            req->Invoke().Subscribe(
                BIND(&TTabletCellLookupSession::OnResponse, MakeStrong(this)));
        }

        void OnResponse(const TQueryServiceProxy::TErrorOrRspReadPtr& rspOrError)
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

    using TTabletCellLookupSessionPtr = TIntrusivePtr<TTabletCellLookupSession>;
    using TEncoderWithMapping = std::function<std::vector<TSharedRef>(
        const NTableClient::TColumnFilter&,
        const std::vector<TUnversionedRow>&)>;
    using TDecoderWithMapping = std::function<TTypeErasedRow(
        const TSchemaData&,
        TWireProtocolReader*)>;
    template <class TResult>
    using TReplicaFallbackHandler = std::function<TFuture<TResult>(
        const NApi::IClientPtr&,
        const TTableReplicaInfoPtr&)>;

    static NTableClient::TColumnFilter RemapColumnFilter(
        const NTableClient::TColumnFilter& columnFilter,
        const TNameTableToSchemaIdMapping& idMapping,
        const TNameTablePtr& nameTable)
    {
        if (columnFilter.IsUniversal()) {
            return columnFilter;
        }
        auto remappedFilterIndexes = columnFilter.GetIndexes();
        for (auto& index : remappedFilterIndexes) {
            if (index < 0 || index >= idMapping.size()) {
                THROW_ERROR_EXCEPTION(
                    "Column filter contains invalid index: actual %v, expected in range [0, %v]",
                    index,
                    idMapping.size() - 1);
            }
            if (idMapping[index] == -1) {
                THROW_ERROR_EXCEPTION("Invalid column %Qv in column filter", nameTable->GetName(index));
            }
            index = idMapping[index];
        }
        return NTableClient::TColumnFilter(std::move(remappedFilterIndexes));
    }

    IUnversionedRowsetPtr DoLookupRows(
        const TYPath& path,
        const TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TLookupRowsOptions& options)
    {
        TEncoderWithMapping encoder = [] (
            const NTableClient::TColumnFilter& remappedColumnFilter,
            const std::vector<TUnversionedRow>& remappedKeys) -> std::vector<TSharedRef>
        {
            TReqLookupRows req;
            if (remappedColumnFilter.IsUniversal()) {
                req.clear_column_filter();
            } else {
                ToProto(req.mutable_column_filter()->mutable_indexes(), remappedColumnFilter.GetIndexes());
            }
            TWireProtocolWriter writer;
            writer.WriteCommand(EWireProtocolCommand::LookupRows);
            writer.WriteMessage(req);
            writer.WriteSchemafulRowset(remappedKeys);
            return writer.Finish();
        };

        TDecoderWithMapping decoder = [] (
            const TSchemaData& schemaData,
            TWireProtocolReader* reader) -> TTypeErasedRow
        {
            return reader->ReadSchemafulRow(schemaData, true).ToTypeErasedRow();
        };

        TReplicaFallbackHandler<IUnversionedRowsetPtr> fallbackHandler = [&] (
            const NApi::IClientPtr& replicaClient,
            const TTableReplicaInfoPtr& replicaInfo)
        {
            return replicaClient->LookupRows(replicaInfo->ReplicaPath, nameTable, keys, options);
        };

        return CallAndRetryIfMetadataCacheIsInconsistent([&] () {
            return DoLookupRowsOnce<IUnversionedRowsetPtr, TUnversionedRow>(
                path,
                nameTable,
                keys,
                options,
                Null,
                encoder,
                decoder,
                fallbackHandler);
        });
    }

    IVersionedRowsetPtr DoVersionedLookupRows(
        const TYPath& path,
        const TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TVersionedLookupRowsOptions& options)
    {
        TEncoderWithMapping encoder = [] (
            const NTableClient::TColumnFilter& remappedColumnFilter,
            const std::vector<TUnversionedRow>& remappedKeys) -> std::vector<TSharedRef>
        {
            TReqVersionedLookupRows req;
            if (remappedColumnFilter.IsUniversal()) {
                req.clear_column_filter();
            } else {
                ToProto(req.mutable_column_filter()->mutable_indexes(), remappedColumnFilter.GetIndexes());
            }
            TWireProtocolWriter writer;
            writer.WriteCommand(EWireProtocolCommand::VersionedLookupRows);
            writer.WriteMessage(req);
            writer.WriteSchemafulRowset(remappedKeys);
            return writer.Finish();
        };

        TDecoderWithMapping decoder = [] (
            const TSchemaData& schemaData,
            TWireProtocolReader* reader) -> TTypeErasedRow
        {
            return reader->ReadVersionedRow(schemaData, true).ToTypeErasedRow();
        };

        TReplicaFallbackHandler<IVersionedRowsetPtr> fallbackHandler = [&] (
            const NApi::IClientPtr& replicaClient,
            const TTableReplicaInfoPtr& replicaInfo)
        {
            return replicaClient->VersionedLookupRows(replicaInfo->ReplicaPath, nameTable, keys, options);
        };

        TNullable<TString> retentionConfig;
        if (options.RetentionConfig) {
            retentionConfig = ConvertToYsonString(options.RetentionConfig).GetData();
        }

        return CallAndRetryIfMetadataCacheIsInconsistent([&] () {
            return DoLookupRowsOnce<IVersionedRowsetPtr, TVersionedRow>(
                path,
                nameTable,
                keys,
                options,
                retentionConfig,
                encoder,
                decoder,
                fallbackHandler);
        });
    }


    TFuture<TTableReplicaInfoPtrList> PickInSyncReplicas(
        const TTableMountInfoPtr& tableInfo,
        const TTabletReadOptions& options,
        const std::vector<std::pair<NTableClient::TKey, int>>& keys)
    {
        THashMap<TCellId, std::vector<TTabletId>> cellIdToTabletIds;
        THashSet<TTabletId> tabletIds;
        for (const auto& pair : keys) {
            auto key = pair.first;
            auto tabletInfo = GetSortedTabletForRow(tableInfo, key);
            const auto& tabletId = tabletInfo->TabletId;
            if (tabletIds.insert(tabletId).second) {
                cellIdToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
            }
        }
        return PickInSyncReplicas(tableInfo, options, cellIdToTabletIds);
    }

    TFuture<TTableReplicaInfoPtrList> PickInSyncReplicas(
        const TTableMountInfoPtr& tableInfo,
        const TTabletReadOptions& options)
    {
        THashMap<TCellId, std::vector<TTabletId>> cellIdToTabletIds;
        for (const auto& tabletInfo : tableInfo->Tablets) {
            cellIdToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
        }
        return PickInSyncReplicas(tableInfo, options, cellIdToTabletIds);
    }

    TFuture<TTableReplicaInfoPtrList> PickInSyncReplicas(
        const TTableMountInfoPtr& tableInfo,
        const TTabletReadOptions& options,
        const THashMap<TCellId, std::vector<TTabletId>>& cellIdToTabletIds)
    {
        size_t cellCount = cellIdToTabletIds.size();
        size_t tabletCount = 0;
        for (const auto& pair : cellIdToTabletIds) {
            tabletCount += pair.second.size();
        }

        LOG_DEBUG("Looking for in-sync replicas (Path: %v, CellCount: %v, TabletCount: %v)",
            tableInfo->Path,
            cellCount,
            tabletCount);

        const auto& channelFactory = Connection_->GetChannelFactory();
        const auto& cellDirectory = Connection_->GetCellDirectory();
        std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> asyncResults;
        for (const auto& pair : cellIdToTabletIds) {
            const auto& cellDescriptor = cellDirectory->GetDescriptorOrThrow(pair.first);
            auto channel = CreateTabletReadChannel(
                channelFactory,
                cellDescriptor,
                options,
                Connection_->GetNetworks());

            TQueryServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(options.Timeout.Get(Connection_->GetConfig()->DefaultGetInSyncReplicasTimeout));

            auto req = proxy.GetTabletInfo();
            ToProto(req->mutable_tablet_ids(), pair.second);
            asyncResults.push_back(req->Invoke());
        }

        return Combine(asyncResults).Apply(
            BIND([=, this_ = MakeStrong(this)] (const std::vector<TQueryServiceProxy::TRspGetTabletInfoPtr>& rsps) {
                THashMap<TTableReplicaId, int> replicaIdToCount;
                for (const auto& rsp : rsps) {
                    for (const auto& protoTabletInfo : rsp->tablets()) {
                        for (const auto& protoReplicaInfo : protoTabletInfo.replicas()) {
                            if (IsReplicaInSync(protoReplicaInfo, protoTabletInfo)) {
                                ++replicaIdToCount[FromProto<TTableReplicaId>(protoReplicaInfo.replica_id())];
                            }
                        }
                    }
                }

                TTableReplicaInfoPtrList inSyncReplicaInfos;
                for (const auto& replicaInfo : tableInfo->Replicas) {
                    auto it = replicaIdToCount.find(replicaInfo->ReplicaId);
                    if (it != replicaIdToCount.end() && it->second == tabletCount) {
                        LOG_DEBUG("In-sync replica found (Path: %v, ReplicaId: %v, ClusterName: %v)",
                            tableInfo->Path,
                            replicaInfo->ReplicaId,
                            replicaInfo->ClusterName);
                        inSyncReplicaInfos.push_back(replicaInfo);
                    }
                }

                if (inSyncReplicaInfos.empty()) {
                    THROW_ERROR_EXCEPTION("No in-sync replicas found for table %v",
                        tableInfo->Path);
                }

                return inSyncReplicaInfos;
            }));
    }

    TNullable<TString> PickInSyncClusterAndPatchQuery(
        const TTabletReadOptions& options,
        NAst::TQuery* query)
    {
        std::vector<TYPath> paths{query->Table.Path};
        for (const auto& join : query->Joins) {
            paths.push_back(join.Table.Path);
        }

        const auto& tableMountCache = Connection_->GetTableMountCache();
        std::vector<TFuture<TTableMountInfoPtr>> asyncTableInfos;
        for (const auto& path : paths) {
            asyncTableInfos.push_back(tableMountCache->GetTableInfo(path));
        }

        auto tableInfos = WaitFor(Combine(asyncTableInfos))
            .ValueOrThrow();

        bool someReplicated = false;
        bool someNotReplicated = false;
        for (const auto& tableInfo : tableInfos) {
            if (tableInfo->IsReplicated()) {
                someReplicated = true;
            } else {
                someNotReplicated = true;
            }
        }

        if (someReplicated && someNotReplicated) {
            THROW_ERROR_EXCEPTION("Query involves both replicated and non-replicated tables");
        }

        if (!someReplicated) {
            return Null;
        }

        std::vector<TFuture<TTableReplicaInfoPtrList>> asyncCandidates;
        for (size_t tableIndex = 0; tableIndex < tableInfos.size(); ++tableIndex) {
            asyncCandidates.push_back(PickInSyncReplicas(tableInfos[tableIndex], options));
        }

        auto candidates = WaitFor(Combine(asyncCandidates))
            .ValueOrThrow();

        THashMap<TString, int> clusterNameToCount;
        for (const auto& replicaInfos : candidates) {
            SmallVector<TString, TypicalReplicaCount> clusterNames;
            for (const auto& replicaInfo : replicaInfos) {
                clusterNames.push_back(replicaInfo->ClusterName);
            }
            std::sort(clusterNames.begin(), clusterNames.end());
            clusterNames.erase(std::unique(clusterNames.begin(), clusterNames.end()), clusterNames.end());
            for (const auto& clusterName : clusterNames) {
                ++clusterNameToCount[clusterName];
            }
        }

        SmallVector<TString, TypicalReplicaCount> inSyncClusterNames;
        for (const auto& pair : clusterNameToCount) {
            if (pair.second == paths.size()) {
                inSyncClusterNames.push_back(pair.first);
            }
        }

        if (inSyncClusterNames.empty()) {
            THROW_ERROR_EXCEPTION("No single cluster contains in-sync replicas for all involved tables %v",
                paths);
        }

        // TODO(babenko): break ties in a smarter way
        const auto& inSyncClusterName = inSyncClusterNames[0];
        LOG_DEBUG("In-sync cluster selected (Paths: %v, ClusterName: %v)",
            paths,
            inSyncClusterName);

        auto patchTableDescriptor = [&] (NAst::TTableDescriptor* descriptor, const TTableReplicaInfoPtrList& replicaInfos) {
            for (const auto& replicaInfo : replicaInfos) {
                if (replicaInfo->ClusterName == inSyncClusterName) {
                    descriptor->Path = replicaInfo->ReplicaPath;
                    return;
                }
            }
            Y_UNREACHABLE();
        };

        patchTableDescriptor(&query->Table, candidates[0]);
        for (size_t index = 0; index < query->Joins.size(); ++index) {
            patchTableDescriptor(&query->Joins[index].Table, candidates[index + 1]);
        }
        return inSyncClusterName;
    }


    NApi::IConnectionPtr GetReplicaConnectionOrThrow(const TString& clusterName)
    {
        const auto& clusterDirectory = Connection_->GetClusterDirectory();
        auto replicaConnection = clusterDirectory->FindConnection(clusterName);
        if (replicaConnection) {
            return replicaConnection;
        }

        WaitFor(Connection_->GetClusterDirectorySynchronizer()->Sync())
            .ThrowOnError();

        return clusterDirectory->GetConnectionOrThrow(clusterName);
    }

    NApi::IClientPtr CreateReplicaClient(const TString& clusterName)
    {
        auto replicaConnection = GetReplicaConnectionOrThrow(clusterName);
        return replicaConnection->CreateClient(Options_);
    }

    void RemapValueIds(
        TVersionedRow /*row*/,
        std::vector<TTypeErasedRow>& rows,
        const std::vector<int>& mapping)
    {
        for (auto untypedRow : rows) {
            auto row = TMutableVersionedRow(untypedRow);
            if (!row) {
                continue;
            }
            for (int index = 0; index < row.GetKeyCount(); ++index) {
                auto id = row.BeginKeys()[index].Id;
                YCHECK(id < mapping.size() && mapping[id] != -1);
                row.BeginKeys()[index].Id = mapping[id];
            }
            for (int index = 0; index < row.GetValueCount(); ++index) {
                auto id = row.BeginValues()[index].Id;
                YCHECK(id < mapping.size() && mapping[id] != -1);
                row.BeginValues()[index].Id = mapping[id];
            }
        }

    }

    void RemapValueIds(
        TUnversionedRow /*row*/,
        std::vector<TTypeErasedRow>& rows,
        const std::vector<int>& mapping)
    {
        for (auto untypedRow : rows) {
            auto row = TMutableUnversionedRow(untypedRow);
            if (!row) {
                continue;
            }
            for (int index = 0; index < row.GetCount(); ++index) {
                auto id = row[index].Id;
                YCHECK(id < mapping.size() && mapping[id] != -1);
                row[index].Id = mapping[id];
            }
        }
    }

    std::vector<int> BuildResponseIdMapping(const NTableClient::TColumnFilter& remappedColumnFilter)
    {
        std::vector<int> mapping;
        for (int index = 0; index < remappedColumnFilter.GetIndexes().size(); ++index) {
            int id = remappedColumnFilter.GetIndexes()[index];
            if (id >= mapping.size()) {
                mapping.resize(id + 1, -1);
            }
            mapping[id] = index;
        }

        return mapping;
    }

    template <class TRowset, class TRow>
    TRowset DoLookupRowsOnce(
        const TYPath& path,
        const TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TLookupRowsOptionsBase& options,
        const TNullable<TString> retentionConfig,
        TEncoderWithMapping encoderWithMapping,
        TDecoderWithMapping decoderWithMapping,
        TReplicaFallbackHandler<TRowset> replicaFallbackHandler)
    {
        const auto& tableMountCache = Connection_->GetTableMountCache();
        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
            .ValueOrThrow();

        tableInfo->ValidateDynamic();
        tableInfo->ValidateSorted();

        const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];
        auto idMapping = BuildColumnIdMapping(schema, nameTable);
        auto remappedColumnFilter = RemapColumnFilter(options.ColumnFilter, idMapping, nameTable);
        auto resultSchema = tableInfo->Schemas[ETableSchemaKind::Primary].Filter(remappedColumnFilter);
        auto resultSchemaData = TWireProtocolReader::GetSchemaData(schema, remappedColumnFilter);

        if (keys.Empty()) {
            return CreateRowset(resultSchema, TSharedRange<TRow>());
        }

        // NB: The server-side requires the keys to be sorted.
        std::vector<std::pair<NTableClient::TKey, int>> sortedKeys;
        sortedKeys.reserve(keys.Size());

        auto inputRowBuffer = New<TRowBuffer>(TLookupRowsInputBufferTag());
        auto evaluatorCache = Connection_->GetColumnEvaluatorCache();
        auto evaluator = tableInfo->NeedKeyEvaluation ? evaluatorCache->Find(schema) : nullptr;

        for (int index = 0; index < keys.Size(); ++index) {
            ValidateClientKey(keys[index], schema, idMapping, nameTable);
            auto capturedKey = inputRowBuffer->CaptureAndPermuteRow(keys[index], schema, idMapping, nullptr);

            if (evaluator) {
                evaluator->EvaluateKeys(capturedKey, inputRowBuffer);
            }

            sortedKeys.emplace_back(capturedKey, index);
        }

        if (tableInfo->IsReplicated()) {
            auto inSyncReplicaInfos = WaitFor(PickInSyncReplicas(tableInfo, options, sortedKeys))
                .ValueOrThrow();
            // TODO(babenko): break ties in a smarter way
            const auto& inSyncReplicaInfo = inSyncReplicaInfos[0];
            auto replicaClient = CreateReplicaClient(inSyncReplicaInfo->ClusterName);
            auto asyncResult = replicaFallbackHandler(replicaClient, inSyncReplicaInfo);
            return WaitFor(asyncResult)
                .ValueOrThrow();
        }

        // TODO(sandello): Use code-generated comparer here.
        std::sort(sortedKeys.begin(), sortedKeys.end());
        std::vector<size_t> keyIndexToResultIndex(keys.Size());
        size_t currentResultIndex = 0;

        auto outputRowBuffer = New<TRowBuffer>(TLookupRowsOutputBufferTag());
        std::vector<TTypeErasedRow> uniqueResultRows;

        if (Connection_->GetConfig()->EnableLookupMultiread) {
            struct TBatch
            {
                NObjectClient::TObjectId TabletId;
                i64 MountRevision = 0;
                std::vector<TKey> Keys;
                size_t OffsetInResult;

                TQueryServiceProxy::TRspMultireadPtr Response;
            };

            std::vector<std::vector<TBatch>> batchesByCells;
            THashMap<TCellId, size_t> cellIdToBatchIndex;

            {
                auto itemsBegin = sortedKeys.begin();
                auto itemsEnd = sortedKeys.end();

                size_t keySize = schema.GetKeyColumnCount();

                itemsBegin = std::lower_bound(
                    itemsBegin,
                    itemsEnd,
                    tableInfo->LowerCapBound.Get(),
                    [&] (const auto& item, TKey pivot) {
                        return CompareRows(item.first, pivot, keySize) < 0;
                    });

                itemsEnd = std::upper_bound(
                    itemsBegin,
                    itemsEnd,
                    tableInfo->UpperCapBound.Get(),
                    [&] (TKey pivot, const auto& item) {
                        return CompareRows(pivot, item.first, keySize) < 0;
                    });

                auto nextShardIt = tableInfo->Tablets.begin() + 1;
                for (auto itemsIt = itemsBegin; itemsIt != itemsEnd;) {
                    YCHECK(!tableInfo->Tablets.empty());

                    // Run binary search to find the relevant tablets.
                    nextShardIt = std::upper_bound(
                        nextShardIt,
                        tableInfo->Tablets.end(),
                        itemsIt->first,
                        [&] (TKey key, const TTabletInfoPtr& tabletInfo) {
                            return CompareRows(key, tabletInfo->PivotKey.Get(), keySize) < 0;
                        });

                    const auto& startShard = *(nextShardIt - 1);
                    auto nextPivotKey = (nextShardIt == tableInfo->Tablets.end())
                        ? tableInfo->UpperCapBound
                        : (*nextShardIt)->PivotKey;

                    // Binary search to reduce expensive row comparisons
                    auto endItemsIt = std::lower_bound(
                        itemsIt,
                        itemsEnd,
                        nextPivotKey.Get(),
                        [&] (const auto& item, TKey pivot) {
                            return CompareRows(item.first, pivot) < 0;
                        });

                    ValidateTabletMountedOrFrozen(tableInfo, startShard);

                    auto emplaced = cellIdToBatchIndex.emplace(startShard->CellId, batchesByCells.size());
                    if (emplaced.second) {
                        batchesByCells.emplace_back();
                    }

                    TBatch batch;
                    batch.TabletId = startShard->TabletId;
                    batch.MountRevision = startShard->MountRevision;
                    batch.OffsetInResult = currentResultIndex;

                    std::vector<TKey> rows;
                    rows.reserve(endItemsIt - itemsIt);

                    while (itemsIt != endItemsIt) {
                        auto key = itemsIt->first;
                        rows.push_back(key);

                        do {
                            keyIndexToResultIndex[itemsIt->second] = currentResultIndex;
                            ++itemsIt;
                        } while (itemsIt != endItemsIt && itemsIt->first == key);
                        ++currentResultIndex;
                    }

                    batch.Keys = std::move(rows);
                    batchesByCells[emplaced.first->second].push_back(std::move(batch));
                }
            }

            TTabletCellLookupSession::TEncoder boundEncoder = std::bind(encoderWithMapping, remappedColumnFilter, std::placeholders::_1);
            TTabletCellLookupSession::TDecoder boundDecoder = std::bind(decoderWithMapping, resultSchemaData, std::placeholders::_1);

            auto* codec = NCompression::GetCodec(Connection_->GetConfig()->LookupRowsRequestCodec);

            std::vector<TFuture<TQueryServiceProxy::TRspMultireadPtr>> asyncResults(batchesByCells.size());

            const auto& cellDirectory = Connection_->GetCellDirectory();
            const auto& networks = Connection_->GetNetworks();

            for (const auto& item : cellIdToBatchIndex) {
                size_t cellIndex = item.second;
                const auto& batches = batchesByCells[cellIndex];

                auto channel = CreateTabletReadChannel(
                    ChannelFactory_,
                    cellDirectory->GetDescriptorOrThrow(item.first),
                    options,
                    networks);

                TQueryServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(options.Timeout);
                proxy.SetDefaultRequestAck(false);

                auto req = proxy.Multiread();
                req->SetMultiplexingBand(NRpc::EMultiplexingBand::Heavy);
                req->set_request_codec(static_cast<int>(Connection_->GetConfig()->LookupRowsRequestCodec));
                req->set_response_codec(static_cast<int>(Connection_->GetConfig()->LookupRowsResponseCodec));
                req->set_timestamp(options.Timestamp);


                if (retentionConfig) {
                    req->set_retention_config(*retentionConfig);
                }

                for (const auto& batch : batches) {
                    ToProto(req->add_tablet_ids(), batch.TabletId);
                    req->add_mount_revisions(batch.MountRevision);
                    TSharedRef requestData = codec->Compress(boundEncoder(batch.Keys));
                    req->Attachments().push_back(requestData);
                }

                asyncResults[cellIndex] = req->Invoke();
            }

            auto results = WaitFor(Combine(std::move(asyncResults)))
                .ValueOrThrow();

            uniqueResultRows.resize(currentResultIndex);

            auto* responseCodec = NCompression::GetCodec(Connection_->GetConfig()->LookupRowsResponseCodec);

            for (size_t cellIndex = 0; cellIndex < results.size(); ++cellIndex) {
                const auto& batches = batchesByCells[cellIndex];

                for (size_t batchIndex = 0; batchIndex < batches.size(); ++batchIndex) {
                    auto responseData = responseCodec->Decompress(results[cellIndex]->Attachments()[batchIndex]);
                    TWireProtocolReader reader(responseData, outputRowBuffer);

                    const auto& batch = batches[batchIndex];

                    for (size_t index = 0; index < batch.Keys.size(); ++index) {
                        uniqueResultRows[batch.OffsetInResult + index] = boundDecoder(&reader);
                    }
                }
            }
        } else {
            THashMap<TCellId, TTabletCellLookupSessionPtr> cellIdToSession;

            // TODO(sandello): Reuse code from QL here to partition sorted keys between tablets.
            // Get rid of hash map.
            // TODO(sandello): Those bind states must be in a cross-session shared state. Check this when refactor out batches.
            TTabletCellLookupSession::TEncoder boundEncoder = std::bind(encoderWithMapping, remappedColumnFilter, std::placeholders::_1);
            TTabletCellLookupSession::TDecoder boundDecoder = std::bind(decoderWithMapping, resultSchemaData, std::placeholders::_1);
            for (int index = 0; index < sortedKeys.size();) {
                auto key = sortedKeys[index].first;
                auto tabletInfo = GetSortedTabletForRow(tableInfo, key);
                const auto& cellId = tabletInfo->CellId;
                auto it = cellIdToSession.find(cellId);
                if (it == cellIdToSession.end()) {
                    auto session = New<TTabletCellLookupSession>(
                        Connection_->GetConfig(),
                        Connection_->GetNetworks(),
                        cellId,
                        options,
                        tableInfo,
                        retentionConfig,
                        boundEncoder,
                        boundDecoder);
                    it = cellIdToSession.insert(std::make_pair(cellId, std::move(session))).first;
                }
                const auto& session = it->second;
                session->AddKey(currentResultIndex, std::move(tabletInfo), key);

                do {
                    keyIndexToResultIndex[sortedKeys[index].second] = currentResultIndex;
                    ++index;
                } while (index < sortedKeys.size() && sortedKeys[index].first == key);
                ++currentResultIndex;
            }

            std::vector<TFuture<void>> asyncResults;
            for (const auto& pair : cellIdToSession) {
                const auto& session = pair.second;
                asyncResults.push_back(session->Invoke(
                    ChannelFactory_,
                    Connection_->GetCellDirectory()));
            }

            WaitFor(Combine(std::move(asyncResults)))
                .ThrowOnError();

            // Rows are type-erased here and below to handle different kinds of rowsets.
            uniqueResultRows.resize(currentResultIndex);

            for (const auto& pair : cellIdToSession) {
                const auto& session = pair.second;
                session->ParseResponse(outputRowBuffer, &uniqueResultRows);
            }
        }

        if (!remappedColumnFilter.IsUniversal()) {
            RemapValueIds(TRow(), uniqueResultRows, BuildResponseIdMapping(remappedColumnFilter));
        }

        std::vector<TTypeErasedRow> resultRows;
        resultRows.resize(keys.Size());

        for (int index = 0; index < keys.Size(); ++index) {
            resultRows[index] = uniqueResultRows[keyIndexToResultIndex[index]];
        }

        if (!options.KeepMissingRows) {
            resultRows.erase(
                std::remove_if(
                    resultRows.begin(),
                    resultRows.end(),
                    [] (TTypeErasedRow row) {
                        return !static_cast<bool>(row);
                    }),
                resultRows.end());
        }

        auto rowRange = ReinterpretCastRange<TRow>(MakeSharedRange(std::move(resultRows), outputRowBuffer));
        return CreateRowset(resultSchema, std::move(rowRange));
    }

    TSelectRowsResult DoSelectRows(
        const TString& queryString,
        const TSelectRowsOptions& options)
    {
        return CallAndRetryIfMetadataCacheIsInconsistent([&] () {
            return DoSelectRowsOnce(queryString, options);
        });
    }

    TSelectRowsResult DoSelectRowsOnce(
        const TString& queryString,
        const TSelectRowsOptions& options)
    {
        auto parsedQuery = ParseSource(queryString, EParseMode::Query);
        auto* astQuery = &parsedQuery->AstHead.Ast.As<NAst::TQuery>();
        auto maybeClusterName = PickInSyncClusterAndPatchQuery(options, astQuery);
        if (maybeClusterName) {
            auto replicaClient = CreateReplicaClient(*maybeClusterName);
            auto updatedQueryString = NAst::FormatQuery(*astQuery);
            auto asyncResult = replicaClient->SelectRows(updatedQueryString, options);
            return WaitFor(asyncResult)
                .ValueOrThrow();
        }

        auto inputRowLimit = options.InputRowLimit.Get(Connection_->GetConfig()->DefaultInputRowLimit);
        auto outputRowLimit = options.OutputRowLimit.Get(Connection_->GetConfig()->DefaultOutputRowLimit);

        auto externalCGInfo = New<TExternalCGInfo>();
        auto fetchFunctions = [&] (const std::vector<TString>& names, const TTypeInferrerMapPtr& typeInferrers) {
            MergeFrom(typeInferrers.Get(), *BuiltinTypeInferrersMap);

            std::vector<TString> externalNames;
            for (const auto& name : names) {
                auto found = typeInferrers->find(name);
                if (found == typeInferrers->end()) {
                    externalNames.push_back(name);
                }
            }

            auto descriptors = WaitFor(FunctionRegistry_->FetchFunctions(externalNames))
                .ValueOrThrow();

            AppendUdfDescriptors(typeInferrers, externalCGInfo, externalNames, descriptors);
        };

        auto queryPreparer = New<TQueryPreparer>(Connection_->GetTableMountCache(), Connection_->GetInvoker());

        auto queryExecutor = CreateQueryExecutor(
            Connection_,
            Connection_->GetInvoker(),
            Connection_->GetColumnEvaluatorCache(),
            Connection_->GetQueryEvaluator(),
            ChannelFactory_,
            FunctionImplCache_);

        auto fragment = PreparePlanFragment(
            queryPreparer.Get(),
            *parsedQuery,
            fetchFunctions,
            options.Timestamp);
        const auto& query = fragment->Query;
        const auto& dataSource = fragment->Ranges;

        for (size_t index = 0; index < query->JoinClauses.size(); ++index) {
            if (query->JoinClauses[index]->ForeignKeyPrefix == 0 && !options.AllowJoinWithoutIndex) {
                const auto& ast = parsedQuery->AstHead.Ast.As<NAst::TQuery>();

                THROW_ERROR_EXCEPTION("Foreign table key is not used in the join clause; "
                    "the query is inefficient, consider rewriting it")
                    << TErrorAttribute("source", NAst::FormatJoin(ast.Joins[index]));
            }
        }

        TQueryOptions queryOptions;
        queryOptions.Timestamp = options.Timestamp;
        queryOptions.RangeExpansionLimit = options.RangeExpansionLimit;
        queryOptions.VerboseLogging = options.VerboseLogging;
        queryOptions.EnableCodeCache = options.EnableCodeCache;
        queryOptions.MaxSubqueries = options.MaxSubqueries;
        queryOptions.WorkloadDescriptor = options.WorkloadDescriptor;
        queryOptions.InputRowLimit = inputRowLimit;
        queryOptions.OutputRowLimit = outputRowLimit;
        queryOptions.UseMultijoin = options.UseMultijoin;
        queryOptions.AllowFullScan = options.AllowFullScan;
        queryOptions.ReadSessionId = TReadSessionId::Create();
        queryOptions.Deadline = options.Timeout.Get(Connection_->GetConfig()->DefaultSelectRowsTimeout).ToDeadLine();

        TClientBlockReadOptions blockReadOptions;
        blockReadOptions.WorkloadDescriptor = queryOptions.WorkloadDescriptor;
        blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        blockReadOptions.ReadSessionId = queryOptions.ReadSessionId;

        ISchemafulWriterPtr writer;
        TFuture<IUnversionedRowsetPtr> asyncRowset;
        std::tie(writer, asyncRowset) = CreateSchemafulRowsetWriter(query->GetTableSchema());

        auto statistics = WaitFor(queryExecutor->Execute(
            query,
            externalCGInfo,
            dataSource,
            writer,
            blockReadOptions,
            queryOptions))
            .ValueOrThrow();

        auto rowset = WaitFor(asyncRowset)
            .ValueOrThrow();

        if (options.FailOnIncompleteResult) {
            if (statistics.IncompleteInput) {
                THROW_ERROR_EXCEPTION("Query terminated prematurely due to excessive input; consider rewriting your query or changing input limit")
                    << TErrorAttribute("input_row_limit", inputRowLimit);
            }
            if (statistics.IncompleteOutput) {
                THROW_ERROR_EXCEPTION("Query terminated prematurely due to excessive output; consider rewriting your query or changing output limit")
                    << TErrorAttribute("output_row_limit", outputRowLimit);
            }
        }

        return TSelectRowsResult{rowset, statistics};
    }


    static bool IsReplicaInSync(
        const NQueryClient::NProto::TReplicaInfo& replicaInfo,
        const NQueryClient::NProto::TTabletInfo& tabletInfo)
    {
        return
            ETableReplicaMode(replicaInfo.mode()) == ETableReplicaMode::Sync &&
            replicaInfo.current_replication_row_index() >= tabletInfo.total_row_count();
    }

    static bool IsReplicaInSync(
        const NQueryClient::NProto::TReplicaInfo& replicaInfo,
        const NQueryClient::NProto::TTabletInfo& tabletInfo,
        TTimestamp timestamp)
    {
        return
            replicaInfo.last_replication_timestamp() >= timestamp ||
            IsReplicaInSync(replicaInfo, tabletInfo);
    }


    std::vector<TTableReplicaId> DoGetInSyncReplicas(
        const TYPath& path,
        TNameTablePtr nameTable,
        const TSharedRange<TKey>& keys,
        const TGetInSyncReplicasOptions& options)
    {
        ValidateSyncTimestamp(options.Timestamp);

        const auto& tableMountCache = Connection_->GetTableMountCache();
        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
            .ValueOrThrow();

        tableInfo->ValidateDynamic();
        tableInfo->ValidateSorted();
        tableInfo->ValidateReplicated();

        const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];
        auto idMapping = BuildColumnIdMapping(schema, nameTable);

        auto rowBuffer = New<TRowBuffer>(TGetInSyncReplicasTag());

        auto evaluatorCache = Connection_->GetColumnEvaluatorCache();
        auto evaluator = tableInfo->NeedKeyEvaluation ? evaluatorCache->Find(schema) : nullptr;

        std::vector<TTableReplicaId> replicaIds;

        if (keys.Empty()) {
            for (const auto& replica : tableInfo->Replicas) {
                replicaIds.push_back(replica->ReplicaId);
            }
        } else {
            THashMap<TCellId, std::vector<TTabletId>> cellToTabletIds;
            THashSet<TTabletId> tabletIds;
            for (auto key : keys) {
                ValidateClientKey(key, schema, idMapping, nameTable);
                auto capturedKey = rowBuffer->CaptureAndPermuteRow(key, schema, idMapping, nullptr);

                if (evaluator) {
                    evaluator->EvaluateKeys(capturedKey, rowBuffer);
                }
                auto tabletInfo = tableInfo->GetTabletForRow(capturedKey);
                if (tabletIds.insert(tabletInfo->TabletId).second) {
                    ValidateTabletMountedOrFrozen(tableInfo, tabletInfo);
                    cellToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
                }
            }

            std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> futures;
            for (const auto& pair : cellToTabletIds) {
                const auto& cellId = pair.first;
                const auto& perCellTabletIds = pair.second;
                const auto channel = GetReadCellChannelOrThrow(cellId);

                TQueryServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(options.Timeout.Get(Connection_->GetConfig()->DefaultGetInSyncReplicasTimeout));

                auto req = proxy.GetTabletInfo();
                ToProto(req->mutable_tablet_ids(), perCellTabletIds);
                futures.push_back(req->Invoke());
            }
            auto responsesResult = WaitFor(Combine(futures));
            auto responses = responsesResult.ValueOrThrow();

            THashMap<TTableReplicaId, int> replicaIdToCount;
            for (const auto& response : responses) {
                for (const auto& protoTabletInfo : response->tablets()) {
                    for (const auto& protoReplicaInfo : protoTabletInfo.replicas()) {
                        if (IsReplicaInSync(protoReplicaInfo, protoTabletInfo, options.Timestamp)) {
                            ++replicaIdToCount[FromProto<TTableReplicaId>(protoReplicaInfo.replica_id())];
                        }
                    }
                }
            }

            for (const auto& pair : replicaIdToCount) {
                const auto& replicaId = pair.first;
                auto count = pair.second;
                if (count == tabletIds.size()) {
                    replicaIds.push_back(replicaId);
                }
            }
        }

        LOG_DEBUG("Got table in-sync replicas (TableId: %v, Replicas: %v, Timestamp: %llx)",
            tableInfo->TableId,
            replicaIds,
            options.Timestamp);

        return replicaIds;
    }

    std::vector<TColumnarStatistics> DoGetColumnarStatistics(
        const std::vector<TRichYPath>& paths,
        const TGetColumnarStatisticsOptions& options)
    {
        std::vector<TColumnarStatistics> allStatistics;
        allStatistics.reserve(paths.size());

        std::vector<ui64> chunkCount;
        chunkCount.reserve(paths.size());

        auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        auto fetcher = New<TColumnarStatisticsFetcher>(
            options.FetcherConfig,
            nodeDirectory,
            GetCurrentInvoker(),
            nullptr /* scraper */,
            this,
            Logger);

        for (const auto &path : paths) {
            LOG_INFO("Collecting table input chunks (Path: %v)", path);

            auto inputChunks = CollectTableInputChunks(
                path,
                this,
                nodeDirectory,
                options.FetchChunkSpecConfig,
                options.TransactionId,
                Logger);

            LOG_INFO("Fetching columnar statistics (Columns: %v)", *path.GetColumns());


            for (const auto& inputChunk : inputChunks) {
                fetcher->AddChunk(inputChunk, *path.GetColumns());
            }
            chunkCount.push_back(inputChunks.size());
        }

        WaitFor(fetcher->Fetch())
            .ThrowOnError();

        const auto& chunkStatistics = fetcher->GetChunkStatistics();

        ui64 statisticsIndex = 0;

        for (int pathIndex = 0; pathIndex < paths.size(); ++pathIndex) {
            allStatistics.push_back(TColumnarStatistics::MakeEmpty(paths[pathIndex].GetColumns()->size()));
            for (ui64 chunkIndex = 0; chunkIndex < chunkCount[pathIndex]; ++statisticsIndex, ++chunkIndex) {
                allStatistics[pathIndex] += chunkStatistics[statisticsIndex];
            }
        }
        return allStatistics;
    }

    std::vector<TTabletInfo> DoGetTabletInfos(
        const TYPath& path,
        const std::vector<int>& tabletIndexes,
        const TGetTabletsInfoOptions& options)
    {
        const auto& tableMountCache = Connection_->GetTableMountCache();
        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
            .ValueOrThrow();

        tableInfo->ValidateDynamic();

        struct TSubrequest
        {
            TQueryServiceProxy::TReqGetTabletInfoPtr Request;
            std::vector<size_t> ResultIndexes;
        };

        THashMap<TCellId, TSubrequest> cellIdToSubrequest;

        for (size_t resultIndex = 0; resultIndex < tabletIndexes.size(); ++resultIndex) {
            auto tabletIndex = tabletIndexes[resultIndex];
            auto tabletInfo = tableInfo->GetTabletByIndexOrThrow(tabletIndex);
            auto& subrequest = cellIdToSubrequest[tabletInfo->CellId];
            if (!subrequest.Request) {
                auto channel = GetReadCellChannelOrThrow(tabletInfo->CellId);
                TQueryServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(options.Timeout.Get(Connection_->GetConfig()->DefaultGetTabletInfosTimeout));
                subrequest.Request = proxy.GetTabletInfo();
            }
            ToProto(subrequest.Request->add_tablet_ids(), tabletInfo->TabletId);
            subrequest.ResultIndexes.push_back(resultIndex);
        }

        std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> asyncRspsOrErrors;
        std::vector<const TSubrequest*> subrequests;
        for (const auto& pair : cellIdToSubrequest) {
            const auto& subrequest = pair.second;
            subrequests.push_back(&subrequest);
            asyncRspsOrErrors.push_back(subrequest.Request->Invoke());
        }

        auto rspsOrErrors = WaitFor(Combine(asyncRspsOrErrors))
            .ValueOrThrow();

        std::vector<TTabletInfo> results(tabletIndexes.size());
        for (size_t subrequestIndex = 0; subrequestIndex < rspsOrErrors.size(); ++subrequestIndex) {
            const auto& subrequest = *subrequests[subrequestIndex];
            const auto& rsp = rspsOrErrors[subrequestIndex];
            YCHECK(rsp->tablets_size() == subrequest.ResultIndexes.size());
            for (size_t resultIndexIndex = 0; resultIndexIndex < subrequest.ResultIndexes.size(); ++resultIndexIndex) {
                auto& result = results[subrequest.ResultIndexes[resultIndexIndex]];
                const auto& tabletInfo = rsp->tablets(static_cast<int>(resultIndexIndex));
                result.TotalRowCount = tabletInfo.total_row_count();
                result.TrimmedRowCount = tabletInfo.trimmed_row_count();
            }
        }
        return results;
    }

    void ResolveExternalNode(const TYPath path, TTableId* tableId, TCellTag* cellTag, TString* fullPath = nullptr)
    {
        auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());
        auto batchReq = proxy->ExecuteBatch();
        
        {
            auto req = TTableYPathProxy::Get(path + "/@");
            std::vector<TString> attributeKeys{"id", "external_cell_tag"};
            if (fullPath) {
                attributeKeys.push_back("path");
            }
            ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
            batchReq->AddRequest(req, "get_attributes");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting attributes of table %v", path);
        const auto& batchRsp = batchRspOrError.Value();
        auto getAttributesRspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
        auto& rsp = getAttributesRspOrError.Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
        *tableId = attributes->Get<TTableId>("id");
        *cellTag = attributes->Get<TCellTag>("external_cell_tag", PrimaryMasterCellTag);
        if (fullPath) {
            *fullPath = attributes->Get<TString>("path");
        }
    }

    template <class TReq>
    void ExecuteTabletSerivceRequest(const TYPath& path, TReq* req, TYPath* fullPath = nullptr)
    {
        TTableId tableId;
        TCellTag cellTag;
        ResolveExternalNode(path, &tableId, &cellTag, fullPath);

        TTransactionStartOptions txOptions;
        txOptions.Multicell = cellTag != PrimaryMasterCellTag;
        txOptions.CellTag = cellTag;
        auto asyncTransaction = StartNativeTransaction(
            NTransactionClient::ETransactionType::Master,
            txOptions);
        auto transaction = WaitFor(asyncTransaction)
            .ValueOrThrow();

        ToProto(req->mutable_table_id(), tableId);

        SetDynamicTableCypressRequestFullPath(req, fullPath);

        auto actionData = MakeTransactionActionData(*req);
        auto primaryCellId = GetNativeConnection()->GetPrimaryMasterCellId();
        transaction->AddAction(primaryCellId, actionData);

        if (cellTag != PrimaryMasterCellTag) {
            transaction->AddAction(ReplaceCellTagInId(primaryCellId, cellTag), actionData);
        }

        TTransactionCommitOptions commitOptions;
        commitOptions.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy;
        commitOptions.Force2PC = true;

        WaitFor(transaction->Commit(commitOptions))
            .ThrowOnError();
    }

    void DoMountTable(
        const TYPath& path,
        const TMountTableOptions& options)
    {
        if (Connection_->GetConfig()->UseTabletService) {
            TYPath fullPath;
            NTabletClient::NProto::TReqMount req;

            if (options.FirstTabletIndex) {
                req.set_first_tablet_index(*options.FirstTabletIndex);
            }
            if (options.LastTabletIndex) {
                req.set_last_tablet_index(*options.LastTabletIndex);
            }
            if (options.CellId) {
                ToProto(req.mutable_cell_id(), options.CellId);
            }
            req.set_freeze(options.Freeze);

            auto mountTimestamp = WaitFor(Connection_->GetTimestampProvider()->GenerateTimestamps())
                .ValueOrThrow();
            req.set_mount_timestamp(mountTimestamp);

            ExecuteTabletSerivceRequest(path, &req, &fullPath);
        } else {
            auto req = TTableYPathProxy::Mount(path);
            SetMutationId(req, options);

            if (options.FirstTabletIndex) {
                req->set_first_tablet_index(*options.FirstTabletIndex);
            }
            if (options.LastTabletIndex) {
                req->set_last_tablet_index(*options.LastTabletIndex);
            }
            if (options.CellId) {
                ToProto(req->mutable_cell_id(), options.CellId);
            }
            req->set_freeze(options.Freeze);

            auto mountTimestamp = WaitFor(Connection_->GetTimestampProvider()->GenerateTimestamps())
                .ValueOrThrow();
            req->set_mount_timestamp(mountTimestamp);

            auto proxy = CreateWriteProxy<TObjectServiceProxy>();
            WaitFor(proxy->Execute(req))
                .ThrowOnError();
        }
    }

    void DoUnmountTable(
        const TYPath& path,
        const TUnmountTableOptions& options)
    {
        if (Connection_->GetConfig()->UseTabletService) {
            NTabletClient::NProto::TReqUnmount req;

            if (options.FirstTabletIndex) {
                req.set_first_tablet_index(*options.FirstTabletIndex);
            }
            if (options.LastTabletIndex) {
                req.set_last_tablet_index(*options.LastTabletIndex);
            }
            req.set_force(options.Force);

            ExecuteTabletSerivceRequest(path, &req);
        } else {
            auto req = TTableYPathProxy::Unmount(path);
            SetMutationId(req, options);

            if (options.FirstTabletIndex) {
                req->set_first_tablet_index(*options.FirstTabletIndex);
            }
            if (options.LastTabletIndex) {
                req->set_last_tablet_index(*options.LastTabletIndex);
            }
            req->set_force(options.Force);

            auto proxy = CreateWriteProxy<TObjectServiceProxy>();
            WaitFor(proxy->Execute(req))
                .ThrowOnError();
        }
    }

    void DoRemountTable(
        const TYPath& path,
        const TRemountTableOptions& options)
    {
        if (Connection_->GetConfig()->UseTabletService) {
            NTabletClient::NProto::TReqRemount req;

            if (options.FirstTabletIndex) {
                req.set_first_tablet_index(*options.FirstTabletIndex);
            }
            if (options.LastTabletIndex) {
                req.set_first_tablet_index(*options.LastTabletIndex);
            }

            ExecuteTabletSerivceRequest(path, &req);
        } else {
            auto req = TTableYPathProxy::Remount(path);
            SetMutationId(req, options);

            if (options.FirstTabletIndex) {
                req->set_first_tablet_index(*options.FirstTabletIndex);
            }
            if (options.LastTabletIndex) {
                req->set_first_tablet_index(*options.LastTabletIndex);
            }

            auto proxy = CreateWriteProxy<TObjectServiceProxy>();
            WaitFor(proxy->Execute(req))
                .ThrowOnError();
        }
    }

    void DoFreezeTable(
        const TYPath& path,
        const TFreezeTableOptions& options)
    {
        if (Connection_->GetConfig()->UseTabletService) {
            NTabletClient::NProto::TReqFreeze req;

            if (options.FirstTabletIndex) {
                req.set_first_tablet_index(*options.FirstTabletIndex);
            }
            if (options.LastTabletIndex) {
                req.set_last_tablet_index(*options.LastTabletIndex);
            }

            ExecuteTabletSerivceRequest(path, &req);
        } else {
            auto req = TTableYPathProxy::Freeze(path);
            SetMutationId(req, options);

            if (options.FirstTabletIndex) {
                req->set_first_tablet_index(*options.FirstTabletIndex);
            }
            if (options.LastTabletIndex) {
                req->set_last_tablet_index(*options.LastTabletIndex);
            }

            auto proxy = CreateWriteProxy<TObjectServiceProxy>();
            WaitFor(proxy->Execute(req))
                .ThrowOnError();
        }
    }

    void DoUnfreezeTable(
        const TYPath& path,
        const TUnfreezeTableOptions& options)
    {
        if (Connection_->GetConfig()->UseTabletService) {
            NTabletClient::NProto::TReqUnfreeze req;

            if (options.FirstTabletIndex) {
                req.set_first_tablet_index(*options.FirstTabletIndex);
            }
            if (options.LastTabletIndex) {
                req.set_last_tablet_index(*options.LastTabletIndex);
            }

            ExecuteTabletSerivceRequest(path, &req);
        } else {
            auto req = TTableYPathProxy::Unfreeze(path);
            SetMutationId(req, options);

            if (options.FirstTabletIndex) {
                req->set_first_tablet_index(*options.FirstTabletIndex);
            }
            if (options.LastTabletIndex) {
                req->set_last_tablet_index(*options.LastTabletIndex);
            }

            auto proxy = CreateWriteProxy<TObjectServiceProxy>();
            WaitFor(proxy->Execute(req))
                .ThrowOnError();
        }
    }

    NTabletClient::NProto::TReqReshard MakeReshardRequest(
        const TReshardTableOptions& options)
    {
        NTabletClient::NProto::TReqReshard req;
        if (options.FirstTabletIndex) {
            req.set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req.set_last_tablet_index(*options.LastTabletIndex);
        }
        return req;
    }

    TTableYPathProxy::TReqReshardPtr MakeYpathReshardRequest(
        const TYPath& path,
        const TReshardTableOptions& options)
    {
        auto req = TTableYPathProxy::Reshard(path);
        SetMutationId(req, options);

        if (options.FirstTabletIndex) {
            req->set_first_tablet_index(*options.FirstTabletIndex);
        }
        if (options.LastTabletIndex) {
            req->set_last_tablet_index(*options.LastTabletIndex);
        }
        return req;
    }

    void DoReshardTableWithPivotKeys(
        const TYPath& path,
        const std::vector<NTableClient::TOwningKey>& pivotKeys,
        const TReshardTableOptions& options)
    {
        if (Connection_->GetConfig()->UseTabletService) {
            auto req = MakeReshardRequest(options);
            ToProto(req.mutable_pivot_keys(), pivotKeys);
            req.set_tablet_count(pivotKeys.size());

            ExecuteTabletSerivceRequest(path, &req);
        } else {
            auto req = MakeYpathReshardRequest(path, options);
            ToProto(req->mutable_pivot_keys(), pivotKeys);
            req->set_tablet_count(pivotKeys.size());

            auto proxy = CreateWriteProxy<TObjectServiceProxy>();
            WaitFor(proxy->Execute(req))
                .ThrowOnError();
        }
    }

    void DoReshardTableWithTabletCount(
        const TYPath& path,
        int tabletCount,
        const TReshardTableOptions& options)
    {
        if (Connection_->GetConfig()->UseTabletService) {
            auto req = MakeReshardRequest(options);
            req.set_tablet_count(tabletCount);

            ExecuteTabletSerivceRequest(path, &req);
        } else {
            auto req = MakeYpathReshardRequest(path, options);
            req->set_tablet_count(tabletCount);

            auto proxy = CreateWriteProxy<TObjectServiceProxy>();
            WaitFor(proxy->Execute(req))
                .ThrowOnError();
        }
    }

    void DoAlterTable(
        const TYPath& path,
        const TAlterTableOptions& options)
    {
        auto req = TTableYPathProxy::Alter(path);
        SetTransactionId(req, options, true);
        SetMutationId(req, options);

        if (options.Schema) {
            ToProto(req->mutable_schema(), *options.Schema);
        }
        if (options.Dynamic) {
            req->set_dynamic(*options.Dynamic);
        }
        if (options.UpstreamReplicaId) {
            ToProto(req->mutable_upstream_replica_id(), *options.UpstreamReplicaId);
        }

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }

    void DoTrimTable(
        const TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const TTrimTableOptions& options)
    {
        const auto& tableMountCache = Connection_->GetTableMountCache();
        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
            .ValueOrThrow();

        tableInfo->ValidateDynamic();
        tableInfo->ValidateOrdered();

        auto tabletInfo = tableInfo->GetTabletByIndexOrThrow(tabletIndex);

        auto channel = GetCellChannelOrThrow(tabletInfo->CellId);

        TTabletServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Connection_->GetConfig()->DefaultTrimTableTimeout);

        auto req = proxy.Trim();
        ToProto(req->mutable_tablet_id(), tabletInfo->TabletId);
        req->set_mount_revision(tabletInfo->MountRevision);
        req->set_trimmed_row_count(trimmedRowCount);

        WaitFor(req->Invoke())
            .ValueOrThrow();
    }

    void DoAlterTableReplica(
        const TTableReplicaId& replicaId,
        const TAlterTableReplicaOptions& options)
    {
        auto cellTag = CellTagFromId(replicaId);

        auto req = TTableReplicaYPathProxy::Alter(FromObjectId(replicaId));
        if (options.Enabled) {
            req->set_enabled(*options.Enabled);
        }
        if (options.Mode) {
            req->set_mode(static_cast<int>(*options.Mode));
        }
        if (options.PreserveTimestamps) {
            req->set_preserve_timestamps(*options.PreserveTimestamps);
        }
        if (options.Atomicity) {
            req->set_atomicity(static_cast<int>(*options.Atomicity));
        }
        auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }

    TYsonString DoGetNode(
        const TYPath& path,
        const TGetNodeOptions& options)
    {
        auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
        auto batchReq = proxy->ExecuteBatch();
        SetBalancingHeader(batchReq, options);

        auto req = TYPathProxy::Get(path);
        SetTransactionId(req, options, true);
        SetSuppressAccessTracking(req, options);
        SetCachingHeader(req, options);
        if (options.Attributes) {
            ToProto(req->mutable_attributes()->mutable_keys(), *options.Attributes);
        }
        if (options.MaxSize) {
            req->set_limit(*options.MaxSize);
        }
        if (options.Options) {
            ToProto(req->mutable_options(), *options.Options);
        }
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(0)
            .ValueOrThrow();

        return TYsonString(rsp->value());
    }

    void DoSetNode(
        const TYPath& path,
        const TYsonString& value,
        const TSetNodeOptions& options)
    {
        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        auto batchReq = proxy->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TYPathProxy::Set(path);
        SetTransactionId(req, options, true);
        SetMutationId(req, options);

        // Binarize the value.
        TStringStream stream;
        TBufferedBinaryYsonWriter writer(&stream, EYsonType::Node, false);
        YCHECK(value.GetType() == EYsonType::Node);
        writer.OnRaw(value.GetData(), EYsonType::Node);
        writer.Flush();
        req->set_value(stream.Str());
        req->set_recursive(options.Recursive);

        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        batchRsp->GetResponse<TYPathProxy::TRspSet>(0)
            .ThrowOnError();
    }

    void DoRemoveNode(
        const TYPath& path,
        const TRemoveNodeOptions& options)
    {
        TCellTag cellTag = PrimaryMasterCellTag;

        NYPath::TTokenizer tokenizer(path);
        if (tokenizer.Advance() == NYPath::ETokenType::Literal) {
            const auto& token = tokenizer.GetToken();
            if (token.StartsWith(ObjectIdPathPrefix)) {
                TStringBuf objectIdString(token.begin() + ObjectIdPathPrefix.length(), token.end());
                TObjectId objectId;
                if (TObjectId::FromString(objectIdString, &objectId)) {
                    cellTag = CellTagFromId(objectId);
                }
            }
        }

        auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
        auto batchReq = proxy->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TYPathProxy::Remove(path);
        SetTransactionId(req, options, true);
        SetMutationId(req, options);
        req->set_recursive(options.Recursive);
        req->set_force(options.Force);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        batchRsp->GetResponse<TYPathProxy::TRspRemove>(0)
            .ThrowOnError();
    }

    TYsonString DoListNode(
        const TYPath& path,
        const TListNodeOptions& options)
    {
        auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
        auto batchReq = proxy->ExecuteBatch();
        SetBalancingHeader(batchReq, options);

        auto req = TYPathProxy::List(path);
        SetTransactionId(req, options, true);
        SetSuppressAccessTracking(req, options);
        SetCachingHeader(req, options);
        if (options.Attributes) {
            ToProto(req->mutable_attributes()->mutable_keys(), *options.Attributes);
        }
        if (options.MaxSize) {
            req->set_limit(*options.MaxSize);
        }
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspList>(0)
            .ValueOrThrow();

        return TYsonString(rsp->value());
    }

    TNodeId DoCreateNode(
        const TYPath& path,
        EObjectType type,
        const TCreateNodeOptions& options)
    {
        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        auto batchReq = proxy->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TCypressYPathProxy::Create(path);
        SetTransactionId(req, options, true);
        SetMutationId(req, options);
        req->set_type(static_cast<int>(type));
        req->set_recursive(options.Recursive);
        req->set_ignore_existing(options.IgnoreExisting);
        req->set_force(options.Force);
        if (options.Attributes) {
            ToProto(req->mutable_node_attributes(), *options.Attributes);
        }
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCreate>(0)
            .ValueOrThrow();
        return FromProto<TNodeId>(rsp->node_id());
    }

    TLockNodeResult DoLockNode(
        const TYPath& path,
        ELockMode mode,
        const TLockNodeOptions& options)
    {
        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        auto batchReq = proxy->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TCypressYPathProxy::Lock(path);
        SetTransactionId(req, options, false);
        SetMutationId(req, options);
        req->set_mode(static_cast<int>(mode));
        req->set_waitable(options.Waitable);
        if (options.ChildKey) {
            req->set_child_key(*options.ChildKey);
        }
        if (options.AttributeKey) {
            req->set_attribute_key(*options.AttributeKey);
        }
        auto timestamp = WaitFor(Connection_->GetTimestampProvider()->GenerateTimestamps())
            .ValueOrThrow();
        req->set_timestamp(timestamp);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspLock>(0)
            .ValueOrThrow();

        return TLockNodeResult({FromProto<TLockId>(rsp->lock_id()), FromProto<TNodeId>(rsp->node_id())});
    }

    TNodeId DoCopyNode(
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TCopyNodeOptions& options)
    {
        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        auto batchReq = proxy->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TCypressYPathProxy::Copy(dstPath);
        SetTransactionId(req, options, true);
        SetMutationId(req, options);
        req->set_source_path(srcPath);
        req->set_preserve_account(options.PreserveAccount);
        req->set_preserve_expiration_time(options.PreserveExpirationTime);
        req->set_preserve_creation_time(options.PreserveCreationTime);
        req->set_recursive(options.Recursive);
        req->set_force(options.Force);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCopy>(0)
            .ValueOrThrow();
        return FromProto<TNodeId>(rsp->node_id());
    }

    TNodeId DoMoveNode(
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TMoveNodeOptions& options)
    {
        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        auto batchReq = proxy->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TCypressYPathProxy::Copy(dstPath);
        SetTransactionId(req, options, true);
        SetMutationId(req, options);
        req->set_source_path(srcPath);
        req->set_preserve_account(options.PreserveAccount);
        req->set_preserve_expiration_time(options.PreserveExpirationTime);
        req->set_remove_source(true);
        req->set_recursive(options.Recursive);
        req->set_force(options.Force);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCopy>(0)
            .ValueOrThrow();
        return FromProto<TNodeId>(rsp->node_id());
    }

    TNodeId DoLinkNode(
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TLinkNodeOptions& options)
    {
        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        auto batchReq = proxy->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TCypressYPathProxy::Create(dstPath);
        req->set_type(static_cast<int>(EObjectType::Link));
        req->set_recursive(options.Recursive);
        req->set_ignore_existing(options.IgnoreExisting);
        req->set_force(options.Force);
        SetTransactionId(req, options, true);
        SetMutationId(req, options);
        auto attributes = options.Attributes ? ConvertToAttributes(options.Attributes.get()) : CreateEphemeralAttributes();
        attributes->Set("target_path", srcPath);
        ToProto(req->mutable_node_attributes(), *attributes);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCreate>(0)
            .ValueOrThrow();
        return FromProto<TNodeId>(rsp->node_id());
    }

    void DoConcatenateNodes(
        const std::vector<TYPath>& srcPaths,
        const TYPath& dstPath,
        TConcatenateNodesOptions options)
    {
        if (options.Retry) {
            THROW_ERROR_EXCEPTION("\"concatenate\" command is not retriable");
        }

        using NChunkClient::NProto::TDataStatistics;

        try {
            // Get objects ids.
            std::vector<TObjectId> srcIds;
            TCellTagList srcCellTags;
            TObjectId dstId;
            TCellTag dstCellTag;
            std::unique_ptr<NTableClient::IOutputSchemaInferer> outputSchemaInferer;
            {
                auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());
                auto batchReq = proxy->ExecuteBatch();

                for (const auto& path : srcPaths) {
                    auto req = TObjectYPathProxy::GetBasicAttributes(path);
                    SetTransactionId(req, options, true);
                    batchReq->AddRequest(req, "get_src_attributes");
                }
                {
                    auto req = TObjectYPathProxy::GetBasicAttributes(dstPath);
                    SetTransactionId(req, options, true);
                    batchReq->AddRequest(req, "get_dst_attributes");
                }

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error getting basic attributes of inputs and outputs");
                const auto& batchRsp = batchRspOrError.Value();

                TNullable<EObjectType> commonType;
                TNullable<TString> pathWithCommonType;
                auto checkType = [&] (EObjectType type, const TYPath& path) {
                    if (type != EObjectType::Table && type != EObjectType::File) {
                        THROW_ERROR_EXCEPTION("Type of %v must be either %Qlv or %Qlv",
                            path,
                            EObjectType::Table,
                            EObjectType::File);
                    }
                    if (commonType && *commonType != type) {
                        THROW_ERROR_EXCEPTION("Type of %v (%Qlv) must be the same as type of %v (%Qlv)",
                            path,
                            type,
                            *pathWithCommonType,
                            *commonType);
                    }
                    commonType = type;
                    pathWithCommonType = path;
                };

                {
                    auto rspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_src_attributes");
                    for (int srcIndex = 0; srcIndex < srcPaths.size(); ++srcIndex) {
                        const auto& srcPath = srcPaths[srcIndex];
                        THROW_ERROR_EXCEPTION_IF_FAILED(rspsOrError[srcIndex], "Error getting attributes of %v", srcPath);
                        const auto& rsp = rspsOrError[srcIndex].Value();

                        auto id = FromProto<TObjectId>(rsp->object_id());
                        srcIds.push_back(id);
                        srcCellTags.push_back(rsp->cell_tag());
                        checkType(TypeFromId(id), srcPath);
                    }
                }

                {
                    auto rspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_dst_attributes");
                    THROW_ERROR_EXCEPTION_IF_FAILED(rspsOrError[0], "Error getting attributes of %v", dstPath);
                    const auto& rsp = rspsOrError[0].Value();

                    dstId = FromProto<TObjectId>(rsp->object_id());
                    dstCellTag = rsp->cell_tag();
                    checkType(TypeFromId(dstId), dstPath);
                }

                // Check table schemas.
                if (*commonType == EObjectType::Table) {
                    auto createGetSchemaRequest = [&] (const TObjectId& objectId) {
                        auto req = TYPathProxy::Get(FromObjectId(objectId) + "/@");
                        SetTransactionId(req, options, true);
                        req->mutable_attributes()->add_keys("schema");
                        req->mutable_attributes()->add_keys("schema_mode");
                        return req;
                    };

                    TObjectServiceProxy::TRspExecuteBatchPtr getSchemasRsp;
                    {
                        auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());
                        auto getSchemasReq = proxy->ExecuteBatch();
                        {
                            auto req = createGetSchemaRequest(dstId);
                            getSchemasReq->AddRequest(req, "get_dst_schema");
                        }
                        for (const auto& id : srcIds) {
                            auto req = createGetSchemaRequest(id);
                            getSchemasReq->AddRequest(req, "get_src_schema");
                        }

                        auto batchResponseOrError = WaitFor(getSchemasReq->Invoke());
                        THROW_ERROR_EXCEPTION_IF_FAILED(batchResponseOrError, "Error fetching table schemas");

                        getSchemasRsp = batchResponseOrError.Value();
                    }

                    {
                        const auto& rspOrErrorList = getSchemasRsp->GetResponses<TYPathProxy::TRspGet>("get_dst_schema");
                        YCHECK(rspOrErrorList.size() == 1);
                        const auto& rspOrError = rspOrErrorList[0];
                        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching schema for %v", dstPath);

                        const auto& rsp = rspOrError.Value();
                        const auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
                        const auto schema = attributes->Get<TTableSchema>("schema");
                        const auto schemaMode = attributes->Get<ETableSchemaMode>("schema_mode");
                        switch (schemaMode) {
                            case ETableSchemaMode::Strong:
                                if (schema.IsSorted()) {
                                    THROW_ERROR_EXCEPTION("Destination path %v has sorted schema, concatenation into sorted table is not supported",
                                        dstPath);
                                }
                                outputSchemaInferer = CreateSchemaCompatibilityChecker(dstPath, schema);
                                break;
                            case ETableSchemaMode::Weak:
                                outputSchemaInferer = CreateOutputSchemaInferer();
                                if (options.Append) {
                                    outputSchemaInferer->AddInputTableSchema(dstPath, schema, schemaMode);
                                }
                                break;
                            default:
                                Y_UNREACHABLE();
                        }
                    }

                    {
                        const auto& rspOrErrorList = getSchemasRsp->GetResponses<TYPathProxy::TRspGet>("get_src_schema");
                        YCHECK(rspOrErrorList.size() == srcPaths.size());
                        for (size_t i = 0; i < rspOrErrorList.size(); ++i) {
                            const auto& path = srcPaths[i];
                            const auto& rspOrError = rspOrErrorList[i];
                            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching schema for %v", path);

                            const auto& rsp = rspOrError.Value();
                            const auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
                            const auto schema = attributes->Get<TTableSchema>("schema");
                            const auto schemaMode = attributes->Get<ETableSchemaMode>("schema_mode");
                            outputSchemaInferer->AddInputTableSchema(path, schema, schemaMode);
                        }
                    }
                }
            }

            // Get source chunk ids.
            // Maps src index -> list of chunk ids for this src.
            std::vector<std::vector<TChunkId>> groupedChunkIds(srcPaths.size());
            {
                THashMap<TCellTag, std::vector<int>> cellTagToIndexes;
                for (int srcIndex = 0; srcIndex < srcCellTags.size(); ++srcIndex) {
                    cellTagToIndexes[srcCellTags[srcIndex]].push_back(srcIndex);
                }

                for (const auto& pair : cellTagToIndexes) {
                    auto srcCellTag = pair.first;
                    const auto& srcIndexes = pair.second;

                    auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions(), srcCellTag);
                    auto batchReq = proxy->ExecuteBatch();

                    for (int localIndex = 0; localIndex < srcIndexes.size(); ++localIndex) {
                        int srcIndex = srcIndexes[localIndex];
                        auto req = TChunkOwnerYPathProxy::Fetch(FromObjectId(srcIds[srcIndex]));
                        SetTransactionId(req, options, true);
                        ToProto(req->mutable_ranges(), std::vector<TReadRange>{TReadRange()});
                        batchReq->AddRequest(req, "fetch");
                    }

                    auto batchRspOrError = WaitFor(batchReq->Invoke());
                    THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error fetching inputs");

                    const auto& batchRsp = batchRspOrError.Value();
                    auto rspsOrError = batchRsp->GetResponses<TChunkOwnerYPathProxy::TRspFetch>("fetch");
                    for (int localIndex = 0; localIndex < srcIndexes.size(); ++localIndex) {
                        int srcIndex = srcIndexes[localIndex];
                        const auto& rspOrError = rspsOrError[localIndex];
                        const auto& path = srcPaths[srcIndex];
                        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching %v", path);
                        const auto& rsp = rspOrError.Value();

                        for (const auto& chunk : rsp->chunks()) {
                            groupedChunkIds[srcIndex].push_back(FromProto<TChunkId>(chunk.chunk_id()));
                        }
                    }
                }
            }

            // Begin upload.
            TTransactionId uploadTransactionId;
            const auto dstIdPath = FromObjectId(dstId);
            {
                auto proxy = CreateWriteProxy<TObjectServiceProxy>();

                auto req = TChunkOwnerYPathProxy::BeginUpload(dstIdPath);
                req->set_update_mode(static_cast<int>(options.Append ? EUpdateMode::Append : EUpdateMode::Overwrite));
                req->set_lock_mode(static_cast<int>(options.Append ? ELockMode::Shared : ELockMode::Exclusive));
                req->set_upload_transaction_title(Format("Concatenating %v to %v",
                    srcPaths,
                    dstPath));
                // NB: Replicate upload transaction to each secondary cell since we have
                // no idea as of where the chunks we're about to attach may come from.
                ToProto(req->mutable_upload_transaction_secondary_cell_tags(), Connection_->GetSecondaryMasterCellTags());
                req->set_upload_transaction_timeout(ToProto<i64>(Connection_->GetConfig()->TransactionManager->DefaultTransactionTimeout));
                NRpc::GenerateMutationId(req);
                SetTransactionId(req, options, true);

                auto rspOrError = WaitFor(proxy->Execute(req));
                THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error starting upload to %v", dstPath);
                const auto& rsp = rspOrError.Value();

                uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
            }

            NTransactionClient::TTransactionAttachOptions attachOptions;
            attachOptions.PingAncestors = options.PingAncestors;
            attachOptions.AutoAbort = true;
            auto uploadTransaction = TransactionManager_->Attach(uploadTransactionId, attachOptions);

            // Flatten chunk ids.
            std::vector<TChunkId> flatChunkIds;
            for (const auto& ids : groupedChunkIds) {
                flatChunkIds.insert(flatChunkIds.end(), ids.begin(), ids.end());
            }

            // Teleport chunks.
            {
                auto teleporter = New<TChunkTeleporter>(
                    Connection_->GetConfig(),
                    this,
                    Connection_->GetInvoker(),
                    uploadTransactionId,
                    Logger);

                for (const auto& chunkId : flatChunkIds) {
                    teleporter->RegisterChunk(chunkId, dstCellTag);
                }

                WaitFor(teleporter->Run())
                    .ThrowOnError();
            }

            // Get upload params.
            TChunkListId chunkListId;
            {
                auto proxy = CreateWriteProxy<TObjectServiceProxy>(dstCellTag);

                auto req = TChunkOwnerYPathProxy::GetUploadParams(dstIdPath);
                NCypressClient::SetTransactionId(req, uploadTransactionId);

                auto rspOrError = WaitFor(proxy->Execute(req));
                THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting upload parameters for %v", dstPath);
                const auto& rsp = rspOrError.Value();

                chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
            }

            // Attach chunks to chunk list.
            TDataStatistics dataStatistics;
            {
                auto proxy = CreateWriteProxy<TChunkServiceProxy>(dstCellTag);

                auto batchReq = proxy->ExecuteBatch();
                NRpc::GenerateMutationId(batchReq);
                batchReq->set_suppress_upstream_sync(true);

                auto req = batchReq->add_attach_chunk_trees_subrequests();
                ToProto(req->mutable_parent_id(), chunkListId);
                ToProto(req->mutable_child_ids(), flatChunkIds);
                req->set_request_statistics(true);

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error attaching chunks to %v", dstPath);
                const auto& batchRsp = batchRspOrError.Value();

                const auto& rsp = batchRsp->attach_chunk_trees_subresponses(0);
                dataStatistics = rsp.statistics();
            }

            // End upload.
            {
                auto proxy = CreateWriteProxy<TObjectServiceProxy>();

                auto req = TChunkOwnerYPathProxy::EndUpload(dstIdPath);
                *req->mutable_statistics() = dataStatistics;
                if (outputSchemaInferer) {
                    ToProto(req->mutable_table_schema(), outputSchemaInferer->GetOutputTableSchema());
                    req->set_schema_mode(static_cast<int>(outputSchemaInferer->GetOutputTableSchemaMode()));
                }
                NCypressClient::SetTransactionId(req, uploadTransactionId);
                NRpc::GenerateMutationId(req);

                auto rspOrError = WaitFor(proxy->Execute(req));
                THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error finishing upload to %v", dstPath);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error concatenating %v to %v",
                srcPaths,
                dstPath)
                << ex;
        }
    }

    bool DoNodeExists(
        const TYPath& path,
        const TNodeExistsOptions& options)
    {
        auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
        auto batchReq = proxy->ExecuteBatch();
        SetBalancingHeader(batchReq, options);

        auto req = TYPathProxy::Exists(path);
        SetTransactionId(req, options, true);
        SetSuppressAccessTracking(req, options);
        SetCachingHeader(req, options);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspExists>(0)
            .ValueOrThrow();

        return rsp->value();
    }


    TObjectId DoCreateObject(
        EObjectType type,
        const TCreateObjectOptions& options)
    {
        auto attributes = options.Attributes;
        auto cellTag = PrimaryMasterCellTag;

        if (type == EObjectType::TableReplica) {
            TNullable<TString> path;
            if (!attributes || !(path = attributes->Find<TString>("table_path"))) {
                THROW_ERROR_EXCEPTION("Attribute \"table_path\" is not found");
            }

            TTableId tableId;
            ResolveExternalNode(*path, &tableId, &cellTag);

            auto newAttributes = options.Attributes->Clone();   
            newAttributes->Set("table_path", FromObjectId(tableId));

            attributes = std::move(newAttributes);
        } else if (type == EObjectType::TabletAction) {
            TNullable<std::vector<TTabletId>> tabletIds;
            if (!attributes ||
                !(tabletIds = attributes->Find<std::vector<TTabletId>>("tablet_ids")) ||
                tabletIds->empty())
            {
                THROW_ERROR_EXCEPTION("Attribute \"tablet_ids\" is not found or is empty");
            }

            cellTag = CellTagFromId((*tabletIds)[0]);
        }

        auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
        auto batchReq = proxy->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TMasterYPathProxy::CreateObject();
        SetMutationId(req, options);
        req->set_type(static_cast<int>(type));
        if (attributes) {
            ToProto(req->mutable_object_attributes(), *attributes);
        }
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObject>(0)
            .ValueOrThrow();

        return FromProto<TObjectId>(rsp->object_id());
    }

    TCheckPermissionByAclResult DoCheckPermissionByAcl(
        const TNullable<TString>& user,
        EPermission permission,
        INodePtr acl,
        const TCheckPermissionByAclOptions& options)
    {
        auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
        auto batchReq = proxy->ExecuteBatch();
        SetBalancingHeader(batchReq, options);

        auto req = TMasterYPathProxy::CheckPermissionByAcl();
        if (user) {
            req->set_user(*user);
        }
        req->set_permission(static_cast<int>(permission));
        req->set_acl(ConvertToYsonString(acl).GetData());
        SetCachingHeader(req, options);

        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCheckPermissionByAcl>(0)
            .ValueOrThrow();

        TCheckPermissionByAclResult result;
        result.Action = ESecurityAction(rsp->action());
        result.SubjectId = FromProto<TSubjectId>(rsp->subject_id());
        result.SubjectName = rsp->has_subject_name() ? MakeNullable(rsp->subject_name()) : Null;
        return result;
    }

    void SetTouchedAttribute(
        const TString& destination,
        const TPrerequisiteOptions& options = TPrerequisiteOptions(),
        const TTransactionId& transactionId = NullTransactionId)
    {
        auto fileCacheClient = Connection_->CreateNativeClient(TClientOptions(NSecurityClient::FileCacheUserName));

        // Set /@touched attribute.
        {
            auto setNodeOptions = TSetNodeOptions();
            setNodeOptions.PrerequisiteTransactionIds = options.PrerequisiteTransactionIds;
            setNodeOptions.PrerequisiteRevisions = options.PrerequisiteRevisions;
            setNodeOptions.TransactionId = transactionId;

            auto asyncResult = fileCacheClient->SetNode(destination + "/@touched", ConvertToYsonString(true), setNodeOptions);
            auto rspOrError = WaitFor(asyncResult);

            if (rspOrError.GetCode() != NCypressClient::EErrorCode::ConcurrentTransactionLockConflict) {
                THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error setting /@touched attribute");
            }

            LOG_DEBUG(
                "Attribute /@touched set (Destination: %v)",
                destination);
        }
    }

    TGetFileFromCacheResult DoGetFileFromCache(
        const TString& md5,
        const TGetFileFromCacheOptions& options)
    {
        TGetFileFromCacheResult result;
        auto destination = GetFilePathInCache(md5, options.CachePath);

        auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
        auto req = TYPathProxy::Get(destination + "/@");

        std::vector<TString> attributeKeys{
            "md5"
        };
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

        auto rspOrError = WaitFor(proxy->Execute(req));
        if (!rspOrError.IsOK()) {
            LOG_DEBUG(
                rspOrError,
                "File is missing "
                "(Destination: %v, MD5: %v)",
                destination,
                md5);

            return result;
        }

        auto rsp = rspOrError.Value();
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

        auto originalMD5 = attributes->Get<TString>("md5", "");
        if (md5 != originalMD5) {
            LOG_DEBUG(
                "File has incorrect md5 hash "
                "(Destination: %v, expectedMD5: %v, originalMD5: %v)",
                destination,
                md5,
                originalMD5);

            return result;
        }

        SetTouchedAttribute(destination);

        result.Path = destination;
        return result;
    }

    TPutFileToCacheResult DoAttemptPutFileToCache(
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options,
        NLogging::TLogger logger)
    {
        auto Logger = logger;

        TPutFileToCacheResult result;

        // Start transaction.
        NApi::ITransactionPtr transaction;
        {
            auto transactionStartOptions = TTransactionStartOptions();
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Format("Putting file %v to cache", path));
            transactionStartOptions.Attributes = std::move(attributes);

            auto asyncTransaction = StartTransaction(ETransactionType::Master, transactionStartOptions);
            transaction = WaitFor(asyncTransaction)
                .ValueOrThrow();

            LOG_DEBUG(
                "Transaction started (TransactionId: %v)",
                transaction->GetId());
        }

        Logger.AddTag("TransactionId: %v", transaction->GetId());

        // Acquire lock.
        NYPath::TYPath objectIdPath;
        {
            TLockNodeOptions lockNodeOptions;
            lockNodeOptions.TransactionId = transaction->GetId();
            auto lockResult = DoLockNode(path, ELockMode::Exclusive, lockNodeOptions);
            objectIdPath = FromObjectId(lockResult.NodeId);

            LOG_DEBUG(
                "Lock for node acquired (LockId: %v)",
                lockResult.LockId);
        }

        // Check permissions.
        {
            auto checkPermissionOptions = TCheckPermissionOptions();
            checkPermissionOptions.TransactionId = transaction->GetId();

            auto readPermissionResult = DoCheckPermission(Options_.GetUser(), objectIdPath, EPermission::Read, checkPermissionOptions);
            readPermissionResult.ToError(Options_.GetUser(), EPermission::Read)
                .ThrowOnError();

            auto removePermissionResult = DoCheckPermission(Options_.GetUser(), objectIdPath, EPermission::Remove, checkPermissionOptions);
            removePermissionResult.ToError(Options_.GetUser(), EPermission::Remove)
                .ThrowOnError();

            auto usePermissionResult = DoCheckPermission(Options_.GetUser(), options.CachePath, EPermission::Use, checkPermissionOptions);

            auto writePermissionResult = DoCheckPermission(Options_.GetUser(), options.CachePath, EPermission::Write, checkPermissionOptions);

            if (usePermissionResult.Action == ESecurityAction::Deny && writePermissionResult.Action == ESecurityAction::Deny) {
                THROW_ERROR_EXCEPTION("You need write or use permission to use file cache")
                    << usePermissionResult.ToError(Options_.GetUser(), EPermission::Use)
                    << writePermissionResult.ToError(Options_.GetUser(), EPermission::Write);
            }
        }

        // Check that MD5 hash is equal to the original MD5 hash of the file.
        {
            auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
            auto req = TYPathProxy::Get(objectIdPath + "/@");

            std::vector<TString> attributeKeys{
                "md5"
            };
            ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

            NCypressClient::SetTransactionId(req, transaction->GetId());

            auto rspOrError = WaitFor(proxy->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(
                rspOrError,
                "Error requesting md5 hash of file %v",
                path);

            auto rsp = rspOrError.Value();
            auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

            auto md5 = attributes->Get<TString>("md5");
            if (expectedMD5 != md5) {
                THROW_ERROR_EXCEPTION(
                    "MD5 mismatch; expected %v, got %v",
                    expectedMD5,
                    md5);
            }

            LOG_DEBUG(
                "MD5 hash checked (MD5: %v)",
                expectedMD5);
        }

        auto destination = GetFilePathInCache(expectedMD5, options.CachePath);
        auto fileCacheClient = Connection_->CreateNativeClient(TClientOptions(NSecurityClient::FileCacheUserName));

        // Move file.
        {
            auto copyOptions = TCopyNodeOptions();
            copyOptions.TransactionId = transaction->GetId();
            copyOptions.Recursive = true;
            copyOptions.Force = true;
            copyOptions.PrerequisiteRevisions = options.PrerequisiteRevisions;
            copyOptions.PrerequisiteTransactionIds = options.PrerequisiteTransactionIds;

            WaitFor(fileCacheClient->CopyNode(objectIdPath, destination, copyOptions))
                .ValueOrThrow();

            LOG_DEBUG(
                "File has been copied to cache (Destination: %v)",
                destination);
        }

        SetTouchedAttribute(destination, options, transaction->GetId());

        WaitFor(transaction->Commit())
            .ThrowOnError();

        result.Path = destination;
        return result;
    }

    TPutFileToCacheResult DoPutFileToCache(
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options)
    {
        NLogging::TLogger logger = Logger.AddTag("Path: %v", path).AddTag("Command: PutFileToCache");
        auto Logger = logger;

        int retryAttempts = 0;
        while (true) {
            try {
                return DoAttemptPutFileToCache(path, expectedMD5, options, logger);
            } catch (const TErrorException& ex) {
                auto error = ex.Error();
                ++retryAttempts;
                if (retryAttempts < options.RetryCount && error.FindMatching(NCypressClient::EErrorCode::ConcurrentTransactionLockConflict)) {
                    LOG_DEBUG(error, "Put file to cache failed, make next retry");
                } else {
                    throw;
                }
            }
        }
    }

    void DoAddMember(
        const TString& group,
        const TString& member,
        const TAddMemberOptions& options)
    {
        auto req = TGroupYPathProxy::AddMember(GetGroupPath(group));
        req->set_name(member);
        SetMutationId(req, options);

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }

    void DoRemoveMember(
        const TString& group,
        const TString& member,
        const TRemoveMemberOptions& options)
    {
        auto req = TGroupYPathProxy::RemoveMember(GetGroupPath(group));
        req->set_name(member);
        SetMutationId(req, options);

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }

    TCheckPermissionResult DoCheckPermission(
        const TString& user,
        const TYPath& path,
        EPermission permission,
        const TCheckPermissionOptions& options)
    {
        auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
        auto batchReq = proxy->ExecuteBatch();
        SetBalancingHeader(batchReq, options);

        auto req = TObjectYPathProxy::CheckPermission(path);
        req->set_user(user);
        req->set_permission(static_cast<int>(permission));
        SetTransactionId(req, options, true);
        SetCachingHeader(req, options);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TObjectYPathProxy::TRspCheckPermission>(0)
            .ValueOrThrow();

        TCheckPermissionResult result;
        result.Action = ESecurityAction(rsp->action());
        result.ObjectId = FromProto<TObjectId>(rsp->object_id());
        result.ObjectName = rsp->has_object_name() ? MakeNullable(rsp->object_name()) : Null;
        result.SubjectId = FromProto<TSubjectId>(rsp->subject_id());
        result.SubjectName = rsp->has_subject_name() ? MakeNullable(rsp->subject_name()) : Null;
        return result;
    }


    TOperationId DoStartOperation(
        EOperationType type,
        const TYsonString& spec,
        const TStartOperationOptions& options)
    {
        auto req = SchedulerProxy_->StartOperation();
        SetTransactionId(req, options, true);
        SetMutationId(req, options);
        req->set_type(static_cast<int>(type));
        req->set_spec(spec.GetData());

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        return FromProto<TOperationId>(rsp->operation_id());
    }

    void DoAbortOperation(
        const TOperationId& operationId,
        const TAbortOperationOptions& options)
    {
        auto req = SchedulerProxy_->AbortOperation();
        ToProto(req->mutable_operation_id(), operationId);
        if (options.AbortMessage) {
            req->set_abort_message(*options.AbortMessage);
        }

        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    void DoSuspendOperation(
        const TOperationId& operationId,
        const TSuspendOperationOptions& options)
    {
        auto req = SchedulerProxy_->SuspendOperation();
        ToProto(req->mutable_operation_id(), operationId);
        req->set_abort_running_jobs(options.AbortRunningJobs);

        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    void DoResumeOperation(
        const TOperationId& operationId,
        const TResumeOperationOptions& /*options*/)
    {
        auto req = SchedulerProxy_->ResumeOperation();
        ToProto(req->mutable_operation_id(), operationId);

        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    void DoCompleteOperation(
        const TOperationId& operationId,
        const TCompleteOperationOptions& /*options*/)
    {
        auto req = SchedulerProxy_->CompleteOperation();
        ToProto(req->mutable_operation_id(), operationId);

        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    void DoUpdateOperationParameters(
        const TOperationId& operationId,
        const TYsonString& parameters,
        const TUpdateOperationParametersOptions& options)
    {
        auto req = SchedulerProxy_->UpdateOperationParameters();
        ToProto(req->mutable_operation_id(), operationId);
        req->set_parameters(parameters.GetData());

        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    bool DoesOperationsArchiveExist()
    {
        // NB: we suppose that archive should exist and work correctly if this map node is presented.
        return WaitFor(NodeExists("//sys/operations_archive", TNodeExistsOptions()))
            .ValueOrThrow();
    }

    int DoGetOperationsArchiveVersion()
    {
        auto asyncVersionResult = GetNode(GetOperationsArchiveVersionPath(), TGetNodeOptions());
        auto versionNodeOrError = WaitFor(asyncVersionResult);

        if (!versionNodeOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to get operations archive version")
                << versionNodeOrError;
        }

        int version = 0;
        try {
            version = ConvertTo<int>(versionNodeOrError.Value());
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to parse operations archive version")
                << ex;
        }

        return version;
    }

    // Attribute names allowed for 'get_operation' and 'list_operation' commands.
    const THashSet<TString> SupportedOperationAttributes = {
        "id",
        "state",
        "authenticated_user",
        "type",
        // COMPAT(levysotsky): "operation_type" is deprecated
        "operation_type",
        "progress",
        "spec",
        "full_spec",
        "unrecognized_spec",
        "brief_progress",
        "brief_spec",
        "runtime_parameters",
        "start_time",
        "finish_time",
        "result",
        "events",
        "memory_usage",
        "suspended",
    };

    // Map operation attribute names as they are requested in 'get_operation' or 'list_operations'
    // commands to Cypress node attribute names.
    std::vector<TString> MakeCypressOperationAttributes(const THashSet<TString>& attributes)
    {
        std::vector<TString> result;
        result.reserve(attributes.size());
        for (const auto& attribute : attributes) {
            if (!SupportedOperationAttributes.has(attribute)) {
                THROW_ERROR_EXCEPTION("Attribute %Qv is not allowed",
                    attribute);
            }
            if (attribute == "id") {
                result.push_back("key");
            } else if (attribute == "type") {
                result.push_back("operation_type");
            } else {
                result.push_back(attribute);
            }
        }
        return result;
    };

    // Map operation attribute names as they are requested in 'get_operation' or 'list_operations'
    // commands to operations archive column names.
    std::vector<TString> MakeArchiveOperationAttributes(const THashSet<TString>& attributes)
    {
        std::vector<TString> result;
        result.reserve(attributes.size() + 1); // Plus 1 for 'id_lo' and 'id_hi' instead of 'id'.
        for (const auto& attribute : attributes) {
            if (!SupportedOperationAttributes.has(attribute)) {
                THROW_ERROR_EXCEPTION("Attribute %Qv is not allowed",
                    attribute);
            }
            if (attribute == "id") {
                result.push_back("id_hi");
                result.push_back("id_lo");
            } else if (attribute == "type") {
                result.push_back("operation_type");
            } else {
                result.push_back(attribute);
            }
        }
        return result;
    };

    TYsonString DoGetOperationFromCypress(
        const NScheduler::TOperationId& operationId,
        TInstant deadline,
        const TGetOperationOptions& options)
    {
        TNullable<std::vector<TString>> cypressAttributes;
        if (options.Attributes) {
            cypressAttributes = MakeCypressOperationAttributes(*options.Attributes);

            if (!options.Attributes->has("controller_agent_address")) {
                cypressAttributes->push_back("controller_agent_address");
            }
        }

        auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
        auto batchReq = proxy->ExecuteBatch();
        SetBalancingHeader(batchReq, options);

        {
            auto req = TYPathProxy::Get(GetNewOperationPath(operationId) + "/@");
            if (cypressAttributes) {
                ToProto(req->mutable_attributes()->mutable_keys(), *cypressAttributes);
            }
            batchReq->AddRequest(req, "get_operation");
        }

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();

        auto cypressNodeRspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_operation");

        INodePtr cypressNode;
        if (cypressNodeRspOrError.IsOK()) {
            const auto& cypressNodeRsp = cypressNodeRspOrError.Value();
            cypressNode = ConvertToNode(TYsonString(cypressNodeRsp->value()));
        } else {
            if (!cypressNodeRspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                THROW_ERROR cypressNodeRspOrError;
            }
        }

        if (!cypressNode) {
            return TYsonString();
        }

        auto attrNode = cypressNode->AsMap();

        // XXX(ignat): remove opaque from node. Make option to ignore it in conversion methods.
        if (auto fullSpecNode = attrNode->FindChild("full_spec")) {
            fullSpecNode->MutableAttributes()->Remove("opaque");
        }

        if (auto child = attrNode->FindChild("operation_type")) {
            // COMPAT(levysotsky): When "operation_type" is disallowed, this code
            // will be simplified to unconditionally removing the child
            // (and also child will not have to be cloned).
            if (options.Attributes && !options.Attributes->has("operation_type")) {
                attrNode->RemoveChild("operation_type");
            }

            attrNode->RemoveChild("type");
            YCHECK(attrNode->AddChild("type", CloneNode(child)));
        }

        if (auto child = attrNode->FindChild("key")) {
            attrNode->RemoveChild("key");
            attrNode->RemoveChild("id");
            YCHECK(attrNode->AddChild("id", child));
        }

        if (options.Attributes && !options.Attributes->has("state")) {
            attrNode->RemoveChild("state");
        }

        if (!options.Attributes) {
            auto keysToKeep = ConvertTo<THashSet<TString>>(attrNode->GetChild("user_attribute_keys"));
            keysToKeep.insert("id");
            keysToKeep.insert("type");
            for (const auto& key : attrNode->GetKeys()) {
                if (!keysToKeep.has(key)) {
                    attrNode->RemoveChild(key);
                }
            }
        }

        TNullable<TString> controllerAgentAddress;
        if (auto child = attrNode->FindChild("controller_agent_address")) {
            controllerAgentAddress = child->AsString()->GetValue();
            if (options.Attributes && !options.Attributes->has("controller_agent_address")) {
                attrNode->RemoveChild(child);
            }
        }

        std::vector<std::pair<TString, bool>> runtimeAttributes ={
            /* {Name, ShouldRequestFromScheduler} */
            {"progress", true},
            {"brief_progress", false},
            {"memory_usage", false}
        };

        if (options.IncludeRuntime) {
            auto batchReq = proxy->ExecuteBatch();

            auto addProgressAttributeRequest = [&] (const TString& attribute, bool shouldRequestFromScheduler) {
                if (shouldRequestFromScheduler) {
                    auto req = TYPathProxy::Get(GetSchedulerOrchidOperationPath(operationId) + "/" + attribute);
                    batchReq->AddRequest(req, "get_operation_" + attribute);
                }
                if (controllerAgentAddress) {
                    auto path = GetControllerAgentOrchidOperationPath(*controllerAgentAddress, operationId);
                    auto req = TYPathProxy::Get(path + "/" + attribute);
                    batchReq->AddRequest(req, "get_operation_" + attribute);
                }
            };

            for (const auto& attribute : runtimeAttributes) {
                if (!options.Attributes || options.Attributes->has(attribute.first)) {
                    addProgressAttributeRequest(attribute.first, attribute.second);
                }
            }

            if (batchReq->GetSize() != 0) {
                auto batchRsp = WaitFor(batchReq->Invoke())
                    .ValueOrThrow();

                auto handleProgressAttributeRequest = [&] (const TString& attribute) {
                    INodePtr progressAttributeNode;

                    auto responses = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_operation_" + attribute);
                    for (const auto& rsp : responses) {
                        if (rsp.IsOK()) {
                            auto node = ConvertToNode(TYsonString(rsp.Value()->value()));
                            if (!progressAttributeNode) {
                                progressAttributeNode = node;
                            } else {
                                progressAttributeNode = PatchNode(progressAttributeNode, node);
                            }
                        } else {
                            if (!rsp.FindMatching(NYTree::EErrorCode::ResolveError)) {
                                THROW_ERROR rsp;
                            }
                        }

                        if (progressAttributeNode) {
                            attrNode->RemoveChild(attribute);
                            YCHECK(attrNode->AddChild(attribute, progressAttributeNode));
                        }
                    }
                };

                for (const auto& attribute : runtimeAttributes) {
                    if (!options.Attributes || options.Attributes->has(attribute.first)) {
                        handleProgressAttributeRequest(attribute.first);
                    }
                }
            }
        }

        return ConvertToYsonString(attrNode);
    }

    TYsonString DoGetOperationFromArchive(
        const NScheduler::TOperationId& operationId,
        TInstant deadline,
        const TGetOperationOptions& options)
    {
        auto attributes = options.Attributes.Get(SupportedOperationAttributes);

        if (DoGetOperationsArchiveVersion() < 22) {
            attributes.erase("runtime_parameters");
        }
        // Ignoring memory_usage and suspended in archive.
        attributes.erase("memory_usage");
        attributes.erase("suspended");

        auto fieldsVector = MakeArchiveOperationAttributes(attributes);
        THashSet<TString> fields(fieldsVector.begin(), fieldsVector.end());

        TOrderedByIdTableDescriptor tableDescriptor;
        auto rowBuffer = New<TRowBuffer>();

        std::vector<TUnversionedRow> keys;
        auto key = rowBuffer->AllocateUnversioned(2);
        key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], tableDescriptor.Index.IdHi);
        key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], tableDescriptor.Index.IdLo);
        keys.push_back(key);

        std::vector<int> columnIndexes;
        THashMap<TString, int> fieldToIndex;

        int index = 0;
        for (const auto& field : fields) {
            columnIndexes.push_back(tableDescriptor.NameTable->GetIdOrThrow(field));
            fieldToIndex[field] = index++;
        }

        TLookupRowsOptions lookupOptions;
        lookupOptions.ColumnFilter = NTableClient::TColumnFilter(columnIndexes);
        lookupOptions.KeepMissingRows = true;
        lookupOptions.Timeout = deadline - Now();

        auto rowset = WaitFor(LookupRows(
            GetOperationsArchivePathOrderedById(),
            tableDescriptor.NameTable,
            MakeSharedRange(std::move(keys), std::move(rowBuffer)),
            lookupOptions))
            .ValueOrThrow();

        auto rows = rowset->GetRows();
        YCHECK(!rows.Empty());

        if (rows[0]) {
#define SET_ITEM_STRING_VALUE_WITH_FIELD(itemKey, fieldName) \
            SET_ITEM_VALUE_WITH_FIELD(itemKey, fieldName, TString(rows[0][index].Data.String, rows[0][index].Length))
#define SET_ITEM_STRING_VALUE(itemKey) SET_ITEM_STRING_VALUE_WITH_FIELD(itemKey, itemKey)
#define SET_ITEM_YSON_STRING_VALUE(itemKey) \
            SET_ITEM_VALUE(itemKey, TYsonString(rows[0][index].Data.String, rows[0][index].Length))
#define SET_ITEM_INSTANT_VALUE(itemKey) \
            SET_ITEM_VALUE(itemKey, TInstant::MicroSeconds(rows[0][index].Data.Int64))
#define SET_ITEM_VALUE_WITH_FIELD(itemKey, fieldName, operation) \
            .DoIf(fields.find(fieldName) != fields.end() && rows[0][GET_INDEX(fieldName)].Type != EValueType::Null, \
            [&] (TFluentMap fluent) { \
                auto index = GET_INDEX(fieldName); \
                fluent.Item(itemKey).Value(operation); \
            })
#define SET_ITEM_VALUE(itemKey, operation) SET_ITEM_VALUE_WITH_FIELD(itemKey, itemKey, operation)
#define GET_INDEX(itemKey) fieldToIndex.find(itemKey)->second

            auto ysonResult = BuildYsonStringFluently()
                .BeginMap()
                    .DoIf(fields.has("id_lo"), [&] (TFluentMap fluent) {
                        fluent.Item("id").Value(operationId);
                    })
                    SET_ITEM_STRING_VALUE("state")
                    SET_ITEM_STRING_VALUE("authenticated_user")
                    SET_ITEM_STRING_VALUE_WITH_FIELD("type", "operation_type")
                    // COMPAT(levysotsky): Add this field under old name for
                    // backward compatibility. Should be removed when all the clients migrate.
                    SET_ITEM_STRING_VALUE("operation_type")
                    SET_ITEM_YSON_STRING_VALUE("progress")
                    SET_ITEM_YSON_STRING_VALUE("spec")
                    SET_ITEM_YSON_STRING_VALUE("full_spec")
                    SET_ITEM_YSON_STRING_VALUE("unrecognized_spec")
                    SET_ITEM_YSON_STRING_VALUE("brief_progress")
                    SET_ITEM_YSON_STRING_VALUE("brief_spec")
                    SET_ITEM_YSON_STRING_VALUE("runtime_parameters")
                    SET_ITEM_INSTANT_VALUE("start_time")
                    SET_ITEM_INSTANT_VALUE("finish_time")
                    SET_ITEM_YSON_STRING_VALUE("result")
                    SET_ITEM_YSON_STRING_VALUE("events")
                .EndMap();
#undef SET_ITEM_STRING_VALUE
#undef SET_ITEM_YSON_STRING_VALUE
#undef SET_ITEM_INSTANT_VALUE
#undef SET_ITEM_VALUE
#undef GET_INDEX
            return ysonResult;
        }

        return TYsonString();
    }

    TYsonString DoGetOperation(
        const NScheduler::TOperationId& operationId,
        const TGetOperationOptions& options)
    {
        auto timeout = options.Timeout.Get(Connection_->GetConfig()->DefaultGetOperationTimeout);
        auto deadline = timeout.ToDeadLine();

        if (auto result = DoGetOperationFromCypress(operationId, deadline, options)) {
            return result;
        }

        LOG_DEBUG("Operation is not found in Cypress (OperationId: %v)",
            operationId);

        if (DoesOperationsArchiveExist()) {
            try {
                if (auto result = DoGetOperationFromArchive(operationId, deadline, options)) {
                    return result;
                }
            } catch (const TErrorException& ex) {
                if (!ex.Error().FindMatching(NYTree::EErrorCode::ResolveError)) {
                    THROW_ERROR_EXCEPTION("Failed to get operation from archive")
                        << TErrorAttribute("operation_id", operationId)
                        << ex.Error();
                }
            }
        }

        THROW_ERROR_EXCEPTION(
            NApi::EErrorCode::NoSuchOperation,
            "No such operation %v",
            operationId);
    }

    void ValidateJobAcl(
        const TJobId& jobId,
        TNullable<NJobTrackerClient::NProto::TJobSpec> jobSpec = Null)
    {
        if (!jobSpec) {
            jobSpec = GetJobSpecFromArchive(jobId);
        }

        auto* schedulerJobSpecExt = jobSpec->MutableExtension(NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext);

        if (schedulerJobSpecExt && schedulerJobSpecExt->has_acl()) {
            auto aclYson = TYsonString(schedulerJobSpecExt->acl());
            auto checkResult = WaitFor(CheckPermissionByAcl(Null, EPermission::Read, ConvertToNode(aclYson), TCheckPermissionByAclOptions()))
                .ValueOrThrow();
            if (checkResult.Action != ESecurityAction::Allow) {
                THROW_ERROR_EXCEPTION("No permissions to read job from archive")
                    << TErrorAttribute("job_id", jobId)
                    << TErrorAttribute("user", checkResult.SubjectName);
            }
        }
    }

    void DoDumpJobContext(
        const TJobId& jobId,
        const TYPath& path,
        const TDumpJobContextOptions& /*options*/)
    {
        auto req = JobProberProxy_->DumpInputContext();
        ToProto(req->mutable_job_id(), jobId);
        ToProto(req->mutable_path(), path);

        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    void ValidateJobSpecVersion(const TJobId& jobId, const NYT::NJobTrackerClient::NProto::TJobSpec& jobSpec)
    {
        if (!jobSpec.has_version() || jobSpec.version() != GetJobSpecVersion()) {
            THROW_ERROR_EXCEPTION("Job spec found in operation archive is of unsupported version")
                << TErrorAttribute("job_id", jobId)
                << TErrorAttribute("found_version", jobSpec.version())
                << TErrorAttribute("supported_version", GetJobSpecVersion());
        }
    }

    TNodeDescriptor GetJobNodeDescriptor(const TJobId& jobId)
    {
        TNodeDescriptor jobNodeDescriptor;
        auto req = JobProberProxy_->GetJobNode();
        ToProto(req->mutable_job_id(), jobId);
        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();
        FromProto(&jobNodeDescriptor, rsp->node_descriptor());
        return jobNodeDescriptor;
    }

    TNullable<NJobTrackerClient::NProto::TJobSpec> GetJobSpecFromJobNode(const TJobId& jobId)
    {
        try {
            TNodeDescriptor jobNodeDescriptor = GetJobNodeDescriptor(jobId);

            auto nodeChannel = ChannelFactory_->CreateChannel(jobNodeDescriptor);
            NJobProberClient::TJobProberServiceProxy jobProberServiceProxy(nodeChannel);

            auto req = jobProberServiceProxy.GetSpec();
            ToProto(req->mutable_job_id(), jobId);
            auto rsp = WaitFor(req->Invoke())
                .ValueOrThrow();

            ValidateJobSpecVersion(jobId, rsp->spec());
            return rsp->spec();
        } catch (const TErrorException& exception) {
            auto matchedError = exception.Error().FindMatching(NScheduler::EErrorCode::NoSuchJob);
            if (!matchedError) {
                THROW_ERROR_EXCEPTION("Failed to get job spec from job node")
                    << TErrorAttribute("job_id", jobId)
                    << exception;
            }
        }
        return Null;
    }

    NJobTrackerClient::NProto::TJobSpec GetJobSpecFromArchive(const TJobId& jobId)
    {
        auto nameTable = New<TNameTable>();

        TLookupRowsOptions lookupOptions;
        lookupOptions.ColumnFilter = NTableClient::TColumnFilter({nameTable->RegisterName("spec")});
        lookupOptions.KeepMissingRows = true;

        auto owningKey = CreateJobKey(jobId, nameTable);

        std::vector<TUnversionedRow> keys;
        keys.push_back(owningKey);

        auto lookupResult = WaitFor(LookupRows(
            GetOperationsArchiveJobSpecsPath(),
            nameTable,
            MakeSharedRange(keys, owningKey),
            lookupOptions));

        if (!lookupResult.IsOK()) {
            THROW_ERROR_EXCEPTION(lookupResult)
                .Wrap("Lookup job spec in operation archive failed")
                << TErrorAttribute("job_id", jobId);
        }

        auto rows = lookupResult.Value()->GetRows();
        YCHECK(!rows.Empty());

        if (!rows[0]) {
            THROW_ERROR_EXCEPTION("Missing job spec in job archive table")
                << TErrorAttribute("job_id", jobId);
        }

        auto value = rows[0][0];

        if (value.Type != EValueType::String) {
            THROW_ERROR_EXCEPTION("Found job spec has unexpected value type")
                << TErrorAttribute("job_id", jobId)
                << TErrorAttribute("value_type", value.Type);
        }

        NJobTrackerClient::NProto::TJobSpec jobSpec;
        bool ok = jobSpec.ParseFromArray(value.Data.String, value.Length);
        if (!ok) {
            THROW_ERROR_EXCEPTION("Cannot parse job spec")
                << TErrorAttribute("job_id", jobId);
        }
        ValidateJobSpecVersion(jobId, jobSpec);

        return jobSpec;
    }

    IAsyncZeroCopyInputStreamPtr DoGetJobInput(
        const TJobId& jobId,
        const TGetJobInputOptions& /*options*/)
    {
        NJobTrackerClient::NProto::TJobSpec jobSpec;
        if (auto jobSpecFromProxy = GetJobSpecFromJobNode(jobId)) {
            jobSpec.Swap(jobSpecFromProxy.GetPtr());
        } else {
            jobSpec = GetJobSpecFromArchive(jobId);
        }

        ValidateJobAcl(jobId, jobSpec);

        auto* schedulerJobSpecExt = jobSpec.MutableExtension(NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext);

        auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        auto locateChunks = BIND([=] {
            std::vector<TChunkSpec*> chunkSpecList;
            for (auto& tableSpec : *schedulerJobSpecExt->mutable_input_table_specs()) {
                for (auto& chunkSpec : *tableSpec.mutable_chunk_specs()) {
                    chunkSpecList.push_back(&chunkSpec);
                }
            }

            for (auto& tableSpec : *schedulerJobSpecExt->mutable_foreign_input_table_specs()) {
                for (auto& chunkSpec : *tableSpec.mutable_chunk_specs()) {
                    chunkSpecList.push_back(&chunkSpec);
                }
            }

            LocateChunks(
                MakeStrong(this),
                New<TMultiChunkReaderConfig>()->MaxChunksPerLocateRequest,
                chunkSpecList,
                nodeDirectory,
                Logger);
            nodeDirectory->DumpTo(schedulerJobSpecExt->mutable_input_node_directory());
        });

        auto locateChunksResult = WaitFor(locateChunks
            .AsyncVia(GetConnection()->GetInvoker())
            .Run());

        if (!locateChunksResult.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to locate chunks used in job input")
                << TErrorAttribute("job_id", jobId);
        }

        auto jobSpecHelper = NJobProxy::CreateJobSpecHelper(jobSpec);

        TClientBlockReadOptions blockReadOptions;
        blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();

        auto userJobReadController = CreateUserJobReadController(
            jobSpecHelper,
            MakeStrong(this),
            GetConnection()->GetInvoker(),
            TNodeDescriptor(),
            BIND([] { }) /* onNetworkRelease */,
            Null /* udfDirectory */,
            blockReadOptions,
            nullptr /* trafficMeter */,
            NConcurrency::GetUnlimitedThrottler() /* bandwidthThrottler */,
            NConcurrency::GetUnlimitedThrottler() /* rpsThrottler */);

        auto jobInputReader = New<TJobInputReader>(std::move(userJobReadController), GetConnection()->GetInvoker());
        jobInputReader->Open();
        return jobInputReader;
    }

    TSharedRef DoGetJobStderrFromNode(
        const TOperationId& operationId,
        const TJobId& jobId)
    {
        try {
            TNodeDescriptor jobNodeDescriptor = GetJobNodeDescriptor(jobId);

            auto nodeChannel = ChannelFactory_->CreateChannel(jobNodeDescriptor);
            NJobProberClient::TJobProberServiceProxy jobProberServiceProxy(nodeChannel);

            auto req = jobProberServiceProxy.GetStderr();
            req->SetMultiplexingBand(EMultiplexingBand::Heavy);
            ToProto(req->mutable_job_id(), jobId);
            auto rsp = WaitFor(req->Invoke())
                .ValueOrThrow();
            return TSharedRef::FromString(rsp->stderr_data());
        } catch (const TErrorException& exception) {
            auto matchedError = exception.Error().FindMatching(NScheduler::EErrorCode::NoSuchJob) ||
                exception.Error().FindMatching(NJobProberClient::EErrorCode::JobIsNotRunning);

            if (!matchedError) {
                THROW_ERROR_EXCEPTION("Failed to get job stderr from job proxy")
                    << TErrorAttribute("operation_id", operationId)
                    << TErrorAttribute("job_id", jobId)
                    << exception.Error();
            }
        }

        return TSharedRef();
    }

    TSharedRef DoGetJobStderrFromCypress(
        const TOperationId& operationId,
        const TJobId& jobId)
    {
        auto createFileReader = [&] (const NYPath::TYPath& path) {
            return WaitFor(static_cast<IClientBase*>(this)->CreateFileReader(path));
        };

        try {
            auto fileReaderOrError = createFileReader(NScheduler::GetNewStderrPath(operationId, jobId));
            // COMPAT
            if (fileReaderOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                fileReaderOrError = createFileReader(NScheduler::GetStderrPath(operationId, jobId));
            }

            auto fileReader = fileReaderOrError.ValueOrThrow();

            std::vector<TSharedRef> blocks;
            while (true) {
                auto block = WaitFor(fileReader->Read())
                    .ValueOrThrow();

                if (!block) {
                    break;
                }

                blocks.push_back(std::move(block));
            }

            i64 size = GetByteSize(blocks);
            YCHECK(size);
            auto stderrFile = TSharedMutableRef::Allocate(size);
            auto memoryOutput = TMemoryOutput(stderrFile.Begin(), size);

            for (const auto& block : blocks) {
                memoryOutput.Write(block.Begin(), block.Size());
            }

            return stderrFile;
        } catch (const TErrorException& exception) {
            auto matchedError = exception.Error().FindMatching(NYTree::EErrorCode::ResolveError);

            if (!matchedError) {
                THROW_ERROR_EXCEPTION("Failed to get job stderr from Cypress")
                    << TErrorAttribute("operation_id", operationId)
                    << TErrorAttribute("job_id", jobId)
                    << exception.Error();
            }
        }

        return TSharedRef();
    }

    TSharedRef DoGetJobStderrFromArchive(
        const TOperationId& operationId,
        const TJobId& jobId)
    {
        try {
            TJobStderrTableDescriptor tableDescriptor;

            auto rowBuffer = New<TRowBuffer>();

            std::vector<TUnversionedRow> keys;
            auto key = rowBuffer->AllocateUnversioned(4);
            key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], tableDescriptor.Ids.OperationIdHi);
            key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], tableDescriptor.Ids.OperationIdLo);
            key[2] = MakeUnversionedUint64Value(jobId.Parts64[0], tableDescriptor.Ids.JobIdHi);
            key[3] = MakeUnversionedUint64Value(jobId.Parts64[1], tableDescriptor.Ids.JobIdLo);
            keys.push_back(key);

            TLookupRowsOptions lookupOptions;
            lookupOptions.ColumnFilter = NTableClient::TColumnFilter({tableDescriptor.Ids.Stderr});
            lookupOptions.KeepMissingRows = true;

            auto rowset = WaitFor(LookupRows(
                GetOperationsArchiveJobStderrsPath(),
                tableDescriptor.NameTable,
                MakeSharedRange(keys, rowBuffer),
                lookupOptions))
                .ValueOrThrow();

            auto rows = rowset->GetRows();
            YCHECK(!rows.Empty());

            if (rows[0]) {
                auto value = rows[0][0];

                YCHECK(value.Type == EValueType::String);
                return TSharedRef::MakeCopy<char>(TRef(value.Data.String, value.Length));
            }
        } catch (const TErrorException& exception) {
            auto matchedError = exception.Error().FindMatching(NYTree::EErrorCode::ResolveError);

            if (!matchedError) {
                THROW_ERROR_EXCEPTION("Failed to get job stderr from archive")
                    << TErrorAttribute("operation_id", operationId)
                    << TErrorAttribute("job_id", jobId)
                    << exception.Error();
            }
        }

        return TSharedRef();
    }

    TSharedRef DoGetJobStderr(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobStderrOptions& /*options*/)
    {
        auto stderrRef = DoGetJobStderrFromNode(operationId, jobId);
        if (stderrRef) {
            return stderrRef;
        }

        stderrRef = DoGetJobStderrFromCypress(operationId, jobId);
        if (stderrRef) {
            return stderrRef;
        }

        ValidateJobAcl(jobId);

        stderrRef = DoGetJobStderrFromArchive(operationId, jobId);
        if (stderrRef) {
            return stderrRef;
        }

        THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::NoSuchJob, "Job stderr is not found")
            << TErrorAttribute("operation_id", operationId)
            << TErrorAttribute("job_id", jobId);
    }

    TSharedRef DoGetJobFailContextFromArchive(
        const TOperationId& operationId,
        const TJobId& jobId)
    {
        try {
            TJobFailContextTableDescriptor tableDescriptor;

            auto rowBuffer = New<TRowBuffer>();

            std::vector<TUnversionedRow> keys;
            auto key = rowBuffer->AllocateUnversioned(4);
            key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], tableDescriptor.Ids.OperationIdHi);
            key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], tableDescriptor.Ids.OperationIdLo);
            key[2] = MakeUnversionedUint64Value(jobId.Parts64[0], tableDescriptor.Ids.JobIdHi);
            key[3] = MakeUnversionedUint64Value(jobId.Parts64[1], tableDescriptor.Ids.JobIdLo);
            keys.push_back(key);

            TLookupRowsOptions lookupOptions;
            lookupOptions.ColumnFilter = NTableClient::TColumnFilter({tableDescriptor.Ids.FailContext});
            lookupOptions.KeepMissingRows = true;

            auto rowset = WaitFor(LookupRows(
                GetOperationsArchiveJobFailContextsPath(),
                tableDescriptor.NameTable,
                MakeSharedRange(keys, rowBuffer),
                lookupOptions))
                .ValueOrThrow();

            auto rows = rowset->GetRows();
            YCHECK(!rows.Empty());

            if (rows[0]) {
                auto value = rows[0][0];

                YCHECK(value.Type == EValueType::String);
                return TSharedRef::MakeCopy<char>(TRef(value.Data.String, value.Length));
            }
        } catch (const TErrorException& exception) {
            auto matchedError = exception.Error().FindMatching(NYTree::EErrorCode::ResolveError);

            if (!matchedError) {
                THROW_ERROR_EXCEPTION("Failed to get job fail_context from archive")
                    << TErrorAttribute("operation_id", operationId)
                    << TErrorAttribute("job_id", jobId)
                    << exception.Error();
            }
        }

        return TSharedRef();
    }

    TSharedRef DoGetJobFailContextFromCypress(
        const TOperationId& operationId,
        const TJobId& jobId)
    {
        auto createFileReader = [&] (const NYPath::TYPath& path) {
            return WaitFor(static_cast<IClientBase*>(this)->CreateFileReader(path));
        };

        try {
            auto fileReaderOrError = createFileReader(NScheduler::GetNewFailContextPath(operationId, jobId));
            // COMPAT
            if (fileReaderOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                fileReaderOrError = createFileReader(NScheduler::GetFailContextPath(operationId, jobId));
            }

            auto fileReader = fileReaderOrError.ValueOrThrow();

            std::vector<TSharedRef> blocks;
            while (true) {
                auto block = WaitFor(fileReader->Read())
                    .ValueOrThrow();

                if (!block) {
                    break;
                }

                blocks.push_back(std::move(block));
            }

            i64 size = GetByteSize(blocks);
            YCHECK(size);
            auto failContextFile = TSharedMutableRef::Allocate(size);
            auto memoryOutput = TMemoryOutput(failContextFile.Begin(), size);

            for (const auto& block : blocks) {
                memoryOutput.Write(block.Begin(), block.Size());
            }

            return failContextFile;
        } catch (const TErrorException& exception) {
            auto matchedError = exception.Error().FindMatching(NYTree::EErrorCode::ResolveError);

            if (!matchedError) {
                THROW_ERROR_EXCEPTION("Failed to get job fail context from Cypress")
                    << TErrorAttribute("operation_id", operationId)
                    << TErrorAttribute("job_id", jobId)
                    << exception.Error();
            }
        }

        return TSharedRef();
    }

    TSharedRef DoGetJobFailContext(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobFailContextOptions& /*options*/)
    {
        {
            auto failContextRef = DoGetJobFailContextFromCypress(operationId, jobId);
            if (failContextRef) {
                return failContextRef;
            }
        }

        if (DoGetOperationsArchiveVersion() >= 21) {
            ValidateJobAcl(jobId);

            auto failContextRef = DoGetJobFailContextFromArchive(operationId, jobId);
            if (failContextRef) {
                return failContextRef;
            }
        }

        THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::NoSuchJob, "Job fail context is not found")
            << TErrorAttribute("operation_id", operationId)
            << TErrorAttribute("job_id", jobId);
    }

    template <class T>
    static bool LessNullable(const T& lhs, const T& rhs)
    {
        return lhs < rhs;
    }

    template <class T>
    static bool LessNullable(const TNullable<T>& lhs, const TNullable<T>& rhs)
    {
        return rhs && (!lhs || *lhs < *rhs);
    }

    TString ExtractTextFactorForCypressItem(const TOperation& operation)
    {
        std::vector<TString> textFactors;

        if (operation.Id) {
            textFactors.push_back(ToString(*operation.Id));
        }
        if (operation.AuthenticatedUser) {
            textFactors.push_back(*operation.AuthenticatedUser);
        }
        if (operation.State) {
            textFactors.push_back(ToString(*operation.State));
        }
        if (operation.Type) {
            textFactors.push_back(ToString(*operation.Type));
        }

        if (operation.BriefSpec) {
            auto briefSpecMapNode = ConvertToNode(operation.BriefSpec)->AsMap();
            if (briefSpecMapNode->FindChild("title")) {
                textFactors.push_back(briefSpecMapNode->GetChild("title")->AsString()->GetValue());
            }
            if (briefSpecMapNode->FindChild("input_table_paths")) {
                auto inputTablesNode = briefSpecMapNode->GetChild("input_table_paths")->AsList();
                if (inputTablesNode->GetChildCount() > 0) {
                    textFactors.push_back(inputTablesNode->GetChildren()[0]->AsString()->GetValue());
                }
            }
            if (briefSpecMapNode->FindChild("output_table_paths")) {
                auto outputTablesNode = briefSpecMapNode->GetChild("output_table_paths")->AsList();
                if (outputTablesNode->GetChildCount() > 0) {
                    textFactors.push_back(outputTablesNode->GetChildren()[0]->AsString()->GetValue());
                }
            }
        }

        if (operation.RuntimeParameters) {
            auto pools = GetPoolsFromRuntimeParameters(operation.RuntimeParameters);
            textFactors.insert(textFactors.end(), pools.begin(), pools.end());
        }

        return to_lower(JoinToString(textFactors, AsStringBuf(" ")));
    }

    std::vector<TString> GetPoolsFromRuntimeParameters(const TYsonString& runtimeParameters)
    {
        YCHECK(runtimeParameters);

        std::vector<TString> result;
        auto runtimeParametersNode = ConvertToNode(runtimeParameters)->AsMap();
        if (auto schedulingOptionsNode = runtimeParametersNode->FindChild("scheduling_options_per_pool_tree")) {
            for (const auto& entry : schedulingOptionsNode->AsMap()->GetChildren()) {
                if (auto poolNode = entry.second->AsMap()->FindChild("pool")) {
                    result.push_back(poolNode->GetValue<TString>());
                }
            }
        }
        return result;
    }

    struct TCountingFilter
    {
        THashMap<TString, i64> PoolCounts;
        THashMap<TString, i64> UserCounts;
        TEnumIndexedVector<i64, NScheduler::EOperationState> StateCounts;
        TEnumIndexedVector<i64, NScheduler::EOperationType> TypeCounts;
        i64 FailedJobsCount = 0;

        const TListOperationsOptions& Options;

        TCountingFilter(const TListOperationsOptions& options)
            : Options(options)
        { }

        bool Filter(
            TNullable<std::vector<TString>> pools,
            TStringBuf user,
            const EOperationState& state,
            const EOperationType& type,
            i64 count)
        {
            if (pools) {
                for (const auto& pool : *pools) {
                    PoolCounts[pool] += count;
                }
            }

            if (Options.Pool && (!pools || std::find(pools->begin(), pools->end(), *Options.Pool) == pools->end())) {
                return false;
            }

            UserCounts[user] += count;

            if (Options.UserFilter && *Options.UserFilter != user) {
                return false;
            }

            StateCounts[state] += count;

            if (Options.StateFilter && *Options.StateFilter != state) {
                return false;
            }

            TypeCounts[type] += count;

            if (Options.TypeFilter && *Options.TypeFilter != type) {
                return false;
            }

            return true;
        }

        bool FilterByFailedJobs(const TYsonString& briefProgress)
        {
            if (briefProgress) {
                auto briefProgressMapNode = ConvertToNode(briefProgress)->AsMap();
                auto jobsNode = briefProgressMapNode->FindChild("jobs");
                bool hasFailedJobs = jobsNode && jobsNode->AsMap()->GetChild("failed")->GetValue<i64>() > 0;

                FailedJobsCount += hasFailedJobs;
                if (Options.WithFailedJobs && *Options.WithFailedJobs != hasFailedJobs) {
                    return false;
                }
            }

            return true;
        }
    };

    TOperation CreateOperationFromNode(
        const INodePtr& node,
        const TNullable<THashSet<TString>>& attributes = Null)
    {
        const auto& nodeAttributes = node->Attributes();

        TOperation operation;

        if (!attributes || attributes->has("id")) {
            if (auto id = nodeAttributes.Find<TString>("key")) {
                operation.Id = TGuid::FromString(*id);
            }
        }
        if (!attributes || attributes->has("type")) {
            if (auto type = nodeAttributes.Find<TString>("operation_type")) {
                operation.Type = ParseEnum<NScheduler::EOperationType>(*type);
            }
        }
        if (!attributes || attributes->has("state")) {
            if (auto state = nodeAttributes.Find<TString>("state")) {
                operation.State = ParseEnum<NScheduler::EOperationState>(*state);
            }
        }
        if (!attributes || attributes->has("start_time")) {
            if (auto startTime = nodeAttributes.Find<TString>("start_time")) {
                operation.StartTime = ConvertTo<TInstant>(*startTime);
            }
        }
        if (!attributes || attributes->has("finish_time")) {
            if (auto finishTime = nodeAttributes.Find<TString>("finish_time")) {
                operation.FinishTime = ConvertTo<TInstant>(*finishTime);
            }
        }
        if (!attributes || attributes->has("authenticated_user")) {
            operation.AuthenticatedUser = nodeAttributes.Find<TString>("authenticated_user");
        }

        if (!attributes || attributes->has("brief_spec")) {
            operation.BriefSpec = nodeAttributes.FindYson("brief_spec");
        }
        if (!attributes || attributes->has("spec")) {
            operation.Spec = nodeAttributes.FindYson("spec");
        }
        if (!attributes || attributes->has("full_spec")) {
            operation.FullSpec = nodeAttributes.FindYson("full_spec");
        }
        if (!attributes || attributes->has("unrecognized_spec")) {
            operation.UnrecognizedSpec = nodeAttributes.FindYson("unrecognized_spec");
        }

        if (!attributes || attributes->has("brief_progress")) {
            operation.BriefProgress = nodeAttributes.FindYson("brief_progress");
        }
        if (!attributes || attributes->has("progress")) {
            operation.Progress = nodeAttributes.FindYson("progress");
        }

        if (!attributes || attributes->has("runtime_parameters")) {
            operation.RuntimeParameters = nodeAttributes.FindYson("runtime_parameters");

            if (operation.RuntimeParameters) {
                operation.Pools = GetPoolsFromRuntimeParameters(operation.RuntimeParameters);
            }
        }

        if (!attributes || attributes->has("suspended")) {
            operation.Suspended = nodeAttributes.Find<bool>("suspended");
        }

        if (!attributes || attributes->has("events")) {
            operation.Events = nodeAttributes.FindYson("events");
        }
        if (!attributes || attributes->has("result")) {
            operation.Result = nodeAttributes.FindYson("result");
        }

        return operation;
    }

    THashSet<TString> MakeFinalAttrbibuteSet(
        const TNullable<THashSet<TString>>& originalAttributes,
        const THashSet<TString>& requiredAttrbiutes,
        const THashSet<TString>& defaultAttrbiutes,
        const THashSet<TString>& ignoredAttrbiutes)
    {
        auto attributes = originalAttributes.Get(defaultAttrbiutes);
        attributes.insert(requiredAttrbiutes.begin(), requiredAttrbiutes.end());
        for (const auto& attribute : ignoredAttrbiutes) {
            attributes.erase(attribute);
        }
        return attributes;
    }

    // Searches in cypress for operations satisfying given filters.
    // Adds found operations to |idToOperation| map.
    // The operations are returned with requested fields plus necessarily "start_time" and "id".
    void DoListOperationsFromCypress(
        TInstant deadline,
        TCountingFilter& countingFilter,
        const TListOperationsOptions& options,
        THashMap<NScheduler::TOperationId, TOperation>* idToOperation)
    {
        // These attributes will be requested for every operation in Cypress.
        // All the other attributes are considered heavy and if they are present in
        // the set of requested attributes an extra batch of "get" requests
        // (one for each operation satisfying filters) will be issued, so:
        // XXX(levysotsky): maintain this list up-to-date.
        static const THashSet<TString> LightAttributes = {
            "authenticated_user",
            "brief_progress",
            "brief_spec",
            "events",
            "finish_time",
            "id",
            "type",
            "result",
            "runtime_parameters",
            "start_time",
            "state",
            "suspended",
        };

        static const THashSet<TString> RequiredAttrbiutes = {"id", "start_time"};

        static const THashSet<TString> DefaultAttributes = {
            "authenticated_user",
            "brief_progress",
            "brief_spec",
            "finish_time",
            "id",
            "type",
            "runtime_parameters",
            "start_time",
            "state",
            "suspended",
        };

        static const THashSet<TString> IgnoredAttributes = {};

        auto requestedAttributes = MakeFinalAttrbibuteSet(options.Attributes, RequiredAttrbiutes, DefaultAttributes, IgnoredAttributes);

        bool areAllRequestedAttributesLight = std::all_of(
            requestedAttributes.begin(),
            requestedAttributes.end(),
            [&] (const TString& attribute) {
                return LightAttributes.has(attribute);
            });

        TObjectServiceProxy proxy(OperationsArchiveChannels_[options.ReadFrom]);
        auto listBatchReq = proxy.ExecuteBatch();
        SetBalancingHeader(listBatchReq, options);

        for (int hash = 0x0; hash <= 0xFF; ++hash) {
            auto hashStr = Format("%02x", hash);
            auto req = TYPathProxy::List("//sys/operations/" + hashStr);
            SetCachingHeader(req, options);
            ToProto(req->mutable_attributes()->mutable_keys(), MakeCypressOperationAttributes(LightAttributes));
            listBatchReq->AddRequest(req, "list_operations_" + hashStr);
        }

        auto listBatchRsp = WaitFor(listBatchReq->Invoke())
            .ValueOrThrow();

        auto substrFilter = options.SubstrFilter;
        if (substrFilter) {
            *substrFilter = to_lower(*substrFilter);
        }

        std::vector<TOperation> filteredOperations;
        for (int hash = 0x0; hash <= 0xFF; ++hash) {
            auto rspOrError = listBatchRsp->GetResponse<TYPathProxy::TRspList>(Format("list_operations_%02x", hash));

            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                continue;
            }

            auto rsp = rspOrError.ValueOrThrow();
            auto operationNodes = ConvertToNode(TYsonString(rsp->value()))->AsList();

            for (const auto& operationNode : operationNodes->GetChildren()) {
                auto operation = CreateOperationFromNode(operationNode);

                if (options.FromTime && *operation.StartTime < *options.FromTime ||
                    options.ToTime && *operation.StartTime >= *options.ToTime) {
                    continue;
                }

                auto textFactor = ExtractTextFactorForCypressItem(operation);
                if (substrFilter && textFactor.find(*substrFilter) == std::string::npos) {
                    continue;
                }

                EOperationState state = *operation.State;
                if (state != EOperationState::Pending && IsOperationInProgress(state)) {
                    state = EOperationState::Running;
                }

                if (!countingFilter.Filter(operation.Pools, *operation.AuthenticatedUser, state, *operation.Type, 1)) {
                    continue;
                }

                if (!countingFilter.FilterByFailedJobs(operation.BriefProgress)) {
                    continue;
                }

                if (options.CursorTime) {
                    if (options.CursorDirection == EOperationSortDirection::Past && *operation.StartTime >= *options.CursorTime) {
                        continue;
                    } else if (options.CursorDirection == EOperationSortDirection::Future && *operation.StartTime <= *options.CursorTime) {
                        continue;
                    }
                }

                if (areAllRequestedAttributesLight) {
                    filteredOperations.push_back(CreateOperationFromNode(operationNode, requestedAttributes));
                } else {
                    filteredOperations.push_back(std::move(operation));
                }
            }
        }

        // Retain more operations than limit to track (in)completeness of the response.
        auto operationsToRetain = options.Limit + 1;
        if (filteredOperations.size() > operationsToRetain) {
            std::nth_element(
                filteredOperations.begin(),
                filteredOperations.begin() + operationsToRetain,
                filteredOperations.end(),
                [&] (const TOperation& lhs, const TOperation& rhs) {
                    // Leave only |operationsToRetain| operations:
                    // either oldest (cursor_direction == "future") or newest (cursor_direction == "past").
                    return (options.CursorDirection == EOperationSortDirection::Future) && (*lhs.StartTime < *rhs.StartTime) ||
                           (options.CursorDirection == EOperationSortDirection::Past  ) && (*lhs.StartTime > *rhs.StartTime);
                });
            filteredOperations.resize(operationsToRetain);
        }

        idToOperation->reserve(idToOperation->size() + filteredOperations.size());
        if (areAllRequestedAttributesLight) {
            for (auto& operation : filteredOperations) {
                (*idToOperation)[*operation.Id] = std::move(operation);
            }
        } else {
            auto getBatchReq = proxy.ExecuteBatch();
            SetBalancingHeader(getBatchReq, options);

            for (const auto& operation: filteredOperations) {
                auto req = TYPathProxy::Get(GetNewOperationPath(*operation.Id));
                SetCachingHeader(req, options);
                ToProto(req->mutable_attributes()->mutable_keys(), MakeCypressOperationAttributes(requestedAttributes));
                getBatchReq->AddRequest(req);
            }

            auto getBatchRsp = WaitFor(getBatchReq->Invoke())
                .ValueOrThrow();

            for (const auto& rspOrError : getBatchRsp->GetResponses<TYPathProxy::TRspGet>()) {
                if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    continue;
                }
                auto node = ConvertToNode(TYsonString(rspOrError.ValueOrThrow()->value()));
                auto operation = CreateOperationFromNode(node);
                (*idToOperation)[*operation.Id] = std::move(operation);
            }
        }
    }

    // Searches in archive for operations satisfying given filters.
    // Returns operations with requested fields plus necessarily "start_time" and "id".
    THashMap<NScheduler::TOperationId, TOperation> DoListOperationsFromArchive(
        TInstant deadline,
        TCountingFilter& countingFilter,
        const TListOperationsOptions& options)
    {
        if (!options.FromTime) {
            THROW_ERROR_EXCEPTION("Missing required parameter \"from_time\"");
        }

        if (!options.ToTime) {
            THROW_ERROR_EXCEPTION("Missing required parameter \"to_time\"");
        }

        int version = DoGetOperationsArchiveVersion();

        if (options.Pool && version < 15) {
            THROW_ERROR_EXCEPTION(
                "Failed to get operation's pool: operations archive version is too old: expected >= 15, got %v",
                version);
        }

        if (version < 9) {
            THROW_ERROR_EXCEPTION("Operations archive version is too old: expected >= 9, got %v",
                version);
        }

        const auto archiveHasPools = (version >= 24);

        if (options.IncludeCounters) {
            TQueryBuilder builder;
            builder.SetSource(GetOperationsArchivePathOrderedByStartTime());

            for (const auto selectExpr : {"pool_or_pools", "authenticated_user", "state", "operation_type", "pool", "sum(1) AS count"}) {
                builder.AddSelectExpression(selectExpr);
            }

            builder.AddWhereExpression(Format("start_time > %v AND start_time <= %v",
                (*options.FromTime).MicroSeconds(),
                (*options.ToTime).MicroSeconds()));

            if (options.SubstrFilter) {
                builder.AddWhereExpression(
                    Format("is_substr(%Qv, filter_factors)", to_lower(*options.SubstrFilter)));
            }

            const TString poolQueryExpression = archiveHasPools ? "any_to_yson_string(pools)" : "pool";
            builder.SetGroupByExpression(poolQueryExpression + " AS pool_or_pools, authenticated_user, state, operation_type, pool");

            TSelectRowsOptions selectOptions;
            selectOptions.Timeout = deadline - Now();

            auto resultCounts = WaitFor(SelectRows(builder.Build(), selectOptions))
                .ValueOrThrow();

            for (auto row : resultCounts.Rowset->GetRows()) {
                TNullable<std::vector<TString>> pools;
                if (row[0].Type != EValueType::Null) {
                    pools = archiveHasPools
                        ? ConvertTo<std::vector<TString>>(TYsonString(row[0].Data.String, row[0].Length))
                        : std::vector<TString>{FromUnversionedValue<TString>(row[0])};
                }
                auto user = TStringBuf(row[1].Data.String, row[1].Length);
                auto state = ParseEnum<EOperationState>(TStringBuf(row[2].Data.String, row[2].Length));
                auto type = ParseEnum<EOperationType>(TStringBuf(row[3].Data.String, row[3].Length));
                if (row[4].Type != EValueType::Null) {
                    if (!pools) {
                        pools.Emplace();
                    }
                    pools->push_back(FromUnversionedValue<TString>(row[4]));
                }
                auto count = row[5].Data.Int64;

                countingFilter.Filter(pools, user, state, type, count);
            }
        }

        TQueryBuilder builder;
        builder.SetSource(GetOperationsArchivePathOrderedByStartTime());

        builder.AddSelectExpression("id_hi");
        builder.AddSelectExpression("id_lo");

        builder.AddWhereExpression(Format("start_time > %v AND start_time <= %v",
            (*options.FromTime).MicroSeconds(),
            (*options.ToTime).MicroSeconds()));

        if (options.SubstrFilter) {
            builder.AddWhereExpression(
                Format("is_substr(%Qv, filter_factors)", to_lower(*options.SubstrFilter)));
        }

        builder.SetOrderByExpression("start_time");

        if (options.CursorDirection == EOperationSortDirection::Past) {
            if (options.CursorTime) {
                builder.AddWhereExpression(Format("start_time <= %v", (*options.CursorTime).MicroSeconds()));
            }
            builder.SetOrderByDirection("DESC");
        }

        if (options.CursorDirection == EOperationSortDirection::Future) {
            if (options.CursorTime) {
                builder.AddWhereExpression(Format("start_time > %v", (*options.CursorTime).MicroSeconds()));
            }
            builder.SetOrderByDirection("ASC");
        }

        if (options.Pool) {
            builder.AddWhereExpression(archiveHasPools
                ? Format("list_contains(pools, %Qv) or pool = %Qv", *options.Pool, *options.Pool)
                : Format("pool = %Qv", *options.Pool));
        }

        if (options.StateFilter) {
            builder.AddWhereExpression(Format("state = %Qv", FormatEnum(*options.StateFilter)));
        }

        if (options.TypeFilter) {
            builder.AddWhereExpression(Format("operation_type = %Qv", FormatEnum(*options.TypeFilter)));
        }

        if (options.UserFilter) {
            builder.AddWhereExpression(Format("authenticated_user = %Qv", *options.UserFilter));
        }

        // Retain more operations than limit to track (in)completeness of the response.
        builder.SetLimit(1 + options.Limit);

        TSelectRowsOptions selectOptions;
        selectOptions.Timeout = deadline - Now();

        auto rowsItemsId = WaitFor(SelectRows(builder.Build(), selectOptions))
            .ValueOrThrow();

        TOrderedByIdTableDescriptor tableDescriptor;
        auto rowBuffer = New<TRowBuffer>();
        std::vector<TUnversionedRow> keys;

        keys.reserve(rowsItemsId.Rowset->GetRows().Size());
        for (auto row : rowsItemsId.Rowset->GetRows()) {
            auto key = rowBuffer->AllocateUnversioned(2);
            key[0] = MakeUnversionedUint64Value(row[0].Data.Uint64, tableDescriptor.Index.IdHi);
            key[1] = MakeUnversionedUint64Value(row[1].Data.Uint64, tableDescriptor.Index.IdLo);
            keys.push_back(key);
        }

        static const THashSet<TString> RequiredAttrbiutes = {"id", "start_time", "brief_progress"};
        static const THashSet<TString> DefaultAttributes = {
            "authenticated_user",
            "brief_progress",
            "brief_spec",
            "finish_time",
            "id",
            "runtime_parameters",
            "start_time",
            "state",
            "type",
        };
        static const THashSet<TString> IgnoredAttributes = {"suspended", "memory_usage"};

        auto attributesToRequest = MakeFinalAttrbibuteSet(options.Attributes, RequiredAttrbiutes, DefaultAttributes, IgnoredAttributes);
        bool needBriefProgress = !options.Attributes || options.Attributes->has("brief_progress");

        if (version < 22) {
            attributesToRequest.erase("runtime_parameters");
        }

        std::vector<int> columns;
        for (const auto columnName : MakeArchiveOperationAttributes(attributesToRequest)) {
            columns.push_back(tableDescriptor.NameTable->GetIdOrThrow(columnName));
        }

        NTableClient::TColumnFilter columnFilter(columns);

        TLookupRowsOptions lookupOptions;
        lookupOptions.ColumnFilter = columnFilter;
        lookupOptions.KeepMissingRows = true;
        lookupOptions.Timeout = deadline - Now();

        auto rowset = WaitFor(LookupRows(
            GetOperationsArchivePathOrderedById(),
            tableDescriptor.NameTable,
            MakeSharedRange(std::move(keys), std::move(rowBuffer)),
            lookupOptions))
            .ValueOrThrow();

        auto rows = rowset->GetRows();

        auto getYson = [&] (const TUnversionedValue& value) {
            return value.Type == EValueType::Null
                ? TYsonString()
                : TYsonString(value.Data.String, value.Length);
        };
        auto getString = [&] (const TUnversionedValue& value, TStringBuf name) {
            if (value.Type == EValueType::Null) {
                THROW_ERROR_EXCEPTION("Unexpected null value in column %Qv in job archive", name);
            }
            return TStringBuf(value.Data.String, value.Length);
        };

        THashMap<NScheduler::TOperationId, TOperation> idToOperation;

        auto& tableIndex = tableDescriptor.Index;
        for (auto row : rows) {
            if (!row) {
                continue;
            }

            auto briefProgress = getYson(row[columnFilter.GetPosition(tableIndex.BriefProgress)]);
            if (!countingFilter.FilterByFailedJobs(briefProgress)) {
                continue;
            }

            TOperation operation;

            TGuid operationId(
                row[columnFilter.GetPosition(tableIndex.IdHi)].Data.Uint64,
                row[columnFilter.GetPosition(tableIndex.IdLo)].Data.Uint64);

            operation.Id = operationId;

            if (auto indexOrNull = columnFilter.FindPosition(tableIndex.OperationType)) {
                operation.Type = ParseEnum<EOperationType>(getString(row[*indexOrNull], "operation_type"));
            }

            if (auto indexOrNull = columnFilter.FindPosition(tableIndex.State)) {
                operation.State = ParseEnum<EOperationState>(getString(row[*indexOrNull], "state"));
            }

            if (auto indexOrNull = columnFilter.FindPosition(tableIndex.AuthenticatedUser)) {
                operation.AuthenticatedUser = TString(getString(row[*indexOrNull], "authenticated_user"));
            }

            if (auto indexOrNull = columnFilter.FindPosition(tableIndex.StartTime)) {
                auto value = row[*indexOrNull];
                if (value.Type == EValueType::Null) {
                    THROW_ERROR_EXCEPTION("Unexpected null value in column start_time in operations archive");
                }
                operation.StartTime = TInstant::MicroSeconds(value.Data.Int64);
            }

            if (auto indexOrNull = columnFilter.FindPosition(tableIndex.FinishTime)) {
                if (row[*indexOrNull].Type != EValueType::Null) {
                    operation.FinishTime = TInstant::MicroSeconds(row[*indexOrNull].Data.Int64);
                }
            }

            if (auto indexOrNull = columnFilter.FindPosition(tableIndex.BriefSpec)) {
                operation.BriefSpec = getYson(row[*indexOrNull]);
            }
            if (auto indexOrNull = columnFilter.FindPosition(tableIndex.FullSpec)) {
                operation.FullSpec = getYson(row[*indexOrNull]);
            }
            if (auto indexOrNull = columnFilter.FindPosition(tableIndex.Spec)) {
                operation.Spec = getYson(row[*indexOrNull]);
            }
            if (auto indexOrNull = columnFilter.FindPosition(tableIndex.UnrecognizedSpec)) {
                operation.UnrecognizedSpec = getYson(row[*indexOrNull]);
            }

            if (needBriefProgress) {
                operation.BriefProgress = std::move(briefProgress);
            }
            if (auto indexOrNull = columnFilter.FindPosition(tableIndex.Progress)) {
                operation.Progress = getYson(row[*indexOrNull]);
            }

            if (DoGetOperationsArchiveVersion() >= 22) {
                if (auto indexOrNull = columnFilter.FindPosition(tableIndex.RuntimeParameters)) {
                    operation.RuntimeParameters = getYson(row[*indexOrNull]);
                }

                if (operation.RuntimeParameters) {
                    operation.Pools = GetPoolsFromRuntimeParameters(operation.RuntimeParameters);
                }
            }

            if (auto indexOrNull = columnFilter.FindPosition(tableIndex.Events)) {
                operation.Events = getYson(row[*indexOrNull]);
            }
            if (auto indexOrNull = columnFilter.FindPosition(tableIndex.Result)) {
                operation.Result = getYson(row[*indexOrNull]);
            }

            idToOperation.emplace(*operation.Id, std::move(operation));
        }

        return idToOperation;
    }

    // XXX(levysotsky): The counters may be incorrect if |options.IncludeArchive| is |true|
    // and an operation is in both Cypress and archive.
    // XXX(levysotsky): The "failed_jobs_count" counter is incorrect if corresponding failed operations
    // are in archive and outside of queried range.
    TListOperationsResult DoListOperations(
        const TListOperationsOptions& options)
    {
        auto timeout = options.Timeout.Get(Connection_->GetConfig()->DefaultListOperationsTimeout);
        auto deadline = timeout.ToDeadLine();

        if (options.CursorTime && (
            options.ToTime && *options.CursorTime > *options.ToTime ||
            options.FromTime && *options.CursorTime < *options.FromTime))
        {
            THROW_ERROR_EXCEPTION("Time cursor (%v) is out of range [from_time (%v), to_time (%v)]",
                *options.CursorTime,
                *options.FromTime,
                *options.ToTime);
        }

        constexpr ui64 MaxLimit = 100;
        if (options.Limit > MaxLimit) {
            THROW_ERROR_EXCEPTION("Requested result limit (%v) exceeds maximum allowed limit (%v)",
                options.Limit,
                MaxLimit);
        }

        TCountingFilter countingFilter(options);

        THashMap<NScheduler::TOperationId, TOperation> idToOperation;
        if (options.IncludeArchive && DoesOperationsArchiveExist()) {
            idToOperation = DoListOperationsFromArchive(deadline, countingFilter, options);
        }

        DoListOperationsFromCypress(deadline, countingFilter, options, &idToOperation);

        std::vector<TOperation> operations;
        operations.reserve(idToOperation.size());
        for (auto& item : idToOperation) {
            operations.push_back(std::move(item.second));
        }

        std::sort(operations.begin(), operations.end(), [&] (const TOperation& lhs, const TOperation& rhs) {
            // Reverse order: most recent first.
            return *lhs.StartTime > *rhs.StartTime;
        });

        TListOperationsResult result;

        result.Operations = std::move(operations);
        if (result.Operations.size() > options.Limit) {
            if (options.CursorDirection == EOperationSortDirection::Past) {
                result.Operations.resize(options.Limit);
            } else {
                result.Operations.erase(result.Operations.begin(), result.Operations.end() - options.Limit);
            }
            result.Incomplete = true;
        }

        if (options.IncludeCounters) {
            result.PoolCounts = std::move(countingFilter.PoolCounts);
            result.UserCounts = std::move(countingFilter.UserCounts);
            result.StateCounts = std::move(countingFilter.StateCounts);
            result.TypeCounts = std::move(countingFilter.TypeCounts);
            result.FailedJobsCount = countingFilter.FailedJobsCount;;
        }

        return result;
    }

    class TQueryBuilder
    {
    public:
        void SetSource(const TString& source)
        {
            Source_ = source;
        }

        int AddSelectExpression(const TString& expression)
        {
            SelectExpressions_.push_back(expression);
            return SelectExpressions_.size() - 1;
        }

        int AddWhereExpression(const TString& expression)
        {
            WhereExpressions_.push_back(expression);
            return WhereExpressions_.size() - 1;
        }

        void SetGroupByExpression(const TString& expression)
        {
            GroupByExpression_ = expression;
        }

        void SetOrderByExpression(const TString& expression)
        {
            OrderByExpression_ = expression;
        }

        void SetOrderByDirection(const TString& direction)
        {
            OrderByDirection_ = direction;
        }

        void SetLimit(int limit)
        {
            Limit_ = limit;
        }

        TString Build()
        {
            std::vector<TString> clauses;
            clauses.reserve(8);
            clauses.push_back(JoinSeq(", ", SelectExpressions_));
            clauses.push_back(Format("FROM [%v]", Source_));
            if (!WhereExpressions_.empty()) {
                clauses.push_back("WHERE");
                clauses.push_back(JoinSeq(" AND ", WhereExpressions_));
            }
            if (!OrderByExpression_.empty()) {
                clauses.push_back("ORDER BY");
                clauses.push_back(OrderByExpression_);
                if (!OrderByDirection_.empty()) {
                    clauses.push_back(OrderByDirection_);
                }
            }
            if (!GroupByExpression_.empty()) {
                clauses.push_back("GROUP BY");
                clauses.push_back(GroupByExpression_);
            }
            if (Limit_ >= 0) {
                clauses.push_back(Format("LIMIT %v", Limit_));
            }
            return JoinSeq(" ", clauses);
        }

    private:
        TString Source_;
        std::vector<TString> SelectExpressions_;
        std::vector<TString> WhereExpressions_;
        TString OrderByExpression_;
        TString OrderByDirection_;
        TString GroupByExpression_;

        int Limit_ = -1;
    };


    TFuture<std::pair<std::vector<TJob>, TListJobsStatistics>> DoListJobsFromArchive(
        const TOperationId& operationId,
        TInstant deadline,
        const TListJobsOptions& options)
    {
        std::vector<TJob> jobs;

        int archiveVersion = DoGetOperationsArchiveVersion();

        TQueryBuilder itemsQueryBuilder;
        itemsQueryBuilder.SetSource(GetOperationsArchiveJobsPath());
        itemsQueryBuilder.SetLimit(options.Offset + options.Limit);

        TQueryBuilder statisticsQueryBuilder;
        statisticsQueryBuilder.SetSource(GetOperationsArchiveJobsPath());

        auto operationIdExpression = Format(
            "(operation_id_hi, operation_id_lo) = (%vu, %vu)",
            operationId.Parts64[0],
            operationId.Parts64[1]);

        itemsQueryBuilder.AddWhereExpression(operationIdExpression);
        statisticsQueryBuilder.AddWhereExpression(operationIdExpression);

        auto jobIdHiIndex = itemsQueryBuilder.AddSelectExpression("job_id_hi");
        auto jobIdLoIndex = itemsQueryBuilder.AddSelectExpression("job_id_lo");
        auto typeIndex = itemsQueryBuilder.AddSelectExpression("type AS job_type");
        auto stateIndex = archiveVersion >= 16
            ? itemsQueryBuilder.AddSelectExpression("if(is_null(state), transient_state, state) AS job_state")
            : itemsQueryBuilder.AddSelectExpression("state AS job_state");
        auto startTimeIndex = itemsQueryBuilder.AddSelectExpression("start_time");
        auto finishTimeIndex = itemsQueryBuilder.AddSelectExpression("finish_time");
        auto addressIndex = itemsQueryBuilder.AddSelectExpression("address");
        auto errorIndex = itemsQueryBuilder.AddSelectExpression("error");
        auto statisticsIndex = itemsQueryBuilder.AddSelectExpression("statistics");
        auto stderrSizeIndex = itemsQueryBuilder.AddSelectExpression("stderr_size");
        auto hasSpecIndex = itemsQueryBuilder.AddSelectExpression("has_spec");
        auto hasFailContextIndex = itemsQueryBuilder.AddSelectExpression("has_fail_context");

        TNullable<int> failContextSizeIndex;
        if (archiveVersion >= 23) {
            failContextSizeIndex = itemsQueryBuilder.AddSelectExpression("fail_context_size");
        }

        if (archiveVersion >= 18) {
            auto updateTimeExpression = Format(
                "(job_state = \"aborted\" OR job_state = \"failed\" OR job_state = \"completed\" OR job_state = \"lost\" "
                "OR (NOT is_null(update_time) AND update_time >= %v))",
                (TInstant::Now() - options.RunningJobsLookbehindPeriod).MicroSeconds());
            itemsQueryBuilder.AddWhereExpression(updateTimeExpression);
            statisticsQueryBuilder.AddWhereExpression(updateTimeExpression);
        }

        if (options.WithStderr) {
            if (*options.WithStderr) {
                itemsQueryBuilder.AddWhereExpression("(stderr_size != 0 AND NOT is_null(stderr_size))");
            } else {
                itemsQueryBuilder.AddWhereExpression("(stderr_size = 0 OR is_null(stderr_size))");
            }
        }

        if (archiveVersion >= 20 && options.WithSpec) {
            if (*options.WithSpec) {
                itemsQueryBuilder.AddWhereExpression("(has_spec AND NOT is_null(has_spec))");
            } else {
                itemsQueryBuilder.AddWhereExpression("(NOT has_spec OR is_null(has_spec))");
            }
        }

        if (options.WithFailContext) {
            if (archiveVersion >= 23) {
                if (*options.WithFailContext) {
                    itemsQueryBuilder.AddWhereExpression("(fail_context_size != 0 AND NOT is_null(fail_context_size))");
                } else {
                    itemsQueryBuilder.AddWhereExpression("(fail_context_size = 0 OR is_null(fail_context_size))");
                }
            } else if (archiveVersion >= 21) {
                if (*options.WithFailContext) {
                    itemsQueryBuilder.AddWhereExpression("(has_fail_context AND NOT is_null(has_fail_context))");
                } else {
                    itemsQueryBuilder.AddWhereExpression("(NOT has_fail_context OR is_null(has_fail_context))");
                }
            } else {
                itemsQueryBuilder.AddWhereExpression("false");
            }
        }

        if (options.Address) {
            itemsQueryBuilder.AddWhereExpression(Format("address = %Qv", *options.Address));
        }

        if (options.Type) {
            itemsQueryBuilder.AddWhereExpression(Format("job_type = %Qv", FormatEnum(*options.Type)));
        }

        if (options.State) {
            itemsQueryBuilder.AddWhereExpression(Format("job_state = %Qv", FormatEnum(*options.State)));
        }

        if (options.SortField != EJobSortField::None) {
            switch (options.SortField) {
                case EJobSortField::Type:
                    itemsQueryBuilder.SetOrderByExpression("job_type");
                    break;
                case EJobSortField::State:
                    itemsQueryBuilder.SetOrderByExpression("job_state");
                    break;
                case EJobSortField::StartTime:
                    itemsQueryBuilder.SetOrderByExpression("start_time");
                    break;
                case EJobSortField::FinishTime:
                    itemsQueryBuilder.SetOrderByExpression("finish_time");
                    break;
                case EJobSortField::Address:
                    itemsQueryBuilder.SetOrderByExpression("address");
                    break;
                case EJobSortField::Duration:
                    itemsQueryBuilder.SetOrderByExpression(Format(
                        "if(is_null(finish_time), %v, finish_time) - start_time",
                        TInstant::Now().MicroSeconds()));
                    break;
                case EJobSortField::Id:
                    itemsQueryBuilder.SetOrderByExpression("format_guid(job_id_hi, job_id_lo)");
                    break;
                // XXX: progress is not presented in archive table.
                default:
                    break;
            }
            switch (options.SortOrder) {
                case EJobSortDirection::Ascending:
                    itemsQueryBuilder.SetOrderByDirection("ASC");
                    break;
                case EJobSortDirection::Descending:
                    itemsQueryBuilder.SetOrderByDirection("DESC");
                    break;
            }
        }

        auto itemsQuery = itemsQueryBuilder.Build();

        if (options.Address) {
            statisticsQueryBuilder.AddWhereExpression(Format("address = %Qv", *options.Address));
        }
        statisticsQueryBuilder.AddSelectExpression("type as job_type");
        if (archiveVersion >= 16) {
            statisticsQueryBuilder.AddSelectExpression("if(is_null(state), transient_state, state) AS job_state");
        } else {
            statisticsQueryBuilder.AddSelectExpression("state as job_state");
        }
        statisticsQueryBuilder.AddSelectExpression("SUM(1) AS count");
        statisticsQueryBuilder.SetGroupByExpression("job_type, job_state");
        auto statisticsQuery = statisticsQueryBuilder.Build();

        TSelectRowsOptions selectRowsOptions;
        selectRowsOptions.Timestamp = AsyncLastCommittedTimestamp;
        selectRowsOptions.Timeout = deadline - Now();

        auto checkIsNotNull = [&] (const TUnversionedValue& value, TStringBuf name, const TJobId& jobId = TJobId()) {
            if (value.Type == EValueType::Null) {
                auto error = TError("Unexpected null value in column %Qv in job archive", name)
                    << TErrorAttribute("operation_id", operationId);
                if (jobId) {
                    error = error
                        << TErrorAttribute("job_id", jobId);
                }
                THROW_ERROR error;
            }
        };

        auto itemsFuture = SelectRows(itemsQuery, selectRowsOptions).Apply(BIND([=] (const TSelectRowsResult& result) {
            std::vector<TJob> jobs;

            auto rows = result.Rowset->GetRows();

            for (auto row : rows) {
                checkIsNotNull(row[jobIdHiIndex], "job_id_hi", TGuid());
                checkIsNotNull(row[jobIdLoIndex], "job_id_lo", TGuid());

                TGuid id(row[jobIdHiIndex].Data.Uint64, row[jobIdLoIndex].Data.Uint64);

                jobs.emplace_back();
                auto& job = jobs.back();

                job.Id = id;

                checkIsNotNull(row[typeIndex], "type", id);
                job.Type = ParseEnum<EJobType>(TString(row[typeIndex].Data.String, row[typeIndex].Length));

                checkIsNotNull(row[stateIndex], "state", id);
                job.State = ParseEnum<EJobState>(TString(row[stateIndex].Data.String, row[stateIndex].Length));

                if (row[startTimeIndex].Type != EValueType::Null) {
                    job.StartTime = TInstant::MicroSeconds(row[startTimeIndex].Data.Int64);
                }

                if (row[finishTimeIndex].Type != EValueType::Null) {
                    job.FinishTime = TInstant::MicroSeconds(row[finishTimeIndex].Data.Int64);
                }

                if (row[addressIndex].Type != EValueType::Null) {
                    job.Address = TString(row[addressIndex].Data.String, row[addressIndex].Length);
                }

                if (row[stderrSizeIndex].Type != EValueType::Null) {
                    job.StderrSize = row[stderrSizeIndex].Data.Uint64;
                }

                if (failContextSizeIndex) {
                    if (row[*failContextSizeIndex].Type != EValueType::Null) {
                        job.FailContextSize = row[*failContextSizeIndex].Data.Uint64;
                    }
                } else {
                    if (row[hasFailContextIndex].Type != EValueType::Null && row[hasFailContextIndex].Data.Boolean) {
                        job.FailContextSize = 1024;
                    }
                }

                if (row[hasSpecIndex].Type != EValueType::Null) {
                    job.HasSpec = row[hasSpecIndex].Data.Boolean;
                }

                if (row[errorIndex].Type != EValueType::Null) {
                    job.Error = TYsonString(TString(row[errorIndex].Data.String, row[errorIndex].Length));
                }

                if (row[statisticsIndex].Type != EValueType::Null) {
                    auto briefStatisticsYson = TYsonString(TString(row[statisticsIndex].Data.String, row[statisticsIndex].Length));
                    auto briefStatistics = ConvertToNode(briefStatisticsYson);

                    // See BuildBriefStatistics.
                    auto rowCount = FindNodeByYPath(briefStatistics, "/data/input/row_count/sum");
                    auto uncompressedDataSize = FindNodeByYPath(briefStatistics, "/data/input/uncompressed_data_size/sum");
                    auto compressedDataSize = FindNodeByYPath(briefStatistics, "/data/input/compressed_data_size/sum");
                    auto dataWeight = FindNodeByYPath(briefStatistics, "/data/input/data_weight/sum");
                    auto inputPipeIdleTime = FindNodeByYPath(briefStatistics, "/user_job/pipes/input/idle_time/sum");
                    auto jobProxyCpuUsage = FindNodeByYPath(briefStatistics, "/job_proxy/cpu/user/sum");

                    job.BriefStatistics = BuildYsonStringFluently()
                        .BeginMap()
                            .DoIf(static_cast<bool>(rowCount), [&] (TFluentMap fluent) {
                                fluent.Item("processed_input_row_count").Value(rowCount->AsInt64()->GetValue());
                            })
                            .DoIf(static_cast<bool>(uncompressedDataSize), [&] (TFluentMap fluent) {
                                fluent.Item("processed_input_uncompressed_data_size").Value(uncompressedDataSize->AsInt64()->GetValue());
                            })
                            .DoIf(static_cast<bool>(compressedDataSize), [&] (TFluentMap fluent) {
                                fluent.Item("processed_input_compressed_data_size").Value(compressedDataSize->AsInt64()->GetValue());
                            })
                            .DoIf(static_cast<bool>(dataWeight), [&] (TFluentMap fluent) {
                                fluent.Item("processed_input_data_weight").Value(dataWeight->AsInt64()->GetValue());
                            })
                            .DoIf(static_cast<bool>(inputPipeIdleTime), [&] (TFluentMap fluent) {
                                fluent.Item("input_pipe_idle_time").Value(inputPipeIdleTime->AsInt64()->GetValue());
                            })
                            .DoIf(static_cast<bool>(jobProxyCpuUsage), [&] (TFluentMap fluent) {
                                fluent.Item("job_proxy_cpu_usage").Value(jobProxyCpuUsage->AsInt64()->GetValue());
                            })
                        .EndMap();
                }
            }

            return jobs;
        }));

        auto statisticsFuture = SelectRows(statisticsQuery, selectRowsOptions).Apply(BIND([=] (const TSelectRowsResult& result) {
            TListJobsStatistics statistics;

            auto rows = result.Rowset->GetRows();
            for (const auto& row : rows) {
                checkIsNotNull(row[0], "type");
                checkIsNotNull(row[1], "state");
                auto jobType = ParseEnum<EJobType>(TString(row[0].Data.String, row[0].Length));
                auto jobState = ParseEnum<EJobState>(TString(row[1].Data.String, row[1].Length));
                i64 count = row[2].Data.Int64;

                statistics.TypeCounts[jobType] += count;
                if (options.Type && *options.Type != jobType) {
                    continue;
                }

                statistics.StateCounts[jobState] += count;
                if (options.State && *options.State != jobState) {
                    continue;
                }
            }
            return statistics;
        }));

        return
            CombineAll(std::vector<TFuture<void>>{itemsFuture.As<void>(), statisticsFuture.As<void>()})
            .Apply(BIND([itemsFuture, statisticsFuture] (const std::vector<TError>&) {
                const auto& items = itemsFuture.Get();
                const auto& statistics = statisticsFuture.Get();
                if (!items.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to get jobs from the operation archive")
                        << items;
                }
                if (!statistics.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to get job statistics from the operation archive")
                        << statistics;
                }
                return std::make_pair(items.Value(), statistics.Value());
            }));
    }

    TFuture<std::pair<std::vector<TJob>, int>> DoListJobsFromCypress(
        const TOperationId& operationId,
        TInstant deadline,
        const TListJobsOptions& options)
    {
        TObjectServiceProxy proxy(OperationsArchiveChannels_[options.ReadFrom]);

        auto attributeFilter = std::vector<TString>{
            "job_type",
            "state",
            "start_time",
            "finish_time",
            "address",
            "error",
            "brief_statistics",
            "input_paths",
            "core_infos",
            "uncompressed_data_size"
        };

        auto batchReq = proxy.ExecuteBatch();
        batchReq->SetTimeout(deadline - Now());

        {
            auto getReq = TYPathProxy::Get(GetJobsPath(operationId));
            ToProto(getReq->mutable_attributes()->mutable_keys(), attributeFilter);
            batchReq->AddRequest(getReq, "get_jobs");
        }

        {
            auto getReqNew = TYPathProxy::Get(GetNewJobsPath(operationId));
            ToProto(getReqNew->mutable_attributes()->mutable_keys(), attributeFilter);
            batchReq->AddRequest(getReqNew, "get_jobs_new");
        }

        return batchReq->Invoke().Apply(BIND([options] (
            const TErrorOr<TObjectServiceProxy::TRspExecuteBatchPtr>& batchRspOrError)
        {
            const auto& batchRsp = batchRspOrError.ValueOrThrow();

            auto getReqRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_jobs_new");
            if (!getReqRsp.IsOK()) {
                if (getReqRsp.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    getReqRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_jobs");
                } else {
                    THROW_ERROR getReqRsp;
                }
            }

            const auto& rsp = getReqRsp.ValueOrThrow();

            std::pair<std::vector<TJob>, int> result;
            auto& jobs = result.first;

            auto items = ConvertToNode(NYson::TYsonString(rsp->value()))->AsMap();
            result.second = items->GetChildren().size();

            for (const auto& item : items->GetChildren()) {
                const auto& attributes = item.second->Attributes();
                auto children = item.second->AsMap();

                auto id = TGuid::FromString(item.first);

                auto type = ParseEnum<NJobTrackerClient::EJobType>(attributes.Get<TString>("job_type"));
                auto state = ParseEnum<NJobTrackerClient::EJobState>(attributes.Get<TString>("state"));
                auto address = attributes.Get<TString>("address");

                i64 stderrSize = -1;
                if (auto stderrNode = children->FindChild("stderr")) {
                    stderrSize = stderrNode->Attributes().Get<i64>("uncompressed_data_size");
                }

                if (options.WithSpec) {
                    if (*options.WithSpec == (state == EJobState::Running)) {
                        continue;
                    }
                }

                if (options.WithStderr) {
                    if (*options.WithStderr && stderrSize <= 0) {
                        continue;
                    }
                    if (!(*options.WithStderr) && stderrSize > 0) {
                        continue;
                    }
                }

                i64 failContextSize = -1;
                if (auto failContextNode = children->FindChild("fail_context")) {
                    failContextSize = failContextNode->Attributes().Get<i64>("uncompressed_data_size");
                }

                if (options.WithFailContext) {
                    if (*options.WithFailContext && failContextSize <= 0) {
                        continue;
                    }
                    if (!(*options.WithFailContext) && failContextSize > 0) {
                        continue;
                    }
                }

                jobs.emplace_back();
                auto& job = jobs.back();

                job.Id = id;
                job.Type = type;
                job.State = state;
                job.StartTime = ConvertTo<TInstant>(attributes.Get<TString>("start_time"));
                job.FinishTime = ConvertTo<TInstant>(attributes.Get<TString>("finish_time"));
                job.Address = address;
                if (stderrSize >= 0) {
                    job.StderrSize = stderrSize;
                }
                if (failContextSize >= 0) {
                    job.FailContextSize = failContextSize;
                }
                job.HasSpec = true;
                job.Error = attributes.FindYson("error");
                job.BriefStatistics = attributes.FindYson("brief_statistics");
                job.InputPaths = attributes.FindYson("input_paths");
                job.CoreInfos = attributes.FindYson("core_infos");
            }

            return result;
        }));
    }

    TFuture<std::pair<std::vector<TJob>, int>> DoListJobsFromControllerAgent(
        const TOperationId& operationId,
        const TNullable<TString>& controllerAgentAddress,
        TInstant deadline,
        const TListJobsOptions& options)
    {
        TObjectServiceProxy proxy(GetMasterChannelOrThrow(EMasterChannelKind::Follower));
        proxy.SetDefaultTimeout(deadline - Now());

        if (!controllerAgentAddress) {
            return MakeFuture(std::pair<std::vector<TJob>, int>{});
        }

        auto path = GetControllerAgentOrchidOperationPath(*controllerAgentAddress, operationId) + "/running_jobs";
        auto getReq = TYPathProxy::Get(path);

        return proxy.Execute(getReq).Apply(BIND([options] (const TYPathProxy::TRspGetPtr& rsp) {
            std::pair<std::vector<TJob>, int> result;
            auto& jobs = result.first;

            auto items = ConvertToNode(NYson::TYsonString(rsp->value()))->AsMap();
            result.second = items->GetChildren().size();

            for (const auto& item : items->GetChildren()) {
                auto values = item.second->AsMap();

                auto id = TGuid::FromString(item.first);

                auto type = ParseEnum<NJobTrackerClient::EJobType>(
                    values->GetChild("job_type")->AsString()->GetValue());
                auto state = ParseEnum<NJobTrackerClient::EJobState>(
                    values->GetChild("state")->AsString()->GetValue());
                auto address = values->GetChild("address")->AsString()->GetValue();

                auto stderrSize = values->GetChild("stderr_size")->AsInt64()->GetValue();

                if (options.WithSpec) {
                    if (*options.WithSpec == (state == EJobState::Running)) {
                        continue;
                    }
                }

                if (options.WithStderr) {
                    if (*options.WithStderr && stderrSize <= 0) {
                        continue;
                    }
                    if (!(*options.WithStderr) && stderrSize > 0) {
                        continue;
                    }
                }

                if (options.WithFailContext && *options.WithFailContext) {
                    continue;
                }

                jobs.emplace_back();
                auto& job = jobs.back();

                job.Id = id;
                job.Type = type;
                job.State = state;
                job.StartTime = ConvertTo<TInstant>(values->GetChild("start_time")->AsString()->GetValue());
                job.Address = address;
                job.Progress = values->GetChild("progress")->AsDouble()->GetValue();
                if (stderrSize > 0) {
                    job.StderrSize = stderrSize;
                }
                job.BriefStatistics = ConvertToYsonString(values->GetChild("brief_statistics"));
            }

            return result;
        }));
    }

    std::function<bool(const TJob&, const TJob&)> GetJobsComparator(EJobSortField sortField, EJobSortDirection sortOrder)
    {
        switch (sortField) {
#define XX(field) \
            case EJobSortField::field: \
                switch (sortOrder) { \
                    case EJobSortDirection::Ascending: \
                        return [] (const TJob& lhs, const TJob& rhs) { \
                            return LessNullable(lhs.field, rhs.field); \
                        }; \
                        break; \
                    case EJobSortDirection::Descending: \
                        return [&] (const TJob& lhs, const TJob& rhs) { \
                            return LessNullable(rhs.field, lhs.field); \
                        }; \
                        break; \
                    default: \
                        Y_UNREACHABLE(); \
                } \
                break;

            XX(Type);
            XX(State);
            XX(StartTime);
            XX(FinishTime);
            XX(Address);
            XX(Progress);
#undef XX

            case EJobSortField::None:
                switch (sortOrder) {
                    case EJobSortDirection::Ascending:
                        return [] (const TJob& lhs, const TJob& rhs) {
                            return lhs.Id < rhs.Id;
                        };
                        break;
                    case EJobSortDirection::Descending:
                        return [] (const TJob& lhs, const TJob& rhs) {
                            return !(lhs.Id < rhs.Id || lhs.Id == rhs.Id);
                        };
                        break;
                    default:
                        Y_UNREACHABLE();
                }
                break;

            case EJobSortField::Duration:
                switch (sortOrder) {
                    case EJobSortDirection::Ascending:
                        return [now = TInstant::Now()] (const TJob& lhs, const TJob& rhs) {
                            auto lhsDuration = (lhs.FinishTime ? *lhs.FinishTime : now) - lhs.StartTime;
                            auto rhsDuration = (rhs.FinishTime ? *rhs.FinishTime : now) - rhs.StartTime;
                            return lhsDuration < rhsDuration;
                        };
                        break;
                    case EJobSortDirection::Descending:
                        return [now = TInstant::Now()] (const TJob& lhs, const TJob& rhs) {
                            auto lhsDuration = (lhs.FinishTime ? *lhs.FinishTime : now) - lhs.StartTime;
                            auto rhsDuration = (rhs.FinishTime ? *rhs.FinishTime : now) - rhs.StartTime;
                            return lhsDuration > rhsDuration;
                        };
                        break;
                    default:
                        Y_UNREACHABLE();
                }
                break;

            case EJobSortField::Id:
                switch (sortOrder) {
                    case EJobSortDirection::Ascending:
                        return [] (const TJob& lhs, const TJob& rhs) {
                            return ToString(lhs.Id) < ToString(rhs.Id);
                        };
                        break;
                    case EJobSortDirection::Descending:
                        return [] (const TJob& lhs, const TJob& rhs) {
                            return ToString(lhs.Id) > ToString(rhs.Id);
                        };
                        break;
                    default:
                        Y_UNREACHABLE();
                }
                break;

            default:
                Y_UNREACHABLE();
        }
    }

    void UpdateJobsList(const std::vector<TJob>& delta, std::vector<TJob>* result)
    {
        auto mergeJob = [] (TJob* target, const TJob& source) {
#define MERGE_FIELD(name) target->name = source.name
#define MERGE_NULLABLE_FIELD(name) \
            if (source.name) { \
                target->name = source.name; \
            }
            MERGE_FIELD(Type);
            MERGE_FIELD(State);
            MERGE_FIELD(StartTime);
            MERGE_NULLABLE_FIELD(FinishTime);
            MERGE_FIELD(Address);
            MERGE_NULLABLE_FIELD(Progress);
            MERGE_NULLABLE_FIELD(StderrSize);
            MERGE_NULLABLE_FIELD(Error);
            MERGE_NULLABLE_FIELD(BriefStatistics);
            MERGE_NULLABLE_FIELD(InputPaths);
            MERGE_NULLABLE_FIELD(CoreInfos);
#undef MERGE_FIELD
#undef MERGE_NULLABLE_FIELD
        };

        auto mergeJobs = [&] (const std::vector<TJob>& source1, const std::vector<TJob>& source2) {
            std::vector<TJob> result;

            auto it1 = source1.begin();
            auto end1 = source1.end();

            auto it2 = source2.begin();
            auto end2 = source2.end();

            while (it1 != end1 && it2 != end2) {
                if (it1->Id == it2->Id) {
                    result.push_back(*it1);
                    mergeJob(&result.back(), *it2);
                    ++it1;
                    ++it2;
                } else if (it1->Id < it2->Id) {
                    result.push_back(*it1);
                    ++it1;
                } else {
                    result.push_back(*it2);
                    ++it2;
                }
            }

            result.insert(result.end(), it1, end1);
            result.insert(result.end(), it2, end2);

            return result;
        };

        if (result->empty()) {
            *result = delta;
            return;
        }

        auto sortedDelta = delta;
        std::sort(
            sortedDelta.begin(),
            sortedDelta.end(),
            [] (const TJob& lhs, const TJob& rhs) {
                return lhs.Id < rhs.Id;
            });
        *result = mergeJobs(*result, sortedDelta);
    }

    TListJobsResult DoListJobs(
        const TOperationId& operationId,
        const TListJobsOptions& options)
    {
        auto timeout = options.Timeout.Get(Connection_->GetConfig()->DefaultListJobsTimeout);
        auto deadline = timeout.ToDeadLine();

        TListJobsResult result;

        auto controllerAgentAddress = GetControllerAgentAddressFromCypress(
            operationId,
            GetMasterChannelOrThrow(EMasterChannelKind::Follower));

        auto dataSource = options.DataSource;
        if (dataSource == EDataSource::Auto) {
            if (controllerAgentAddress) {
                dataSource = EDataSource::Runtime;
            } else {
                dataSource = EDataSource::Archive;
            }
        }

        bool includeCypress;
        bool includeControllerAgent;
        bool includeArchive;

        switch (dataSource) {
            case EDataSource::Archive:
                includeCypress = false;
                includeControllerAgent = false;
                includeArchive = true;
                break;
            case EDataSource::Runtime:
                includeCypress = true;
                includeControllerAgent = true;
                includeArchive = false;
                break;
            case EDataSource::Manual:
                // NB: if 'cypress'/'controller_agent' included simultanously with 'archive' then pagintaion may be broken.
                includeCypress = options.IncludeCypress;
                includeControllerAgent = options.IncludeControllerAgent;
                includeArchive = options.IncludeArchive;
                break;
            default:
                Y_UNREACHABLE();
        }

        LOG_DEBUG("Starting list jobs (IncludeCypress: %v, IncludeControllerAgent: %v, IncludeArchive: %v)",
            includeCypress,
            includeControllerAgent,
            includeArchive);

        TFuture<std::pair<std::vector<TJob>, int>> cypressJobsFuture;
        TFuture<std::pair<std::vector<TJob>, int>> controllerAgentJobsFuture;
        TFuture<std::pair<std::vector<TJob>, TListJobsStatistics>> archiveJobsFuture;

        TNullable<TListJobsStatistics> statistics;

        bool doesArchiveExists = DoesOperationsArchiveExist();

        // Issue the requests in parallel.
        if (includeArchive && doesArchiveExists) {
            archiveJobsFuture = DoListJobsFromArchive(operationId, deadline, options);
        }

        if (includeCypress) {
            cypressJobsFuture = DoListJobsFromCypress(operationId, deadline, options);
        }

        if (includeControllerAgent) {
            controllerAgentJobsFuture = DoListJobsFromControllerAgent(
                operationId,
                controllerAgentAddress,
                deadline,
                options);
        }

        if (includeArchive && doesArchiveExists) {
            auto archiveJobsOrError = WaitFor(archiveJobsFuture);
            if (!archiveJobsOrError.IsOK()) {
                THROW_ERROR_EXCEPTION(EErrorCode::JobArchiveUnavailable, "Job archive is unavailable")
                    << archiveJobsOrError;
            }

            const auto& archiveJobs = archiveJobsOrError.Value();
            statistics = archiveJobs.second;
            UpdateJobsList(archiveJobs.first, &result.Jobs);

            result.ArchiveJobCount = 0;
            for (const auto& count : archiveJobs.second.TypeCounts) {
                *result.ArchiveJobCount += count;
            }
        }

        if (includeCypress) {
            auto cypressJobsOrError = WaitFor(cypressJobsFuture);
            if (cypressJobsOrError.IsOK()) {
                const auto& cypressJobs = cypressJobsOrError.Value();
                result.CypressJobCount = cypressJobs.second;
                UpdateJobsList(cypressJobs.first, &result.Jobs);
            } else if (cypressJobsOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                // No such operation in Cypress.
                result.CypressJobCount = 0;
            } else {
                THROW_ERROR cypressJobsOrError;
            }
        }

        if (includeControllerAgent) {
            auto controllerAgentJobsOrError = WaitFor(controllerAgentJobsFuture);
            if (controllerAgentJobsOrError.IsOK()) {
                const auto& controllerAgentJobs = controllerAgentJobsOrError.Value();
                result.ControllerAgentJobCount = controllerAgentJobs.second;
                UpdateJobsList(controllerAgentJobs.first, &result.Jobs);
            } else if (controllerAgentJobsOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                // No such operation in the scheduler.
                result.ControllerAgentJobCount = 0;
            } else {
                THROW_ERROR controllerAgentJobsOrError;
            }
        }

        if (options.DataSource != EDataSource::Archive) {
            statistics = TListJobsStatistics();

            std::vector<TJob> filteredJobs;
            for (const auto& job : result.Jobs) {
                if (options.Address && job.Address != *options.Address) {
                    continue;
                }

                statistics->TypeCounts[job.Type] += 1;
                if (options.Type && job.Type != *options.Type) {
                    continue;
                }

                statistics->StateCounts[job.State] += 1;
                if (options.State && job.State != *options.State) {
                    continue;
                }

                filteredJobs.push_back(job);
            }

            result.Jobs = filteredJobs;
            result.Statistics = *statistics;

            auto comparer = GetJobsComparator(options.SortField, options.SortOrder);
            std::sort(result.Jobs.begin(), result.Jobs.end(), comparer);

            auto beginIt = result.Jobs.begin() + std::min(options.Offset,
                std::distance(result.Jobs.begin(), result.Jobs.end()));
            auto endIt = beginIt + std::min(options.Limit,
                std::distance(beginIt, result.Jobs.end()));

            result.Jobs = std::vector<TJob>(beginIt, endIt);
        } else {
            result.Jobs = std::vector<TJob>(std::min(result.Jobs.end(), result.Jobs.begin() + options.Offset), result.Jobs.end());
        }

        return result;
    }

    template <typename TValue>
    static void TryAddFluentItem(
        TFluentMap fluent,
        TStringBuf key,
        TUnversionedRow row,
        const NTableClient::TColumnFilter& columnFilter,
        int columnIndex)
    {
        auto valueIndexOrNull = columnFilter.FindPosition(columnIndex);
        if (valueIndexOrNull && row[*valueIndexOrNull].Type != EValueType::Null) {
            fluent.Item(key).Value(FromUnversionedValue<TValue>(row[*valueIndexOrNull]));
        }
    }

    // Attribute names allowed for 'get_job' and 'list_jobs' commands.
    const THashSet<TString> SupportedJobAttributes = {
        "operation_id",
        "job_id",
        "type",
        "state",
        "start_time",
        "finish_time",
        "address",
        "error",
        "statistics",
        "events",
    };

    std::vector<TString> MakeJobArchiveAttributes(const THashSet<TString>& attributes, int archiveVersion)
    {
        std::vector<TString> result;
        result.reserve(attributes.size() + 2); // Plus 2 as operation_id and job_id are split into hi and lo.
        for (const auto& attribute : attributes) {
            if (!SupportedJobAttributes.has(attribute)) {
                THROW_ERROR_EXCEPTION("Job attribute %Qv is not allowed", attribute);
            }
            if (attribute.EndsWith("_id")) {
                result.push_back(attribute + "_hi");
                result.push_back(attribute + "_lo");
            } else {
                result.push_back(attribute);
                if (attribute == "state" && archiveVersion > 16) {
                    result.push_back("transient_state");
                }
            }
        }
        return result;
    }

    TYsonString DoGetJob(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobOptions& options)
    {
        auto timeout = options.Timeout.Get(Connection_->GetConfig()->DefaultGetJobTimeout);
        auto deadline = timeout.ToDeadLine();

        int archiveVersion = DoGetOperationsArchiveVersion();

        TJobTableDescriptor table;
        auto rowBuffer = New<TRowBuffer>();

        std::vector<TUnversionedRow> keys;
        auto key = rowBuffer->AllocateUnversioned(4);
        key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], table.Index.OperationIdHi);
        key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], table.Index.OperationIdLo);
        key[2] = MakeUnversionedUint64Value(jobId.Parts64[0], table.Index.JobIdHi);
        key[3] = MakeUnversionedUint64Value(jobId.Parts64[1], table.Index.JobIdLo);
        keys.push_back(key);

        TLookupRowsOptions lookupOptions;

        const THashSet<TString> DefaultAttributes = {
            "operation_id",
            "job_id",
            "type",
            "state",
            "start_time",
            "finish_time",
            "address",
            "error",
            "statistics",
            "events",
        };

        std::vector<int> columnIndexes;
        auto fields = MakeJobArchiveAttributes(options.Attributes.Get(DefaultAttributes), archiveVersion);
        for (const auto& field : fields) {
            columnIndexes.push_back(table.NameTable->GetIdOrThrow(field));
        }

        lookupOptions.ColumnFilter = NTableClient::TColumnFilter(columnIndexes);
        lookupOptions.KeepMissingRows = true;
        lookupOptions.Timeout = deadline - Now();

        auto rowset = WaitFor(LookupRows(
            GetOperationsArchiveJobsPath(),
            table.NameTable,
            MakeSharedRange(std::move(keys), std::move(rowBuffer)),
            lookupOptions))
            .ValueOrThrow();

        auto rows = rowset->GetRows();
        YCHECK(!rows.Empty());
        auto row = rows[0];

        if (!row) {
            THROW_ERROR_EXCEPTION("No such job %v or operation %v", jobId, operationId);
        }

        const auto& columnFilter = lookupOptions.ColumnFilter;

        TStringBuf state;
        {
            auto indexOrNull = columnFilter.FindPosition(table.Index.State);
            if (indexOrNull && row[*indexOrNull].Type != EValueType::Null) {
                state = FromUnversionedValue<TStringBuf>(row[*indexOrNull]);
            }
        }
        if (!state.IsInited() && archiveVersion >= 16) {
            auto indexOrNull = columnFilter.FindPosition(table.Index.TransientState);
            if (indexOrNull && row[*indexOrNull].Type != EValueType::Null) {
                state = FromUnversionedValue<TStringBuf>(row[*indexOrNull]);
            }
        }

        // NB: We need a separate function for |TInstant| because it has type "int64" in table
        // but |FromUnversionedValue<TInstant>| expects it to be "uint64".
        auto tryAddInstantFluentItem = [&] (TFluentMap fluent, TStringBuf key, int columnIndex) {
            auto valueIndexOrNull = columnFilter.FindPosition(columnIndex);
            if (valueIndexOrNull && row[*valueIndexOrNull].Type != EValueType::Null) {
                fluent.Item(key).Value(TInstant::MicroSeconds(row[*valueIndexOrNull].Data.Int64));
            }
        };

        return BuildYsonStringFluently()
            .BeginMap()
                .DoIf(columnFilter.ContainsIndex(table.Index.OperationIdHi), [&] (TFluentMap fluent) {
                    fluent.Item("operation_id").Value(operationId);
                })
                .DoIf(columnFilter.ContainsIndex(table.Index.JobIdHi), [&] (TFluentMap fluent) {
                    fluent.Item("job_id").Value(jobId);
                })
                .DoIf(state.IsInited(), [&] (TFluentMap fluent) {
                    fluent.Item("state").Value(state);
                })

                .Do(std::bind(tryAddInstantFluentItem, std::placeholders::_1, "start_time", table.Index.StartTime))
                .Do(std::bind(tryAddInstantFluentItem, std::placeholders::_1, "finish_time", table.Index.FinishTime))

                .Do(std::bind(TryAddFluentItem<TString>,     std::placeholders::_1, "address",     row, columnFilter, table.Index.Address))
                .Do(std::bind(TryAddFluentItem<TString>,     std::placeholders::_1, "type",        row, columnFilter, table.Index.Type))
                .Do(std::bind(TryAddFluentItem<TYsonString>, std::placeholders::_1, "error",       row, columnFilter, table.Index.Error))
                .Do(std::bind(TryAddFluentItem<TYsonString>, std::placeholders::_1, "statistics",  row, columnFilter, table.Index.Statistics))
                .Do(std::bind(TryAddFluentItem<TYsonString>, std::placeholders::_1, "events",      row, columnFilter, table.Index.Events))
            .EndMap();
    }

    TYsonString DoStraceJob(
        const TJobId& jobId,
        const TStraceJobOptions& /*options*/)
    {
        auto req = JobProberProxy_->Strace();
        ToProto(req->mutable_job_id(), jobId);

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        return TYsonString(rsp->trace());
    }

    void DoSignalJob(
        const TJobId& jobId,
        const TString& signalName,
        const TSignalJobOptions& /*options*/)
    {
        auto req = JobProberProxy_->SignalJob();
        ToProto(req->mutable_job_id(), jobId);
        ToProto(req->mutable_signal_name(), signalName);

        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    void DoAbandonJob(
        const TJobId& jobId,
        const TAbandonJobOptions& /*options*/)
    {
        auto req = JobProberProxy_->AbandonJob();
        ToProto(req->mutable_job_id(), jobId);

        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    TYsonString DoPollJobShell(
        const TJobId& jobId,
        const TYsonString& parameters,
        const TPollJobShellOptions& options)
    {
        auto req = JobProberProxy_->PollJobShell();
        ToProto(req->mutable_job_id(), jobId);
        ToProto(req->mutable_parameters(), parameters.GetData());

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        return TYsonString(rsp->result());
    }

    void DoAbortJob(
        const TJobId& jobId,
        const TAbortJobOptions& options)
    {
        auto req = JobProberProxy_->AbortJob();
        ToProto(req->mutable_job_id(), jobId);
        if (options.InterruptTimeout) {
            req->set_interrupt_timeout(ToProto<i64>(*options.InterruptTimeout));
        }

        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    TClusterMeta DoGetClusterMeta(
        const TGetClusterMetaOptions& options)
    {
        auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
        auto batchReq = proxy->ExecuteBatch();
        SetBalancingHeader(batchReq, options);

        auto req = TMasterYPathProxy::GetClusterMeta();
        req->set_populate_node_directory(options.PopulateNodeDirectory);
        req->set_populate_cluster_directory(options.PopulateClusterDirectory);
        req->set_populate_medium_directory(options.PopulateMediumDirectory);
        SetCachingHeader(req, options);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspGetClusterMeta>(0)
            .ValueOrThrow();

        TClusterMeta meta;
        if (options.PopulateNodeDirectory) {
            meta.NodeDirectory = std::make_shared<NNodeTrackerClient::NProto::TNodeDirectory>();
            meta.NodeDirectory->Swap(rsp->mutable_node_directory());
        }
        if (options.PopulateClusterDirectory) {
            meta.ClusterDirectory = std::make_shared<NHiveClient::NProto::TClusterDirectory>();
            meta.ClusterDirectory->Swap(rsp->mutable_cluster_directory());
        }
        if (options.PopulateMediumDirectory) {
            meta.MediumDirectory = std::make_shared<NChunkClient::NProto::TMediumDirectory>();
            meta.MediumDirectory->Swap(rsp->mutable_medium_directory());
        }
        return meta;
    }
};

DEFINE_REFCOUNTED_TYPE(TClient)

IClientPtr CreateClient(
    IConnectionPtr connection,
    const TClientOptions& options)
{
    YCHECK(connection);

    return New<TClient>(std::move(connection), options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT
