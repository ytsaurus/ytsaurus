#include "client.h"
#include "private.h"
#include "box.h"
#include "config.h"
#include "native_connection.h"
#include "native_transaction.h"
#include "file_reader.h"
#include "file_writer.h"
#include "journal_reader.h"
#include "journal_writer.h"
#include "rowset.h"
#include "table_reader.h"
#include "table_writer.h"
#include "tablet_helpers.h"
#include "skynet.h"
#include "operation_archive_schema.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_replica.h>
#include <yt/ytlib/chunk_client/read_limit.h>
#include <yt/ytlib/chunk_client/chunk_teleporter.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/medium_directory.pb.h>

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

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/ytlib/object_client/helpers.h>
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
#include <yt/ytlib/scheduler/resource_limits.h>
#include <yt/ytlib/scheduler/proto/job.pb.h>
#include <yt/ytlib/scheduler/job_prober_service_proxy.h>
#include <yt/ytlib/scheduler/scheduler_service_proxy.h>

#include <yt/ytlib/security_client/group_ypath_proxy.h>
#include <yt/ytlib/security_client/helpers.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/schema_inferer.h>
#include <yt/ytlib/table_client/table_ypath_proxy.h>
#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/row_merger.h>

#include <yt/ytlib/tablet_client/table_mount_cache.h>
#include <yt/ytlib/tablet_client/tablet_service_proxy.h>
#include <yt/ytlib/tablet_client/wire_protocol.h>
#include <yt/ytlib/tablet_client/wire_protocol.pb.h>
#include <yt/ytlib/tablet_client/table_replica_ypath.h>

#include <yt/ytlib/transaction_client/timestamp_provider.h>
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

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_proxy.h>

#include <yt/core/misc/collection_helpers.h>

#include <util/string/join.h>

namespace NYT {
namespace NApi {

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

using NChunkClient::TReadLimit;
using NChunkClient::TReadRange;
using NTableClient::TColumnSchema;
using NNodeTrackerClient::INodeChannelFactoryPtr;
using NNodeTrackerClient::CreateNodeChannelFactory;
using NNodeTrackerClient::TNetworkPreferenceList;
using NNodeTrackerClient::TNodeDescriptor;

using TTableReplicaInfoPtrList = SmallVector<TTableReplicaInfoPtr, TypicalReplicaCount>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJobInputReader)
DECLARE_REFCOUNTED_CLASS(TNativeClient)
DECLARE_REFCOUNTED_CLASS(TNativeTransaction)

////////////////////////////////////////////////////////////////////////////////

namespace {

EWorkloadCategory FromUserWorkloadCategory(EUserWorkloadCategory category)
{
    switch (category) {
        case EUserWorkloadCategory::Realtime:
            return EWorkloadCategory::UserRealtime;
        case EUserWorkloadCategory::Interactive:
            return EWorkloadCategory::UserInteractive;
        case EUserWorkloadCategory::Batch:
            return EWorkloadCategory::UserBatch;
        default:
            Y_UNREACHABLE();
    }
}

} // namespace

TUserWorkloadDescriptor::operator TWorkloadDescriptor() const
{
    TWorkloadDescriptor result;
    result.Category = FromUserWorkloadCategory(Category);
    result.Band = Band;
    return result;
}

struct TSerializableUserWorkloadDescriptor
    : public TYsonSerializableLite
{
    TUserWorkloadDescriptor Underlying;

    TSerializableUserWorkloadDescriptor()
    {
        RegisterParameter("category", Underlying.Category);
        RegisterParameter("band", Underlying.Band)
            .Optional();
    }
};

void Serialize(const TUserWorkloadDescriptor& workloadDescriptor, NYson::IYsonConsumer* consumer)
{
    TSerializableUserWorkloadDescriptor serializableWorkloadDescriptor;
    serializableWorkloadDescriptor.Underlying = workloadDescriptor;
    Serialize(serializableWorkloadDescriptor, consumer);
}

void Deserialize(TUserWorkloadDescriptor& workloadDescriptor, INodePtr node)
{
    TSerializableUserWorkloadDescriptor serializableWorkloadDescriptor;
    Deserialize(serializableWorkloadDescriptor, node);
    workloadDescriptor = serializableWorkloadDescriptor.Underlying;
}

////////////////////////////////////////////////////////////////////////////////

NRpc::TMutationId TMutatingOptions::GetOrGenerateMutationId() const
{
    if (Retry && !MutationId) {
        THROW_ERROR_EXCEPTION("Cannot execute retry without mutation id");
    }
    return MutationId ? MutationId : NRpc::GenerateMutationId();
}

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

TError TCheckPermissionResult::ToError(const TString& user, EPermission permission) const
{
    switch (Action) {
        case NSecurityClient::ESecurityAction::Allow:
            return TError();

        case NSecurityClient::ESecurityAction::Deny: {
            TError error;
            if (ObjectName && SubjectName) {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied: %Qlv permission is denied for %Qv by ACE at %v",
                    permission,
                    *SubjectName,
                    *ObjectName);
            } else {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied: %Qlv permission is not allowed by any matching ACE",
                    permission);
            }
            error.Attributes().Set("user", user);
            error.Attributes().Set("permission", permission);
            if (ObjectId) {
                error.Attributes().Set("denied_by", ObjectId);
            }
            if (SubjectId) {
                error.Attributes().Set("denied_for", SubjectId);
            }
            return error;
        }

        default:
            Y_UNREACHABLE();
    }
}

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
        TransferResultFuture_.Cancel();
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


class TQueryPreparer
    : public virtual TRefCounted
    , public IPrepareCallbacks
{
public:
    explicit TQueryPreparer(INativeConnectionPtr connection)
        : Connection_(std::move(connection))
    { }

    // IPrepareCallbacks implementation.

    virtual TFuture<TDataSplit> GetInitialSplit(
        const TYPath& path,
        TTimestamp timestamp) override
    {
        return BIND(&TQueryPreparer::DoGetInitialSplit, MakeStrong(this))
            .AsyncVia(Connection_->GetInvoker())
            .Run(path, timestamp);
    }

private:
    const INativeConnectionPtr Connection_;

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
        const auto& tableMountCache = Connection_->GetTableMountCache();
        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path.GetPath()))
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

class TNativeClient
    : public INativeClient
{
public:
    TNativeClient(
        INativeConnectionPtr connection,
        const TClientOptions& options)
        : Connection_(std::move(connection))
        , Options_(options)
        , ConcurrentRequestsSemaphore_(New<TAsyncSemaphore>(Connection_->GetConfig()->MaxConcurrentRequests))
    {
        auto wrapChannel = [&] (IChannelPtr channel) {
            channel = CreateAuthenticatedChannel(channel, options.User);
            return channel;
        };
        auto wrapChannelFactory = [&] (IChannelFactoryPtr factory) {
            factory = CreateAuthenticatedChannelFactory(factory, options.User);
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
            Connection_->GetMasterChannelOrThrow(EMasterChannelKind::Leader),
            Options_.User,
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


    virtual IConnectionPtr GetConnection() override
    {
        return Connection_;
    }

    virtual const INativeConnectionPtr& GetNativeConnection() override
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
        return CreateAuthenticatedChannel(std::move(channel), Options_.User);
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


    virtual TFuture<INativeTransactionPtr> StartNativeTransaction(
        ETransactionType type,
        const TTransactionStartOptions& options) override
    {
        return TransactionManager_->Start(type, options).Apply(
            BIND([=, this_ = MakeStrong(this)] (const NTransactionClient::TTransactionPtr& transaction) {
                auto wrappedTransaction = CreateNativeTransaction(this_, transaction, Logger);
                if (options.Sticky) {
                    Connection_->RegisterStickyTransaction(wrappedTransaction);
                }
                return wrappedTransaction;
            }));
    }

    virtual INativeTransactionPtr AttachNativeTransaction(
        const TTransactionId& transactionId,
        const TTransactionAttachOptions& options) override
    {
        if (options.Sticky) {
            return Connection_->GetStickyTransaction(transactionId);
        } else {
            auto wrappedTransaction = TransactionManager_->Attach(transactionId, options);
            return CreateNativeTransaction(this, std::move(wrappedTransaction), Logger);
        }
    }

    virtual TFuture<ITransactionPtr> StartTransaction(
        ETransactionType type,
        const TTransactionStartOptions& options) override
    {
        return StartNativeTransaction(type, options).As<ITransactionPtr>();
    }

    virtual ITransactionPtr AttachTransaction(
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
                &TNativeClient::doMethod, \
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


    virtual TFuture<IAsyncZeroCopyInputStreamPtr> CreateFileReader(
        const TYPath& path,
        const TFileReaderOptions& options) override
    {
        return NApi::CreateFileReader(this, path, options);
    }

    virtual IFileWriterPtr CreateFileWriter(
        const TYPath& path,
        const TFileWriterOptions& options) override
    {
        return NApi::CreateFileWriter(this, path, options);
    }


    virtual IJournalReaderPtr CreateJournalReader(
        const TYPath& path,
        const TJournalReaderOptions& options) override
    {
        return NApi::CreateJournalReader(this, path, options);
    }

    virtual IJournalWriterPtr CreateJournalWriter(
        const TYPath& path,
        const TJournalWriterOptions& options) override
    {
        return NApi::CreateJournalWriter(this, path, options);
    }

    virtual TFuture<ISchemalessMultiChunkReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options) override
    {
        return NApi::CreateTableReader(this, path, options, New<TNameTable>());
    }

    virtual TFuture<TSkynetSharePartsLocationsPtr> LocateSkynetShare(
        const NYPath::TRichYPath& path,
        const TLocateSkynetShareOptions& options) override
    {
        return NApi::LocateSkynetShare(this, path, options);
    }

    virtual TFuture<NTableClient::ISchemalessWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const NApi::TTableWriterOptions& options) override
    {
        return NApi::CreateTableWriter(this, path, options);
    }

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
        const TUpdateOperationParametersOptions& options),
        (operationId, options))
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
    friend class TNativeTransaction;

    const INativeConnectionPtr Connection_;
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
                auto& Logger = client->Logger;
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
            TNativeConnectionConfigPtr config,
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
                Batches_.back()->Indexes.size() >= Config_->MaxRowsPerReadRequest)
            {
                Batches_.emplace_back(new TBatch(std::move(tabletInfo)));
            }

            auto& batch = Batches_.back();
            batch->Indexes.push_back(index);
            batch->Keys.push_back(key);
        }

        TFuture<void> Invoke(IChannelFactoryPtr channelFactory, TCellDirectoryPtr cellDirectory)
        {
            auto* codec = NCompression::GetCodec(Config_->LookupRequestCodec);

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
            InvokeProxy_->SetDefaultTimeout(Options_.Timeout);
            InvokeProxy_->SetDefaultRequestAck(false);

            InvokeNextBatch();
            return InvokePromise_;
        }

        void ParseResponse(
            const TRowBufferPtr& rowBuffer,
            std::vector<TTypeErasedRow>* resultRows)
        {
            auto* responseCodec = NCompression::GetCodec(Config_->LookupResponseCodec);
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
        const TNativeConnectionConfigPtr Config_;
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
            req->set_request_codec(static_cast<int>(Config_->LookupRequestCodec));
            req->set_response_codec(static_cast<int>(Config_->LookupResponseCodec));
            req->Attachments().push_back(batch->RequestData);
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
        const IClientPtr&,
        const TTableReplicaInfoPtr&)>;

    static NTableClient::TColumnFilter RemapColumnFilter(
        const NTableClient::TColumnFilter& columnFilter,
        const TNameTableToSchemaIdMapping& idMapping,
        const TNameTablePtr& nameTable)
    {
        NTableClient::TColumnFilter remappedColumnFilter(columnFilter);
        if (!remappedColumnFilter.All) {
            for (auto& index : remappedColumnFilter.Indexes) {
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
        }
        return remappedColumnFilter;
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
            if (remappedColumnFilter.All) {
                req.clear_column_filter();
            } else {
                ToProto(req.mutable_column_filter()->mutable_indexes(), remappedColumnFilter.Indexes);
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
            const IClientPtr& replicaClient,
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
            if (remappedColumnFilter.All) {
                req.clear_column_filter();
            } else {
                ToProto(req.mutable_column_filter()->mutable_indexes(), remappedColumnFilter.Indexes);
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
            const IClientPtr& replicaClient,
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
            proxy.SetDefaultTimeout(options.Timeout);

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


    IConnectionPtr GetReplicaConnectionOrThrow(const TString& clusterName)
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

    IClientPtr CreateReplicaClient(const TString& clusterName)
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

    std::vector<int> BuildResponseIdMaping(const NTableClient::TColumnFilter& remappedColumnFilter)
    {
        std::vector<int> mapping;
        for (int index = 0; index < remappedColumnFilter.Indexes.size(); ++index) {
            int id = remappedColumnFilter.Indexes[index];
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
            auto capturedKey = inputRowBuffer->CaptureAndPermuteRow(keys[index], schema, idMapping);

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
        std::vector<int> keyIndexToResultIndex(keys.Size());
        int currentResultIndex = -1;

        THashMap<TCellId, TTabletCellLookupSessionPtr> cellIdToSession;

        // TODO(sandello): Reuse code from QL here to partition sorted keys between tablets.
        // Get rid of hash map.
        // TODO(sandello): Those bind states must be in a cross-session shared state. Check this when refactor out batches.
        TTabletCellLookupSession::TEncoder boundEncoder = std::bind(encoderWithMapping, remappedColumnFilter, std::placeholders::_1);
        TTabletCellLookupSession::TDecoder boundDecoder = std::bind(decoderWithMapping, resultSchemaData, std::placeholders::_1);
        for (int index = 0; index < sortedKeys.size(); ++index) {
            if (index == 0 || sortedKeys[index].first != sortedKeys[index - 1].first) {
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
                session->AddKey(++currentResultIndex, std::move(tabletInfo), key);
            }

            keyIndexToResultIndex[sortedKeys[index].second] = currentResultIndex;
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
        std::vector<TTypeErasedRow> uniqueResultRows;
        uniqueResultRows.resize(currentResultIndex + 1);

        auto outputRowBuffer = New<TRowBuffer>(TLookupRowsOutputBufferTag());

        for (const auto& pair : cellIdToSession) {
            const auto& session = pair.second;
            session->ParseResponse(outputRowBuffer, &uniqueResultRows);
        }

        if (!remappedColumnFilter.All) {
            auto mapping = BuildResponseIdMaping(remappedColumnFilter);
            RemapValueIds(TRow(), uniqueResultRows, mapping);
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

        auto queryPreparer = New<TQueryPreparer>(Connection_);

        auto queryExecutor = CreateQueryExecutor(
            Connection_,
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

        ISchemafulWriterPtr writer;
        TFuture<IUnversionedRowsetPtr> asyncRowset;
        std::tie(writer, asyncRowset) = CreateSchemafulRowsetWriter(query->GetTableSchema());

        auto statistics = WaitFor(queryExecutor->Execute(
            query,
            externalCGInfo,
            dataSource,
            writer,
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

        std::vector<TTableReplicaId> replicas;

        if (keys.Empty()) {
            for (const auto& replica : tableInfo->Replicas) {
                replicas.push_back(replica->ReplicaId);
            }
        } else {
            THashMap<TCellId, std::vector<TTabletId>> cellToTabletIds;
            THashSet<TTabletId> tabletIds;
            for (auto key : keys) {
                ValidateClientKey(key, schema, idMapping, nameTable);
                auto capturedKey = rowBuffer->CaptureAndPermuteRow(key, schema, idMapping);

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
                proxy.SetDefaultTimeout(options.Timeout);

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
                    replicas.push_back(replicaId);
                }
            }
        }

        LOG_DEBUG("Got table in-sync replicas (TableId: %v, Replicas: %v, Timestamp: %llx)",
            tableInfo->TableId,
            replicas,
            options.Timestamp);

        return replicas;
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
                proxy.SetDefaultTimeout(options.Timeout);
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

    void DoMountTable(
        const TYPath& path,
        const TMountTableOptions& options)
    {
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

    void DoUnmountTable(
        const TYPath& path,
        const TUnmountTableOptions& options)
    {
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

    void DoRemountTable(
        const TYPath& path,
        const TRemountTableOptions& options)
    {
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

    void DoFreezeTable(
        const TYPath& path,
        const TFreezeTableOptions& options)
    {
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

    void DoUnfreezeTable(
        const TYPath& path,
        const TUnfreezeTableOptions& options)
    {
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

    TTableYPathProxy::TReqReshardPtr MakeReshardRequest(
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
        auto req = MakeReshardRequest(path, options);
        ToProto(req->mutable_pivot_keys(), pivotKeys);
        req->set_tablet_count(pivotKeys.size());

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
    }

    void DoReshardTableWithTabletCount(
        const TYPath& path,
        int tabletCount,
        const TReshardTableOptions& options)
    {
        auto req = MakeReshardRequest(path, options);
        req->set_tablet_count(tabletCount);

        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        WaitFor(proxy->Execute(req))
            .ThrowOnError();
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
        proxy.SetDefaultTimeout(Connection_->GetConfig()->WriteTimeout);

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
        auto req = TTableReplicaYPathProxy::Alter(FromObjectId(replicaId));
        if (options.Enabled) {
            req->set_enabled(*options.Enabled);
        }
        if (options.Mode) {
            req->set_mode(static_cast<int>(*options.Mode));
        }
        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
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
        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
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
        if (!options.SourceTransactionId.IsEmpty()) {
            ToProto(req->mutable_source_transaction_id(), options.SourceTransactionId);
        }
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
                auto proxy = CreateWriteProxy<TObjectServiceProxy>();
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
                        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
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

                    auto proxy = CreateWriteProxy<TObjectServiceProxy>(srcCellTag);
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

            uploadTransaction->Detach();
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
        auto proxy = CreateWriteProxy<TObjectServiceProxy>();
        auto batchReq = proxy->ExecuteBatch();
        SetPrerequisites(batchReq, options);

        auto req = TMasterYPathProxy::CreateObject();
        SetMutationId(req, options);
        req->set_type(static_cast<int>(type));
        if (options.Attributes) {
            ToProto(req->mutable_object_attributes(), *options.Attributes);
        }
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObject>(0)
            .ValueOrThrow();

        return FromProto<TObjectId>(rsp->object_id());
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

        SetTransactionId(req, options, true);

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

        result.Path = destination;
        return result;
    }


    TPutFileToCacheResult DoPutFileToCache(
        const NYPath::TYPath &path,
        const TString &expectedMD5,
        const TPutFileToCacheOptions &options)
    {
        NLogging::TLogger logger = Logger.AddTag("Path: %v", path).AddTag("Command: PutFileToCache");
        auto Logger = logger;

        TPutFileToCacheResult result;

        // Start transaction.
        ITransactionPtr transaction;
        {
            auto transactionStartOptions = TTransactionStartOptions();
            transactionStartOptions.ParentId = GetTransactionId(options, true);
            transactionStartOptions.Sticky = true;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Format("Putting file %v to cache", path));
            transactionStartOptions.Attributes = std::move(attributes);

            auto asyncTransaction = StartTransaction(ETransactionType::Master, transactionStartOptions);
            transaction = WaitFor(asyncTransaction)
                .ValueOrThrow();

            auto transactionAttachOptions = TTransactionAttachOptions();
            transactionAttachOptions.AutoAbort = true;
            transactionAttachOptions.PingAncestors = options.PingAncestors;
            transaction = AttachTransaction(transaction->GetId(), transactionAttachOptions);

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

            auto readPermissionResult = DoCheckPermission(Options_.User, objectIdPath, EPermission::Read, checkPermissionOptions);
            readPermissionResult.ToError(Options_.User, EPermission::Read)
                .ThrowOnError();

            auto removePermissionResult = DoCheckPermission(Options_.User, objectIdPath, EPermission::Remove, checkPermissionOptions);
            removePermissionResult.ToError(Options_.User, EPermission::Remove)
                .ThrowOnError();

            auto usePermissionResult = DoCheckPermission(Options_.User, options.CachePath, EPermission::Use, checkPermissionOptions);
            usePermissionResult.ToError(Options_.User, EPermission::Use)
                .ThrowOnError();
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
            auto moveOptions = TMoveNodeOptions();
            moveOptions.TransactionId = transaction->GetId();
            moveOptions.Recursive = true;
            moveOptions.Force = true;
            moveOptions.PrerequisiteRevisions = options.PrerequisiteRevisions;
            moveOptions.PrerequisiteTransactionIds = options.PrerequisiteTransactionIds;

            WaitFor(fileCacheClient->MoveNode(objectIdPath, destination, moveOptions))
                .ValueOrThrow();

            LOG_DEBUG(
                "File has been moved to cache (Destination: %v)",
                destination);
        }

        // Set /@touched attribute.
        {
            auto setNodeOptions = TSetNodeOptions();
            setNodeOptions.PrerequisiteTransactionIds = options.PrerequisiteTransactionIds;
            setNodeOptions.PrerequisiteRevisions = options.PrerequisiteRevisions;
            setNodeOptions.TransactionId = transaction->GetId();

            auto asyncResult = fileCacheClient->SetNode(destination + "/@touched", ConvertToYsonString(true), setNodeOptions);
            auto rspOrError = WaitFor(asyncResult);

            if (rspOrError.GetCode() != NCypressClient::EErrorCode::ConcurrentTransactionLockConflict) {
                THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error setting /@touched attribute");
            }

            LOG_DEBUG(
                "Attribute /@touched set (Destination: %v)",
                destination);
        }

        WaitFor(transaction->Commit())
            .ThrowOnError();

        result.Path = destination;
        return result;
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
        const TUpdateOperationParametersOptions& options)
    {
        auto req = SchedulerProxy_->UpdateOperationParameters();
        ToProto(req->mutable_operation_id(), operationId);

        for (const auto& pair : options.SchedulingOptionsPerPoolTree) {
            const auto& treeId = pair.first;
            const auto& schedulingOptions = pair.second;

            auto* protoOptions = req->add_options();
            protoOptions->set_tree_id(treeId);

            auto* protoSchedulingOptions = protoOptions->mutable_scheduling_options();
            if (schedulingOptions->Weight) {
                protoSchedulingOptions->set_weight(*schedulingOptions->Weight);
            }
            ToProto(protoSchedulingOptions->mutable_resource_limits(),
                *schedulingOptions->ResourceLimits);
        }

        if (options.Owners) {
            ToProto(req->mutable_owner_list()->mutable_owners(), *options.Owners);
        }

        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    bool IsArchiveExists()
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

    TYsonString DoGetOperationFromArchive(
        const NScheduler::TOperationId& operationId,
        const TGetOperationOptions& options)
    {
        TNullable<TInstant> deadline;
        if (options.Timeout) {
            deadline = options.Timeout->ToDeadLine();
        }

        auto nameTable = New<TNameTable>();

        TOrderedByIdTableDescriptor ids(nameTable);
        auto rowBuffer = New<TRowBuffer>();

        std::vector<TUnversionedRow> keys;
        auto key = rowBuffer->AllocateUnversioned(2);
        key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], ids.IdHi);
        key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], ids.IdLo);
        keys.push_back(key);

        TLookupRowsOptions lookupOptions;

        THashSet<TString> fields;
        bool hasId = false;
        if (options.Attributes) {
            for (const auto& field : *options.Attributes) {
                if (field == "id") {
                    hasId = true;
                    fields.insert("id_lo");
                    fields.insert("id_hi");
                } else {
                    fields.insert(field);
                }
            }
        } else {
            hasId = true;
            fields = {
                "id_lo",
                "id_hi",
                "state",
                "authenticated_user",
                "operation_type",
                "progress",
                "spec",
                "brief_progress",
                "brief_spec",
                "start_time",
                "finish_time",
                "result",
                "events"
            };
        }

        std::vector<int> columnIndexes;
        THashMap<TString, int> fieldToIndex;

        int index = 0;
        for (const auto& field : fields) {
            columnIndexes.push_back(nameTable->GetIdOrThrow(field));
            fieldToIndex[field] = index++;
        }

        lookupOptions.ColumnFilter = NTableClient::TColumnFilter(columnIndexes);
        lookupOptions.KeepMissingRows = true;
        if (deadline) {
            lookupOptions.Timeout = *deadline - Now();
        }

        auto rowset = WaitFor(LookupRows(
            "//sys/operations_archive/ordered_by_id",
            nameTable,
            MakeSharedRange(std::move(keys), std::move(rowBuffer)),
            lookupOptions))
            .ValueOrThrow();

        auto rows = rowset->GetRows();
        YCHECK(!rows.Empty());

        if (rows[0]) {
#define SET_ITEM_STRING_VALUE(itemKey) \
            SET_ITEM_VALUE(itemKey, TString(rows[0][index].Data.String, rows[0][index].Length))
#define SET_ITEM_YSON_STRING_VALUE(itemKey) \
            SET_ITEM_VALUE(itemKey, TYsonString(rows[0][index].Data.String, rows[0][index].Length))
#define SET_ITEM_INSTANT_VALUE(itemKey) \
            SET_ITEM_VALUE(itemKey, TInstant(rows[0][index].Data.Int64))
#define SET_ITEM_VALUE(itemKey, operation) \
            .DoIf(fields.find(itemKey) != fields.end() && rows[0][GET_INDEX(itemKey)].Type != EValueType::Null, [&] (TFluentMap fluent) { \
                auto index = GET_INDEX(itemKey); \
                fluent.Item(itemKey).Value(operation); \
            })
#define GET_INDEX(itemKey) fieldToIndex.find(itemKey)->second

            auto ysonResult = BuildYsonStringFluently()
                .BeginMap()
                    .DoIf(hasId, [&] (TFluentMap fluent) {
                        fluent.Item("id").Value(TGuid(
                            rows[0][GET_INDEX("id_hi")].Data.Uint64,
                            rows[0][GET_INDEX("id_lo")].Data.Uint64));
                    })
                    SET_ITEM_STRING_VALUE("state")
                    SET_ITEM_STRING_VALUE("authenticated_user")
                    SET_ITEM_STRING_VALUE("operation_type")
                    SET_ITEM_YSON_STRING_VALUE("progress")
                    SET_ITEM_YSON_STRING_VALUE("spec")
                    SET_ITEM_YSON_STRING_VALUE("brief_progress")
                    SET_ITEM_YSON_STRING_VALUE("brief_spec")
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
        TNullable<TInstant> deadline;
        if (options.Timeout) {
            deadline = options.Timeout->ToDeadLine();
        }

        TNullable<std::vector<TString>> attributes;
        if (options.Attributes) {
            attributes = std::vector<TString>();
            attributes->reserve(options.Attributes->size() + 1);
            attributes->insert(attributes->begin(), options.Attributes->begin(), options.Attributes->end());
            // NOTE(asaitgalin): This attribute helps to distinguish between
            // different cypress storage modes of operation.
            if (options.Attributes->find("state") == options.Attributes->end()) {
                attributes->push_back("state");
            }
        }

        TObjectServiceProxy proxy(OperationsArchiveChannels_[options.ReadFrom]);
        auto batchReq = proxy.ExecuteBatch();
        SetBalancingHeader(batchReq, options);

        {
            auto req = TYPathProxy::Get(GetNewOperationPath(operationId) + "/@");
            if (attributes) {
                ToProto(req->mutable_attributes()->mutable_keys(), *attributes);
            }
            batchReq->AddRequest(req, "get_operation_new");
        }

        {
            auto req = TYPathProxy::Get(GetOperationPath(operationId) + "/@");
            if (attributes) {
                ToProto(req->mutable_attributes()->mutable_keys(), *attributes);
            }
            batchReq->AddRequest(req, "get_operation");
        }

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();

        auto attrNodeOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_operation_new");
        auto attrOldNodeOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_operation");

        auto getCypressNode = [&] (const auto& rsp) -> INodePtr {
            if (rsp.IsOK()) {
                auto node = ConvertToNode(TYsonString(rsp.Value()->value()));
                if (!node->AsMap()->FindChild("state")) {
                    return nullptr;
                }
                return node;
            }
            if (!rsp.FindMatching(NYTree::EErrorCode::ResolveError)) {
                THROW_ERROR rsp;
            }
            return nullptr;
        };

        INodePtr newCypressNode = getCypressNode(attrNodeOrError);
        INodePtr oldCypressNode = getCypressNode(attrOldNodeOrError);
        INodePtr cypressNode;
        if (newCypressNode && oldCypressNode) {
            cypressNode = PatchNode(oldCypressNode, newCypressNode);
        } else if (newCypressNode) {
            cypressNode = newCypressNode;
        } else {
            cypressNode = oldCypressNode;
        }

        if (cypressNode) {
            auto attrNode = cypressNode->AsMap();

            if (!attributes) {
                auto userAttributeKeys = ConvertTo<THashSet<TString>>(attrNode->GetChild("user_attribute_keys"));
                for (const auto& key : attrNode->GetKeys()) {
                    if (userAttributeKeys.find(key) == userAttributeKeys.end()) {
                        attrNode->RemoveChild(key);
                    }
                }
            }

            if (options.Attributes && options.Attributes->find("state") == options.Attributes->end()) {
                attrNode->RemoveChild("state");
            }

            TGetNodeOptions optionsToScheduler;
            if (deadline) {
                optionsToScheduler.Timeout = *deadline - Now();
            }

            bool shouldRequestProgress = false;
            if (options.Attributes) {
                const auto& attributes = *options.Attributes;
                shouldRequestProgress = std::find(attributes.begin(), attributes.end(), "progress") != attributes.end();
            } else {
                shouldRequestProgress = true;
            }

            if (shouldRequestProgress) {
                auto asyncSchedulerProgressValue = GetNode(GetOperationProgressFromOrchid(operationId), optionsToScheduler);
                auto schedulerProgressValueOrError = WaitFor(asyncSchedulerProgressValue);

                if (schedulerProgressValueOrError.IsOK()) {
                    auto schedulerProgressNode = ConvertToNode(schedulerProgressValueOrError.Value());
                    attrNode->RemoveChild("progress");
                    YCHECK(attrNode->AddChild(schedulerProgressNode, "progress"));

                    return ConvertToYsonString(attrNode);
                } else if (schedulerProgressValueOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    LOG_DEBUG("No such operation %v in the scheduler", operationId);
                } else {
                    THROW_ERROR_EXCEPTION("Failed to get operation %v from the scheduler", operationId)
                        << schedulerProgressValueOrError;
                }
            }

            return ConvertToYsonString(cypressNode);
        } else if (IsArchiveExists()) {
            LOG_DEBUG("No such operation %v in Cypress", operationId);

            int version = DoGetOperationsArchiveVersion();

            if (version < 7) {
                THROW_ERROR_EXCEPTION("Failed to get operation: operations archive version is too old: expected >= 7, got %v", version);
            }

            try {
                auto result = DoGetOperationFromArchive(operationId, options);
                if (result) {
                    return result;
                }
            } catch (const TErrorException& exception) {
                auto matchedError = exception.Error().FindMatching(NYTree::EErrorCode::ResolveError);

                if (!matchedError) {
                    THROW_ERROR_EXCEPTION("Failed to get operation from archive")
                        << TErrorAttribute("operation_id", operationId)
                        << exception.Error();
                }
            }
        }

        THROW_ERROR_EXCEPTION(EErrorCode::NoSuchOperation, "No such operation %v", operationId);
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
        int version = DoGetOperationsArchiveVersion();

        if (version < 7) {
            THROW_ERROR_EXCEPTION("Failed to get job input: operations archive version is too old: expected >= 7, got %v", version);
        }

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

        auto userJobReadController = CreateUserJobReadController(
            jobSpecHelper,
            MakeStrong(this),
            GetConnection()->GetInvoker(),
            TNodeDescriptor(),
            BIND([] { }) /* onNetworkRelease */,
            Null /* udfDirectory */,
            nullptr /* trafficMeter */);

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
            auto matchedError = exception.Error().FindMatching(NScheduler::EErrorCode::NoSuchJob);

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
            auto nameTable = New<TNameTable>();

            TStderrArchiveIds ids(nameTable);

            auto rowBuffer = New<TRowBuffer>();

            std::vector<TUnversionedRow> keys;
            auto key = rowBuffer->AllocateUnversioned(4);
            key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], ids.OperationIdHi);
            key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], ids.OperationIdLo);
            key[2] = MakeUnversionedUint64Value(jobId.Parts64[0], ids.JobIdHi);
            key[3] = MakeUnversionedUint64Value(jobId.Parts64[1], ids.JobIdLo);
            keys.push_back(key);

            TLookupRowsOptions lookupOptions;
            lookupOptions.ColumnFilter = NTableClient::TColumnFilter({ids.Stderr});
            lookupOptions.KeepMissingRows = true;

            auto rowset = WaitFor(LookupRows(
                "//sys/operations_archive/stderrs",
                nameTable,
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

        int version = DoGetOperationsArchiveVersion();

        if (version >= 7) {
            stderrRef = DoGetJobStderrFromArchive(operationId, jobId);
            if (stderrRef) {
                return stderrRef;
            }
        } else {
            LOG_DEBUG("Operations archive version is too old: expected >= 7, got %v", version);
        }

        THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::NoSuchJob, "Job stderr is not found")
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
        TString textFactor;

        auto pushTextFactor = [&textFactor] (const auto& text) {
            if (textFactor.size())
                textFactor += " ";
            textFactor += text;
        };

        pushTextFactor(ToString(operation.OperationId));
        pushTextFactor(operation.AuthenticatedUser);
        pushTextFactor(ToString(operation.OperationState));
        pushTextFactor(ToString(operation.OperationType));

        if (operation.BriefSpec) {
            auto briefSpecMapNode = ConvertToNode(operation.BriefSpec)->AsMap();
            if (briefSpecMapNode->FindChild("pool")) {
                pushTextFactor(briefSpecMapNode->GetChild("pool")->AsString()->GetValue());
            }
            if (briefSpecMapNode->FindChild("title")) {
                pushTextFactor(briefSpecMapNode->GetChild("title")->AsString()->GetValue());
            }
            if (briefSpecMapNode->FindChild("input_table_paths")) {
                auto inputTablesNode = briefSpecMapNode->GetChild("input_table_paths")->AsList();
                if (inputTablesNode->GetChildCount() > 0) {
                    pushTextFactor(inputTablesNode->GetChildren()[0]->AsString()->GetValue());
                }
            }
            if (briefSpecMapNode->FindChild("output_table_paths")) {
                auto outputTablesNode = briefSpecMapNode->GetChild("output_table_paths")->AsList();
                if (outputTablesNode->GetChildCount() > 0) {
                    pushTextFactor(outputTablesNode->GetChildren()[0]->AsString()->GetValue());
                }
            }
        }

        textFactor = to_lower(textFactor);

        return textFactor;
    }

    TListOperationsResult DoListOperations(
        const TListOperationsOptions& options)
    {
        TNullable<TInstant> deadline;
        if (options.Timeout) {
            deadline = options.Timeout->ToDeadLine();
        }

        TListOperationsResult result;
        THashMap<TString, i64> poolCounts;
        THashMap<TString, i64> userCounts;
        TEnumIndexedVector<i64, NScheduler::EOperationState> stateCounts;
        TEnumIndexedVector<i64, NScheduler::EOperationType> typeCounts;
        i64 failedJobsCount = 0;

        if (options.IncludeArchive) {
            if (!options.FromTime) {
                THROW_ERROR_EXCEPTION("Missing required parameter \"from_time\"");
            }
            if (!options.ToTime) {
                THROW_ERROR_EXCEPTION("Missing required parameter \"to_time\"");
            }
        }

        TNullable<TString> substrFilter = options.SubstrFilter;

        if (options.CursorTime && (
            options.ToTime && *options.CursorTime > *options.ToTime ||
            options.FromTime && *options.CursorTime < *options.FromTime)) {
            THROW_ERROR_EXCEPTION("Time cursor is out of range");
        }

        if (substrFilter) {
            *substrFilter = to_lower(*substrFilter);
        }

        if (options.Limit > 100) {
            THROW_ERROR_EXCEPTION("Maximum result size exceedes allowed limit");
        }

        std::vector<TString> attributes = {
            "authenticated_user",
            "brief_progress",
            "brief_spec",
            "finish_time",
            "operation_type",
            "start_time",
            "state",
            "suspended",
            "title",
            "weight"
        };

        TObjectServiceProxy proxy(OperationsArchiveChannels_[options.ReadFrom]);
        auto batchReq = proxy.ExecuteBatch();
        SetBalancingHeader(batchReq, options);

        auto req = TYPathProxy::List(GetOperationsPath());
        SetCachingHeader(req, options);
        ToProto(req->mutable_attributes()->mutable_keys(), attributes);
        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspList>(0)
            .ValueOrThrow();

        auto items = ConvertToNode(TYsonString(rsp->value()))->AsList();

        std::vector<TOperation> cypressOperations;
        for (const auto& item : items->GetChildren()) {
            const auto& attributes = item->Attributes();

            if (item->AsString()->GetValue().Size() == 2) {
                continue;
            }

            TOperation operation;
            operation.OperationId = TGuid::FromString(item->AsString()->GetValue());
            operation.OperationType = ParseEnum<NScheduler::EOperationType>(attributes.Get<TString>("operation_type"));
            operation.OperationState = ParseEnum<NScheduler::EOperationState>(attributes.Get<TString>("state"));
            operation.StartTime = ConvertTo<TInstant>(attributes.Get<TString>("start_time"));
            if (attributes.Find<INodePtr>("finish_time")) {
                operation.FinishTime = ConvertTo<TInstant>(attributes.Get<TString>("finish_time"));
            }
            operation.AuthenticatedUser = attributes.Get<TString>("authenticated_user");
            operation.BriefSpec = attributes.FindYson("brief_spec");

            if (operation.BriefSpec) {
                auto briefSpecMapNode = ConvertToNode(operation.BriefSpec)->AsMap();
                auto poolNode = briefSpecMapNode->FindChild("pool");
                if (poolNode) {
                    operation.Pool = poolNode->AsString()->GetValue();
                }
            }

            operation.BriefProgress = attributes.FindYson("brief_progress");
            operation.Suspended = attributes.Get<bool>("suspended");
            operation.Weight = attributes.Find<double>("weight");
            cypressOperations.push_back(operation);
        }

        auto filterAndCount =
            [&] (const TNullable<TString>& pool, const TString& user, const EOperationState& state, const EOperationType& type, i64 count) {
                if (pool) {
                    poolCounts[*pool] += count;

                    if (options.Pool && *options.Pool != *pool) {
                        return false;
                    }
                }

                userCounts[user] += count;

                if (options.UserFilter && *options.UserFilter != user) {
                    return false;
                }

                stateCounts[state] += count;

                if (options.StateFilter && *options.StateFilter != state) {
                    return false;
                }

                typeCounts[type] += count;

                if (options.TypeFilter && *options.TypeFilter != type) {
                    return false;
                }

                return true;
            };

        std::vector<TOperation> cypressData;
        for (const auto& operation : cypressOperations) {
            if (options.FromTime && operation.StartTime < *options.FromTime ||
                options.ToTime && operation.StartTime >= *options.ToTime) {
                continue;
            }

            auto user = operation.AuthenticatedUser;

            EOperationState state;
            switch(operation.OperationState) {
                case EOperationState::Initializing:
                case EOperationState::Preparing:
                case EOperationState::Reviving:
                case EOperationState::Completing:
                case EOperationState::Aborting:
                case EOperationState::Failing:
                    state = EOperationState::Running;
                    break;
                default:
                    state = operation.OperationState;
            };

            auto type = operation.OperationType;

            auto textFactor = ExtractTextFactorForCypressItem(operation);
            if (substrFilter && textFactor.find(*substrFilter) == -1) {
                continue;
            }

            if (!filterAndCount(operation.Pool, user, state, type, 1)) {
                continue;
            }

            bool hasFailedJobs = false;
            if (operation.BriefProgress) {
                auto briefProgressMapNode = ConvertToNode(operation.BriefProgress)->AsMap();
                hasFailedJobs =
                    briefProgressMapNode->FindChild("jobs") &&
                    briefProgressMapNode->GetChild("jobs")->AsMap()->
                    GetChild("failed")->AsInt64()->GetValue() > 0;
            }

            failedJobsCount += hasFailedJobs;

            if (options.WithFailedJobs && *options.WithFailedJobs != (hasFailedJobs > 0)) {
                continue;
            }

            if (options.CursorTime) {
                if (options.CursorDirection == EOperationSortDirection::Past && operation.StartTime >= *options.CursorTime) {
                    continue;
                } else if (options.CursorDirection == EOperationSortDirection::Future && operation.StartTime <= *options.CursorTime) {
                    continue;
                }
            }

            cypressData.push_back(operation);
        }

        std::vector<TOperation> archiveData;

        if (options.IncludeArchive && IsArchiveExists()) {
            int version = DoGetOperationsArchiveVersion();

            if (options.Pool && version < 15) {
                THROW_ERROR_EXCEPTION("Failed to get operation's pool: operations archive version is too old: expected >= 15, got %v", version);
            }

            if (version < 9) {
                THROW_ERROR_EXCEPTION("Failed to get operation: operations archive version is too old: expected >= 9, got %v", version);
            }

            std::vector<TString> itemsConditions;
            std::vector<TString> countsConditions;
            TString itemsSortDirection;

            itemsConditions.push_back(Format("%v > %v AND %v <= %v", "start_time", (*options.FromTime).MicroSeconds(), "start_time", (*options.ToTime).MicroSeconds()));
            countsConditions.push_back(itemsConditions.back());

            if (options.SubstrFilter) {
                itemsConditions.push_back(Format("is_substr(%Qv, filter_factors)", substrFilter));
                countsConditions.push_back(itemsConditions.back());
            }

            if (options.CursorDirection == EOperationSortDirection::Past) {
                if (options.CursorTime) {
                    itemsConditions.push_back(Format("%v <= %v", "start_time", (*options.CursorTime).MicroSeconds()));
                }
                itemsSortDirection = "DESC";
            }

            if (options.CursorDirection == EOperationSortDirection::Future) {
                if (options.CursorTime) {
                    itemsConditions.push_back(Format("%v > %v", "start_time", (*options.CursorTime).MicroSeconds()));
                }
                itemsSortDirection = "ASC";
            }

            if (options.Pool) {
                itemsConditions.push_back(Format("pool = %Qv", *options.Pool));
            }

            if (options.StateFilter) {
                itemsConditions.push_back(Format("state = %Qv", FormatEnum(*options.StateFilter)));
            }

            if (options.TypeFilter) {
                itemsConditions.push_back(Format("operation_type = %Qv", FormatEnum(*options.TypeFilter)));
            }

            if (options.UserFilter) {
                itemsConditions.push_back(Format("authenticated_user = %Qv", *options.UserFilter));
            }

            TString queryForItemsIds = Format(
                "id_hi, id_lo FROM [%v] WHERE %v ORDER BY start_time %v LIMIT %v",
                GetOperationsArchivePathOrderedByStartTime(),
                JoinSeq(" AND ", itemsConditions),
                itemsSortDirection,
                1 + options.Limit);

            TString poolColumnName = version < 15 ? "''" : "pool";

            TString queryForCounts = Format(
                "pool, user, state, type, sum(1) AS count FROM [%v] WHERE %v GROUP BY %v AS pool, authenticated_user AS user, state AS state, operation_type AS type",
                GetOperationsArchivePathOrderedByStartTime(),
                JoinSeq(" AND ", countsConditions),
                poolColumnName);

            TSelectRowsOptions selectRowsOptions;
            if (deadline) {
                selectRowsOptions.Timeout = *deadline - Now();
            }

            auto runResultCounts = SelectRows(queryForCounts, selectRowsOptions);
            auto runRowsItemsId = SelectRows(queryForItemsIds, selectRowsOptions);

            auto resultCounts = WaitFor(runResultCounts)
                .ValueOrThrow();

            const auto& rowsCounts = resultCounts.Rowset->GetRows();

            for (auto row : rowsCounts) {
                auto pool = TString(row[0].Data.String, row[0].Length);
                auto user = TString(row[1].Data.String, row[1].Length);
                auto state = ParseEnum<EOperationState>(TString(row[2].Data.String, row[2].Length));
                auto type = ParseEnum<EOperationType>(TString(row[3].Data.String, row[3].Length));
                i64 count = row[4].Data.Int64;

                filterAndCount(pool, user, state, type, count);
            }

            auto nameTable = New<TNameTable>();

            TOrderedByIdTableDescriptor ids(nameTable);
            auto rowBuffer = New<TRowBuffer>();

            std::vector<TUnversionedRow> keys;

            auto rowsItemsId = WaitFor(runRowsItemsId)
                .ValueOrThrow();

            auto resultItemsIds = rowsItemsId.Rowset->GetRows();

            for (auto row : resultItemsIds) {
                auto key = rowBuffer->AllocateUnversioned(2);
                key[0] = MakeUnversionedUint64Value(row[0].Data.Uint64, ids.IdHi);
                key[1] = MakeUnversionedUint64Value(row[1].Data.Uint64, ids.IdLo);
                keys.push_back(key);
            }

            TLookupRowsOptions lookupOptions;
            lookupOptions.ColumnFilter = NTableClient::TColumnFilter({
                ids.IdHi,
                ids.IdLo,
                ids.OperationType,
                ids.State,
                ids.AuthenticatedUser,
                ids.BriefProgress,
                ids.BriefSpec,
                ids.StartTime,
                ids.FinishTime,
            });
            lookupOptions.KeepMissingRows = true;

            if (deadline) {
                lookupOptions.Timeout = *deadline - Now();
            }

            auto rowset = WaitFor(LookupRows(
                GetOperationsArchivePathOrderedById(),
                nameTable,
                MakeSharedRange(std::move(keys), std::move(rowBuffer)),
                lookupOptions))
                .ValueOrThrow();

            auto rows = rowset->GetRows();

            auto checkIsNotNull = [&] (const TUnversionedValue& value, const TStringBuf& name) {
                if (value.Type == EValueType::Null) {
                    THROW_ERROR_EXCEPTION("Unexpected null value in column %Qv in job archive", name);
                }
            };

            auto checkWithFailedJobsFilter =
                [&options] (bool hasFailedJobs) {
                    if (options.WithFailedJobs) {
                        if (*options.WithFailedJobs) {
                            return hasFailedJobs;
                        } else {
                            return !hasFailedJobs;
                        }
                    }

                    return true;
                };

            for (auto row : rows) {
                TOperation operation;

                checkIsNotNull(row[5], "brief_progress");
                operation.BriefProgress = TYsonString(row[5].Data.String, row[5].Length);

                auto briefProgressMapNode = ConvertToNode(operation.BriefProgress)->AsMap();
                bool hasFailedJobs =
                    briefProgressMapNode->FindChild("jobs") &&
                    briefProgressMapNode->GetChild("jobs")->AsMap()->
                    GetChild("failed")->AsInt64()->GetValue() > 0;

                if (!checkWithFailedJobsFilter(hasFailedJobs)) {
                    continue;
                }

                TGuid operationId(row[0].Data.Uint64, row[1].Data.Uint64);
                operation.OperationId = operationId;

                checkIsNotNull(row[2], "operation_type");
                operation.OperationType = ParseEnum<EOperationType>(TString(row[2].Data.String, row[2].Length));

                checkIsNotNull(row[3], "state");
                operation.OperationState = ParseEnum<EOperationState>(TString(row[3].Data.String, row[3].Length));

                checkIsNotNull(row[4], "authenticated_user");
                operation.AuthenticatedUser = TString(row[4].Data.String, row[4].Length);

                failedJobsCount += hasFailedJobs;

                checkIsNotNull(row[6], "brief_spec");
                operation.BriefSpec = TYsonString(row[6].Data.String, row[6].Length);

                checkIsNotNull(row[7], "start_time");
                operation.StartTime = TInstant(row[7].Data.Int64);

                if (row[8].Type != EValueType::Null) {
                    operation.FinishTime = TInstant(row[8].Data.Int64);
                }

                archiveData.push_back(operation);
            }
        }

        std::sort(
            cypressData.begin(),
            cypressData.end(),
            [] (const TOperation& lhs, const TOperation& rhs) {
                return lhs.OperationId < rhs.OperationId;
            });

        auto mergeOperations = [&] (const std::vector<TOperation>& source1, const std::vector<TOperation>& source2) {
            auto it1 = source1.begin();
            auto end1 = source1.end();

            auto it2 = source2.begin();
            auto end2 = source2.end();

            std::vector<TOperation> result;
            while (it1 != end1 && it2 != end2) {
                if (it1->OperationId == it2->OperationId) {
                    result.push_back(*it1);

                    filterAndCount(it1->Pool, it1->AuthenticatedUser, it1->OperationState, it1->OperationType, -1);

                    ++it1;
                    ++it2;
                } else if (it1->OperationId < it2->OperationId) {
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

        std::vector<TOperation> mergedData = mergeOperations(cypressData, archiveData);

        auto startTimeComparer = [] (const EOperationSortDirection& direction) -> std::function<bool (const TOperation&, const TOperation&)> {
            if (direction == EOperationSortDirection::Future) {
                return [] (const TOperation& lhs, const TOperation& rhs) { return static_cast<bool>(lhs.StartTime < rhs.StartTime); };
            } else {
                return [] (const TOperation& lhs, const TOperation& rhs) { return static_cast<bool>(lhs.StartTime > rhs.StartTime); };
            }
        };

        std::sort(mergedData.begin(), mergedData.end(), startTimeComparer(options.CursorDirection));

        if (mergedData.size() > options.Limit) {
            result.Incomplete = true;
        }

        result.Operations = std::move(mergedData);
        if (result.Operations.size() >= options.Limit) {
            result.Operations.resize(options.Limit);
        }

        std::sort(result.Operations.begin(), result.Operations.end(), startTimeComparer(EOperationSortDirection::Past));

        if (options.IncludeCounters) {
            result.PoolCounts = std::move(poolCounts);
            result.UserCounts = std::move(userCounts);
            result.StateCounts = std::move(stateCounts);
            result.TypeCounts = std::move(typeCounts);
            result.FailedJobsCount = failedJobsCount;
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
        int Limit_ = -1;
    };

    TFuture<std::pair<std::vector<TJob>, int>> DoListJobsFromArchive(
        const TOperationId& operationId,
        TNullable<TInstant> deadline,
        const TListJobsOptions& options)
    {
        std::vector<TJob> jobs;

        int archiveVersion = DoGetOperationsArchiveVersion();

        TQueryBuilder builder;

        builder.SetSource(GetOperationsArchiveJobsPath());
        builder.SetLimit(options.Offset + options.Limit);

        auto jobIdHiIndex = builder.AddSelectExpression("job_id_hi");
        auto jobIdLoIndex = builder.AddSelectExpression("job_id_lo");
        auto typeIndex = builder.AddSelectExpression("type AS job_type");
        auto stateIndex = archiveVersion >= 16
            ? builder.AddSelectExpression("if(is_null(state), transient_state, state) AS job_state")
            : builder.AddSelectExpression("state AS job_state");
        auto startTimeIndex = builder.AddSelectExpression("start_time");
        auto finishTimeIndex = builder.AddSelectExpression("finish_time");
        auto addressIndex = builder.AddSelectExpression("address");
        auto errorIndex = builder.AddSelectExpression("error");
        auto statisticsIndex = builder.AddSelectExpression("statistics");
        auto stderrSizeIndex = builder.AddSelectExpression("stderr_size");

        auto operationIdExpression = Format(
            "(operation_id_hi, operation_id_lo) = (%vu, %vu)",
            operationId.Parts64[0],
            operationId.Parts64[1]);
        builder.AddWhereExpression(operationIdExpression);

        if (options.WithStderr) {
            if (*options.WithStderr) {
                builder.AddWhereExpression("(stderr_size != 0 AND NOT is_null(stderr_size))");
            } else {
                builder.AddWhereExpression("(stderr_size = 0 OR is_null(stderr_size))");
            }
        }

        // TODO(ignat): remove this code? (since we sort result later in DoListJobs implementation)
        if (options.SortField != EJobSortField::None) {
            switch (options.SortField) {
                case EJobSortField::Type:
                    builder.SetOrderByExpression("job_type");
                    break;
                case EJobSortField::State:
                    builder.SetOrderByExpression("job_state");
                    break;
                case EJobSortField::StartTime:
                    builder.SetOrderByExpression("start_time");
                    break;
                case EJobSortField::FinishTime:
                    builder.SetOrderByExpression("finish_time");
                    break;
                case EJobSortField::Address:
                    builder.SetOrderByExpression("address");
                    break;
                case EJobSortField::Duration:
                    builder.SetOrderByExpression(Format(
                        "if(is_null(finish_time), %v, finish_time) - start_time",
                        TInstant::Now().MicroSeconds()));
                    break;
                // XXX: progress is not presented in archive table.
                //case EJobSortField::Progress:
                //    builder.SetOrderByExpression("progress");
                //    break;
                // TODO: sort by string representation.
                //case EJobSortField::Id:
                //    builder.SetOrderByExpression("job_id_hi, job_id_lo");
                //    break;
                default:
                    break;
            }
            switch (options.SortOrder) {
                case EJobSortDirection::Ascending:
                    builder.SetOrderByDirection("ASC");
                    break;
                case EJobSortDirection::Descending:
                    builder.SetOrderByDirection("DESC");
                    break;
            }
        }

        auto itemsQuery = builder.Build();
        auto countQuery = Format(
            "SUM(1) AS count FROM [%v] WHERE %v GROUP BY 1",
            GetOperationsArchiveJobsPath(),
            operationIdExpression);

        TSelectRowsOptions selectRowsOptions;
        selectRowsOptions.Timestamp = AsyncLastCommittedTimestamp;
        if (deadline) {
            selectRowsOptions.Timeout = *deadline - Now();
        }

        auto itemsFuture = SelectRows(itemsQuery, selectRowsOptions).Apply(BIND([=] (const TSelectRowsResult& result) {
            std::vector<TJob> jobs;

            auto rows = result.Rowset->GetRows();

            auto checkIsNotNull = [&] (const TUnversionedValue& value, const TStringBuf& name, const TGuid& jobId) {
                if (value.Type == EValueType::Null) {
                    THROW_ERROR_EXCEPTION("Unexpected null value in column %Qv in job archive", name)
                        << TErrorAttribute("operation_id", operationId)
                        << TErrorAttribute("job_id", jobId);
                }
            };

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
                    job.StartTime = TInstant(row[startTimeIndex].Data.Int64);
                }

                if (row[finishTimeIndex].Type != EValueType::Null) {
                    job.FinishTime = TInstant(row[finishTimeIndex].Data.Int64);
                }

                if (row[addressIndex].Type != EValueType::Null) {
                    job.Address = TString(row[addressIndex].Data.String, row[addressIndex].Length);
                }

                if (row[stderrSizeIndex].Type != EValueType::Null) {
                    job.StderrSize = row[stderrSizeIndex].Data.Int64;
                }

                if (row[errorIndex].Type != EValueType::Null) {
                    job.Error = TYsonString(TString(row[errorIndex].Data.String, row[errorIndex].Length));
                }

                if (row[statisticsIndex].Type != EValueType::Null) {
                    auto briefStatisticsYson = TYsonString(TString(row[statisticsIndex].Data.String, row[statisticsIndex].Length));
                    auto briefStatistics = ConvertToNode(briefStatisticsYson);

                    // See BuildBriefStatistics.
                    auto rowCount = GetNodeByYPath(briefStatistics, "/data/input/row_count/sum");
                    auto uncompressedDataSize = GetNodeByYPath(briefStatistics, "/data/input/uncompressed_data_size/sum");
                    auto compressedDataSize = GetNodeByYPath(briefStatistics, "/data/input/compressed_data_size/sum");
                    auto dataWeight = GetNodeByYPath(briefStatistics, "/data/input/data_weight/sum");
                    auto inputPipeIdleTime = FindNodeByYPath(briefStatistics, "/user_job/pipes/input/idle_time/sum");
                    auto jobProxyCpuUsage = FindNodeByYPath(briefStatistics, "/job_proxy/cpu/user/sum");

                    job.BriefStatistics = BuildYsonStringFluently()
                        .BeginAttributes()
                        .EndAttributes()
                        .BeginMap()
                            .Item("processed_input_row_count").Value(rowCount->AsInt64()->GetValue())
                            .Item("processed_input_uncompressed_data_size").Value(uncompressedDataSize->AsInt64()->GetValue())
                            .Item("processed_input_compressed_data_size").Value(compressedDataSize->AsInt64()->GetValue())
                            .Item("processed_input_data_weight").Value(dataWeight->AsInt64()->GetValue())
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

        auto countFuture = SelectRows(countQuery, selectRowsOptions).Apply(BIND([=] (const TSelectRowsResult& result) {
            auto rows = result.Rowset->GetRows();
            if (rows.Size() == 0) {
                return 0;
            }
            YCHECK(rows.Size() == 1);
            YCHECK(rows[0][0].Type == EValueType::Int64);
            return static_cast<int>(rows[0][0].Data.Int64);
        }));

        return
            CombineAll(std::vector<TFuture<void>>{itemsFuture.As<void>(), countFuture.As<void>()})
            .Apply(BIND([itemsFuture, countFuture] (const std::vector<TError>&) {
                const auto& items = itemsFuture.Get();
                const auto& count = countFuture.Get();
                if (!items.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to get jobs from the operation archive") << items;
                }
                if (!count.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to get job count from the operation archive") << count;
                }
                return std::make_pair(items.Value(), count.Value());
            }));
    }

    TFuture<std::pair<std::vector<TJob>, int>> DoListJobsFromCypress(
        const TOperationId& operationId,
        TNullable<TInstant> deadline,
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

        if (deadline) {
            proxy.SetDefaultTimeout(*deadline - Now());
        }

        auto batchRequestFuture = batchReq->Invoke();

        return batchRequestFuture.Apply(BIND([options] (
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

                if (options.WithStderr) {
                    if (*options.WithStderr && stderrSize <= 0) {
                        continue;
                    }
                    if (!(*options.WithStderr) && stderrSize > 0) {
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
                job.Error = attributes.FindYson("error");
                job.BriefStatistics = attributes.FindYson("brief_statistics");
                job.InputPaths = attributes.FindYson("input_paths");
                job.CoreInfos = attributes.FindYson("core_infos");
            }

            return result;
        }));
    }

    TFuture<std::pair<std::vector<TJob>, int>> DoListJobsFromScheduler(
        const TOperationId& operationId,
        TNullable<TInstant> deadline,
        const TListJobsOptions& options)
    {
        TObjectServiceProxy proxy(GetMasterChannelOrThrow(EMasterChannelKind::Follower));

        auto path = Format("//sys/scheduler/orchid/scheduler/operations/%v/running_jobs", operationId);
        auto getReq = TYPathProxy::Get(path);

        if (deadline) {
            proxy.SetDefaultTimeout(*deadline - Now());
        }

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
                if (options.WithStderr) {
                    if (*options.WithStderr && stderrSize <= 0) {
                        continue;
                    }
                    if (!(*options.WithStderr) && stderrSize > 0) {
                        continue;
                    }
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


    TListJobsResult DoListJobs(
        const TOperationId& operationId,
        const TListJobsOptions& options)
    {
        TListJobsResult result;

        TNullable<TInstant> deadline;
        if (options.Timeout) {
            deadline = options.Timeout->ToDeadLine();
        }

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

        auto updateResultJobs = [&] (const std::vector<TJob>& delta) {
            auto sortedDelta = delta;
            std::sort(
                sortedDelta.begin(),
                sortedDelta.end(),
                [] (const TJob& lhs, const TJob& rhs) {
                    return lhs.Id < rhs.Id;
                });
            result.Jobs = mergeJobs(result.Jobs, sortedDelta);
        };

        TFuture<std::pair<std::vector<TJob>, int>> archiveJobsFuture, cypressJobsFuture, schedulerJobsFuture;

        bool isArchiveExists = IsArchiveExists();

        // Issue the requests in parallel.
        if (options.IncludeArchive && isArchiveExists) {
            archiveJobsFuture = DoListJobsFromArchive(operationId, deadline, options);
        }

        if (options.IncludeCypress) {
            cypressJobsFuture = DoListJobsFromCypress(operationId, deadline, options);
        }

        if (options.IncludeScheduler) {
            schedulerJobsFuture = DoListJobsFromScheduler(operationId, deadline, options);
        }

        if (options.IncludeArchive && isArchiveExists) {
            auto archiveJobsOrError = WaitFor(archiveJobsFuture);
            if (!archiveJobsOrError.IsOK()) {
                THROW_ERROR_EXCEPTION(EErrorCode::JobArchiveUnavailable, "Job archive is unavailable")
                    << archiveJobsOrError;
            }

            auto archiveJobs = archiveJobsOrError.Value();
            result.ArchiveJobCount = archiveJobs.second;
            updateResultJobs(archiveJobs.first);
        }

        if (options.IncludeCypress) {
            auto cypressJobsOrError = WaitFor(cypressJobsFuture);
            if (cypressJobsOrError.IsOK()) {
                const auto& pair = cypressJobsOrError.Value();
                result.CypressJobCount = pair.second;
                updateResultJobs(pair.first);
            } else if (cypressJobsOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                // No such operation in Cypress.
                result.CypressJobCount = 0;
            } else {
                cypressJobsOrError.ThrowOnError();
            }
        }

        if (options.IncludeScheduler) {
            auto schedulerJobsOrError = WaitFor(schedulerJobsFuture);
            if (schedulerJobsOrError.IsOK()) {
                const auto& pair = schedulerJobsOrError.Value();
                result.SchedulerJobCount = pair.second;
                updateResultJobs(pair.first);
            } else if (schedulerJobsOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                // No such operation in the scheduler.
                result.SchedulerJobCount = 0;
            } else {
                schedulerJobsOrError.ThrowOnError();
            }
        }

        std::vector<TJob> filteredJobs;
        for (const auto& job : result.Jobs) {
            result.AddressCounts[job.Address] += 1;
            if (options.Address && job.Address != *options.Address) {
                continue;
            }

            result.TypeCounts[job.Type] += 1;
            if (options.Type && job.Type != *options.Type) {
                continue;
            }

            result.StateCounts[job.State] += 1;
            if (options.State && job.State != *options.State) {
                continue;
            }

            filteredJobs.push_back(job);
        }

        result.Jobs = filteredJobs;

        std::function<bool(const TJob&, const TJob&)> comparer;
        switch (options.SortField) {
#define XX(field) \
            case EJobSortField::field: \
                switch (options.SortOrder) { \
                    case EJobSortDirection::Ascending: \
                        comparer = [] (const TJob& lhs, const TJob& rhs) { \
                            return LessNullable(lhs.field, rhs.field); \
                        }; \
                        break; \
                    case EJobSortDirection::Descending: \
                        comparer = [&] (const TJob& lhs, const TJob& rhs) { \
                            return LessNullable(rhs.field, lhs.field); \
                        }; \
                        break; \
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
                switch (options.SortOrder) {
                    case EJobSortDirection::Ascending:
                        comparer = [] (const TJob& lhs, const TJob& rhs) {
                            return lhs.Id < rhs.Id;
                        };
                        break;
                    case EJobSortDirection::Descending:
                        comparer = [] (const TJob& lhs, const TJob& rhs) {
                            return !(lhs.Id < rhs.Id || lhs.Id == rhs.Id);
                        };
                        break;
                }
                break;

            case EJobSortField::Duration:
                switch (options.SortOrder) {
                    case EJobSortDirection::Ascending:
                        comparer = [now = TInstant::Now()] (const TJob& lhs, const TJob& rhs) {
                            auto lhsDuration = (lhs.FinishTime ? *lhs.FinishTime : now) - lhs.StartTime;
                            auto rhsDuration = (rhs.FinishTime ? *rhs.FinishTime : now) - rhs.StartTime;
                            return lhsDuration < rhsDuration;
                        };
                        break;
                    case EJobSortDirection::Descending:
                        comparer = [now = TInstant::Now()] (const TJob& lhs, const TJob& rhs) {
                            auto lhsDuration = (lhs.FinishTime ? *lhs.FinishTime : now) - lhs.StartTime;
                            auto rhsDuration = (rhs.FinishTime ? *rhs.FinishTime : now) - rhs.StartTime;
                            return lhsDuration > rhsDuration;
                        };
                        break;
                }
                break;

            case EJobSortField::Id:
                switch (options.SortOrder) {
                    case EJobSortDirection::Ascending:
                        comparer = [] (const TJob& lhs, const TJob& rhs) {
                            return ToString(lhs.Id) < ToString(rhs.Id);
                        };
                        break;
                    case EJobSortDirection::Descending:
                        comparer = [] (const TJob& lhs, const TJob& rhs) {
                            return ToString(lhs.Id) > ToString(rhs.Id);
                        };
                        break;
                }
                break;

            default:
                Y_UNREACHABLE();
        }

        std::sort(result.Jobs.begin(), result.Jobs.end(), comparer);

        auto beginIt = result.Jobs.begin() + std::min(options.Offset,
            std::distance(result.Jobs.begin(), result.Jobs.end()));

        auto endIt = beginIt + std::min(options.Limit,
            std::distance(beginIt, result.Jobs.end()));

        result.Jobs = std::vector<TJob>(beginIt, endIt);

        return result;
    }

    TYsonString DoGetJob(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobOptions& options)
    {
        TNullable<TInstant> deadline;
        if (options.Timeout) {
            deadline = options.Timeout->ToDeadLine();
        }

        int archiveVersion = DoGetOperationsArchiveVersion();

        TJobTableDescriptor table;
        auto rowBuffer = New<TRowBuffer>();

        std::vector<TUnversionedRow> keys;
        auto key = rowBuffer->AllocateUnversioned(4);
        key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], table.Ids.OperationIdHi);
        key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], table.Ids.OperationIdLo);
        key[2] = MakeUnversionedUint64Value(jobId.Parts64[0], table.Ids.JobIdHi);
        key[3] = MakeUnversionedUint64Value(jobId.Parts64[1], table.Ids.JobIdLo);
        keys.push_back(key);

        TLookupRowsOptions lookupOptions;

        THashSet<TString> fields = {
            "operation_id_hi",
            "operation_id_lo",
            "job_id_hi",
            "job_id_lo",
            "type",
            "state",
            "start_time",
            "finish_time",
            "address",
            "error",
            "statistics",
            "events"
        };

        if (archiveVersion >= 16) {
            fields.insert("transient_state");
        }

        std::vector<int> columnIndexes;
        THashMap<TString, int> fieldToIndex;

        int index = 0;
        for (const auto& field : fields) {
            columnIndexes.push_back(table.NameTable->GetIdOrThrow(field));
            fieldToIndex[field] = index++;
        }

        lookupOptions.ColumnFilter = NTableClient::TColumnFilter(columnIndexes);
        lookupOptions.KeepMissingRows = true;
        if (deadline) {
            lookupOptions.Timeout = *deadline - Now();
        }

        auto rowset = WaitFor(LookupRows(
            GetOperationsArchiveJobsPath(),
            table.NameTable,
            MakeSharedRange(std::move(keys), std::move(rowBuffer)),
            lookupOptions))
            .ValueOrThrow();

        auto rows = rowset->GetRows();
        YCHECK(!rows.Empty());

        if (rows[0]) {
#define SET_ITEM_STRING_VALUE(itemKey) \
            SET_ITEM_VALUE(itemKey, TString(rows[0][index].Data.String, rows[0][index].Length))
#define SET_ITEM_YSON_STRING_VALUE(itemKey) \
            SET_ITEM_VALUE(itemKey, TYsonString(rows[0][index].Data.String, rows[0][index].Length))
#define SET_ITEM_INSTANT_VALUE(itemKey) \
            SET_ITEM_VALUE(itemKey, TInstant(rows[0][index].Data.Int64))
#define SET_ITEM_VALUE(itemKey, operation) \
            .DoIf(fields.find(itemKey) != fields.end() && rows[0][GET_INDEX(itemKey)].Type != EValueType::Null, [&] (TFluentMap fluent) { \
                auto index = GET_INDEX(itemKey); \
                fluent.Item(itemKey).Value(operation); \
            })
#define GET_INDEX(itemKey) fieldToIndex.find(itemKey)->second

            auto resultOperationId = TGuid(
                rows[0][GET_INDEX("operation_id_hi")].Data.Uint64,
                rows[0][GET_INDEX("operation_id_lo")].Data.Uint64);
            auto resultJobId = TGuid(
                rows[0][GET_INDEX("job_id_hi")].Data.Uint64,
                rows[0][GET_INDEX("job_id_lo")].Data.Uint64);
            auto resultState = TString();
            if (resultState.empty()) {
                auto index = GET_INDEX("state");
                if (rows[0][index].Type != EValueType::Null) {
                    resultState = TString(rows[0][index].Data.String, rows[0][index].Length);
                }
            }
            if (resultState.empty() && archiveVersion >= 16) {
                auto index = GET_INDEX("transient_state");
                if (rows[0][index].Type != EValueType::Null) {
                    resultState = TString(rows[0][index].Data.String, rows[0][index].Length);
                }
            }
            auto ysonResult = BuildYsonStringFluently()
                .BeginMap()
                    .Item("operation_id").Value(resultOperationId)
                    .Item("job_id").Value(resultJobId)
                    .DoIf(!resultState.empty(), [&] (TFluentMap fluent) {
                        fluent.Item("state").Value(resultState);
                    })
                    SET_ITEM_INSTANT_VALUE("start_time")
                    SET_ITEM_INSTANT_VALUE("finish_time")
                    SET_ITEM_STRING_VALUE("address")
                    SET_ITEM_YSON_STRING_VALUE("error")
                    SET_ITEM_YSON_STRING_VALUE("statistics")
                    SET_ITEM_YSON_STRING_VALUE("events")
                .EndMap();
#undef SET_ITEM_STRING_VALUE
#undef SET_ITEM_YSON_STRING_VALUE
#undef SET_ITEM_INSTANT_VALUE
#undef SET_ITEM_VALUE
#undef GET_INDEX
            return ysonResult;
        }

        THROW_ERROR_EXCEPTION("No such job %v", jobId);
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

DEFINE_REFCOUNTED_TYPE(TNativeClient)

INativeClientPtr CreateNativeClient(
    INativeConnectionPtr connection,
    const TClientOptions& options)
{
    YCHECK(connection);

    return New<TNativeClient>(std::move(connection), options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
