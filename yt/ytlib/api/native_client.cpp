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

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_replica.h>
#include <yt/ytlib/chunk_client/read_limit.h>
#include <yt/ytlib/chunk_client/chunk_teleporter.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/medium_directory.pb.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/cluster_directory.h>
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

#include <yt/ytlib/scheduler/helpers.h>
#include <yt/ytlib/scheduler/job.pb.h>
#include <yt/ytlib/scheduler/job_prober_service_proxy.h>
#include <yt/ytlib/scheduler/scheduler_service_proxy.h>

#include <yt/ytlib/security_client/group_ypath_proxy.h>
#include <yt/ytlib/security_client/helpers.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schema.h>
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
#include <yt/ytlib/transaction_client/action.h>
#include <yt/ytlib/transaction_client/transaction_service_proxy.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/async_stream_pipe.h>
#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/profiling/scoped_timer.h>

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

TNameTableToSchemaIdMapping BuildColumnIdMapping(
    const TTableSchema& schema,
    const TNameTablePtr& nameTable)
{
    for (const auto& name : schema.GetKeyColumns()) {
        // We shouldn't consider computed columns below because client doesn't send them.
        if (!nameTable->FindId(name) && !schema.GetColumnOrThrow(name).Expression) {
            THROW_ERROR_EXCEPTION("Missing key column %Qv",
                name);
        }
    }

    TNameTableToSchemaIdMapping mapping;
    mapping.resize(nameTable->GetSize());
    for (int nameTableId = 0; nameTableId < nameTable->GetSize(); ++nameTableId) {
        const auto& name = nameTable->GetName(nameTableId);
        const auto* columnSchema = schema.FindColumn(name);
        mapping[nameTableId] = columnSchema ? schema.GetColumnIndex(*columnSchema) : -1;
    }
    return mapping;
}

template <class TKey>
TTabletInfoPtr GetSortedTabletForRow(
    const TTableMountInfoPtr& tableInfo,
    TKey key)
{
    Y_ASSERT(tableInfo->IsSorted());

    auto tabletInfo = tableInfo->GetTabletForRow(key);
    ValidateTabletMounted(tableInfo, tabletInfo);
    return tabletInfo;
}

TTabletInfoPtr GetOrderedTabletForRow(
    const TTableMountInfoPtr& tableInfo,
    const TTabletInfoPtr& randomTabletInfo,
    TNullable<int> tabletIndexColumnId,
    NTableClient::TKey key)
{
    Y_ASSERT(!tableInfo->IsSorted());

    int tabletIndex = -1;
    for (const auto& value : key) {
        if (tabletIndexColumnId && value.Id == *tabletIndexColumnId) {
            Y_ASSERT(value.Type == EValueType::Null || value.Type == EValueType::Int64);
            if (value.Type == EValueType::Int64) {
                tabletIndex = value.Data.Int64;
                if (tabletIndex < 0 || tabletIndex >= tableInfo->Tablets.size()) {
                    THROW_ERROR_EXCEPTION("Invalid tablet index: actual %v, expected in range [0, %v]",
                        tabletIndex,
                        tableInfo->Tablets.size() - 1);
                }
            }
        }
    }

    if (tabletIndex < 0) {
        return randomTabletInfo;
    }

    auto tabletInfo = tableInfo->Tablets[tabletIndex];
    ValidateTabletMounted(tableInfo, tabletInfo);
    return tabletInfo;
}

TUnversionedOwningRow CreateOperationJobKey(const TOperationId& operationId, const TJobId& jobId, const TNameTablePtr& nameTable)
{
    TOwningRowBuilder keyBuilder(4);

    keyBuilder.AddValue(MakeUnversionedUint64Value(operationId.Parts64[0], nameTable->GetIdOrRegisterName("operation_id_hi")));
    keyBuilder.AddValue(MakeUnversionedUint64Value(operationId.Parts64[1], nameTable->GetIdOrRegisterName("operation_id_lo")));
    keyBuilder.AddValue(MakeUnversionedUint64Value(jobId.Parts64[0], nameTable->GetIdOrRegisterName("job_id_hi")));
    keyBuilder.AddValue(MakeUnversionedUint64Value(jobId.Parts64[1], nameTable->GetIdOrRegisterName("job_id_lo")));

    return keyBuilder.FinishRow();
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
    TJobInputReader(NJobProxy::TUserJobReadControllerPtr userJobReadController, IInvokerPtr invoker)
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
    const NJobProxy::TUserJobReadControllerPtr UserJobReadController_;
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

    virtual TFuture<std::vector<TDataSplit>> GetInitialSplits(
        const std::vector<TRichYPath>& paths,
        TTimestamp timestamp) override
    {
        return BIND(&TQueryPreparer::DoGetInitialSplits, MakeStrong(this))
            .AsyncVia(Connection_->GetLightInvoker())
            .Run(paths, timestamp);
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

    std::vector<TDataSplit> DoGetInitialSplits(
        const std::vector<TRichYPath>& paths,
        TTimestamp timestamp)
    {
        const auto& tableMountCache = Connection_->GetTableMountCache();

        std::vector<TFuture<TTableMountInfoPtr>> asyncTableInfos;
        for (const auto& path : paths) {
            asyncTableInfos.push_back(tableMountCache->GetTableInfo(path.GetPath()));
        }

        auto tableInfos = WaitFor(Combine(asyncTableInfos))
            .ValueOrThrow();

        std::vector<TDataSplit> splits;
        for (size_t index = 0; index < paths.size(); ++index) {
            const auto& path = paths[index];
            const auto& tableInfo = tableInfos[index];

            tableInfo->ValidateNotReplicated();

            TDataSplit split;
            SetObjectId(&split, tableInfo->TableId);
            SetTableSchema(&split, GetTableSchema(path, tableInfo));
            SetTimestamp(&split, timestamp);
            splits.push_back(split);
        }
        return splits;
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

        LightChannelFactory_ = CreateNodeChannelFactory(
            wrapChannelFactory(Connection_->GetLightChannelFactory()),
            Connection_->GetNetworks());
        HeavyChannelFactory_ = CreateNodeChannelFactory(
            wrapChannelFactory(Connection_->GetHeavyChannelFactory()),
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
            Connection_->GetLightInvoker());

        Logger.AddTag("ClientId: %v", TGuid::Create());
    }


    virtual IConnectionPtr GetConnection() override
    {
        return Connection_;
    }

    virtual INativeConnectionPtr GetNativeConnection() override
    {
        return Connection_;
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

    virtual IChannelPtr GetSchedulerChannel() override
    {
        return SchedulerChannel_;
    }

    virtual INodeChannelFactoryPtr GetLightChannelFactory() override
    {
        return LightChannelFactory_;
    }

    virtual INodeChannelFactoryPtr GetHeavyChannelFactory() override
    {
        return HeavyChannelFactory_;
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
        const TTransactionStartOptions& options) override;
    virtual INativeTransactionPtr AttachNativeTransaction(
        const TTransactionId& transactionId,
        const TTransactionAttachOptions& options) override;

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
        return NApi::CreateTableReader(this, path, options);
    }

    virtual TFuture<NTableClient::ISchemalessWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const NApi::TTableWriterOptions& options) override
    {
        return NApi::CreateTableWriter(this, path, options);
    }

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

    IMPLEMENT_METHOD(void, DumpJobContext, (
        const TJobId& jobId,
        const TYPath& path,
        const TDumpJobContextOptions& options),
        (jobId, path, options))
    IMPLEMENT_METHOD(IAsyncZeroCopyInputStreamPtr, GetJobInput, (
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobInputOptions& options),
        (operationId, jobId, options))
    IMPLEMENT_METHOD(TSharedRef, GetJobStderr, (
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobStderrOptions& options),
        (operationId, jobId, options))
    IMPLEMENT_METHOD(std::vector<TJob>, ListJobs, (
        const TOperationId& operationId,
        const TListJobsOptions& options),
        (operationId, options))
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

    TEnumIndexedVector<yhash<TCellTag, IChannelPtr>, EMasterChannelKind> MasterChannels_;
    IChannelPtr SchedulerChannel_;
    INodeChannelFactoryPtr LightChannelFactory_;
    INodeChannelFactoryPtr HeavyChannelFactory_;
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

        // XXX(sandello): Deprecate me in 19.x ; remove two separate thread pools, use just one.
        auto invoker = Connection_->GetLightInvoker();
        if (commandName == "SelectRows" || commandName == "LookupRows" || commandName == "VersionedLookupRows" ||
            commandName == "GetJobStderr") {
            invoker = Connection_->GetHeavyInvoker();
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
            .AsyncVia(Connection_->GetLightInvoker())
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

            auto config = Connection_->GetConfig();
            if (++retryCount <= config->TableMountCache->OnErrorRetryCount) {
                bool retry = false;
                std::vector<NTabletClient::EErrorCode> retriableCodes = {
                    NTabletClient::EErrorCode::NoSuchTablet,
                    NTabletClient::EErrorCode::TabletNotMounted,
                    NTabletClient::EErrorCode::InvalidMountRevision};

                for (const auto& errCode : retriableCodes) {
                    if (auto err = error.FindMatching(errCode)) {
                        error = err.Get();
                        retry = true;
                        break;
                    }
                }

                if (retry) {
                    LOG_DEBUG(error, "Got error, will clear table mount cache and retry");
                    auto tabletId = error.Attributes().Get<TTabletId>("tablet_id");
                    const auto& tableMountCache = Connection_->GetTableMountCache();
                    auto tabletInfo = tableMountCache->FindTablet(tabletId);
                    if (tabletInfo) {
                        tableMountCache->InvalidateTablet(tabletInfo);
                        auto now = Now();
                        auto retryTime = tabletInfo->UpdateTime + config->TableMountCache->OnErrorSlackPeriod;
                        if (retryTime > now) {
                            WaitFor(TDelayedExecutor::MakeDelayed(retryTime - now))
                                .ThrowOnError();
                        }
                    }
                    continue;
                }
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
            cachingHeaderExt->set_success_expiration_time(ToProto(options.ExpireAfterSuccessfulUpdateTime));
            cachingHeaderExt->set_failure_expiration_time(ToProto(options.ExpireAfterFailedUpdateTime));
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

    IChannelPtr GetCellChannelOrThrow(const TTabletCellId& cellId) const
    {
        const auto& cellDirectory = Connection_->GetCellDirectory();
        auto channel = cellDirectory->GetChannelOrThrow(cellId);
        return CreateAuthenticatedChannel(std::move(channel), Options_.User);
    }

    IChannelPtr GetReadCellChannelOrThrow(const TTabletCellId& cellId) const
    {
        const auto& cellDirectory = Connection_->GetCellDirectory();
        const auto& cellDescriptor = cellDirectory->GetDescriptorOrThrow(cellId);
        const auto& primaryPeerDescriptor = GetPrimaryTabletPeerDescriptor(cellDescriptor, EPeerKind::Leader);
        return HeavyChannelFactory_->CreateChannel(primaryPeerDescriptor.GetAddress(Connection_->GetNetworks()));
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
            const TTabletReadOptions& options,
            const TNullable<TDuration>& timeout,
            TTableMountInfoPtr tableInfo,
            TEncoder encoder,
            TDecoder decoder)
            : Config_(std::move(config))
            , Networks_(networks)
            , CellId_(cellId)
            , Options_(options)
            , Timeout_(timeout)
            , TableInfo_(std::move(tableInfo))
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
            InvokeProxy_->SetDefaultTimeout(Timeout_);
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
        const TTabletReadOptions Options_;
        const TNullable<TDuration> Timeout_;
        const TTableMountInfoPtr TableInfo_;
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
            ToProto(req->mutable_tablet_id(), batch->TabletInfo->TabletId);
            req->set_mount_revision(batch->TabletInfo->MountRevision);
            req->set_timestamp(Options_.Timestamp);
            req->set_request_codec(static_cast<int>(Config_->LookupRequestCodec));
            req->set_response_codec(static_cast<int>(Config_->LookupResponseCodec));
            req->Attachments().push_back(batch->RequestData);

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
    using TEncoderWithMapping = std::function<
        std::vector<TSharedRef>(const NTableClient::TColumnFilter&, const std::vector<TUnversionedRow>&)>;
    using TDecoderWithMapping = std::function<
        TTypeErasedRow(const TSchemaData&, TWireProtocolReader*)>;

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

        return CallAndRetryIfMetadataCacheIsInconsistent([&] () {
            TTableSchema schema;
            TSharedRange<TTypeErasedRow> rows;
            std::tie(schema, rows) = DoLookupRowsOnce(
                path,
                nameTable,
                keys,
                options,
                options.Timeout,
                options.ColumnFilter,
                options.KeepMissingRows,
                encoder,
                decoder);
            return CreateRowset(schema, ReinterpretCastRange<TUnversionedRow>(rows));
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

        return CallAndRetryIfMetadataCacheIsInconsistent([&] () {
            TTableSchema schema;
            TSharedRange<TTypeErasedRow> rows;
            std::tie(schema, rows) = DoLookupRowsOnce(
                path,
                nameTable,
                keys,
                options,
                options.Timeout,
                options.ColumnFilter,
                options.KeepMissingRows,
                encoder,
                decoder);
            return CreateRowset(schema, ReinterpretCastRange<TVersionedRow>(rows));
        });
    }

    std::tuple< TTableSchema, TSharedRange<TTypeErasedRow> > DoLookupRowsOnce(
        const TYPath& path,
        const TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TTabletReadOptions& options,
        const TNullable<TDuration>& timeout,
        const NTableClient::TColumnFilter& columnFilter,
        bool keepMissingRows,
        const TEncoderWithMapping& encoderWithMapping,
        const TDecoderWithMapping& decoderWithMapping)
    {
        const auto& tableMountCache = Connection_->GetTableMountCache();
        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
            .ValueOrThrow();

        tableInfo->ValidateDynamic();
        tableInfo->ValidateSorted();
        tableInfo->ValidateNotReplicated();

        const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];
        auto idMapping = BuildColumnIdMapping(schema, nameTable);
        auto remappedColumnFilter = RemapColumnFilter(columnFilter, idMapping, nameTable);
        auto resultSchema = tableInfo->Schemas[ETableSchemaKind::Primary].Filter(remappedColumnFilter);
        auto resultSchemaData = TWireProtocolReader::GetSchemaData(schema, remappedColumnFilter);

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

        // TODO(sandello): Use code-generated comparer here.
        std::sort(sortedKeys.begin(), sortedKeys.end());
        std::vector<int> keyIndexToResultIndex(keys.Size());
        int currentResultIndex = -1;

        yhash<TCellId, TTabletCellLookupSessionPtr> cellIdToSession;

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
                        timeout,
                        tableInfo,
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
                GetHeavyChannelFactory(),
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

        std::vector<TTypeErasedRow> resultRows;
        resultRows.resize(keys.Size());

        for (int index = 0; index < keys.Size(); ++index) {
            resultRows[index] = uniqueResultRows[keyIndexToResultIndex[index]];
        }

        if (!keepMissingRows) {
            resultRows.erase(
                std::remove_if(
                    resultRows.begin(),
                    resultRows.end(),
                    [] (TTypeErasedRow row) {
                        return !static_cast<bool>(row);
                    }),
                resultRows.end());
        }

        return std::make_tuple(resultSchema, MakeSharedRange(std::move(resultRows), outputRowBuffer));
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
            HeavyChannelFactory_,
            FunctionImplCache_);

        TQueryPtr query;
        TDataRanges dataSource;
        std::tie(query, dataSource) = PreparePlanFragment(
            queryPreparer.Get(),
            queryString,
            fetchFunctions,
            inputRowLimit,
            outputRowLimit,
            options.Timestamp);

        TQueryOptions queryOptions;
        queryOptions.Timestamp = options.Timestamp;
        queryOptions.RangeExpansionLimit = options.RangeExpansionLimit;
        queryOptions.VerboseLogging = options.VerboseLogging;
        queryOptions.EnableCodeCache = options.EnableCodeCache;
        queryOptions.MaxSubqueries = options.MaxSubqueries;
        queryOptions.WorkloadDescriptor = options.WorkloadDescriptor;

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

    std::vector<TTableReplicaId> DoGetInSyncReplicas(
        const TYPath& path,
        TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TGetInSyncReplicasOptions& options)
    {
        ValidateSyncTimestamp(options.Timestamp);

        const auto& tableMountCache = Connection_->GetTableMountCache();
        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
            .ValueOrThrow();

        tableInfo->ValidateDynamic();
        tableInfo->ValidateSorted();
        tableInfo->ValidateReplicated();

        yhash<TCellId, std::vector<TTabletId>> cellToTabletIds;
        yhash_set<TTabletId> tabletIds;
        for (const auto& key : keys) {
            auto tabletInfo = tableInfo->GetTabletForRow(key);
            if (tabletIds.count(tabletInfo->TabletId) == 0) {
                tabletIds.insert(tabletInfo->TabletId);
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

        yhash<TTableReplicaId, int> replicaIdToCount;
        for (const auto& response : responses) {
            for (const auto& protoTabletInfo : response->tablet_info()) {
                for (const auto& protoReplicaInfo : protoTabletInfo.replica_info()) {
                    if (protoReplicaInfo.last_replication_timestamp() >= options.Timestamp) {
                        ++replicaIdToCount[FromProto<TTableReplicaId>(protoReplicaInfo.replica_id())];
                    }
                }
            }
        }

        std::vector<TTableReplicaId> replicas;
        for (const auto& pair : replicaIdToCount) {
            const auto& replicaId = pair.first;
            auto count = pair.second;
            if (count == tabletIds.size()) {
                replicas.push_back(replicaId);
            }
        }

        LOG_DEBUG("Got table in-sync replicas (TableId: %v, Replicas: %v, Timestamp: %llx)",
            tableInfo->TableId,
            replicas,
            options.Timestamp);

        return replicas;
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

        if (tabletIndex < 0 || tabletIndex >= tableInfo->Tablets.size()) {
            THROW_ERROR_EXCEPTION("Invalid tablet index: expected in range [0,%v], got %v",
                tableInfo->Tablets.size(),
                tabletIndex);
        }

        const auto& tabletInfo = tableInfo->Tablets[tabletIndex];

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
            }

            auto dstIdPath = FromObjectId(dstId);

            // Get source chunk ids.
            // Maps src index -> list of chunk ids for this src.
            std::vector<std::vector<TChunkId>> groupedChunkIds(srcPaths.size());
            {
                yhash<TCellTag, std::vector<int>> cellTagToIndexes;
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
                req->set_upload_transaction_timeout(ToProto(Connection_->GetConfig()->TransactionManager->DefaultTransactionTimeout));
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
                    Connection_->GetLightInvoker(),
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
                req->set_chunk_properties_update_needed(true);
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

    IAsyncZeroCopyInputStreamPtr DoGetJobInput(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobInputOptions& /*options*/)
    {
        auto nameTable = New<TNameTable>();

        TLookupRowsOptions lookupOptions;
        lookupOptions.ColumnFilter = NTableClient::TColumnFilter({nameTable->RegisterName("spec")});
        lookupOptions.KeepMissingRows = true;

        auto owningKey = CreateOperationJobKey(operationId, jobId, nameTable);

        std::vector<TUnversionedRow> keys;
        keys.push_back(owningKey);

        auto lookupResult = WaitFor(LookupRows(
            GetOperationsArchiveJobsPath(),
            nameTable,
            MakeSharedRange(keys, owningKey),
            lookupOptions));

        if (!lookupResult.IsOK()) {
            THROW_ERROR_EXCEPTION(lookupResult)
                .Wrap("Lookup job spec in operation archive failed")
                << TErrorAttribute("job_id", jobId)
                << TErrorAttribute("operation_id", operationId);
        }

        auto rows = lookupResult.Value()->GetRows();
        YCHECK(!rows.Empty());

        if (!rows[0]) {
            THROW_ERROR_EXCEPTION("Missing job spec in job archive table")
                << TErrorAttribute("job_id", jobId)
                << TErrorAttribute("operation_id", operationId);
        }

        auto value = rows[0][0];

        if (value.Type != EValueType::String) {
            THROW_ERROR_EXCEPTION("Found job spec has unexpected value type")
                << TErrorAttribute("job_id", jobId)
                << TErrorAttribute("operation_id", operationId)
                << TErrorAttribute("value_type", value.Type);
        }

        NJobTrackerClient::NProto::TJobSpec jobSpec;
        bool ok = jobSpec.ParseFromArray(value.Data.String, value.Length);
        if (!ok) {
            THROW_ERROR_EXCEPTION("Cannot parse job spec")
                << TErrorAttribute("job_id", jobId)
                << TErrorAttribute("operation_id", operationId);
        }

        if (!jobSpec.has_version() || jobSpec.version() != GetJobSpecVersion()) {
            THROW_ERROR_EXCEPTION("Job spec found in operation archive is of unsupported version")
                << TErrorAttribute("job_id", jobId)
                << TErrorAttribute("operation_id", operationId)
                << TErrorAttribute("found_version", jobSpec.version())
                << TErrorAttribute("supported_version", GetJobSpecVersion());
        }

        auto* schedulerJobSpecExt = jobSpec.MutableExtension(NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext);

        auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        auto locateChunks = BIND([=] {
            std::vector<TChunkSpec*> chunkSpecList;
            for (auto& tableSpec : *schedulerJobSpecExt->mutable_input_table_specs()) {
                if (tableSpec.chunk_specs_size() == 0) {
                    // COMPAT(psushin): don't forget to promote job spec version after removing this compat.
                    for (auto& dataSliceDescriptor : *tableSpec.mutable_data_slice_descriptors()) {
                        for (auto& chunkSpec : *dataSliceDescriptor.mutable_chunks()) {
                            chunkSpecList.push_back(&chunkSpec);
                        }
                    }
                } else {
                    for (auto& chunkSpec : *tableSpec.mutable_chunk_specs()) {
                        chunkSpecList.push_back(&chunkSpec);
                    }
                }
            }

            for (auto& tableSpec : *schedulerJobSpecExt->mutable_foreign_input_table_specs()) {
                if (tableSpec.chunk_specs_size() == 0) {
                    // COMPAT(psushin): don't forget to promote job spec version after removing this compat.
                    for (auto& dataSliceDescriptor : *tableSpec.mutable_data_slice_descriptors()) {
                        for (auto& chunkSpec : *dataSliceDescriptor.mutable_chunks()) {
                            chunkSpecList.push_back(&chunkSpec);
                        }
                    }
                } else {
                    for (auto& chunkSpec : *tableSpec.mutable_chunk_specs()) {
                        chunkSpecList.push_back(&chunkSpec);
                    }
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
            .AsyncVia(GetConnection()->GetHeavyInvoker())
            .Run());

        if (!locateChunksResult.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to locate chunks used in job input")
                << TErrorAttribute("job_id", jobId)
                << TErrorAttribute("operation_id", operationId);
        }

        auto jobSpecHelper = NJobProxy::CreateJobSpecHelper(jobSpec);

        auto userJobReader = CreateUserJobReadController(
            jobSpecHelper,
            MakeStrong(this),
            GetConnection()->GetHeavyInvoker(),
            NNodeTrackerClient::TNodeDescriptor(),
            BIND([] { }),
            Null);

        auto jobInputReader = New<TJobInputReader>(userJobReader, GetConnection()->GetHeavyInvoker());
        jobInputReader->Open();
        return jobInputReader;
    }

    TSharedRef DoGetJobStderr(
        const TOperationId& operationId,
        const TJobId& jobId,
        const TGetJobStderrOptions& /*options*/)
    {
        try {
            NNodeTrackerClient::TNodeDescriptor jobNodeDescriptor;
            {
                auto req = JobProberProxy_->GetJobNode();
                ToProto(req->mutable_job_id(), jobId);
                auto rsp = WaitFor(req->Invoke())
                    .ValueOrThrow();
                FromProto(&jobNodeDescriptor, rsp->node_descriptor());
            }

            auto nodeChannel = GetHeavyChannelFactory()->CreateChannel(jobNodeDescriptor);
            NJobProberClient::TJobProberServiceProxy jobProberServiceProxy(nodeChannel);

            auto req = jobProberServiceProxy.GetStderr();
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

        try {
            auto path = NScheduler::GetStderrPath(operationId, jobId);

            std::vector<TSharedRef> blocks;
            auto fileReader = WaitFor(static_cast<IClientBase*>(this)->CreateFileReader(path))
                .ValueOrThrow();

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

        try {
            auto nameTable = New<TNameTable>();

            struct TStderrArchiveIds
            {
                explicit TStderrArchiveIds(const TNameTablePtr& nameTable)
                    : OperationIdHi(nameTable->RegisterName("operation_id_hi"))
                    , OperationIdLo(nameTable->RegisterName("operation_id_lo"))
                    , JobIdHi(nameTable->RegisterName("job_id_hi"))
                    , JobIdLo(nameTable->RegisterName("job_id_lo"))
                    , Stderr(nameTable->RegisterName("stderr"))
                { }

                const int OperationIdHi;
                const int OperationIdLo;
                const int JobIdHi;
                const int JobIdLo;
                const int Stderr;
            };

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

    std::vector<TJob> DoListJobs(
        const TOperationId& operationId,
        const TListJobsOptions& options)
    {
        std::vector<TJob> resultJobs;

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
            MERGE_FIELD(JobState);
            MERGE_FIELD(StartTime);
            MERGE_NULLABLE_FIELD(FinishTime);
            MERGE_FIELD(Address);
            MERGE_NULLABLE_FIELD(Error);
            MERGE_NULLABLE_FIELD(Statistics);
            MERGE_NULLABLE_FIELD(StderrSize);
            MERGE_NULLABLE_FIELD(Progress);
            MERGE_NULLABLE_FIELD(CoreInfos);
#undef MERGE_FIELD
#undef MERGE_NULLABLE_FIELD
        };

        auto mergeJobs = [&] (const std::vector<TJob>& source1, const std::vector<TJob>& source2) {
            auto it1 = source1.begin();
            auto end1 = source1.end();

            auto it2 = source2.begin();
            auto end2 = source2.end();

            std::vector<TJob> result;
            while (it1 != end1 && it2 != end2) {
                if (it1->JobId == it2->JobId) {
                    result.push_back(*it1);
                    mergeJob(&result.back(), *it2);
                    ++it1;
                    ++it2;
                } else if (it1->JobId < it2->JobId) {
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

        auto sortJobs = [] (std::vector<TJob>* jobs) {
            std::sort(jobs->begin(), jobs->end(), [] (const TJob& lhs, const TJob& rhs) {
                return lhs.JobId < rhs.JobId;
            });
        };

        if (options.IncludeArchive) {
            TString conditions = Format("(operation_id_hi, operation_id_lo) = (%vu, %vu)",
                operationId.Parts64[0], operationId.Parts64[1]);

            if (options.JobType) {
                conditions = Format("%v and type = %Qv", conditions, *options.JobType);
            }

            if (options.JobState) {
                conditions = Format("%v and state = %Qv", conditions, *options.JobState);
            }

            auto selectFields = JoinSeq(",", {
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
                "stderr_size"});

            TString orderBy;
            if (options.SortField != EJobSortField::None) {
                switch (options.SortField) {
                    case EJobSortField::JobType:
                        orderBy = "type";
                        break;
                    case EJobSortField::JobState:
                        orderBy = "state";
                        break;
                    case EJobSortField::StartTime:
                        orderBy = "start_time";
                        break;
                    case EJobSortField::FinishTime:
                        orderBy = "finish_time";
                        break;
                    case EJobSortField::Address:
                        orderBy = "address";
                        break;
                    default:
                        Y_UNREACHABLE();
                }

                orderBy = Format("order by %v %v", orderBy, options.SortOrder == EJobSortDirection::Descending ? "desc" : "asc");
            }

            auto query = Format("%v from [%v] where %v %v limit %v",
                selectFields,
                GetOperationsArchiveJobsPath(),
                conditions,
                orderBy,
                options.Offset + options.Limit);

            TSelectRowsOptions selectRowsOptions;
            selectRowsOptions.Timestamp = AsyncLastCommittedTimestamp;

            if (deadline) {
                selectRowsOptions.Timeout = *deadline - Now();
            }

            auto result = WaitFor(SelectRows(query, selectRowsOptions))
                .ValueOrThrow();

            const auto& rows = result.Rowset->GetRows();

            auto checkIsNotNull = [&] (const TUnversionedValue& value, const TStringBuf& name) {
                if (value.Type == EValueType::Null) {
                    THROW_ERROR_EXCEPTION("Unexpected null value in column %Qv in job archive", name)
                        << TErrorAttribute("operation_id", operationId);
                }
            };

            for (auto row : rows) {
                checkIsNotNull(row[2], "job_id_hi");
                checkIsNotNull(row[3], "job_id_lo");

                TGuid jobId(row[2].Data.Uint64, row[3].Data.Uint64);

                TJob job;
                job.JobId = jobId;
                checkIsNotNull(row[4], "type");
                job.JobType = ParseEnum<EJobType>(TString(row[4].Data.String, row[4].Length));
                checkIsNotNull(row[5], "state");
                job.JobState = ParseEnum<EJobState>(TString(row[5].Data.String, row[5].Length));
                checkIsNotNull(row[6], "start_time");
                job.StartTime = TInstant(row[6].Data.Int64);

                if (row[7].Type != EValueType::Null) {
                    job.FinishTime = TInstant(row[7].Data.Int64);
                }

                if (row[8].Type != EValueType::Null) {
                    job.Address = TString(row[8].Data.String, row[8].Length);
                }

                if (row[9].Type != EValueType::Null) {
                    job.Error = TYsonString(TString(row[9].Data.String, row[9].Length));
                }

                if (row[10].Type != EValueType::Null) {
                    job.Statistics = TYsonString(TString(row[10].Data.String, row[10].Length));
                }

                if (row[11].Type != EValueType::Null) {
                    job.StderrSize = row[11].Data.Int64;
                }

                resultJobs.push_back(job);
            }

            sortJobs(&resultJobs);
        }

        if (options.IncludeCypress) {
            TObjectServiceProxy proxy(GetMasterChannelOrThrow(EMasterChannelKind::Follower));

            auto getReq = TYPathProxy::Get(GetJobsPath(operationId));
            auto attributeFilter = std::vector<TString>{
                "job_type",
                "state",
                "start_time",
                "finish_time",
                "address",
                "error",
                "statistics",
                "size",
                "uncompressed_data_size"
            };

            ToProto(getReq->mutable_attributes()->mutable_keys(), attributeFilter);

            if (deadline) {
                proxy.SetDefaultTimeout(*deadline - Now());
            }

            auto getRsp = WaitFor(proxy.Execute(getReq))
                .ValueOrThrow();

            auto items = ConvertToNode(NYson::TYsonString(getRsp->value()))->AsMap();

            std::vector<TJob> cypressJobs;
            for (const auto& item : items->GetChildren()) {
                const auto& attributes = item.second->Attributes();

                auto jobType = ParseEnum<NJobTrackerClient::EJobType>(attributes.Get<TString>("job_type"));
                auto jobState = ParseEnum<NJobTrackerClient::EJobState>(attributes.Get<TString>("state"));

                if (options.JobType && jobType != *options.JobType) {
                    continue;
                }

                if (options.JobState && jobState != *options.JobState) {
                    continue;
                }

                auto values = item.second->AsMap();

                TGuid jobId = TGuid::FromString(item.first);

                TJob job;
                job.JobId = jobId;
                job.JobType = jobType;
                job.JobState = jobState;
                job.StartTime = ConvertTo<TInstant>(attributes.Get<TString>("start_time"));
                job.FinishTime = ConvertTo<TInstant>(attributes.Get<TString>("finish_time"));
                job.Address = attributes.Get<TString>("address");
                job.Error = attributes.FindYson("error");
                job.Statistics = attributes.FindYson("statistics");
                if (auto stderr = values->FindChild("stderr")) {
                    job.StderrSize = stderr->Attributes().Get<i64>("uncompressed_data_size");
                }

                job.Progress = attributes.Find<double>("progress");
                job.CoreInfos = attributes.Find<TString>("core_infos");
                cypressJobs.push_back(job);
            }

            sortJobs(&cypressJobs);
            resultJobs = mergeJobs(resultJobs, cypressJobs);
        }

        if (options.IncludeRuntime) {
            TObjectServiceProxy proxy(GetMasterChannelOrThrow(EMasterChannelKind::Follower));

            auto path = Format("//sys/scheduler/orchid/scheduler/operations/%v/running_jobs", operationId);
            auto getReq = TYPathProxy::Get(path);

            if (deadline) {
                proxy.SetDefaultTimeout(*deadline - Now());
            }

            auto getRsp = WaitFor(proxy.Execute(getReq))
                .ValueOrThrow();

            auto items = ConvertToNode(NYson::TYsonString(getRsp->value()))->AsMap();

            std::vector<TJob> runtimeJobs;
            for (const auto& item : items->GetChildren()) {
                auto values = item.second->AsMap();

                auto jobType = ParseEnum<NJobTrackerClient::EJobType>(values->GetChild("job_type")->AsString()->GetValue());
                auto jobState = ParseEnum<NJobTrackerClient::EJobState>(values->GetChild("state")->AsString()->GetValue());

                if (options.JobType && jobType != *options.JobType) {
                    continue;
                }

                if (options.JobState && jobState != *options.JobState) {
                    continue;
                }

                TGuid jobId = TGuid::FromString(item.first);

                TJob job;
                job.JobId = jobId;
                job.JobType = jobType;
                job.JobState = jobState;
                job.StartTime = ConvertTo<TInstant>(values->GetChild("start_time")->AsString()->GetValue());
                job.Address = values->GetChild("address")->AsString()->GetValue();

                if (auto error = values->FindChild("error")) {
                    job.Error = TYsonString(error->AsString()->GetValue());
                }

                if (auto progress = values->FindChild("progress")) {
                    job.Progress = progress->AsDouble()->GetValue();
                }

                resultJobs.push_back(job);
            }
            sortJobs(&runtimeJobs);
            resultJobs = mergeJobs(resultJobs, runtimeJobs);
        }

        std::function<bool(const TJob&, const TJob&)> comparer;
        switch (options.SortField) {
#define XX(name, sortOrder) \
            case EJobSortField::name: \
                comparer = [&] (const TJob& lhs, const TJob& rhs) { \
                    return sortOrder == EJobSortDirection::Descending \
                        ? LessNullable(rhs.name, lhs.name) \
                        : LessNullable(lhs.name, rhs.name); \
                }; \
                break;

            XX(JobType, options.SortOrder);
            XX(JobState, options.SortOrder);
            XX(StartTime, options.SortOrder);
            XX(FinishTime, options.SortOrder);
            XX(Address, options.SortOrder);
#undef XX

            default:
                Y_UNREACHABLE();
        }

        std::sort(resultJobs.begin(), resultJobs.end(), comparer);

        auto startIt = resultJobs.begin() + std::min(options.Offset,
            std::distance(resultJobs.begin(), resultJobs.end()));

        auto endIt = startIt + std::min(options.Limit,
            std::distance(startIt, resultJobs.end()));

        return std::vector<TJob>(startIt, endIt);
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
            req->set_interrupt_timeout(ToProto(*options.InterruptTimeout));
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

DEFINE_ENUM(ETransactionState,
    (Active)
    (Commit)
    (Abort)
    (Flush)
    (Detach)
);

class TNativeTransaction
    : public INativeTransaction
{
public:
    TNativeTransaction(
        TNativeClientPtr client,
        NTransactionClient::TTransactionPtr transaction)
        : Client_(std::move(client))
        , Transaction_(std::move(transaction))
        , CommitInvoker_(CreateSerializedInvoker(Client_->GetConnection()->GetHeavyInvoker()))
        , Logger(NLogging::TLogger(Client_->Logger)
            .AddTag("TransactionId: %v, ConnectionCellTag: %v",
                GetId(),
                Client_->Connection_->GetCellTag()))
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

    virtual EAtomicity GetAtomicity() const override
    {
        return Transaction_->GetAtomicity();
    }

    virtual EDurability GetDurability() const override
    {
        return Transaction_->GetDurability();
    }

    virtual TDuration GetTimeout() const override
    {
        return Transaction_->GetTimeout();
    }


    virtual TFuture<void> Ping() override
    {
        return Transaction_->Ping();
    }

    virtual TFuture<TTransactionCommitResult> Commit(const TTransactionCommitOptions& options) override
    {
    	auto guard = Guard(SpinLock_);

        if (State_ != ETransactionState::Active) {
            return MakeFuture<TTransactionCommitResult>(TError("Cannot commit since transaction %v is already in %Qlv state",
                GetId(),
                State_));
        }

        State_ = ETransactionState::Commit;
        return BIND(&TNativeTransaction::DoCommit, MakeStrong(this))
            .AsyncVia(CommitInvoker_)
            .Run(options);
    }

    virtual TFuture<void> Abort(const TTransactionAbortOptions& options) override
    {
    	auto guard = Guard(SpinLock_);

        if (State_ == ETransactionState::Abort) {
            return AbortResult_;
        }

        if (State_ != ETransactionState::Active && State_ != ETransactionState::Flush) {
            return MakeFuture<void>(TError("Cannot abort since transaction %v is already in %Qlv state",
                GetId(),
                State_));
        }

        State_ = ETransactionState::Abort;
        AbortResult_ = Transaction_->Abort(options);
        return AbortResult_;
    }

    virtual void Detach() override
    {
    	auto guard = Guard(SpinLock_);
        State_ = ETransactionState::Detach;
        Transaction_->Detach();
    }

    virtual TFuture<TTransactionFlushResult> Flush() override
    {
    	auto guard = Guard(SpinLock_);

        if (State_ != ETransactionState::Active) {
            return MakeFuture<TTransactionFlushResult>(TError("Cannot flush since transaction %v is already in %Qlv state",
                GetId(),
                State_));
        }

        LOG_DEBUG("Flushing transaction");
        State_ = ETransactionState::Flush;
        return BIND(&TNativeTransaction::DoFlush, MakeStrong(this))
            .AsyncVia(CommitInvoker_)
            .Run();
    }

    virtual void AddAction(const TCellId& cellId, const TTransactionActionData& data) override
    {
        auto guard = Guard(SpinLock_);

        YCHECK(TypeFromId(cellId) == EObjectType::TabletCell ||
               TypeFromId(cellId) == EObjectType::ClusterCell);

        if (State_ != ETransactionState::Active) {
            THROW_ERROR_EXCEPTION("Cannot add action since transaction %v is already in %Qlv state",
                GetId(),
                State_);
        }

        if (GetAtomicity() != EAtomicity::Full) {
            THROW_ERROR_EXCEPTION("Atomicity must be %Qlv for custom actions",
                EAtomicity::Full);
        }

        auto session = GetOrCreateCellCommitSession(cellId);
        session->RegisterAction(data);

        LOG_DEBUG("Transaction action added (CellId: %v, ActionType: %v)",
            cellId,
            data.Type);
    }


    virtual TFuture<ITransactionPtr> StartForeignTransaction(
        const IClientPtr& client,
        const TForeignTransactionStartOptions& options) override
    {
        if (client->GetConnection()->GetCellTag() == GetConnection()->GetCellTag()) {
            return MakeFuture<ITransactionPtr>(this);
        }

        TTransactionStartOptions adjustedOptions(options);
        adjustedOptions.Id = GetId();
        if (options.InheritStartTimestamp) {
            adjustedOptions.StartTimestamp = GetStartTimestamp();
        }

        return client->StartTransaction(GetType(), adjustedOptions)
            .Apply(BIND([this, this_ = MakeStrong(this)] (const ITransactionPtr& transaction) {
                RegisterForeignTransaction(transaction);
                return transaction;
            }));
    }


    virtual void SubscribeCommitted(const TClosure& callback) override
    {
        Transaction_->SubscribeCommitted(callback);
    }

    virtual void UnsubscribeCommitted(const TClosure& callback) override
    {
        Transaction_->UnsubscribeCommitted(callback);
    }


    virtual void SubscribeAborted(const TClosure& callback) override
    {
        Transaction_->SubscribeAborted(callback);
    }

    virtual void UnsubscribeAborted(const TClosure& callback) override
    {
        Transaction_->UnsubscribeAborted(callback);
    }


    virtual TFuture<INativeTransactionPtr> StartNativeTransaction(
        ETransactionType type,
        const TTransactionStartOptions& options) override
    {
        auto adjustedOptions = options;
        adjustedOptions.ParentId = GetId();
        return Client_->StartNativeTransaction(
            type,
            adjustedOptions);
    }

    virtual TFuture<ITransactionPtr> StartTransaction(
        ETransactionType type,
        const TTransactionStartOptions& options) override
    {
        return StartNativeTransaction(type, options).As<ITransactionPtr>();
    }


    virtual void WriteRows(
        const TYPath& path,
        TNameTablePtr nameTable,
        TSharedRange<TUnversionedRow> rows,
        const TModifyRowsOptions& options) override
    {
        std::vector<TRowModification> modifications;
        modifications.reserve(rows.Size());

        for (auto row : rows) {
            TRowModification modification;
            modification.Type = ERowModificationType::Write;
            modification.Row = row.ToTypeErasedRow();
            modifications.push_back(modification);
        }

        ModifyRows(
            path,
            std::move(nameTable),
            MakeSharedRange(std::move(modifications), std::move(rows)),
            options);
    }

    virtual void WriteRows(
        const TYPath& path,
        TNameTablePtr nameTable,
        TSharedRange<TVersionedRow> rows,
        const TModifyRowsOptions& options) override
    {
        std::vector<TRowModification> modifications;
        modifications.reserve(rows.Size());

        for (auto row : rows) {
            TRowModification modification;
            modification.Type = ERowModificationType::VersionedWrite;
            modification.Row = row.ToTypeErasedRow();
            modifications.push_back(modification);
        }

        ModifyRows(
            path,
            std::move(nameTable),
            MakeSharedRange(std::move(modifications), std::move(rows)),
            options);
    }

    virtual void DeleteRows(
        const TYPath& path,
        TNameTablePtr nameTable,
        TSharedRange<TUnversionedRow> keys,
        const TModifyRowsOptions& options) override
    {
        std::vector<TRowModification> modifications;
        modifications.reserve(keys.Size());

        for (auto key : keys) {
            TRowModification modification;
            modification.Type = ERowModificationType::Delete;
            modification.Row = key.ToTypeErasedRow();
            modifications.push_back(modification);
        }

        ModifyRows(
            path,
            std::move(nameTable),
            MakeSharedRange(std::move(modifications), std::move(keys)),
            options);
    }

    virtual void ModifyRows(
        const TYPath& path,
        TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications,
        const TModifyRowsOptions& options) override
    {
    	auto guard = Guard(SpinLock_);

        ValidateTabletTransaction();

        if (State_ != ETransactionState::Active) {
            THROW_ERROR_EXCEPTION("Cannot modify rows since transaction %v is already in %Qlv state",
                GetId(),
                State_);
        }

        Requests_.push_back(std::make_unique<TModificationRequest>(
            this,
            Client_->GetNativeConnection(),
            path,
            std::move(nameTable),
            std::move(modifications),
            options));

        LOG_DEBUG("Row modifications buffered (Count: %v)",
            modifications.Size());
    }


#define DELEGATE_METHOD(returnType, method, signature, args) \
    virtual returnType method signature override \
    { \
        return Client_->method args; \
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

#define DELEGATE_TIMESTAMPED_METHOD(returnType, method, signature, args) \
    virtual returnType method signature override \
    { \
        auto& originalOptions = options; \
        { \
            auto options = originalOptions; \
            options.Timestamp = GetReadTimestamp(); \
            return Client_->method args; \
        } \
    }

    DELEGATE_TIMESTAMPED_METHOD(TFuture<IUnversionedRowsetPtr>, LookupRows, (
        const TYPath& path,
        TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TLookupRowsOptions& options),
        (path, nameTable, keys, options))
    DELEGATE_TIMESTAMPED_METHOD(TFuture<IVersionedRowsetPtr>, VersionedLookupRows, (
        const TYPath& path,
        TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TVersionedLookupRowsOptions& options),
        (path, nameTable, keys, options))

    DELEGATE_TIMESTAMPED_METHOD(TFuture<TSelectRowsResult>, SelectRows, (
        const TString& query,
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
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TYsonString>, ListNode, (
        const TYPath& path,
        const TListNodeOptions& options),
        (path, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TNodeId>, CreateNode, (
        const TYPath& path,
        EObjectType type,
        const TCreateNodeOptions& options),
        (path, type, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TLockNodeResult>, LockNode, (
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
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<void>, ConcatenateNodes, (
        const std::vector<TYPath>& srcPaths,
        const TYPath& dstPath,
        const TConcatenateNodesOptions& options),
        (srcPaths, dstPath, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<bool>, NodeExists, (
        const TYPath& path,
        const TNodeExistsOptions& options),
        (path, options))


    DELEGATE_METHOD(TFuture<TObjectId>, CreateObject, (
        EObjectType type,
        const TCreateObjectOptions& options),
        (type, options))
    DELEGATE_METHOD(TFuture<std::vector<NTabletClient::TTableReplicaId>>, GetInSyncReplicas, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TGetInSyncReplicasOptions& options),
        (path, nameTable, keys, options))


    DELEGATE_TRANSACTIONAL_METHOD(TFuture<IAsyncZeroCopyInputStreamPtr>, CreateFileReader, (
        const TYPath& path,
        const TFileReaderOptions& options),
        (path, options))
    DELEGATE_TRANSACTIONAL_METHOD(IFileWriterPtr, CreateFileWriter, (
        const TYPath& path,
        const TFileWriterOptions& options),
        (path, options))


    DELEGATE_TRANSACTIONAL_METHOD(IJournalReaderPtr, CreateJournalReader, (
        const TYPath& path,
        const TJournalReaderOptions& options),
        (path, options))
    DELEGATE_TRANSACTIONAL_METHOD(IJournalWriterPtr, CreateJournalWriter, (
        const TYPath& path,
        const TJournalWriterOptions& options),
        (path, options))

    DELEGATE_TRANSACTIONAL_METHOD(TFuture<ISchemalessMultiChunkReaderPtr>, CreateTableReader, (
        const TRichYPath& path,
        const TTableReaderOptions& options),
        (path, options))

    DELEGATE_TRANSACTIONAL_METHOD(TFuture<ISchemalessWriterPtr>, CreateTableWriter, (
        const TRichYPath& path,
        const TTableWriterOptions& options),
        (path, options))

#undef DELEGATE_TRANSACTIONAL_METHOD
#undef DELEGATE_TIMESTAMPED_METHOD

private:
    const TNativeClientPtr Client_;
    const NTransactionClient::TTransactionPtr Transaction_;

    const IInvokerPtr CommitInvoker_;
    const NLogging::TLogger Logger;

    struct TNativeTransactionBufferTag
    { };

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TNativeTransactionBufferTag());

    TSpinLock SpinLock_;
    ETransactionState State_ = ETransactionState::Active;
    TFuture<void> AbortResult_;

    TSpinLock ForeignTransactionsLock_;
    std::vector<ITransactionPtr> ForeignTransactions_;


    class TTableCommitSession;
    using TTableCommitSessionPtr = TIntrusivePtr<TTableCommitSession>;

    class TTabletCommitSession;
    using TTabletCommitSessionPtr = TIntrusivePtr<TTabletCommitSession>;

    class TCellCommitSession;
    using TCellCommitSessionPtr = TIntrusivePtr<TCellCommitSession>;


    class TModificationRequest
    {
    public:
        TModificationRequest(
            TNativeTransaction* transaction,
            INativeConnectionPtr connection,
            const TYPath& path,
            TNameTablePtr nameTable,
            TSharedRange<TRowModification> modifications,
            const TModifyRowsOptions& options)
            : Transaction_(transaction)
            , Connection_(std::move(connection))
            , Path_(path)
            , NameTable_(std::move(nameTable))
            , TabletIndexColumnId_(NameTable_->FindId(TabletIndexColumnName))
            , Modifications_(std::move(modifications))
            , Options_(options)
        { }

        void PrepareTableSessions()
        {
            TableSession_ = Transaction_->GetOrCreateTableSession(Path_, Options_.UpstreamReplicaId);
        }

        void SubmitRows()
        {
            if (!TableSession_->GetInfo()->Replicas.empty() &&
                TableSession_->SyncReplicas().empty() &&
                Options_.RequireSyncReplica)
            {
                THROW_ERROR_EXCEPTION("Table %v has no synchronous replicas",
                    TableSession_->GetInfo()->Path);
            }

            for (const auto& replicaData : TableSession_->SyncReplicas()) {
                auto replicaOptions = Options_;
                replicaOptions.UpstreamReplicaId = replicaData.ReplicaInfo->ReplicaId;
                replicaData.Transaction->ModifyRows(
                    replicaData.ReplicaInfo->ReplicaPath,
                    NameTable_,
                    Modifications_,
                    replicaOptions);
            }

            const auto& tableInfo = TableSession_->GetInfo();

            const auto& primarySchema = tableInfo->Schemas[ETableSchemaKind::Primary];
            const auto& primaryIdMapping = Transaction_->GetColumnIdMapping(tableInfo, NameTable_, ETableSchemaKind::Primary);

            const auto& writeSchema = tableInfo->Schemas[ETableSchemaKind::Write];
            const auto& writeIdMapping = Transaction_->GetColumnIdMapping(tableInfo, NameTable_, ETableSchemaKind::Write);

            const auto& versionedWriteSchema = tableInfo->Schemas[ETableSchemaKind::VersionedWrite];
            const auto& versionedWriteIdMapping = Transaction_->GetColumnIdMapping(tableInfo, NameTable_, ETableSchemaKind::VersionedWrite);

            const auto& deleteSchema = tableInfo->Schemas[ETableSchemaKind::Delete];
            const auto& deleteIdMapping = Transaction_->GetColumnIdMapping(tableInfo, NameTable_, ETableSchemaKind::Delete);

            const auto& rowBuffer = Transaction_->RowBuffer_;

            auto evaluatorCache = Connection_->GetColumnEvaluatorCache();
            auto evaluator = tableInfo->NeedKeyEvaluation ? evaluatorCache->Find(primarySchema) : nullptr;

            auto randomTabletInfo = tableInfo->GetRandomMountedTablet();

            for (const auto& modification : Modifications_) {
                switch (modification.Type) {
                    case ERowModificationType::Write:
                        ValidateClientDataRow(TUnversionedRow(modification.Row), writeSchema, writeIdMapping, NameTable_);
                        break;

                    case ERowModificationType::VersionedWrite:
                        if (!tableInfo->IsSorted()) {
                            THROW_ERROR_EXCEPTION("Cannot perform versioned writes into a non-sorted table %v",
                                tableInfo->Path);
                        }
                        if (tableInfo->IsReplicated()) {
                            THROW_ERROR_EXCEPTION("Cannot perform versioned writes into a replicated table %v",
                                tableInfo->Path);
                        }
                        ValidateClientDataRow(TVersionedRow(modification.Row), versionedWriteSchema, versionedWriteIdMapping, NameTable_);
                        break;

                    case ERowModificationType::Delete:
                        if (!tableInfo->IsSorted()) {
                            THROW_ERROR_EXCEPTION("Cannot perform deletes in a non-sorted table %v",
                                tableInfo->Path);
                        }
                        ValidateClientKey(TUnversionedRow(modification.Row), deleteSchema, deleteIdMapping, NameTable_);
                        break;

                    default:
                        Y_UNREACHABLE();
                }

                switch (modification.Type) {
                    case ERowModificationType::Write:
                    case ERowModificationType::Delete: {
                        auto capturedRow = rowBuffer->CaptureAndPermuteRow(
                            TUnversionedRow(modification.Row),
                            primarySchema,
                            primaryIdMapping);
                        TTabletInfoPtr tabletInfo;
                        if (tableInfo->IsSorted()) {
                            if (evaluator) {
                                evaluator->EvaluateKeys(capturedRow, rowBuffer);
                            }
                            tabletInfo = GetSortedTabletForRow(tableInfo, capturedRow);
                        } else {
                            tabletInfo = GetOrderedTabletForRow(
                                tableInfo,
                                randomTabletInfo,
                                TabletIndexColumnId_,
                                TUnversionedRow(modification.Row));
                        }
                        auto session = Transaction_->GetOrCreateTabletSession(tabletInfo, tableInfo, TableSession_);
                        auto command = GetCommand(modification.Type);
                        session->SubmitRow(command, capturedRow);
                        break;
                    }

                    case ERowModificationType::VersionedWrite: {
                        auto capturedRow = rowBuffer->CaptureAndPermuteRow(
                            TVersionedRow(modification.Row),
                            primarySchema,
                            primaryIdMapping);
                        auto tabletInfo = GetSortedTabletForRow(tableInfo, capturedRow);
                        auto session = Transaction_->GetOrCreateTabletSession(tabletInfo, tableInfo, TableSession_);
                        auto command = GetCommand(modification.Type);
                        session->SubmitRow(command, capturedRow);
                        break;
                    }

                    default:
                        Y_UNREACHABLE();
                }
            }
        }

    protected:
        TNativeTransaction* const Transaction_;
        const INativeConnectionPtr Connection_;
		const TYPath Path_;
        const TNameTablePtr NameTable_;
        const TNullable<int> TabletIndexColumnId_;
        const TSharedRange<TRowModification> Modifications_;
        const TModifyRowsOptions Options_;

        TTableCommitSessionPtr TableSession_;

        static EWireProtocolCommand GetCommand(ERowModificationType modificationType)
        {
            switch (modificationType) {
                case ERowModificationType::Write:
                    return EWireProtocolCommand::WriteRow;

                case ERowModificationType::VersionedWrite:
                    return EWireProtocolCommand::VersionedWriteRow;

                case ERowModificationType::Delete:
                    return EWireProtocolCommand::DeleteRow;

                default:
                    Y_UNREACHABLE();
            }
        }
    };

    std::vector<std::unique_ptr<TModificationRequest>> Requests_;

    struct TSyncReplica
    {
        TTableReplicaInfoPtr ReplicaInfo;
        ITransactionPtr Transaction;
    };

    class TTableCommitSession
        : public TIntrinsicRefCounted
    {
    public:
        TTableCommitSession(
            TNativeTransaction* transaction,
            TTableMountInfoPtr tableInfo,
            const TTableReplicaId& upstreamReplicaId)
            : Transaction_(transaction)
            , TableInfo_(std::move(tableInfo))
            , UpstreamReplicaId_(upstreamReplicaId)
            , Logger(NLogging::TLogger(transaction->Logger)
                .AddTag("Path: %v", TableInfo_->Path))
        { }

        const TTableMountInfoPtr& GetInfo() const
        {
            return TableInfo_;
        }

        const TTableReplicaId& GetUpstreamReplicaId() const
        {
            return UpstreamReplicaId_;
        }

        const std::vector<TSyncReplica>& SyncReplicas() const
        {
            return SyncReplicas_;
        }


        void RegisterSyncReplicas(bool* clusterDirectorySynced)
        {
            for (const auto& replicaInfo : TableInfo_->Replicas) {
                if (replicaInfo->Mode != ETableReplicaMode::Sync) {
                    continue;
                }

                LOG_DEBUG("Sync table replica registered (ReplicaId: %v, ClusterName: %v, ReplicaPath: %v)",
                    replicaInfo->ReplicaId,
                    replicaInfo->ClusterName,
                    replicaInfo->ReplicaPath);

                auto syncReplicaTransaction = Transaction_->GetSyncReplicaTransaction(
                    replicaInfo,
                    clusterDirectorySynced);
                SyncReplicas_.push_back(TSyncReplica{replicaInfo, std::move(syncReplicaTransaction)});
            }
        }

    private:
        TNativeTransaction* const Transaction_;
        const TTableMountInfoPtr TableInfo_;
        const TTableReplicaId UpstreamReplicaId_;
        const NLogging::TLogger Logger;

        std::vector<TSyncReplica> SyncReplicas_;

    };

    //! Maintains per-table commit info.
    yhash<TYPath, TTableCommitSessionPtr> TablePathToSession_;

    class TTabletCommitSession
        : public TIntrinsicRefCounted
    {
    public:
        TTabletCommitSession(
            TNativeTransactionPtr transaction,
            TTabletInfoPtr tabletInfo,
            TTableMountInfoPtr tableInfo,
            TTableCommitSessionPtr tableSession,
            TColumnEvaluatorPtr columnEvauator)
            : Transaction_(transaction)
            , TableInfo_(std::move(tableInfo))
            , TabletInfo_(std::move(tabletInfo))
            , TableSession_(std::move(tableSession))
            , Config_(transaction->Client_->Connection_->GetConfig())
            , ColumnCount_(TableInfo_->Schemas[ETableSchemaKind::Primary].Columns().size())
            , KeyColumnCount_(TableInfo_->Schemas[ETableSchemaKind::Primary].GetKeyColumnCount())
            , ColumnEvaluator_(std::move(columnEvauator))
            , Logger(NLogging::TLogger(transaction->Logger)
                .AddTag("TabletId: %v", TabletInfo_->TabletId))
        { }

        void SubmitRow(
            EWireProtocolCommand command,
            TUnversionedRow row)
        {
            DoSubmitRow(
                &UnversionedSubmittedRows_,
                TUnversionedSubmittedRow{
                    command,
                    row,
                    static_cast<int>(UnversionedSubmittedRows_.size())
                });
        }

        void SubmitRow(
            EWireProtocolCommand command,
            TVersionedRow row)
        {
            DoSubmitRow(
                &VersionedSubmittedRows_,
                TVersionedSubmittedRow{
                    command,
                    row
                });
        }

        int Prepare()
        {
            if (!VersionedSubmittedRows_.empty() && !UnversionedSubmittedRows_.empty()) {
                THROW_ERROR_EXCEPTION("Cannot intermix versioned and unversioned writes to a single table "
                    "within a transaction");
            }

            if (TableInfo_->IsSorted()) {
                PrepareSortedBatches();
            } else {
                PrepareOrderedBatches();
            }

            return static_cast<int>(Batches_.size());
        }

        TFuture<void> Invoke(IChannelPtr channel)
        {
            // Do all the heavy lifting here.
            auto* codec = NCompression::GetCodec(Config_->WriteRequestCodec);
            YCHECK(!Batches_.empty());
            for (const auto& batch : Batches_) {
                batch->RequestData = codec->Compress(batch->Writer.Finish());
            }

            InvokeChannel_ = channel;
            InvokeNextBatch();
            return InvokePromise_;
        }

        const TCellId& GetCellId() const
        {
            return TabletInfo_->CellId;
        }

    private:
        const TWeakPtr<TNativeTransaction> Transaction_;
        const TTableMountInfoPtr TableInfo_;
        const TTabletInfoPtr TabletInfo_;
        const TTableCommitSessionPtr TableSession_;
        const TNativeConnectionConfigPtr Config_;
        const int ColumnCount_;
        const int KeyColumnCount_;

        struct TCommitSessionBufferTag
        { };

        TColumnEvaluatorPtr ColumnEvaluator_;
        TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TCommitSessionBufferTag());

        NLogging::TLogger Logger;

        struct TBatch
        {
            TWireProtocolWriter Writer;
            TSharedRef RequestData;
            int RowCount = 0;
        };

        std::vector<std::unique_ptr<TBatch>> Batches_;

        struct TVersionedSubmittedRow
        {
            EWireProtocolCommand Command;
            TVersionedRow Row;
        };

        std::vector<TVersionedSubmittedRow> VersionedSubmittedRows_;

        struct TUnversionedSubmittedRow
        {
            EWireProtocolCommand Command;
            TUnversionedRow Row;
            int SequentialId;
        };

        std::vector<TUnversionedSubmittedRow> UnversionedSubmittedRows_;

        IChannelPtr InvokeChannel_;
        int InvokeBatchIndex_ = 0;
        TPromise<void> InvokePromise_ = NewPromise<void>();

        template <class TRow>
        void DoSubmitRow(std::vector<TRow>* rows, const TRow& row)
        {
            if (rows->size() >= Config_->MaxRowsPerTransaction) {
                THROW_ERROR_EXCEPTION("Transaction affects too many rows")
                    << TErrorAttribute("limit", Config_->MaxRowsPerTransaction);
            }
            rows->push_back(row);
        }

        void PrepareSortedBatches()
        {
            std::sort(
                UnversionedSubmittedRows_.begin(),
                UnversionedSubmittedRows_.end(),
                [=] (const TUnversionedSubmittedRow& lhs, const TUnversionedSubmittedRow& rhs) {
                    // NB: CompareRows may throw on composite values.
                    int res = CompareRows(lhs.Row, rhs.Row, KeyColumnCount_);
                    return res != 0 ? res < 0 : lhs.SequentialId < rhs.SequentialId;
                });

            std::vector<TUnversionedSubmittedRow> unversionedMergedRows;
            unversionedMergedRows.reserve(UnversionedSubmittedRows_.size());

            TUnversionedRowMerger merger(
                RowBuffer_,
                ColumnCount_,
                KeyColumnCount_,
                ColumnEvaluator_);

            auto addPartialRow = [&] (const TUnversionedSubmittedRow& submittedRow) {
                switch (submittedRow.Command) {
                    case EWireProtocolCommand::DeleteRow:
                        merger.DeletePartialRow(submittedRow.Row);
                        break;

                    case EWireProtocolCommand::WriteRow:
                        merger.AddPartialRow(submittedRow.Row);
                        break;

                    default:
                        Y_UNREACHABLE();
                }
            };

            int index = 0;
            while (index < UnversionedSubmittedRows_.size()) {
                if (index < UnversionedSubmittedRows_.size() - 1 &&
                    CompareRows(UnversionedSubmittedRows_[index].Row, UnversionedSubmittedRows_[index + 1].Row, KeyColumnCount_) == 0)
                {
                    addPartialRow(UnversionedSubmittedRows_[index]);
                    while (index < UnversionedSubmittedRows_.size() - 1 &&
                           CompareRows(UnversionedSubmittedRows_[index].Row, UnversionedSubmittedRows_[index + 1].Row, KeyColumnCount_) == 0)
                    {
                        ++index;
                        addPartialRow(UnversionedSubmittedRows_[index]);
                    }
                    UnversionedSubmittedRows_[index].Row = merger.BuildMergedRow();
                }
                unversionedMergedRows.push_back(UnversionedSubmittedRows_[index]);
                ++index;
            }

            WriteRows(unversionedMergedRows);

            WriteRows(VersionedSubmittedRows_);
        }

        void PrepareOrderedBatches()
        {
            WriteRows(UnversionedSubmittedRows_);
        }

        template <class TRow>
        void WriteRows(const std::vector<TRow>& rows)
        {
            for (const auto& submittedRow : rows) {
                WriteRow(submittedRow);
            }
        }

        TBatch* EnsureBatch()
        {
            if (Batches_.empty() || Batches_.back()->RowCount >= Config_->MaxRowsPerWriteRequest) {
                Batches_.emplace_back(new TBatch());
            }
            return Batches_.back().get();
        }

        void WriteRow(const TVersionedSubmittedRow& submittedRow)
        {
            auto* batch = EnsureBatch();
            ++batch->RowCount;
            auto& writer = batch->Writer;
            writer.WriteCommand(submittedRow.Command);
            writer.WriteVersionedRow(submittedRow.Row);
        }

        void WriteRow(const TUnversionedSubmittedRow& submittedRow)
        {
            auto* batch = EnsureBatch();
            ++batch->RowCount;
            auto& writer = batch->Writer;
            writer.WriteCommand(submittedRow.Command);
            writer.WriteUnversionedRow(submittedRow.Row);
        }

        void InvokeNextBatch()
        {
            if (InvokeBatchIndex_ >= Batches_.size()) {
                InvokePromise_.Set(TError());
                return;
            }

            const auto& batch = Batches_[InvokeBatchIndex_];

            auto transaction = Transaction_.Lock();
            if (!transaction) {
                return;
            }

            auto cellSession = transaction->GetCommitSession(GetCellId());

            TTabletServiceProxy proxy(InvokeChannel_);
            proxy.SetDefaultTimeout(Config_->WriteTimeout);
            proxy.SetDefaultRequestAck(false);

            auto req = proxy.Write();
            ToProto(req->mutable_transaction_id(), transaction->GetId());
            if (transaction->GetAtomicity() == EAtomicity::Full) {
                req->set_transaction_start_timestamp(transaction->GetStartTimestamp());
                req->set_transaction_timeout(ToProto(transaction->GetTimeout()));
            }
            ToProto(req->mutable_tablet_id(), TabletInfo_->TabletId);
            req->set_mount_revision(TabletInfo_->MountRevision);
            req->set_durability(static_cast<int>(transaction->GetDurability()));
            req->set_signature(cellSession->AllocateRequestSignature());
            req->set_request_codec(static_cast<int>(Config_->WriteRequestCodec));
            req->set_row_count(batch->RowCount);
            req->set_versioned(!VersionedSubmittedRows_.empty());
            for (const auto& replicaInfo : TableInfo_->Replicas) {
                if (replicaInfo->Mode == ETableReplicaMode::Sync) {
                    ToProto(req->add_sync_replica_ids(), replicaInfo->ReplicaId);
                }
            }
            if (TableSession_->GetUpstreamReplicaId()) {
                ToProto(req->mutable_upstream_replica_id(), TableSession_->GetUpstreamReplicaId());
            }
            req->Attachments().push_back(batch->RequestData);

            LOG_DEBUG("Sending transaction rows (BatchIndex: %v/%v, RowCount: %v, Signature: %x, "
                "Versioned: %v, UpstreamReplicaId: %v)",
                InvokeBatchIndex_,
                Batches_.size(),
                batch->RowCount,
                req->signature(),
                req->versioned(),
                TableSession_->GetUpstreamReplicaId());

            req->Invoke().Subscribe(
                BIND(&TTabletCommitSession::OnResponse, MakeStrong(this))
                    .Via(transaction->CommitInvoker_));
        }

        void OnResponse(const TTabletServiceProxy::TErrorOrRspWritePtr& rspOrError)
        {
            if (!rspOrError.IsOK()) {
                LOG_DEBUG(rspOrError, "Error sending transaction rows");
                InvokePromise_.Set(rspOrError);
                return;
            }

            auto owner = Transaction_.Lock();
            if (!owner) {
                return;
            }

            LOG_DEBUG("Transaction rows sent successfully (BatchIndex: %v/%v)",
                InvokeBatchIndex_,
                Batches_.size());

            owner->Transaction_->ConfirmParticipant(TabletInfo_->CellId);
            ++InvokeBatchIndex_;
            InvokeNextBatch();
        }
    };

    //! Maintains per-tablet commit info.
    yhash<TTabletId, TTabletCommitSessionPtr> TabletIdToSession_;

    class TCellCommitSession
        : public TIntrinsicRefCounted
    {
    public:
        TCellCommitSession(TNativeTransactionPtr transaction, const TCellId& cellId)
            : Transaction_(transaction)
            , CellId_(cellId)
            , Logger(NLogging::TLogger(transaction->Logger)
                .AddTag("CellId: %v", CellId_))
        { }

        void RegisterRequests(int count)
        {
            RequestsRemaining_ += count;
        }

        TTransactionSignature AllocateRequestSignature()
        {
            YCHECK(--RequestsRemaining_ >= 0);
            if (RequestsRemaining_ == 0) {
                return FinalTransactionSignature - CurrentSignature_;
            } else {
                ++CurrentSignature_;
                return 1;
            }
        }

        void RegisterAction(const TTransactionActionData& data)
        {
            if (Actions_.empty()) {
                RegisterRequests(1);
            }
            Actions_.push_back(data);
        }

        TFuture<void> Invoke(const IChannelPtr& channel)
        {
            if (Actions_.empty()) {
                return VoidFuture;
            }

            auto transaction = Transaction_.Lock();
            if (!transaction) {
                return MakeFuture(TError("Transaction is no longer available"));
            }

            LOG_DEBUG("Sending transaction actions (ActionCount: %v)",
                Actions_.size());

            TFuture<void> asyncResult;
            switch (TypeFromId(CellId_)) {
                case EObjectType::TabletCell:
                    asyncResult = SendTabletActions(transaction, channel);
                    break;
                case EObjectType::ClusterCell:
                    asyncResult = SendMasterActions(transaction, channel);
                    break;
                default:
                    Y_UNREACHABLE();
            }

            return asyncResult.Apply(
                BIND(&TCellCommitSession::OnResponse, MakeStrong(this))
                    .AsyncVia(transaction->CommitInvoker_));
        }

    private:
        const TWeakPtr<TNativeTransaction> Transaction_;
        const TCellId CellId_;

        std::vector<TTransactionActionData> Actions_;
        TTransactionSignature CurrentSignature_ = InitialTransactionSignature;
        int RequestsRemaining_ = 0;

        NLogging::TLogger Logger;


        TFuture<void> SendTabletActions(const TNativeTransactionPtr& owner, const IChannelPtr& channel)
        {
            TTabletServiceProxy proxy(channel);
            auto req = proxy.RegisterTransactionActions();
            ToProto(req->mutable_transaction_id(), owner->GetId());
            req->set_transaction_start_timestamp(owner->GetStartTimestamp());
            req->set_transaction_timeout(ToProto(owner->GetTimeout()));
            req->set_signature(AllocateRequestSignature());
            ToProto(req->mutable_actions(), Actions_);
            return req->Invoke().As<void>();
        }

        TFuture<void> SendMasterActions(const TNativeTransactionPtr& owner, const IChannelPtr& channel)
        {
            TTransactionServiceProxy proxy(channel);
            auto req = proxy.RegisterTransactionActions();
            ToProto(req->mutable_transaction_id(), owner->GetId());
            ToProto(req->mutable_actions(), Actions_);
            return req->Invoke().As<void>();
        }

        void OnResponse(const TError& result)
        {
            if (!result.IsOK()) {
                LOG_DEBUG(result, "Error sending transaction actions");
                THROW_ERROR result;
            }

            auto transaction = Transaction_.Lock();
            if (!transaction) {
                THROW_ERROR_EXCEPTION("Transaction is no longer available");
            }

            if (TypeFromId(CellId_) == EObjectType::TabletCell) {
                transaction->Transaction_->ConfirmParticipant(CellId_);
            }

            LOG_DEBUG("Transaction actions sent successfully");
        }
    };

    //! Maintains per-cell commit info.
    yhash<TCellId, TCellCommitSessionPtr> CellIdToSession_;

    //! Maps replica cluster name to sync replica transaction.
    yhash<TString, ITransactionPtr> ClusterNameToSyncReplicaTransaction_;

    //! Caches mappings from name table ids to schema ids.
    yhash<std::pair<TNameTablePtr, ETableSchemaKind>, TNameTableToSchemaIdMapping> IdMappingCache_;


    const TNameTableToSchemaIdMapping& GetColumnIdMapping(
        const TTableMountInfoPtr& tableInfo,
        const TNameTablePtr& nameTable,
        ETableSchemaKind kind)
    {
        auto key = std::make_pair(nameTable, kind);
        auto it = IdMappingCache_.find(key);
        if (it == IdMappingCache_.end()) {
            auto mapping = BuildColumnIdMapping(tableInfo->Schemas[kind], nameTable);
            it = IdMappingCache_.insert(std::make_pair(key, std::move(mapping))).first;
        }
        return it->second;
    }

    ITransactionPtr GetSyncReplicaTransaction(
        const TTableReplicaInfoPtr& replicaInfo,
        bool* clusterDirectorySynched)
    {
        auto it = ClusterNameToSyncReplicaTransaction_.find(replicaInfo->ClusterName);
        if (it != ClusterNameToSyncReplicaTransaction_.end()) {
            return it->second;
        }

        const auto& clusterDirectory = Client_->Connection_->GetClusterDirectory();
        auto connection = clusterDirectory->FindConnection(replicaInfo->ClusterName);
        if (!connection) {
            if (!*clusterDirectorySynched) {
                LOG_DEBUG("Replica cluster is not known; synchronizing cluster directory");
                WaitFor(Client_->Connection_->SyncClusterDirectory())
                    .ThrowOnError();
                *clusterDirectorySynched = true;
            }
            connection = clusterDirectory->GetConnectionOrThrow(replicaInfo->ClusterName);
        }

        auto client = connection->CreateClient(Client_->Options_);

        TForeignTransactionStartOptions options;
        options.InheritStartTimestamp = true;
        auto transaction = WaitFor(StartForeignTransaction(client, options))
            .ValueOrThrow();

        YCHECK(ClusterNameToSyncReplicaTransaction_.emplace(replicaInfo->ClusterName, transaction).second);

        LOG_DEBUG("Sync replica transaction started (ClusterName: %v)",
            replicaInfo->ClusterName);

        return transaction;
    }

    TTableCommitSessionPtr GetOrCreateTableSession(const TYPath& path, const TTableReplicaId& upstreamReplicaId)
    {
        auto it = TablePathToSession_.find(path);
        if (it == TablePathToSession_.end()) {
            const auto& tableMountCache = Client_->Connection_->GetTableMountCache();
            auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
                .ValueOrThrow();

            it = TablePathToSession_.emplace(
                path,
                New<TTableCommitSession>(this, std::move(tableInfo), upstreamReplicaId)
            ).first;
        } else {
            const auto& session = it->second;
            if (session->GetUpstreamReplicaId() != upstreamReplicaId) {
                THROW_ERROR_EXCEPTION("Mismatched upstream replica is specified for modifications to table %v: %v != !v",
                    path,
                    upstreamReplicaId,
                    session->GetUpstreamReplicaId());
            }
        }
        return it->second;
    }

    TTabletCommitSessionPtr GetOrCreateTabletSession(
        const TTabletInfoPtr& tabletInfo,
        const TTableMountInfoPtr& tableInfo,
        const TTableCommitSessionPtr& tableSession)
    {
        const auto& tabletId = tabletInfo->TabletId;
        auto it = TabletIdToSession_.find(tabletId);
        if (it == TabletIdToSession_.end()) {
            auto evaluatorCache = Client_->GetNativeConnection()->GetColumnEvaluatorCache();
            auto evaluator = evaluatorCache->Find(tableInfo->Schemas[ETableSchemaKind::Primary]);
            it = TabletIdToSession_.emplace(
                tabletId,
                New<TTabletCommitSession>(
                    this,
                    tabletInfo,
                    tableInfo,
                    tableSession,
                    evaluator)
                ).first;
        }
        return it->second;
    }

    TFuture<void> SendRequests()
    {
        for (const auto& request : Requests_) {
            request->PrepareTableSessions();
        }

        bool clusterDirectorySynched = false;
        for (const auto& pair : TablePathToSession_) {
            const auto& tableSession = pair.second;
            tableSession->RegisterSyncReplicas(&clusterDirectorySynched);
        }

        for (const auto& request : Requests_) {
            request->SubmitRows();
        }

        for (const auto& pair : TabletIdToSession_) {
            const auto& tabletSession = pair.second;
            const auto& cellId = tabletSession->GetCellId();
            int requestCount = tabletSession->Prepare();
            auto cellSession = GetOrCreateCellCommitSession(cellId);
            cellSession->RegisterRequests(requestCount);
        }

        for (auto& pair : CellIdToSession_) {
            const auto& cellId = pair.first;
            Transaction_->RegisterParticipant(cellId);
        }

        std::vector<TFuture<void>> asyncResults;

        for (const auto& pair : TabletIdToSession_) {
            const auto& session = pair.second;
            const auto& cellId = session->GetCellId();
            auto channel = Client_->GetCellChannelOrThrow(cellId);
            asyncResults.push_back(session->Invoke(std::move(channel)));
        }

        for (auto& pair : CellIdToSession_) {
            const auto& cellId = pair.first;
            const auto& session = pair.second;
            auto channel = Client_->GetCellChannelOrThrow(cellId);
            asyncResults.push_back(session->Invoke(std::move(channel)));
        }

        return Combine(asyncResults);
    }

    TTransactionCommitOptions AdjustCommitOptions(TTransactionCommitOptions options)
    {
        for (const auto& pair : TablePathToSession_) {
            const auto& session = pair.second;
            if (session->GetInfo()->IsReplicated()) {
                options.Force2PC = true;
            }
            if (!session->SyncReplicas().empty()) {
                options.CoordinatorCellTag = Client_->Connection_->GetPrimaryMasterCellTag();
            }
        }
        return options;
    }

    TTransactionCommitResult DoCommit(const TTransactionCommitOptions& options)
    {
        try {
            std::vector<TFuture<void>> asyncRequestResults{
                SendRequests()
            };

            std::vector<TFuture<TTransactionFlushResult>> asyncFlushResults;
            for (const auto& transaction : GetForeignTransactions()) {
                asyncFlushResults.push_back(transaction->Flush());
            }

            auto flushResults = WaitFor(Combine(asyncFlushResults))
                .ValueOrThrow();

            for (const auto& flushResult : flushResults) {
                asyncRequestResults.push_back(flushResult.AsyncResult);
                for (const auto& cellId : flushResult.ParticipantCellIds) {
                    Transaction_->RegisterParticipant(cellId);
                    Transaction_->ConfirmParticipant(cellId);
                }
            }

            WaitFor(Combine(asyncRequestResults))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            // Fire and forget.
            Transaction_->Abort();
            for (const auto& transaction : GetForeignTransactions()) {
                transaction->Abort();
            }
            throw;
        }

        auto commitResult = WaitFor(Transaction_->Commit(AdjustCommitOptions(options)))
            .ValueOrThrow();

        return commitResult;
    }

    TTransactionFlushResult DoFlush()
    {
        auto asyncResult = SendRequests();
        asyncResult.Subscribe(BIND([transaction = Transaction_] (const TError& error) {
            if (!error.IsOK()) {
                transaction->Abort();
            }
        }));

        TTransactionFlushResult result;
        result.AsyncResult = asyncResult;
        result.ParticipantCellIds = GetKeys(CellIdToSession_);
        return result;
    }


    TCellCommitSessionPtr GetOrCreateCellCommitSession(const TCellId& cellId)
    {
        auto it = CellIdToSession_.find(cellId);
        if (it == CellIdToSession_.end()) {
            it = CellIdToSession_.emplace(cellId, New<TCellCommitSession>(this, cellId)).first;
        }
        return it->second;
    }

    TCellCommitSessionPtr GetCommitSession(const TCellId& cellId)
    {
        auto it = CellIdToSession_.find(cellId);
        YCHECK(it != CellIdToSession_.end());
        return it->second;
    }


    TTimestamp GetReadTimestamp() const
    {
        switch (Transaction_->GetAtomicity()) {
            case EAtomicity::Full:
                return GetStartTimestamp();
            case EAtomicity::None:
                // NB: Start timestamp is approximate.
                return SyncLastCommittedTimestamp;
            default:
                Y_UNREACHABLE();
        }
    }

    void ValidateTabletTransaction()
    {
        if (TypeFromId(GetId()) == EObjectType::NestedTransaction) {
            THROW_ERROR_EXCEPTION("Nested master transactions cannot be used for updating dynamic tables");
        }
    }


    void RegisterForeignTransaction(ITransactionPtr transaction)
    {
        auto guard = Guard(ForeignTransactionsLock_);
        ForeignTransactions_.emplace_back(std::move(transaction));
    }

    std::vector<ITransactionPtr> GetForeignTransactions()
    {
        auto guard = Guard(ForeignTransactionsLock_);
        return ForeignTransactions_;
    }
};

DEFINE_REFCOUNTED_TYPE(TNativeTransaction)

TFuture<INativeTransactionPtr> TNativeClient::StartNativeTransaction(
    ETransactionType type,
    const TTransactionStartOptions& options)
{
    return TransactionManager_->Start(type, options).Apply(
        BIND([=, this_ = MakeStrong(this)] (const NTransactionClient::TTransactionPtr& transaction) -> INativeTransactionPtr {
            auto wrappedTransaction = New<TNativeTransaction>(this_, transaction);
            if (options.Sticky) {
                Connection_->RegisterStickyTransaction(wrappedTransaction);
            }
            return wrappedTransaction;
        }));
}

INativeTransactionPtr TNativeClient::AttachNativeTransaction(
    const TTransactionId& transactionId,
    const TTransactionAttachOptions& options)
{
    if (options.Sticky) {
        return Connection_->GetStickyTransaction(transactionId);
    } else {
        auto nativeTransaction = TransactionManager_->Attach(transactionId, options);
        return New<TNativeTransaction>(this, std::move(nativeTransaction));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
