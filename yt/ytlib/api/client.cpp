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

#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/security_client/group_ypath_proxy.h>

#include <ytlib/driver/dispatcher.h>

#include <ytlib/new_table_client/name_table.h>

#include <ytlib/hive/config.h>
#include <ytlib/hive/cell_directory.h>

#include <ytlib/hydra/rpc_helpers.h>

#include <ytlib/new_table_client/schemaful_writer.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/executor.h>

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
using namespace NTableClient;
using namespace NTabletClient;
using namespace NSecurityClient;
using namespace NHydra;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClient)
DECLARE_REFCOUNTED_CLASS(TTransaction)

////////////////////////////////////////////////////////////////////////////////

namespace {

TWireProtocolWriter::TColumnIdMapping BuildColumnIdMapping(
    const TTableMountInfoPtr& tableInfo,
    const TNameTablePtr& nameTable)
{
    for (const auto& name : tableInfo->KeyColumns) {
        if (!nameTable->FindId(name)) {
            THROW_ERROR_EXCEPTION("Missing key column %s in name table",
                ~name.Quote());
        }
    }

    TWireProtocolWriter::TColumnIdMapping mapping;
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

class TClient
    : public IClient
{
public:
    TClient(
        IConnectionPtr connection,
        const TClientOptions& options)
        : Connection_(std::move(connection))
        , Invoker_(NDriver::TDispatcher::Get()->GetLightInvoker())
    {
        MasterChannel_ = Connection_->GetMasterChannel();
        SchedulerChannel_ = Connection_->GetSchedulerChannel();

        if (options.User) {
            MasterChannel_ = CreateAuthenticatedChannel(MasterChannel_, *options.User);
            SchedulerChannel_ = CreateAuthenticatedChannel(SchedulerChannel_, *options.User);
        }

        MasterChannel_ = CreateScopedChannel(MasterChannel_);
        SchedulerChannel_ = CreateScopedChannel(SchedulerChannel_);
            
        TransactionManager_ = New<TTransactionManager>(
            Connection_->GetConfig()->TransactionManager,
            Connection_->GetConfig()->Master->CellGuid,
            MasterChannel_,
            Connection_->GetTimestampProvider(),
            Connection_->GetCellDirectory());

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

    virtual TTransactionManagerPtr GetTransactionManager() override
    {
        return TransactionManager_;
    }


    virtual TFuture<void> Terminate() override
    {
        TransactionManager_->AbortAll();

        TError error("Client terminated");
        auto awaiter = New<TParallelAwaiter>(GetSyncInvoker());
        awaiter->Await(MasterChannel_->Terminate(error));
        awaiter->Await(SchedulerChannel_->Terminate(error));
        return awaiter->Complete();
    }


    virtual TFuture<TErrorOr<ITransactionPtr>> StartTransaction(
        ETransactionType type,
        const TTransactionStartOptions& options) override;

#define DROP_BRACES(...) __VA_ARGS__
#define IMPLEMENT_METHOD(returnType, method, signature, args) \
    virtual TFuture<TErrorOr<returnType>> method signature override \
    { \
        return Execute<returnType>(BIND( \
            &TClient::Do ## method, \
            MakeStrong(this), \
            DROP_BRACES args)); \
    }

    virtual TFuture<TErrorOr<IRowsetPtr>> LookupRow(
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

    virtual TFuture<TErrorOr<std::pair<IRowsetPtr, TQueryStatistics>>> SelectRows(
        const Stroka& query,
        const TSelectRowsOptions& options) override
    {
        auto result = NewPromise<TErrorOr<std::pair<IRowsetPtr, TQueryStatistics>>>();

        ISchemafulWriterPtr writer;
        TPromise<TErrorOr<IRowsetPtr>> rowset;
        std::tie(writer, rowset) = CreateSchemafulRowsetWriter();

        SelectRows(query, writer, options).Subscribe(BIND([=] (TErrorOr<TQueryStatistics> error) mutable {
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

private:
    friend class TTransaction;

    IConnectionPtr Connection_;

    IChannelPtr MasterChannel_;
    IChannelPtr SchedulerChannel_;
    TTransactionManagerPtr TransactionManager_;
    std::unique_ptr<TObjectServiceProxy> ObjectProxy_;
    IInvokerPtr Invoker_;


    template <class TResult, class TSignature>
    TFuture<TErrorOr<TResult>> Execute(TCallback<TSignature> callback)
    {
        return callback
            .Guarded()
            .AsyncVia(Invoker_)
            .Run();
    }


    TTableMountInfoPtr SyncGetTableInfo(const TYPath& path)
    {
        const auto& tableMountCache = Connection_->GetTableMountCache();
        auto tableInfoOrError = WaitFor(tableMountCache->GetTableInfo(path));
        THROW_ERROR_EXCEPTION_IF_FAILED(tableInfoOrError);
        return tableInfoOrError.Value();
    }

    static TTabletInfoPtr SyncGetTabletInfo(
        TTableMountInfoPtr tableInfo,
        NVersionedTableClient::TKey key)
    {
        auto tabletInfo = tableInfo->GetTablet(key);
        if (tabletInfo->State != ETabletState::Mounted) {
            THROW_ERROR_EXCEPTION("Tablet %s of table %s is in %s state",
                ~ToString(tabletInfo->TabletId),
                ~tableInfo->Path,
                ~FormatEnum(tabletInfo->State).Quote());
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
            commandOptions.MutationId = NHydra::GenerateMutationId();
        }
        auto result = commandOptions.MutationId;
        ++commandOptions.MutationId.Parts[0];
        return result;
    }
    

    TTransactionId GetTransactionId(const TTransactionalOptions& commandOptions, bool allowNullTransaction)
    {
        auto transaction = GetTransaction(commandOptions, allowNullTransaction, true);
        return transaction ? transaction->GetId() : NullTransactionId;
    }

    NTransactionClient::TTransactionPtr GetTransaction(const TTransactionalOptions& commandOptions, bool allowNullTransaction, bool pingTransaction)
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

    void SetTransactionId(IClientRequestPtr request, const TTransactionalOptions& commandOptions, bool allowNullTransaction)
    {
        NCypressClient::SetTransactionId(request, GetTransactionId(commandOptions, allowNullTransaction));
    }


    static void SetSuppressAccessTracking(IClientRequestPtr request, const TSuppressableAccessTrackingOptions& commandOptions)
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
            const TWireProtocolWriter::TColumnIdMapping& idMapping)
            : Config_(owner->Connection_->GetConfig())
            , TabletId_(tabletInfo->TabletId)
            , Options_(options)
            , IdMapping_(idMapping)
        { }

        void AddKey(int index, NVersionedTableClient::TKey key)
        {
            if (Batches_.empty() || Batches_.back()->Indexes.size() >= Config_->MaxRowsPerRead) {
                Batches_.emplace_back(new TBatch());
            }

            auto& batch = Batches_.back();
            batch->Indexes.push_back(index);
            batch->Keys.push_back(key);
        }

        TAsyncError Invoke(IChannelPtr channel)
        {
            // Do all the heavy lifting here.
            for (auto& batch : Batches_) {
                TWireProtocolWriter writer;
                writer.WriteCommand(EWireProtocolCommand::LookupRows);
                writer.WriteColumnFilter(Options_.ColumnFilter);
                writer.WriteUnversionedRowset(batch->Keys, &IdMapping_);
                writer.WriteCommand(EWireProtocolCommand::End);
                batch->RequestData = NCompression::CompressWithEnvelope(
                    writer.Flush(),
                    Config_->LookupRequestCodec);
            }

            InvokeChannel_ = channel;
            InvokeNextBatch();
            return InvokePromise_;
        }

        void ParseResponse(
            std::vector<TUnversionedRow>* pooledRows,
            std::vector<TUnversionedRow>* resultRows,
            std::vector<std::unique_ptr<TWireProtocolReader>>* readers)
        {
            for (const auto& batch : Batches_) {
                auto data = NCompression::DecompressWithEnvelope(batch->Response->Attachments());
                auto reader = std::make_unique<TWireProtocolReader>(data);
                    
                pooledRows->clear();
                reader->ReadUnversionedRowset(pooledRows);

                for (int index = 0; index < static_cast<int>(pooledRows->size()); ++index) {
                    (*resultRows)[batch->Indexes[index]] = (*pooledRows)[index];
                }

                readers->push_back(std::move(reader));
            }
        }

    private:
        TConnectionConfigPtr Config_;
        TTabletId TabletId_;
        TLookupRowsOptions Options_;
        TWireProtocolWriter::TColumnIdMapping IdMapping_;

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
        TAsyncErrorPromise InvokePromise_ = NewPromise<TError>();


        void InvokeNextBatch()
        {
            if (InvokeBatchIndex_ >= Batches_.size()) {
                InvokePromise_.Set(TError());
                return;
            }

            const auto& batch = Batches_[InvokeBatchIndex_];

            TTabletServiceProxy proxy(InvokeChannel_);
            auto req = proxy.Read();
            ToProto(req->mutable_tablet_id(), TabletId_);
            req->set_timestamp(Options_.Timestamp);
            req->Attachments() = std::move(batch->RequestData);

            req->Invoke().Subscribe(
                BIND(&TTabletLookupSession::OnResponse, MakeStrong(this)));
        }

        void OnResponse(TTabletServiceProxy::TRspReadPtr rsp)
        {
            if (rsp->IsOK()) {
                Batches_[InvokeBatchIndex_]->Response = rsp;
                ++InvokeBatchIndex_;
                InvokeNextBatch();
            } else {
                InvokePromise_.Set(rsp->GetError());
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
        auto resultSchema = tableInfo->Schema.Filter(options.ColumnFilter);
        int keyColumnCount = static_cast<int>(tableInfo->KeyColumns.size());
        auto idMapping = BuildColumnIdMapping(tableInfo, nameTable);

        yhash_map<TTabletInfoPtr, TLookupTabletSessionPtr> tabletToSession;
                
        for (int index = 0; index < static_cast<int>(keys.size()); ++index) {
            auto key = keys[index];
            ValidateClientKey(key, keyColumnCount);
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

        const auto& cellDirectory = Connection_->GetCellDirectory();
        auto collector = New<TParallelCollector<void>>();

        for (const auto& pair : tabletToSession) {
            const auto& tabletInfo = pair.first;
            const auto& session = pair.second;
            auto channel = cellDirectory->GetChannelOrThrow(tabletInfo->CellId);
            collector->Collect(session->Invoke(channel));
        }

        {
            auto result = WaitFor(collector->Complete());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        std::vector<TUnversionedRow> resultRows;
        resultRows.resize(keys.size());
        
        std::vector<TUnversionedRow> pooledRows;
        pooledRows.reserve(keys.size());

        std::vector<std::unique_ptr<TWireProtocolReader>> readers;

        for (const auto& pair : tabletToSession) {
            const auto& session = pair.second;
            session->ParseResponse(
                &pooledRows,
                &resultRows,
                &readers);
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
        auto fragment = TPlanFragment::Prepare(
            Connection_->GetQueryPrepareCallbacks(),
            query,
            options.InputRowLimit.Get(Connection_->GetConfig()->DefaultInputRowLimit),
            options.OutputRowLimit.Get(Connection_->GetConfig()->DefaultOutputRowLimit),
            options.Timestamp);

        auto executor = Connection_->GetQueryExecutor();
        auto error = WaitFor(executor->Execute(fragment, writer));
        THROW_ERROR_EXCEPTION_IF_FAILED(error);

        return error.Value();
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
            req->set_first_tablet_index(*options.LastTabletIndex);
        }

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
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

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
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

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
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

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
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

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        return TYsonString(rsp->value());
    }

    void DoSetNode(
        const TYPath& path,
        const TYsonString& value,
        TSetNodeOptions options)
    {
        auto req = TYPathProxy::Set(path);
        SetTransactionId(req, options, true);
        GenerateMutationId(req, options);
        req->set_value(value.Data());

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
    }

    void DoRemoveNode(
        const TYPath& path,
        TRemoveNodeOptions options)
    {
        auto req = TYPathProxy::Remove(path);
        SetTransactionId(req, options, true);
        GenerateMutationId(req, options);
        req->set_recursive(options.Recursive);
        req->set_force(options.Force);

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
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

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        return TYsonString(rsp->keys());
    }

    TNodeId DoCreateNode(
        const TYPath& path,
        EObjectType type,
        TCreateNodeOptions options)
    {
        auto req = TCypressYPathProxy::Create(path);
        SetTransactionId(req, options, true);
        GenerateMutationId(req, options);
        req->set_type(type);
        req->set_recursive(options.Recursive);
        req->set_ignore_existing(options.IgnoreExisting);

        if (options.Attributes) {
            ToProto(req->mutable_node_attributes(), *options.Attributes);
        }

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        return FromProto<TNodeId>(rsp->node_id());
    }

    TLockId DoLockNode(
        const TYPath& path,
        ELockMode mode,
        TLockNodeOptions options)
    {
        auto lockReq = TCypressYPathProxy::Lock(path);
        SetTransactionId(lockReq, options, false);
        GenerateMutationId(lockReq, options);
        lockReq->set_mode(mode);
        lockReq->set_waitable(options.Waitable);

        auto lockRsp = WaitFor(ObjectProxy_->Execute(lockReq));
        THROW_ERROR_EXCEPTION_IF_FAILED(*lockRsp);

        return FromProto<TLockId>(lockRsp->lock_id());
    }

    TNodeId DoCopyNode(
        const TYPath& srcPath,
        const TYPath& dstPath,
        TCopyNodeOptions options)
    {
        auto req = TCypressYPathProxy::Copy(dstPath);
        SetTransactionId(req, options, true);
        GenerateMutationId(req, options);
        req->set_source_path(srcPath);
        req->set_preserve_account(options.PreserveAccount);

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        return FromProto<TNodeId>(rsp->object_id());
    }

    TNodeId DoMoveNode(
        const TYPath& srcPath,
        const TYPath& dstPath,
        TMoveNodeOptions options)
    {
        auto req = TCypressYPathProxy::Copy(dstPath);
        SetTransactionId(req, options, true);
        GenerateMutationId(req, options);
        req->set_source_path(srcPath);
        req->set_remove_source(true);

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        return FromProto<TNodeId>(rsp->object_id());
    }

    TNodeId DoLinkNode(
        const TYPath& srcPath,
        const TYPath& dstPath,
        TLinkNodeOptions options)
    {
        auto req = TCypressYPathProxy::Create(dstPath);
        req->set_type(EObjectType::Link);
        req->set_recursive(options.Recursive);
        req->set_ignore_existing(options.IgnoreExisting);
        SetTransactionId(req, options, true);
        GenerateMutationId(req, options);

        auto attributes = options.Attributes ? ConvertToAttributes(options.Attributes) : CreateEphemeralAttributes();
        attributes->Set("target_path", srcPath);
        ToProto(req->mutable_node_attributes(), *attributes);

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        return FromProto<TNodeId>(rsp->node_id());
    }

    bool DoNodeExists(
        const TYPath& path,
        const TNodeExistsOptions& options)
    {
        auto req = TYPathProxy::Exists(path);
        SetTransactionId(req, options, true);

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        return rsp->value();
    }


    TObjectId DoCreateObject(
        EObjectType type,
        TCreateObjectOptions options)
    {
        auto req = TMasterYPathProxy::CreateObjects();
        GenerateMutationId(req, options);
        if (options.TransactionId != NullTransactionId) {
            ToProto(req->mutable_transaction_id(), options.TransactionId);
        }
        req->set_type(type);
        if (options.Attributes) {
            ToProto(req->mutable_object_attributes(), *options.Attributes);
        }

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

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

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
    }

    void DoRemoveMember(
        const Stroka& group,
        const Stroka& member,
        TRemoveMemberOptions options)
    {
        auto req = TGroupYPathProxy::RemoveMember(GetGroupPath(group));
        req->set_name(member);
        GenerateMutationId(req, options);

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
    }

    TCheckPermissionResult DoCheckPermission(
        const Stroka& user,
        const TYPath& path,
        EPermission permission,
        TCheckPermissionOptions options)
    {
        auto req = TObjectYPathProxy::CheckPermission(path);
        req->set_user(user);
        req->set_permission(permission);
        SetTransactionId(req, options, true);

        auto rsp = WaitFor(ObjectProxy_->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

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


    virtual TAsyncError Commit() override
    {
        return BIND(&TTransaction::DoCommit, MakeStrong(this))
            .Guarded()
            .AsyncVia(Client_->Invoker_)
            .Run();
    }

    virtual TAsyncError Abort() override
    {
        return Transaction_->Abort();
    }


    virtual TFuture<TErrorOr<ITransactionPtr>> StartTransaction(
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
        TUnversionedRow row) override
    {
        WriteRows(
            path,
            std::move(nameTable),
            std::vector<TUnversionedRow>(1, row));
    }

    virtual void WriteRows(
        const TYPath& path,
        TNameTablePtr nameTable,
        std::vector<TUnversionedRow> rows) override
    {
        Requests_.push_back(std::unique_ptr<TRequestBase>(new TWriteRequest(
            this,
            path,
            std::move(nameTable),
            std::move(rows))));
    }


    virtual void DeleteRow(
        const TYPath& path,
        TNameTablePtr nameTable,
        NVersionedTableClient::TKey key) override
    {
        DeleteRows(
            path,
            std::move(nameTable),
            std::vector<NVersionedTableClient::TKey>(1, key));
    }

    virtual void DeleteRows(
        const TYPath& path,
        TNameTablePtr nameTable,
        std::vector<NVersionedTableClient::TKey> keys) override
    {
        Requests_.push_back(std::unique_ptr<TRequestBase>(new TDeleteRequest(
            this,
            path,
            std::move(nameTable),
            std::move(keys))));
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

    DELEGATE_TIMESTAMPTED_METHOD(TFuture<TErrorOr<IRowsetPtr>>, LookupRow, (
        const TYPath& path,
        TNameTablePtr nameTable,
        NVersionedTableClient::TKey key,
        const TLookupRowsOptions& options),
        (path, nameTable, key, options))
    DELEGATE_TIMESTAMPTED_METHOD(TFuture<TErrorOr<IRowsetPtr>>, LookupRows, (
        const TYPath& path,
        TNameTablePtr nameTable,
        const std::vector<NVersionedTableClient::TKey>& keys,
        const TLookupRowsOptions& options),
        (path, nameTable, keys, options))
    DELEGATE_TIMESTAMPTED_METHOD(TFuture<TErrorOr<NQueryClient::TQueryStatistics>>, SelectRows, (
        const Stroka& query,
        ISchemafulWriterPtr writer,
        const TSelectRowsOptions& options),
        (query, writer, options))

    typedef std::pair<IRowsetPtr, NQueryClient::TQueryStatistics> TSelectRowsResult;
    DELEGATE_TIMESTAMPTED_METHOD(TFuture<TErrorOr<TSelectRowsResult>>, SelectRows, (
        const Stroka& query,
        const TSelectRowsOptions& options),
        (query, options))


    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TErrorOr<TYsonString>>, GetNode, (
        const TYPath& path,
        const TGetNodeOptions& options),
        (path, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TError>, SetNode, (
        const TYPath& path,
        const TYsonString& value,
        const TSetNodeOptions& options),
        (path, value, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TError>, RemoveNode, (
        const TYPath& path,
        const TRemoveNodeOptions& options),
        (path, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TErrorOr<TYsonString>>, ListNodes, (
        const TYPath& path,
        const TListNodesOptions& options),
        (path, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TErrorOr<TNodeId>>, CreateNode, (
        const TYPath& path,
        EObjectType type,
        const TCreateNodeOptions& options),
        (path, type, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TErrorOr<TLockId>>, LockNode, (
        const TYPath& path,
        ELockMode mode,
        const TLockNodeOptions& options),
        (path, mode, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TErrorOr<TNodeId>>, CopyNode, (
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TCopyNodeOptions& options),
        (srcPath, dstPath, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TErrorOr<TNodeId>>, MoveNode, (
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TMoveNodeOptions& options),
        (srcPath, dstPath, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TErrorOr<TNodeId>>, LinkNode, (
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TLinkNodeOptions& options),
        (srcPath, dstPath, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TErrorOr<bool>>, NodeExists, (
        const TYPath& path,
        const TNodeExistsOptions& options),
        (path, options))


    DELEGATE_TRANSACTIONAL_METHOD(TFuture<TErrorOr<TObjectId>>, CreateObject, (
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
            std::vector<TUnversionedRow> rows)
            : TRequestBase(transaction, path, std::move(nameTable))
            , Rows_(std::move(rows))
        { }

        virtual void Run() override
        {
            TRequestBase::Run();

            const auto& idMapping = Transaction_->GetColumnIdMapping(TableInfo_, NameTable_);
            int keyColumnCount = static_cast<int>(TableInfo_->KeyColumns.size());
            for (auto row : Rows_) {
                ValidateClientDataRow(row, keyColumnCount);
                auto tabletInfo = Transaction_->Client_->SyncGetTabletInfo(TableInfo_, row);
                auto* writer = Transaction_->GetTabletWriter(tabletInfo);
                writer->WriteCommand(EWireProtocolCommand::WriteRow);
                writer->WriteUnversionedRow(row, &idMapping);
            }
        }

    private:
        std::vector<TUnversionedRow> Rows_;

    };

    class TDeleteRequest
        : public TRequestBase
    {
    public:
        TDeleteRequest(
            TTransaction* transaction,
            const TYPath& path,
            TNameTablePtr nameTable,
            std::vector<NVersionedTableClient::TKey> keys)
            : TRequestBase(transaction, path, std::move(nameTable))
            , Keys_(std::move(keys))
        { }

        virtual void Run() override
        {
            TRequestBase::Run();

            const auto& idMapping = Transaction_->GetColumnIdMapping(TableInfo_, NameTable_);
            int keyColumnCount = static_cast<int>(TableInfo_->KeyColumns.size());
            for (auto key : Keys_) {
                ValidateClientKey(key, keyColumnCount);
                auto tabletInfo = Transaction_->Client_->SyncGetTabletInfo(TableInfo_, key);
                auto* writer = Transaction_->GetTabletWriter(tabletInfo);
                writer->WriteCommand(EWireProtocolCommand::DeleteRow);
                writer->WriteUnversionedRow(key, &idMapping);
            }
        }

    private:
        std::vector<TUnversionedRow> Keys_;

    };

    std::vector<std::unique_ptr<TRequestBase>> Requests_;

    class TTabletCommitSession
        : public TIntrinsicRefCounted
    {
    public:
        TTabletCommitSession(
            TTransaction* owner,
            TTabletInfoPtr tabletInfo)
            : TransactionId_(owner->Transaction_->GetId())
            , TabletId_(tabletInfo->TabletId)
            , Config_(owner->Client_->Connection_->GetConfig())
        { }
        
        TWireProtocolWriter* GetWriter()
        {
            if (Batches_.empty() || CurrentRowCount_ >= Config_->MaxRowsPerWrite) {
                Batches_.emplace_back(new TBatch());
                CurrentRowCount_ = 0;
            }
            ++CurrentRowCount_;
            return &Batches_.back()->Writer;
        }

        TAsyncError Invoke(IChannelPtr channel)
        {
            // Do all the heavy lifting here.
            for (auto& batch : Batches_) {
                batch->Writer.WriteCommand(EWireProtocolCommand::End);
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
        TAsyncErrorPromise InvokePromise_ = NewPromise<TError>();


        void InvokeNextBatch()
        {
            if (InvokeBatchIndex_ >= Batches_.size()) {
                InvokePromise_.Set(TError());
                return;
            }

            const auto& batch = Batches_[InvokeBatchIndex_];

            TTabletServiceProxy tabletProxy(InvokeChannel_);
            auto req = tabletProxy.Write();
            ToProto(req->mutable_transaction_id(), TransactionId_);
            ToProto(req->mutable_tablet_id(), TabletId_);
            req->Attachments() = std::move(batch->RequestData);

            req->Invoke().Subscribe(
                BIND(&TTabletCommitSession::OnResponse, MakeStrong(this)));
        }

        void OnResponse(TTabletServiceProxy::TRspWritePtr rsp)
        {
            if (rsp->IsOK()) {
                ++InvokeBatchIndex_;
                InvokeNextBatch();
            } else {
                InvokePromise_.Set(rsp->GetError());
            }
        }

    };

    typedef TIntrusivePtr<TTabletCommitSession> TTabletSessionPtr;

    yhash_map<TTabletInfoPtr, TTabletSessionPtr> TabletToSession_;
    
    TIntrusivePtr<TParallelCollector<void>> TransactionStartCollector_;

    // Maps ids from name table to schema, for each involved name table.
    typedef TWireProtocolWriter::TColumnIdMapping TColumnIdMapping;
    yhash_map<TNameTablePtr, TWireProtocolWriter::TColumnIdMapping> NameTableToIdMapping_;


    const TColumnIdMapping& GetColumnIdMapping(const TTableMountInfoPtr& tableInfo, const TNameTablePtr& nameTable)
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

    void DoCommit()
    {
        for (const auto& request : Requests_) {
            request->Run();
        }

        {
            auto result = WaitFor(TransactionStartCollector_->Complete());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        auto cellDirectory = Client_->Connection_->GetCellDirectory();

        auto writeCollector = New<TParallelCollector<void>>();

        for (const auto& pair : TabletToSession_) {
            const auto& tabletInfo = pair.first;
            const auto& session = pair.second;
            auto channel = cellDirectory->GetChannelOrThrow(tabletInfo->CellId);
            writeCollector->Collect(session->Invoke(std::move(channel)));
        }

        {
            auto result = WaitFor(writeCollector->Complete());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        {
            auto result = WaitFor(Transaction_->Commit());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
    }

};

DEFINE_REFCOUNTED_TYPE(TTransaction)

TFuture<TErrorOr<ITransactionPtr>> TClient::StartTransaction(
    ETransactionType type,
    const TTransactionStartOptions& options)
{
    auto this_ = MakeStrong(this);
    return TransactionManager_->Start(type, options).Apply(
        BIND([=] (TErrorOr<NTransactionClient::TTransactionPtr> transactionOrError) -> TErrorOr<ITransactionPtr> {
            if (!transactionOrError.IsOK()) {
                return TError(transactionOrError);
            }
            return TErrorOr<ITransactionPtr>(New<TTransaction>(this_, transactionOrError.Value()));
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

