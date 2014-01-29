#include "stdafx.h"
#include "client.h"
#include "connection.h"
#include "file_reader.h"
#include "file_writer.h"
#include "config.h"

#include <core/concurrency/fiber.h>
#include <core/concurrency/parallel_collector.h>
#include <core/concurrency/parallel_awaiter.h>

#include <core/ytree/attribute_helpers.h>
#include <core/ytree/ypath_proxy.h>

#include <core/rpc/helpers.h>
#include <core/rpc/scoped_channel.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/tablet_client/protocol.h>
#include <ytlib/tablet_client/table_mount_cache.h>
#include <ytlib/tablet_client/tablet_service_proxy.h>

#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/security_client/group_ypath_proxy.h>

#include <ytlib/driver/dispatcher.h>

#include <ytlib/new_table_client/name_table.h>

#include <ytlib/hive/cell_directory.h>
#include <ytlib/hive/timestamp_provider.h>

#include <ytlib/hydra/rpc_helpers.h>

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

////////////////////////////////////////////////////////////////////////////////

class TRowset;
typedef TIntrusivePtr<TRowset> TRowsetPtr;

class TTransaction;
typedef TIntrusivePtr<TTransaction> TTransactionPtr;

class TClient;
typedef TIntrusivePtr<TClient> TClientPtr;

////////////////////////////////////////////////////////////////////////////////

class TRowset
    : public IRowset
{
public:
    TRowset(std::unique_ptr<TProtocolReader> reader, std::vector<TUnversionedRow> rows)
        : Reader_(std::move(reader))
        , Rows_(std::move(rows))
    { }

    const std::vector<TUnversionedRow>& Rows() const
    {
        return Rows_;
    }

private:
    std::unique_ptr<TProtocolReader> Reader_;
    std::vector<TUnversionedRow> Rows_;

};

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
{
public:
    TClient(
        IConnectionPtr connection,
        const TClientOptions& options)
        : Connection_(std::move(connection))
        // TODO(babenko): consider using pool
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
            Connection_->GetConfig()->Masters->CellGuid,
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
        const TTransactionStartOptions& options) override;

#define DROP_BRACES(...) __VA_ARGS__
#define IMPLEMENT_METHOD(returnType, method, signature, args) \
    virtual returnType method signature override \
    { \
        return \
            BIND( \
                &TClient::Do ## method, \
                MakeStrong(this), \
                DROP_BRACES args) \
            .AsyncVia(Invoker_) \
            .Run(); \
    }

    IMPLEMENT_METHOD(TFuture<TErrorOr<IRowsetPtr>>, LookupRow, (
        const TYPath& path,
        TKey key,
        const TLookupRowsOptions& options),
        (path, key, options))

    virtual TFuture<TErrorOr<IRowsetPtr>> LookupRows(
        const TYPath& path,
        const std::vector<NVersionedTableClient::TKey>& keys,
        const TLookupRowsOptions& options) override
    {
        YUNIMPLEMENTED();
    }

    virtual TFuture<TErrorOr<IRowsetPtr>> SelectRows(
        const TYPath& path,
        const Stroka& query,
        const TSelectRowsOptions& options) override
    {
        YUNIMPLEMENTED();
    }

    IMPLEMENT_METHOD(TAsyncError, MountTable, (
        const TYPath& path,
        const TMountTableOptions& options),
        (path, options))
    IMPLEMENT_METHOD(TAsyncError, UnmountTable, (
        const TYPath& path,
        const TUnmountTableOptions& options),
        (path, options))
    IMPLEMENT_METHOD(TAsyncError, ReshardTable, (
        const TYPath& path,
        const std::vector<TKey>& pivotKeys,
        const TReshardTableOptions& options),
        (path, pivotKeys, options))


    IMPLEMENT_METHOD(TFuture<TErrorOr<TYsonString>>, GetNode, (
        const TYPath& path,
        const TGetNodeOptions& options),
        (path, options))
    IMPLEMENT_METHOD(TFuture<TError>, SetNode, (
        const TYPath& path,
        const TYsonString& value,
        const TSetNodeOptions& options),
        (path, value, options))
    IMPLEMENT_METHOD(TFuture<TError>, RemoveNode, (
        const TYPath& path,
        const TRemoveNodeOptions& options),
        (path, options))
    IMPLEMENT_METHOD(TFuture<TErrorOr<TYsonString>>, ListNodes, (
        const TYPath& path,
        const TListNodesOptions& options),
        (path, options))
    IMPLEMENT_METHOD(TFuture<TErrorOr<TNodeId>>, CreateNode, (
        const TYPath& path,
        EObjectType type,
        const TCreateNodeOptions& options),
        (path, type, options))
    IMPLEMENT_METHOD(TFuture<TErrorOr<TLockId>>, LockNode, (
        const TYPath& path,
        ELockMode mode,
        const TLockNodeOptions& options),
        (path, mode, options))
    IMPLEMENT_METHOD(TFuture<TErrorOr<TNodeId>>, CopyNode, (
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TCopyNodeOptions& options),
        (srcPath, dstPath, options))
    IMPLEMENT_METHOD(TFuture<TErrorOr<TNodeId>>, MoveNode, (
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TMoveNodeOptions& options),
        (srcPath, dstPath, options))
    IMPLEMENT_METHOD(TFuture<TErrorOr<TNodeId>>, LinkNode, (
        const TYPath& srcPath,
        const TYPath& dstPath,
        const TLinkNodeOptions& options),
        (srcPath, dstPath, options))
    IMPLEMENT_METHOD(TFuture<TErrorOr<bool>>, NodeExists, (
        const TYPath& path,
        const TNodeExistsOptions& options),
        (path, options))


    IMPLEMENT_METHOD(TFuture<TErrorOr<TObjectId>>, CreateObject, (
        EObjectType type,
        const TCreateObjectOptions& options),
        (type, options))


    virtual IFileReaderPtr CreateFileReader(
        const TYPath& path,
        const TFileReaderOptions& options,
        TFileReaderConfigPtr config) override;

    virtual IFileWriterPtr CreateFileWriter(
        const TYPath& path,
        const TFileWriterOptions& options,
        TFileWriterConfigPtr config) override;


    IMPLEMENT_METHOD(TAsyncError, AddMember, (
        const Stroka& group,
        const Stroka& member,
        const TAddMemberOptions& options),
        (group, member, options))
    IMPLEMENT_METHOD(TAsyncError, RemoveMember, (
        const Stroka& group,
        const Stroka& member,
        const TRemoveMemberOptions& options),
        (group, member, options))

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


    TTableMountInfoPtr GetTableMountInfo(const TYPath& path)
    {
        const auto& tableMountCache = Connection_->GetTableMountCache();
        auto mountInfoOrError = WaitFor(tableMountCache->LookupInfo(path));
        THROW_ERROR_EXCEPTION_IF_FAILED(mountInfoOrError);
        return mountInfoOrError.GetValue();
    }

    static const TTabletInfo& GetTabletInfo(
        TTableMountInfoPtr mountInfo,
        const TYPath& path,
        TKey key)
    {
        if (mountInfo->Tablets.empty()) {
            THROW_ERROR_EXCEPTION("Table %s is not mounted",
                ~path);
        }
        const auto& tabletInfo = mountInfo->GetTablet(key);
        if (tabletInfo.State != ETabletState::Mounted) {
            THROW_ERROR_EXCEPTION("Tablet %s of table %s is in %s state",
                ~ToString(tabletInfo.TabletId),
                ~path,
                ~FormatEnum(tabletInfo.State).Quote());
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


    TErrorOr<IRowsetPtr> DoLookupRow(
        const TYPath& path,
        TKey key,
        TLookupRowsOptions options)
    {
        try {
            auto mountInfo = GetTableMountInfo(path);
            const auto& tabletInfo = GetTabletInfo(mountInfo, path, key);

            const auto& cellDirectory = Connection_->GetCellDirectory();
            auto channel = cellDirectory->GetChannelOrThrow(tabletInfo.CellId);

            TTabletServiceProxy proxy(channel);
            auto req = proxy.Read();

            ToProto(req->mutable_tablet_id(), tabletInfo.TabletId);
            req->set_timestamp(options.Timestamp);

            TProtocolWriter writer;
            writer.WriteCommand(EProtocolCommand::LookupRow);
            writer.WriteUnversionedRow(key);
            writer.WriteColumnFilter(options.ColumnFilter);
            req->set_encoded_request(writer.Finish());

            auto rsp = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

            std::unique_ptr<TProtocolReader> reader(new TProtocolReader(rsp->encoded_response()));
            std::vector<TUnversionedRow> rows;
            reader->ReadUnversionedRowset(&rows);

            if (rows.empty()) {
                rows.push_back(TUnversionedRow());
            }

            return TErrorOr<IRowsetPtr>(New<TRowset>(
                std::move(reader),
                std::move(rows)));
        } catch (const std::exception& ex) {
            return TError(ex);
        }
    }

    TError DoMountTable(
        const TYPath& path,
        const TMountTableOptions& options)
    {
        try {
            auto req = TTableYPathProxy::Mount(path);
            if (options.FirstTabletIndex) {
                req->set_first_tablet_index(*options.FirstTabletIndex);
            }
            if (options.LastTabletIndex) {
                req->set_first_tablet_index(*options.LastTabletIndex);
            }

            auto rsp = WaitFor(ObjectProxy_->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

            return TError();
        } catch (const std::exception& ex) {
            return TError(ex);
        }
    }

    TError DoUnmountTable(
        const TYPath& path,
        const TUnmountTableOptions& options)
    {
        try {
            auto req = TTableYPathProxy::Unmount(path);
            if (options.FirstTabletIndex) {
                req->set_first_tablet_index(*options.FirstTabletIndex);
            }
            if (options.LastTabletIndex) {
                req->set_first_tablet_index(*options.LastTabletIndex);
            }

            auto rsp = WaitFor(ObjectProxy_->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

            return TError();
        } catch (const std::exception& ex) {
            return TError(ex);
        }
    }

    TError DoReshardTable(
        const TYPath& path,
        const std::vector<TKey>& pivotKeys,
        const TReshardTableOptions& options)
    {
        try {
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

            return TError();
        } catch (const std::exception& ex) {
            return TError(ex);
        }
    }


    TErrorOr<TYsonString> DoGetNode(
        const TYPath& path,
        TGetNodeOptions options)
    {
        try {
            auto req = TYPathProxy::Get(path);
            SetTransactionId(req, options, true);
            SetSuppressAccessTracking(req, options);

            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly, options.Attributes);
            ToProto(req->mutable_attribute_filter(), attributeFilter);
            if (options.MaxSize) {
                req->set_max_size(*options.MaxSize);
            }
            if (options.Options) {
                ToProto(req->mutable_options(), *options.Options);
            }

            auto rsp = WaitFor(ObjectProxy_->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

            return TYsonString(rsp->value());
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TError DoSetNode(
        const TYPath& path,
        const TYsonString& value,
        TSetNodeOptions options)
    {
        try {
            auto req = TYPathProxy::Set(path);
            SetTransactionId(req, options, true);
            GenerateMutationId(req, options);
            req->set_value(value.Data());

            auto rsp = WaitFor(ObjectProxy_->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

            return TError();
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TError DoRemoveNode(
        const TYPath& path,
        TRemoveNodeOptions options)
    {
        try {
            auto req = TYPathProxy::Remove(path);
            SetTransactionId(req, options, true);
            GenerateMutationId(req, options);
            req->set_recursive(options.Recursive);
            req->set_force(options.Force);

            auto rsp = WaitFor(ObjectProxy_->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

            return TError();
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TErrorOr<TYsonString> DoListNodes(
        const TYPath& path,
        TListNodesOptions options)
    {
        try {
            auto req = TYPathProxy::List(path);
            SetTransactionId(req, options, true);
            SetSuppressAccessTracking(req, options);

            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly, options.Attributes);
            ToProto(req->mutable_attribute_filter(), attributeFilter);
            if (options.MaxSize) {
                req->set_max_size(*options.MaxSize);
            }

            auto rsp = WaitFor(ObjectProxy_->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

            return TYsonString(rsp->keys());
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TErrorOr<TNodeId> DoCreateNode(
        const TYPath& path,
        EObjectType type,
        TCreateNodeOptions options)
    {
        try {
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
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TErrorOr<TLockId> DoLockNode(
        const TYPath& path,
        ELockMode mode,
        TLockNodeOptions options)
    {
        try {
            auto lockReq = TCypressYPathProxy::Lock(path);
            SetTransactionId(lockReq, options, false);
            GenerateMutationId(lockReq, options);
            lockReq->set_mode(mode);
            lockReq->set_waitable(options.Waitable);

            auto lockRsp = WaitFor(ObjectProxy_->Execute(lockReq));
            THROW_ERROR_EXCEPTION_IF_FAILED(*lockRsp);

            return FromProto<TLockId>(lockRsp->lock_id());
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TErrorOr<TNodeId> DoCopyNode(
        const TYPath& srcPath,
        const TYPath& dstPath,
        TCopyNodeOptions options)
    {
        try {
            auto req = TCypressYPathProxy::Copy(dstPath);
            SetTransactionId(req, options, true);
            GenerateMutationId(req, options);
            req->set_source_path(srcPath);
            req->set_preserve_account(options.PreserveAccount);

            auto rsp = WaitFor(ObjectProxy_->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

            return FromProto<TNodeId>(rsp->object_id());
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TErrorOr<TNodeId> DoMoveNode(
        const TYPath& srcPath,
        const TYPath& dstPath,
        TMoveNodeOptions options)
    {
        try {
            auto req = TCypressYPathProxy::Copy(dstPath);
            SetTransactionId(req, options, true);
            GenerateMutationId(req, options);
            req->set_source_path(srcPath);
            req->set_remove_source(true);

            auto rsp = WaitFor(ObjectProxy_->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

            return FromProto<TNodeId>(rsp->object_id());
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TErrorOr<TNodeId> DoLinkNode(
        const TYPath& srcPath,
        const TYPath& dstPath,
        TLinkNodeOptions options)
    {
        try {
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
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TErrorOr<bool> DoNodeExists(
        const TYPath& path,
        const TNodeExistsOptions& options)
    {
        try {
            auto req = TYPathProxy::Exists(path);
            SetTransactionId(req, options, true);

            auto rsp = WaitFor(ObjectProxy_->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

            return rsp->value();
        } catch (const std::exception& ex) {
            return ex;
        }
    }


    TErrorOr<TObjectId> DoCreateObject(
        EObjectType type,
        TCreateObjectOptions options)
    {
        try {
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
        } catch (const std::exception& ex) {
            return ex;
        }
    }


    static Stroka GetGroupPath(const Stroka& name)
    {
        return "//sys/groups/" + ToYPathLiteral(name);
    }

    TError DoAddMember(
        const Stroka& group,
        const Stroka& member,
        TAddMemberOptions options)
    {
        try {
            auto req = TGroupYPathProxy::AddMember(GetGroupPath(group));
            req->set_name(member);
            GenerateMutationId(req, options);

            auto rsp = WaitFor(ObjectProxy_->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

            return TError();
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TError DoRemoveMember(
        const Stroka& group,
        const Stroka& member,
        TRemoveMemberOptions options)
    {
        try {
            auto req = TGroupYPathProxy::RemoveMember(GetGroupPath(group));
            req->set_name(member);
            GenerateMutationId(req, options);

            auto rsp = WaitFor(ObjectProxy_->Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

            return TError();
        } catch (const std::exception& ex) {
            return ex;
        }
    }

};

IClientPtr CreateClient(
    IConnectionPtr connection,
    const TClientOptions& options)
{
    YCHECK(connection);

    return New<TClient>(
        std::move(connection),
        options);
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
        return
            BIND(
                &TTransaction::DoCommit,
                MakeStrong(this))
            .AsyncVia(Client_->Invoker_)
            .Run();
    }

    virtual TAsyncError Abort() override
    {
        return Transaction_->Abort();
    }


    virtual TFuture<TErrorOr<ITransactionPtr>> StartTransaction(
        const TTransactionStartOptions& options) override
    {
        auto adjustedOptions = options;
        adjustedOptions.ParentId = GetId();
        return Client_->StartTransaction(adjustedOptions);
    }

        
    virtual void WriteRow(
        const TYPath& path,
        TUnversionedRow row) override
    {
        WriteRows(
            path,
            std::vector<TUnversionedRow>(1, row));
    }

    virtual void WriteRows(
        const TYPath& path,
        std::vector<TUnversionedRow> rows) override
    {
        Requests_.push_back(std::unique_ptr<TRequestBase>(new TWriteRequest(
            this,
            path,
            std::move(rows))));
    }


    virtual void DeleteRow(
        const TYPath& path,
        TKey key) override
    {
        DeleteRows(
            path,
            std::vector<TKey>(1, key));
    }

    virtual void DeleteRows(
        const TYPath& path,
        std::vector<TKey> keys) override
    {
        Requests_.push_back(std::unique_ptr<TRequestBase>(new TDeleteRequest(
            this,
            path,
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
        TKey key,
        const TLookupRowsOptions& options),
        (path, key, options));
    DELEGATE_TIMESTAMPTED_METHOD(TFuture<TErrorOr<IRowsetPtr>>, LookupRows, (
        const TYPath& path,
        const std::vector<TKey>& keys,
        const TLookupRowsOptions& options),
        (path, keys, options))
    DELEGATE_TIMESTAMPTED_METHOD(TFuture<TErrorOr<IRowsetPtr>>, SelectRows, (
        const TYPath& path,
        const Stroka& query,
        const TSelectRowsOptions& options),
        (path, query, options))


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

#undef DELEGATE_TRANSACTIONAL_METHOD
#undef DELEGATE_TIMESTAMPTED_METHOD

private:
    TClientPtr Client_;
    NTransactionClient::TTransactionPtr Transaction_;

    class TRequestBase
    {
    public:
        ~TRequestBase()
        { }

        virtual void Run() = 0;

    protected:
        explicit TRequestBase(
            TTransaction* transaction,
            const TYPath& path)
            : Transaction_(transaction)
            , path_(path)
        { }

        TTransaction* Transaction_;
        TYPath path_;

    };

    class TWriteRequest
        : public TRequestBase
    {
    public:
        TWriteRequest(
            TTransaction* transaction,
            const TYPath& path,
            std::vector<TUnversionedRow> rows)
            : TRequestBase(transaction, path)
            , Rows_(std::move(rows))
        { }

        virtual void Run() override
        {
            const auto& mountInfo = Transaction_->Client_->GetTableMountInfo(path_);
            for (auto row : Rows_) {
                const auto& tabletInfo = Transaction_->Client_->GetTabletInfo(mountInfo, path_, row);
                auto* writer = Transaction_->AddTabletParticipant(tabletInfo);
                writer->WriteCommand(EProtocolCommand::WriteRow);
                writer->WriteUnversionedRow(row);
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
            std::vector<TKey> keys)
            : TRequestBase(transaction, path)
            , Keys_(std::move(keys))
        { }

        virtual void Run() override
        {
            const auto& mountInfo = Transaction_->Client_->GetTableMountInfo(path_);
            for (auto key : Keys_) {
                const auto& tabletInfo = Transaction_->Client_->GetTabletInfo(mountInfo, path_, key);
                auto* writer = Transaction_->AddTabletParticipant(tabletInfo);
                writer->WriteCommand(EProtocolCommand::DeleteRow);
                writer->WriteUnversionedRow(key);
            }
        }

    private:
        std::vector<TUnversionedRow> Keys_;

    };

    std::vector<std::unique_ptr<TRequestBase>> Requests_;

    std::map<const TTabletInfo*, std::unique_ptr<TProtocolWriter>> TabletToWriter_;
    TIntrusivePtr<TParallelCollector<void>> TransactionStartCollector_;


    TProtocolWriter* AddTabletParticipant(const TTabletInfo& tabletInfo)
    {
        auto it = TabletToWriter_.find(&tabletInfo);
        if (it == TabletToWriter_.end()) {
            TransactionStartCollector_->Collect(Transaction_->AddTabletParticipant(tabletInfo.CellId));
            it = TabletToWriter_.insert(std::make_pair(
                &tabletInfo,
                std::make_unique<TProtocolWriter>())).first;
        }
        return it->second.get();
    }

    TError DoCommit()
    {
        try {
            for (const auto& request : Requests_) {
                request->Run();
            }

            auto startResult = WaitFor(TransactionStartCollector_->Complete());
            THROW_ERROR_EXCEPTION_IF_FAILED(startResult);

            auto cellDirectory = Client_->Connection_->GetCellDirectory();

            auto writeCollector = New<TParallelCollector<void>>();

            for (const auto& pair : TabletToWriter_) {
                const auto& tabletInfo = *pair.first;
                auto* writer = pair.second.get();

                auto channel = cellDirectory->GetChannelOrThrow(tabletInfo.CellId);

                TTabletServiceProxy tabletProxy(channel);
                auto writeReq = tabletProxy.Write();
                ToProto(writeReq->mutable_transaction_id(), Transaction_->GetId());
                ToProto(writeReq->mutable_tablet_id(), tabletInfo.TabletId);
                writeReq->set_encoded_request(writer->Finish());

                writeCollector->Collect(
                    writeReq->Invoke().Apply(BIND([] (TTabletServiceProxy::TRspWritePtr rsp) {
                        return rsp->GetError();
                    })));
            }

            auto writeResult = WaitFor(writeCollector->Complete());
            THROW_ERROR_EXCEPTION_IF_FAILED(writeResult);

            auto commitResult = WaitFor(Transaction_->Commit());
            THROW_ERROR_EXCEPTION_IF_FAILED(commitResult);

            return TError();
        } catch (const std::exception& ex) {
            return ex;
        }
    }

};

TFuture<TErrorOr<ITransactionPtr>> TClient::StartTransaction(const TTransactionStartOptions& options)
{
    auto this_ = MakeStrong(this);
    return TransactionManager_->Start(options).Apply(
        BIND([=] (TErrorOr<NTransactionClient::TTransactionPtr> transactionOrError) -> TErrorOr<ITransactionPtr> {
            if (!transactionOrError.IsOK()) {
                return TError(transactionOrError);
            }
            return TErrorOr<ITransactionPtr>(New<TTransaction>(this_, transactionOrError.GetValue()));
        }));
}

////////////////////////////////////////////////////////////////////////////////

class TFileReader
    : public IFileReader
{
public:
    TFileReader(
        TClientPtr client,
        const TYPath& path,
        const TFileReaderOptions& options,
        TFileReaderConfigPtr config)
        : Client_(client)
        , Path_(path)
        , Options_(options)
        , Config_(config ? config : New<TFileReaderConfig>())
    { }

    virtual TAsyncError Open() override
    {
        YUNIMPLEMENTED();
    }

    virtual TFuture<TErrorOr<TSharedRef>> Read() override
    {
        YUNIMPLEMENTED();
    }

    virtual i64 GetSize() const override
    {
        YUNIMPLEMENTED();
    }

private:
    TClientPtr Client_;
    TYPath Path_;
    TFileReaderOptions Options_;

    TFileReaderConfigPtr Config_;

};

IFileReaderPtr TClient::CreateFileReader(
    const TYPath& path,
    const TFileReaderOptions& options,
    TFileReaderConfigPtr config)
{
    return New<TFileReader>(
        this,
        path,
        options,
        config);
}

////////////////////////////////////////////////////////////////////////////////

class TFileWriter
    : public IFileWriter
{
public:
    TFileWriter(
        TClientPtr client,
        const TYPath& path,
        const TFileWriterOptions& options,
        TFileWriterConfigPtr config)
        : Client_(client)
        , Path_(path)
        , Options_(options)
        , Config_(config ? config : New<TFileWriterConfig>())
    { }

    virtual TAsyncError Open() override
    {
        YUNIMPLEMENTED();
    }

    virtual TAsyncError Write(const TRef& data) override
    {
        YUNIMPLEMENTED();
    }

    virtual TAsyncError Close() override
    {
        YUNIMPLEMENTED();
    }

private:
    TClientPtr Client_;
    TYPath Path_;
    TFileWriterOptions Options_;

    TFileWriterConfigPtr Config_;

};

IFileWriterPtr TClient::CreateFileWriter(
    const TYPath& path,
    const TFileWriterOptions& options,
    TFileWriterConfigPtr config)
{
    return New<TFileWriter>(
        this,
        path,
        options,
        config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

