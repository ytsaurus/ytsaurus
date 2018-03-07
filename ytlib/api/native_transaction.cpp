#include "native_transaction.h"
#include "native_connection.h"
#include "tablet_helpers.h"
#include "config.h"

#include <yt/ytlib/transaction_client/timestamp_provider.h>
#include <yt/ytlib/transaction_client/transaction_manager.h>
#include <yt/ytlib/transaction_client/action.h>
#include <yt/ytlib/transaction_client/transaction_service_proxy.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/tablet_client/table_mount_cache.h>
#include <yt/ytlib/tablet_client/tablet_service_proxy.h>
#include <yt/ytlib/tablet_client/wire_protocol.h>
#include <yt/ytlib/tablet_client/wire_protocol.pb.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/row_merger.h>

#include <yt/ytlib/hive/cluster_directory.h>
#include <yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/ytlib/query_client/column_evaluator.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/compression/codec.h>

namespace NYT {
namespace NApi {

using namespace NYPath;
using namespace NTransactionClient;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NTabletClient;
using namespace NTableClient;
using namespace NQueryClient;
using namespace NYson;
using namespace NConcurrency;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionState,
    (Active)
    (Commit)
    (Abort)
    (Flush)
    (Detach)
);

DECLARE_REFCOUNTED_CLASS(TNativeTransaction)

class TNativeTransaction
    : public INativeTransaction
{
public:
    TNativeTransaction(
        INativeClientPtr client,
        NTransactionClient::TTransactionPtr transaction,
        NLogging::TLogger logger)
        : Client_(std::move(client))
        , Transaction_(std::move(transaction))
        , CommitInvoker_(CreateSerializedInvoker(Client_->GetConnection()->GetInvoker()))
        , Logger(logger.AddTag("TransactionId: %v, ConnectionCellTag: %v",
            GetId(),
            Client_->GetConnection()->GetCellTag()))
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

        LOG_DEBUG("Buffering client row modifications (Count: %v)",
            modifications.Size());

        EnqueueModificationRequest(std::make_unique<TModificationRequest>(
            this,
            Client_->GetNativeConnection(),
            path,
            std::move(nameTable),
            std::move(modifications),
            options));
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
    const INativeClientPtr Client_;
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
            , Modifications_(std::move(modifications))
            , Options_(options)
            , Logger(Transaction_->Logger)
        { }

        void PrepareTableSessions()
        {
            TableSession_ = Transaction_->GetOrCreateTableSession(Path_, Options_.UpstreamReplicaId);
        }

        void SubmitRows()
        {
            const auto& tableInfo = TableSession_->GetInfo();
            if (Options_.UpstreamReplicaId && tableInfo->IsReplicated()) {
                THROW_ERROR_EXCEPTION("Replicated table %v cannot act as a replication sink",
                    tableInfo->Path);
            }
            
            if (!tableInfo->Replicas.empty() &&
                TableSession_->SyncReplicas().empty() &&
                Options_.RequireSyncReplica)
            {
                THROW_ERROR_EXCEPTION("Table %v has no synchronous replicas",
                    tableInfo->Path);
            }

            for (const auto& replicaData : TableSession_->SyncReplicas()) {
                auto replicaOptions = Options_;
                replicaOptions.UpstreamReplicaId = replicaData.ReplicaInfo->ReplicaId;
                if (replicaData.Transaction) {
                    LOG_DEBUG("Submitting remote sync replication modifications (Count: %v)",
                        Modifications_.Size());
                    replicaData.Transaction->ModifyRows(
                        replicaData.ReplicaInfo->ReplicaPath,
                        NameTable_,
                        Modifications_,
                        replicaOptions);                    
                } else {
                    // YT-7551: Local sync replicas must be handled differenly.
                    // We cannot add more modifications via ITransactions interface since
                    // the transaction is already committing.
                    LOG_DEBUG("Bufferring local sync replication modifications (Count: %v)",
                        Modifications_.Size());
                    Transaction_->EnqueueModificationRequest(std::make_unique<TModificationRequest>(
                        Transaction_,
                        Connection_,
                        replicaData.ReplicaInfo->ReplicaPath,
                        NameTable_,
                        Modifications_,
                        replicaOptions));
                }
            }

            TNullable<int> tabletIndexColumnId;
            if (!tableInfo->IsSorted()) {
                tabletIndexColumnId = NameTable_->GetIdOrRegisterName(TabletIndexColumnName);
            }

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
                        ValidateClientDataRow(TUnversionedRow(modification.Row), writeSchema, writeIdMapping, NameTable_, tabletIndexColumnId);
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
                            tabletInfo = GetSortedTabletForRow(tableInfo, capturedRow, true);
                        } else {
                            tabletInfo = GetOrderedTabletForRow(
                                tableInfo,
                                randomTabletInfo,
                                tabletIndexColumnId,
                                TUnversionedRow(modification.Row),
                                true);
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
                        auto tabletInfo = GetSortedTabletForRow(tableInfo, capturedRow, true);
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
        const TSharedRange<TRowModification> Modifications_;
        const TModifyRowsOptions Options_;

        const NLogging::TLogger& Logger;


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
    std::vector<TModificationRequest*> PendingRequests_;

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
    THashMap<TYPath, TTableCommitSessionPtr> TablePathToSession_;
    std::vector<TTableCommitSessionPtr> PendingSessions_;

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
            , Config_(transaction->Client_->GetNativeConnection()->GetConfig())
            , ColumnEvaluator_(std::move(columnEvauator))
            , TableMountCache_(transaction->Client_->GetNativeConnection()->GetTableMountCache())
            , ColumnCount_(TableInfo_->Schemas[ETableSchemaKind::Primary].Columns().size())
            , KeyColumnCount_(TableInfo_->Schemas[ETableSchemaKind::Primary].GetKeyColumnCount())
            , Logger(NLogging::TLogger(transaction->Logger)
                .AddTag("TabletId: %v", TabletInfo_->TabletId))
        { }

        void SubmitRow(
            EWireProtocolCommand command,
            TUnversionedRow row)
        {
            UnversionedSubmittedRows_.push_back({
                command,
                row,
                static_cast<int>(UnversionedSubmittedRows_.size())});
        }

        void SubmitRow(
            EWireProtocolCommand command,
            TVersionedRow row)
        {
            VersionedSubmittedRows_.push_back({
                command,
                row});
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
        const TColumnEvaluatorPtr ColumnEvaluator_;
        const ITableMountCachePtr TableMountCache_;
        const int ColumnCount_;
        const int KeyColumnCount_;

        struct TCommitSessionBufferTag
        { };

        TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TCommitSessionBufferTag());

        NLogging::TLogger Logger;

        struct TBatch
        {
            TWireProtocolWriter Writer;
            TSharedRef RequestData;
            int RowCount = 0;
            size_t DataWeight = 0;
        };

        int TotalBatchedRowCount_ = 0;
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

        template <typename TRow>
        void WriteRow(const TRow& submittedRow)
        {
            if (++TotalBatchedRowCount_ > Config_->MaxRowsPerTransaction) {
                THROW_ERROR_EXCEPTION("Transaction affects too many rows")
                    << TErrorAttribute("limit", Config_->MaxRowsPerTransaction);
            }

            auto* batch = EnsureBatch();
            auto& writer = batch->Writer;
            writer.WriteCommand(submittedRow.Command);
            WriteRowToWriter(writer, submittedRow.Row);
            ++batch->RowCount;
            batch->DataWeight += GetDataWeight(submittedRow.Row);
        }

        static void WriteRowToWriter(TWireProtocolWriter& writer, TVersionedRow row)
        {
            writer.WriteVersionedRow(row);
        }

        static void WriteRowToWriter(TWireProtocolWriter& writer, TUnversionedRow row)
        {
            writer.WriteUnversionedRow(row);
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
            req->SetMultiplexingBand(EMultiplexingBand::Heavy);
            ToProto(req->mutable_transaction_id(), transaction->GetId());
            if (transaction->GetAtomicity() == EAtomicity::Full) {
                req->set_transaction_start_timestamp(transaction->GetStartTimestamp());
                req->set_transaction_timeout(ToProto<i64>(transaction->GetTimeout()));
            }
            ToProto(req->mutable_tablet_id(), TabletInfo_->TabletId);
            req->set_mount_revision(TabletInfo_->MountRevision);
            req->set_durability(static_cast<int>(transaction->GetDurability()));
            req->set_signature(cellSession->AllocateRequestSignature());
            req->set_request_codec(static_cast<int>(Config_->WriteRequestCodec));
            req->set_row_count(batch->RowCount);
            req->set_data_weight(batch->DataWeight);
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
                TableMountCache_->InvalidateOnError(rspOrError);
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
    THashMap<TTabletId, TTabletCommitSessionPtr> TabletIdToSession_;

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

        const NLogging::TLogger Logger;


        TFuture<void> SendTabletActions(const TNativeTransactionPtr& owner, const IChannelPtr& channel)
        {
            TTabletServiceProxy proxy(channel);
            auto req = proxy.RegisterTransactionActions();
            ToProto(req->mutable_transaction_id(), owner->GetId());
            req->set_transaction_start_timestamp(owner->GetStartTimestamp());
            req->set_transaction_timeout(ToProto<i64>(owner->GetTimeout()));
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
    THashMap<TCellId, TCellCommitSessionPtr> CellIdToSession_;

    //! Maps replica cluster name to sync replica transaction.
    THashMap<TString, ITransactionPtr> ClusterNameToSyncReplicaTransaction_;

    //! Caches mappings from name table ids to schema ids.
    THashMap<std::pair<TNameTablePtr, ETableSchemaKind>, TNameTableToSchemaIdMapping> IdMappingCache_;


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

        const auto& clusterDirectory = Client_->GetNativeConnection()->GetClusterDirectory();
        auto connection = clusterDirectory->FindConnection(replicaInfo->ClusterName);
        if (!connection) {
            if (!*clusterDirectorySynched) {
                LOG_DEBUG("Replica cluster is not known; synchronizing cluster directory");
                WaitFor(Client_->GetNativeConnection()->GetClusterDirectorySynchronizer()->Sync())
                    .ThrowOnError();
                *clusterDirectorySynched = true;
            }
            connection = clusterDirectory->GetConnectionOrThrow(replicaInfo->ClusterName);
        }

        if (connection->GetCellTag() == Client_->GetConnection()->GetCellTag()) {
            return nullptr;
        }

        auto client = connection->CreateClient(Client_->GetOptions());

        TForeignTransactionStartOptions options;
        options.InheritStartTimestamp = true;
        auto transaction = WaitFor(StartForeignTransaction(client, options))
            .ValueOrThrow();

        YCHECK(ClusterNameToSyncReplicaTransaction_.emplace(replicaInfo->ClusterName, transaction).second);

        LOG_DEBUG("Sync replica transaction started (ClusterName: %v)",
            replicaInfo->ClusterName);

        return transaction;
    }

    void EnqueueModificationRequest(std::unique_ptr<TModificationRequest> request)
    {
        PendingRequests_.push_back(request.get());
        Requests_.push_back(std::move(request));
    }

    TTableCommitSessionPtr GetOrCreateTableSession(const TYPath& path, const TTableReplicaId& upstreamReplicaId)
    {
        auto it = TablePathToSession_.find(path);
        if (it == TablePathToSession_.end()) {
            const auto& tableMountCache = Client_->GetConnection()->GetTableMountCache();
            auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
                .ValueOrThrow();

            auto session = New<TTableCommitSession>(this, std::move(tableInfo), upstreamReplicaId);
            PendingSessions_.push_back(session);
            it = TablePathToSession_.emplace(path, session).first;
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
        bool clusterDirectorySynched = false;

        // Tables with local sync replicas pose a problem since modifications in such tables
        // induce more modifications that need to be taken care of.
        // Here we iterate over requests and sessions until no more new items are added.
        while (!PendingRequests_.empty() || !PendingSessions_.empty()) {
            decltype(PendingRequests_) pendingRequests;
            std::swap(PendingRequests_, pendingRequests);

            for (auto* request : pendingRequests) {
                request->PrepareTableSessions();
            }
 
            decltype(PendingSessions_) pendingSessions;
            std::swap(PendingSessions_, pendingSessions);
 
            for (const auto& tableSession : pendingSessions) {
                tableSession->RegisterSyncReplicas(&clusterDirectorySynched);
            }

            for (auto* request : pendingRequests) {
                request->SubmitRows();
            }
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
                options.CoordinatorCellTag = Client_->GetNativeConnection()->GetPrimaryMasterCellTag();
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

            auto commitResult = WaitFor(Transaction_->Commit(AdjustCommitOptions(options)))
                .ValueOrThrow();

            for (const auto& transaction : GetForeignTransactions()) {
                transaction->Detach();
            }

            return commitResult;
        } catch (const std::exception& ex) {
            // Fire and forget.
            Transaction_->Abort();
            for (const auto& transaction : GetForeignTransactions()) {
                transaction->Abort();
            }
            throw;
        }
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

INativeTransactionPtr CreateNativeTransaction(
    INativeClientPtr client,
    NTransactionClient::TTransactionPtr transaction,
    const NLogging::TLogger& logger)
{
    return New<TNativeTransaction>(
        std::move(client),
        std::move(transaction),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
