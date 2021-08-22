#include "transaction.h"
#include "connection.h"
#include "config.h"

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/ytlib/api/native/tablet_helpers.h>

#include <yt/yt/ytlib/transaction_client/transaction_manager.h>
#include <yt/yt/ytlib/transaction_client/action.h>
#include <yt/yt/ytlib/transaction_client/transaction_service_proxy.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/table_client/row_merger.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/query_client/column_evaluator.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/misc/sliding_window.h>

namespace NYT::NApi::NNative {

using namespace NYPath;
using namespace NTransactionClient;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NTabletClient;
using namespace NTableClient;
using namespace NQueryClient;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionState,
    (Active)
    (Committing)
    (Committed)
    (Flushing)
    (Flushed)
    (Aborted)
    (Detached)
);

DECLARE_REFCOUNTED_CLASS(TTransaction)

class TTransaction
    : public ITransaction
{
public:
    TTransaction(
        IClientPtr client,
        NTransactionClient::TTransactionPtr transaction,
        NLogging::TLogger logger)
        : Client_(std::move(client))
        , Transaction_(std::move(transaction))
        , Logger(logger.WithTag("TransactionId: %v, ConnectionCellTag: %v",
            GetId(),
            Client_->GetConnection()->GetCellTag()))
        , SerializedInvoker_(CreateSerializedInvoker(
            Client_->GetConnection()->GetInvoker()))
        , OrderedRequestsSlidingWindow_(
            Client_->GetNativeConnection()->GetConfig()->MaxRequestWindowSize)
    { }


    virtual NApi::IConnectionPtr GetConnection() override
    {
        return Client_->GetConnection();
    }

    virtual NApi::IClientPtr GetClient() const override
    {
        return Client_;
    }

    virtual NTransactionClient::ETransactionType GetType() const override
    {
        return Transaction_->GetType();
    }

    virtual TTransactionId GetId() const override
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


    virtual TFuture<void> Ping(const TTransactionPingOptions& options = {}) override
    {
        return Transaction_->Ping(options);
    }

    virtual TFuture<TTransactionCommitResult> Commit(const TTransactionCommitOptions& options) override
    {
        bool needsFlush;
        {
            auto guard = Guard(SpinLock_);

            if (State_ != ETransactionState::Active && State_ != ETransactionState::Flushed) {
                return MakeFuture<TTransactionCommitResult>(TError(
                    NTransactionClient::EErrorCode::InvalidTransactionState,
                    "Cannot commit since transaction %v is in %Qlv state",
                    GetId(),
                    State_));
            }

            needsFlush = (State_ == ETransactionState::Active);
            State_ = ETransactionState::Committing;
        }

        return BIND(&TTransaction::DoCommit, MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run(options, needsFlush);
    }

    virtual TFuture<void> Abort(const TTransactionAbortOptions& options = {}) override
    {
        auto guard = Guard(SpinLock_);

        if (State_ == ETransactionState::Committed || State_ == ETransactionState::Detached) {
            return MakeFuture<void>(TError(
                NTransactionClient::EErrorCode::InvalidTransactionState,
                "Cannot abort since transaction %v is in %Qlv state",
                GetId(),
                State_));
        }

        return DoAbort(&guard, options);
    }

    virtual void Detach() override
    {
        auto guard = Guard(SpinLock_);

        if (State_ != ETransactionState::Aborted) {
            State_ = ETransactionState::Detached;
            Transaction_->Detach();
        }
    }

    virtual TFuture<TTransactionFlushResult> Flush() override
    {
        {
            auto guard = Guard(SpinLock_);

            if (State_ != ETransactionState::Active) {
                return MakeFuture<TTransactionFlushResult>(TError(
                    NTransactionClient::EErrorCode::InvalidTransactionState,
                    "Cannot flush transaction %v since it is in %Qlv state",
                    GetId(),
                    State_));
            }

            if (!AlienTransactions_.empty()) {
                return MakeFuture<TTransactionFlushResult>(TError(
                    NTransactionClient::EErrorCode::AlienTransactionsForbidden,
                    "Cannot flush transaction %v since it has %v alien transaction(s)",
                    GetId(),
                    AlienTransactions_.size()));
            }

            State_ = ETransactionState::Flushing;
        }

        YT_LOG_DEBUG("Flushing transaction");

        return BIND(&TTransaction::DoFlush, MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    virtual void AddAction(TCellId cellId, const TTransactionActionData& data) override
    {
        auto guard = Guard(SpinLock_);

        YT_VERIFY(
            TypeFromId(cellId) == EObjectType::TabletCell ||
            TypeFromId(cellId) == EObjectType::MasterCell);

        if (State_ != ETransactionState::Active) {
            THROW_ERROR_EXCEPTION(
                NTransactionClient::EErrorCode::InvalidTransactionState,
                "Cannot add action since transaction %v is in %Qlv state",
                GetId(),
                State_);
        }

        if (GetAtomicity() != EAtomicity::Full) {
            THROW_ERROR_EXCEPTION(
                NTransactionClient::EErrorCode::InvalidTransactionAtomicity,
                "Cannot add action since transaction %v has wrong atomicity: actual %Qlv, expected %Qlv",
                GetId(),
                GetAtomicity(),
                EAtomicity::Full);
        }

        auto session = GetOrCreateCellCommitSession(cellId);
        session->RegisterAction(data);

        YT_LOG_DEBUG("Transaction action added (CellId: %v, ActionType: %v)",
            cellId,
            data.Type);
    }


    virtual void RegisterAlienTransaction(const NApi::ITransactionPtr& transaction) override
    {
        {
            auto guard = Guard(SpinLock_);

            if (State_ != ETransactionState::Active) {
                THROW_ERROR_EXCEPTION(
                    NTransactionClient::EErrorCode::InvalidTransactionState,
                    "Transaction %v is in %Qlv state",
                    GetId(),
                    State_);
            }

            if (GetType() != ETransactionType::Tablet) {
                THROW_ERROR_EXCEPTION(
                    NTransactionClient::EErrorCode::MalformedAlienTransaction,
                    "Transaction %v is of type %Qlv and hence does not allow alien transactions",
                    GetId(),
                    GetType());
            }

            if (GetId() != transaction->GetId()) {
                THROW_ERROR_EXCEPTION(
                    NTransactionClient::EErrorCode::MalformedAlienTransaction,
                    "Transaction id mismatch: local %v, alien %v",
                    GetId(),
                    transaction->GetId());
            }

            AlienTransactions_.push_back(transaction);
        }

        YT_LOG_DEBUG("Alien transaction registered (AlienConnectionId: %v)",
            transaction->GetConnection()->GetLoggingTag());
    }


    virtual void SubscribeCommitted(const TCommittedHandler& callback) override
    {
        Transaction_->SubscribeCommitted(callback);
    }

    virtual void UnsubscribeCommitted(const TCommittedHandler& callback) override
    {
        Transaction_->UnsubscribeCommitted(callback);
    }


    virtual void SubscribeAborted(const TAbortedHandler& callback) override
    {
        Transaction_->SubscribeAborted(callback);
    }

    virtual void UnsubscribeAborted(const TAbortedHandler& callback) override
    {
        Transaction_->UnsubscribeAborted(callback);
    }


    virtual TFuture<ITransactionPtr> StartNativeTransaction(
        ETransactionType type,
        const TTransactionStartOptions& options) override
    {
        auto adjustedOptions = options;
        adjustedOptions.ParentId = GetId();
        return Client_->StartNativeTransaction(
            type,
            adjustedOptions);
    }

    virtual TFuture<NApi::ITransactionPtr> StartTransaction(
        ETransactionType type,
        const TTransactionStartOptions& options) override
    {
        return StartNativeTransaction(type, options).As<NApi::ITransactionPtr>();
    }

    virtual void ModifyRows(
        const TYPath& path,
        TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications,
        const TModifyRowsOptions& options) override
    {
        ValidateTabletTransactionId(GetId());

        YT_LOG_DEBUG("Buffering client row modifications (Count: %v, SequenceNumber: %v)",
            modifications.Size(),
            options.SequenceNumber);

        auto guard = Guard(SpinLock_);

        try {
            if (State_ != ETransactionState::Active) {
                THROW_ERROR_EXCEPTION(
                    NTransactionClient::EErrorCode::InvalidTransactionState,
                    "Cannot modify rows since transaction %v is in %Qlv state",
                    GetId(),
                    State_);
            }

            EnqueueModificationRequest(
                std::make_unique<TModificationRequest>(
                    this,
                    Client_->GetNativeConnection(),
                    path,
                    std::move(nameTable),
                    std::move(modifications),
                    options));
        } catch (const std::exception& ex) {
            DoAbort(&guard);
            throw;
        }
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
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options),
        (path, nameTable, keys, options))
    DELEGATE_TIMESTAMPED_METHOD(TFuture<IVersionedRowsetPtr>, VersionedLookupRows, (
        const TYPath& path,
        TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TVersionedLookupRowsOptions& options),
        (path, nameTable, keys, options))
    DELEGATE_TIMESTAMPED_METHOD(TFuture<std::vector<IUnversionedRowsetPtr>>, MultiLookup, (
        const std::vector<TMultiLookupSubrequest>& subrequests,
        const TMultiLookupOptions& options),
        (subrequests, options))

    DELEGATE_TIMESTAMPED_METHOD(TFuture<TSelectRowsResult>, SelectRows, (
        const TString& query,
        const TSelectRowsOptions& options),
        (query, options))

    DELEGATE_TIMESTAMPED_METHOD(TFuture<NYson::TYsonString>, ExplainQuery, (
        const TString& query,
        const TExplainQueryOptions& options),
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
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<void>, MultisetAttributesNode, (
        const TYPath& path,
        const IMapNodePtr& attributes,
        const TMultisetAttributesNodeOptions& options),
        (path, attributes, options))
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
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<void>, UnlockNode, (
        const TYPath& path,
        const TUnlockNodeOptions& options),
        (path, options))
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
        const std::vector<TRichYPath>& srcPaths,
        const TRichYPath& dstPath,
        const TConcatenateNodesOptions& options),
        (srcPaths, dstPath, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<void>, ExternalizeNode, (
        const TYPath& path,
        TCellTag cellTag,
        const TExternalizeNodeOptions& options),
        (path, cellTag, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<void>, InternalizeNode, (
        const TYPath& path,
        const TInternalizeNodeOptions& options),
        (path, options))
    DELEGATE_TRANSACTIONAL_METHOD(TFuture<bool>, NodeExists, (
        const TYPath& path,
        const TNodeExistsOptions& options),
        (path, options))


    DELEGATE_METHOD(TFuture<TObjectId>, CreateObject, (
        EObjectType type,
        const TCreateObjectOptions& options),
        (type, options))


    DELEGATE_TRANSACTIONAL_METHOD(TFuture<IFileReaderPtr>, CreateFileReader, (
        const TYPath& path,
        const TFileReaderOptions& options),
        (path, options))
    DELEGATE_TRANSACTIONAL_METHOD(IFileWriterPtr, CreateFileWriter, (
        const TRichYPath& path,
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

    DELEGATE_TRANSACTIONAL_METHOD(TFuture<ITableReaderPtr>, CreateTableReader, (
        const TRichYPath& path,
        const TTableReaderOptions& options),
        (path, options))

    DELEGATE_TRANSACTIONAL_METHOD(TFuture<ITableWriterPtr>, CreateTableWriter, (
        const TRichYPath& path,
        const TTableWriterOptions& options),
        (path, options))

#undef DELEGATE_METHOD
#undef DELEGATE_TRANSACTIONAL_METHOD
#undef DELEGATE_TIMESTAMPED_METHOD

private:
    const IClientPtr Client_;
    const NTransactionClient::TTransactionPtr Transaction_;

    const NLogging::TLogger Logger;

    struct TNativeTransactionBufferTag
    { };

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TNativeTransactionBufferTag());

    const IInvokerPtr SerializedInvoker_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    ETransactionState State_ = ETransactionState::Active;
    TPromise<void> AbortPromise_;
    std::vector<NApi::ITransactionPtr> AlienTransactions_;

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
            TTransaction* transaction,
            IConnectionPtr connection,
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
            , Logger(transaction->Logger)
            , TableSession_(transaction->GetOrCreateTableSession(Path_, Options_.UpstreamReplicaId))
        { }

        std::optional<i64> GetSequenceNumber()
        {
            return Options_.SequenceNumber;
        }

        void SubmitRows()
        {
            auto transaction = Transaction_.Lock();
            if (!transaction) {
                return;
            }

            const auto& tableInfo = TableSession_->GetInfo();
            if (Options_.UpstreamReplicaId && tableInfo->IsReplicated()) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::TableMustNotBeReplicated,
                    "Replicated table %v cannot act as a replication sink",
                    tableInfo->Path);
            }

            const auto& syncReplicas = TableSession_->GetSyncReplicas();

            if (!tableInfo->Replicas.empty() &&
                syncReplicas.empty() &&
                Options_.RequireSyncReplica)
            {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::NoSyncReplicas,
                    "Table %v has no synchronous replicas and \"require_sync_replica\" option is set",
                    tableInfo->Path);
            }

            for (const auto& syncReplica : syncReplicas) {
                auto replicaOptions = Options_;
                replicaOptions.UpstreamReplicaId = syncReplica.ReplicaInfo->ReplicaId;
                replicaOptions.SequenceNumber.reset();
                if (syncReplica.Transaction) {
                    YT_LOG_DEBUG("Submitting remote sync replication modifications (Count: %v)",
                        Modifications_.Size());
                    syncReplica.Transaction->ModifyRows(
                        syncReplica.ReplicaInfo->ReplicaPath,
                        NameTable_,
                        Modifications_,
                        replicaOptions);
                } else {
                    // YT-7571: Local sync replicas must be handled differenly.
                    // We cannot add more modifications via ITransactions interface since
                    // the transaction is already committing.
                    YT_LOG_DEBUG("Buffering local sync replication modifications (Count: %v)",
                        Modifications_.Size());
                    transaction->EnqueueModificationRequest(std::make_unique<TModificationRequest>(
                        transaction.Get(),
                        Connection_,
                        syncReplica.ReplicaInfo->ReplicaPath,
                        NameTable_,
                        Modifications_,
                        replicaOptions));
                }
            }

            std::optional<int> tabletIndexColumnId;
            if (!tableInfo->IsSorted()) {
                tabletIndexColumnId = NameTable_->GetIdOrRegisterName(TabletIndexColumnName);
            }

            const auto& primarySchema = tableInfo->Schemas[ETableSchemaKind::Primary];
            const auto& primaryIdMapping = transaction->GetColumnIdMapping(tableInfo, NameTable_, ETableSchemaKind::Primary);

            const auto& primarySchemaWithTabletIndex = tableInfo->Schemas[ETableSchemaKind::PrimaryWithTabletIndex];
            const auto& primaryWithTabletIndexIdMapping = transaction->GetColumnIdMapping(tableInfo, NameTable_, ETableSchemaKind::PrimaryWithTabletIndex);

            const auto& writeSchema = tableInfo->Schemas[ETableSchemaKind::Write];
            const auto& writeIdMapping = transaction->GetColumnIdMapping(tableInfo, NameTable_, ETableSchemaKind::Write);

            const auto& versionedWriteSchema = tableInfo->Schemas[ETableSchemaKind::VersionedWrite];
            const auto& versionedWriteIdMapping = transaction->GetColumnIdMapping(tableInfo, NameTable_, ETableSchemaKind::VersionedWrite);

            const auto& deleteSchema = tableInfo->Schemas[ETableSchemaKind::Delete];
            const auto& deleteIdMapping = transaction->GetColumnIdMapping(tableInfo, NameTable_, ETableSchemaKind::Delete);

            const auto& modificationSchema = !tableInfo->IsReplicated() && !tableInfo->IsSorted() ? primarySchema : primarySchemaWithTabletIndex;
            const auto& modificationIdMapping = !tableInfo->IsReplicated() && !tableInfo->IsSorted() ? primaryIdMapping : primaryWithTabletIndexIdMapping;

            const auto& rowBuffer = transaction->RowBuffer_;

            auto evaluatorCache = Connection_->GetColumnEvaluatorCache();
            auto evaluator = tableInfo->NeedKeyEvaluation ? evaluatorCache->Find(primarySchema) : nullptr;

            auto randomTabletInfo = tableInfo->GetRandomMountedTablet();

            std::vector<bool> columnPresenceBuffer(modificationSchema->GetColumnCount());

            for (const auto& modification : Modifications_) {
                switch (modification.Type) {
                    case ERowModificationType::Write:
                        ValidateClientDataRow(
                            TUnversionedRow(modification.Row),
                            *writeSchema,
                            writeIdMapping,
                            NameTable_,
                            tabletIndexColumnId);
                        break;

                    case ERowModificationType::VersionedWrite:
                        if (tableInfo->IsReplicated()) {
                            THROW_ERROR_EXCEPTION(
                                NTabletClient::EErrorCode::TableMustNotBeReplicated,
                                "Cannot perform versioned writes into a replicated table %v",
                                tableInfo->Path);
                        }
                        if (tableInfo->IsSorted()) {
                            ValidateClientDataRow(
                                TVersionedRow(modification.Row),
                                *versionedWriteSchema,
                                versionedWriteIdMapping,
                                NameTable_);
                        } else {
                            ValidateClientDataRow(
                                TUnversionedRow(modification.Row),
                                *versionedWriteSchema,
                                versionedWriteIdMapping,
                                NameTable_,
                                tabletIndexColumnId);
                        }
                        break;

                    case ERowModificationType::Delete:
                        if (!tableInfo->IsSorted()) {
                            THROW_ERROR_EXCEPTION(
                                NTabletClient::EErrorCode::TableMustBeSorted,
                                "Cannot perform deletes in a non-sorted table %v",
                                tableInfo->Path);
                        }
                        ValidateClientKey(
                            TUnversionedRow(modification.Row),
                            *deleteSchema,
                            deleteIdMapping,
                            NameTable_);
                        break;

                    case ERowModificationType::ReadLockWrite:
                        if (!tableInfo->IsSorted()) {
                            THROW_ERROR_EXCEPTION(
                                NTabletClient::EErrorCode::TableMustBeSorted,
                                "Cannot perform lock in a non-sorted table %v",
                                tableInfo->Path);
                        }
                        ValidateClientKey(
                            TUnversionedRow(modification.Row),
                            *deleteSchema,
                            deleteIdMapping,
                            NameTable_);
                        break;

                    default:
                        YT_ABORT();
                }

                switch (modification.Type) {
                    case ERowModificationType::Write:
                    case ERowModificationType::Delete:
                    case ERowModificationType::ReadLockWrite: {
                        auto capturedRow = rowBuffer->CaptureAndPermuteRow(
                            TUnversionedRow(modification.Row),
                            *modificationSchema,
                            modificationSchema->GetKeyColumnCount(),
                            modificationIdMapping,
                            modification.Type == ERowModificationType::Write ? &columnPresenceBuffer : nullptr);
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

                        auto modificationType = modification.Type;
                        if (tableInfo->IsReplicated() && modificationType == ERowModificationType::ReadLockWrite) {
                            modificationType = ERowModificationType::Write;
                        }

                        auto session = transaction->GetOrCreateTabletSession(tabletInfo, tableInfo, TableSession_);
                        auto command = GetCommand(modificationType);
                        session->SubmitRow(command, capturedRow, modification.Locks);
                        break;
                    }

                    case ERowModificationType::VersionedWrite: {
                        TTypeErasedRow row;
                        TTabletInfoPtr tabletInfo;
                        if (tableInfo->IsSorted()) {
                            auto capturedRow = rowBuffer->CaptureAndPermuteRow(
                                TVersionedRow(modification.Row),
                                *primarySchema,
                                primaryIdMapping,
                                &columnPresenceBuffer);
                            if (evaluator) {
                                evaluator->EvaluateKeys(capturedRow, rowBuffer);
                            }
                            row = capturedRow.ToTypeErasedRow();
                            tabletInfo = GetSortedTabletForRow(tableInfo, capturedRow, true);
                        } else {
                            auto capturedRow = rowBuffer->CaptureAndPermuteRow(
                                TUnversionedRow(modification.Row),
                                *primarySchema,
                                primarySchema->GetKeyColumnCount(),
                                primaryIdMapping,
                                &columnPresenceBuffer);
                            row = capturedRow.ToTypeErasedRow();
                            tabletInfo = GetOrderedTabletForRow(
                                tableInfo,
                                randomTabletInfo,
                                tabletIndexColumnId,
                                TUnversionedRow(modification.Row),
                                true);
                        }

                        auto session = transaction->GetOrCreateTabletSession(tabletInfo, tableInfo, TableSession_);
                        session->SubmitRow(row);
                        break;
                    }

                    default:
                        YT_ABORT();
                }
            }
        }

    protected:
        const TWeakPtr<TTransaction> Transaction_;
        const IConnectionPtr Connection_;
        const TYPath Path_;
        const TNameTablePtr NameTable_;
        const TSharedRange<TRowModification> Modifications_;
        const TModifyRowsOptions Options_;

        const NLogging::TLogger& Logger;
        const TTableCommitSessionPtr TableSession_;


        static EWireProtocolCommand GetCommand(ERowModificationType modificationType)
        {
            switch (modificationType) {
                case ERowModificationType::Write:
                    return EWireProtocolCommand::WriteRow;

                case ERowModificationType::VersionedWrite:
                    return EWireProtocolCommand::VersionedWriteRow;

                case ERowModificationType::Delete:
                    return EWireProtocolCommand::DeleteRow;

                case ERowModificationType::ReadLockWrite:
                    return EWireProtocolCommand::ReadLockWriteRow;

                default:
                    YT_ABORT();
            }
        }
    };

    std::vector<std::unique_ptr<TModificationRequest>> Requests_;
    std::vector<TModificationRequest*> PendingRequests_;
    TSlidingWindow<TModificationRequest*> OrderedRequestsSlidingWindow_;

    struct TSyncReplica
    {
        TTableReplicaInfoPtr ReplicaInfo;
        NApi::ITransactionPtr Transaction;
    };

    class TTableCommitSession
        : public TRefCounted
    {
    public:
        TTableCommitSession(
            TTransaction* transaction,
            const TYPath& path,
            TTableReplicaId upstreamReplicaId)
            : Transaction_(transaction)
            , UpstreamReplicaId_(upstreamReplicaId)
            , Logger(transaction->Logger.WithTag("Path: %v", path))
        {
            const auto& tableMountCache = transaction->Client_->GetTableMountCache();
            auto tableInfoFuture = tableMountCache->GetTableInfo(path);
            auto tableInfoOrError = tableInfoFuture.TryGet();
            PrepareFuture_  = tableInfoOrError && tableInfoOrError->IsOK()
                ? OnGotTableInfo(tableInfoOrError->Value())
                : tableInfoFuture
                    .Apply(BIND(&TTableCommitSession::OnGotTableInfo, MakeStrong(this))
                        .AsyncVia(transaction->SerializedInvoker_));
        }

        const TFuture<void>& GetPrepareFuture()
        {
            return PrepareFuture_;
        }

        const TTableMountInfoPtr& GetInfo() const
        {
            YT_VERIFY(TableInfo_);
            return TableInfo_;
        }

        TTableReplicaId GetUpstreamReplicaId() const
        {
            return UpstreamReplicaId_;
        }

        const std::vector<TSyncReplica>& GetSyncReplicas() const
        {
            return SyncReplicas_;
        }

    private:
        const TWeakPtr<TTransaction> Transaction_;
        const TTableReplicaId UpstreamReplicaId_;
        const NLogging::TLogger Logger;

        TFuture<void> PrepareFuture_;
        TTableMountInfoPtr TableInfo_;
        std::vector<TSyncReplica> SyncReplicas_;


        TFuture<void> OnGotTableInfo(const TTableMountInfoPtr& tableInfo)
        {
            TableInfo_ = tableInfo;

            std::vector<TFuture<void>> futures;
            CheckPermissions(&futures);
            RegisterSyncReplicas(&futures);

            return AllSucceeded(std::move(futures));
        }

        void CheckPermissions(std::vector<TFuture<void>>* futures)
        {
            auto transaction = Transaction_.Lock();
            if (!transaction) {
                return;
            }

            const auto& client = transaction->Client_;
            const auto& permissionCache = client->GetNativeConnection()->GetPermissionCache();
            NSecurityClient::TPermissionKey permissionKey{
                .Object = FromObjectId(TableInfo_->TableId),
                .User = client->GetOptions().GetAuthenticatedUser(),
                .Permission = NYTree::EPermission::Write
            };
            auto future = permissionCache->Get(permissionKey);
            auto result = future.TryGet();
            if (!result || !result->IsOK()) {
                futures->push_back(std::move(future));
            }
        }

        void RegisterSyncReplicas(std::vector<TFuture<void>>* futures)
        {
            auto transaction = Transaction_.Lock();
            if (!transaction) {
                return;
            }

            for (const auto& replicaInfo : TableInfo_->Replicas) {
                if (replicaInfo->Mode != ETableReplicaMode::Sync) {
                    continue;
                }

                YT_LOG_DEBUG("Sync table replica registered (ReplicaId: %v, ClusterName: %v, ReplicaPath: %v)",
                    replicaInfo->ReplicaId,
                    replicaInfo->ClusterName,
                    replicaInfo->ReplicaPath);

                futures->push_back(
                    transaction->GetSyncReplicaTransaction(replicaInfo)
                        .Apply(BIND([=, this_ = MakeStrong(this)] (const NApi::ITransactionPtr& transaction) {
                            SyncReplicas_.push_back(TSyncReplica{
                                replicaInfo,
                                transaction
                            });
                        }).AsyncVia(transaction->SerializedInvoker_)));
            }
        }
    };

    //! Maintains per-table commit info.
    THashMap<TYPath, TTableCommitSessionPtr> TablePathToSession_;
    std::vector<TTableCommitSessionPtr> PendingSessions_;

    class TTabletCommitSession
        : public TRefCounted
    {
    public:
        TTabletCommitSession(
            TTransactionPtr transaction,
            TTabletInfoPtr tabletInfo,
            TTableMountInfoPtr tableInfo,
            TTableCommitSessionPtr tableSession,
            TColumnEvaluatorPtr columnEvaluator)
            : Transaction_(transaction)
            , TableInfo_(std::move(tableInfo))
            , TabletInfo_(std::move(tabletInfo))
            , TableSession_(std::move(tableSession))
            , Config_(transaction->Client_->GetNativeConnection()->GetConfig())
            , ColumnEvaluator_(std::move(columnEvaluator))
            , TableMountCache_(transaction->Client_->GetNativeConnection()->GetTableMountCache())
            , IsSortedTable_(TableInfo_->Schemas[ETableSchemaKind::Primary]->IsSorted())
            , ColumnCount_(TableInfo_->Schemas[ETableSchemaKind::Primary]->GetColumnCount())
            , KeyColumnCount_(TableInfo_->Schemas[ETableSchemaKind::Primary]->GetKeyColumnCount())
            , EnforceRowCountLimit_(transaction->Client_->GetOptions().GetAuthenticatedUser() != NSecurityClient::ReplicatorUserName)
            , Logger(transaction->Logger.WithTag("TabletId: %v", TabletInfo_->TabletId))
        { }

        void SubmitRow(
            EWireProtocolCommand command,
            TUnversionedRow row,
            TLockMask lockMask)
        {
            UnversionedSubmittedRows_.push_back({
                command,
                row,
                lockMask,
                static_cast<int>(UnversionedSubmittedRows_.size())});
        }

        void SubmitRow(TTypeErasedRow row)
        {
            VersionedSubmittedRows_.push_back(row);
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
            auto* codec = NCompression::GetCodec(Config_->WriteRowsRequestCodec);
            YT_VERIFY(!Batches_.empty());
            for (const auto& batch : Batches_) {
                batch->RequestData = codec->Compress(batch->Writer.Finish());
            }

            InvokeChannel_ = channel;
            InvokeNextBatch();
            return InvokePromise_;
        }

        TCellId GetCellId() const
        {
            return TabletInfo_->CellId;
        }

    private:
        const TWeakPtr<TTransaction> Transaction_;
        const TTableMountInfoPtr TableInfo_;
        const TTabletInfoPtr TabletInfo_;
        const TTableCommitSessionPtr TableSession_;
        const TConnectionConfigPtr Config_;
        const TColumnEvaluatorPtr ColumnEvaluator_;
        const ITableMountCachePtr TableMountCache_;
        const bool IsSortedTable_;
        const int ColumnCount_;
        const int KeyColumnCount_;
        const bool EnforceRowCountLimit_;

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

        std::vector<TTypeErasedRow> VersionedSubmittedRows_;

        struct TUnversionedSubmittedRow
        {
            EWireProtocolCommand Command;
            TUnversionedRow Row;
            TLockMask Locks;
            int SequentialId;
        };

        std::vector<TUnversionedSubmittedRow> UnversionedSubmittedRows_;

        IChannelPtr InvokeChannel_;
        int InvokeBatchIndex_ = 0;
        const TPromise<void> InvokePromise_ = NewPromise<void>();

        void PrepareVersionedRows()
        {
            for (const auto& typeErasedRow : VersionedSubmittedRows_) {
                IncrementAndCheckRowCount();

                auto* batch = EnsureBatch();
                ++batch->RowCount;

                auto& writer = batch->Writer;
                writer.WriteCommand(EWireProtocolCommand::VersionedWriteRow);

                if (IsSortedTable_) {
                    TVersionedRow row(typeErasedRow);
                    batch->DataWeight += GetDataWeight(row);
                    writer.WriteVersionedRow(row);
                } else {
                    TUnversionedRow row(typeErasedRow);
                    batch->DataWeight += GetDataWeight(row);
                    writer.WriteUnversionedRow(row);
                }
            }
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

            for (auto it = UnversionedSubmittedRows_.begin(); it != UnversionedSubmittedRows_.end();) {
                auto startIt = it;
                merger.InitPartialRow(startIt->Row);

                TLockMask lockMask;
                EWireProtocolCommand resultCommand;

                do {
                    switch (it->Command) {
                        case EWireProtocolCommand::DeleteRow:
                            merger.DeletePartialRow(it->Row);
                            break;

                        case EWireProtocolCommand::WriteRow:
                            merger.AddPartialRow(it->Row);
                            break;

                        case EWireProtocolCommand::ReadLockWriteRow:
                            merger.AddPartialRow(it->Row);
                            lockMask = MaxMask(lockMask, it->Locks);
                            break;

                        default:
                            YT_ABORT();
                    }
                    resultCommand = it->Command;
                    ++it;
                } while (it != UnversionedSubmittedRows_.end() &&
                    CompareRows(it->Row, startIt->Row, KeyColumnCount_) == 0);

                TUnversionedRow mergedRow;
                if (resultCommand == EWireProtocolCommand::DeleteRow) {
                    mergedRow = merger.BuildDeleteRow();
                } else {
                    if (lockMask) {
                        resultCommand = EWireProtocolCommand::ReadLockWriteRow;
                    }
                    mergedRow = merger.BuildMergedRow();
                }

                unversionedMergedRows.push_back({resultCommand, mergedRow, lockMask, /*sequentialId*/ 0});
            }

            for (const auto& submittedRow : unversionedMergedRows) {
                WriteRow(submittedRow);
            }

            PrepareVersionedRows();
        }

        void WriteRow(const TUnversionedSubmittedRow& submittedRow)
        {
            IncrementAndCheckRowCount();

            auto* batch = EnsureBatch();
            auto& writer = batch->Writer;
            ++batch->RowCount;
            batch->DataWeight += GetDataWeight(submittedRow.Row);

            writer.WriteCommand(submittedRow.Command);

            if (submittedRow.Command == EWireProtocolCommand::ReadLockWriteRow) {
                writer.WriteLockBitmap(submittedRow.Locks);
            }

            writer.WriteUnversionedRow(submittedRow.Row);
        }

        void PrepareOrderedBatches()
        {
            for (const auto& submittedRow : UnversionedSubmittedRows_) {
                WriteRow(submittedRow);
            }

            PrepareVersionedRows();
        }

        bool IsNewBatchNeeded()
        {
            if (Batches_.empty()) {
                return true;
            }

            const auto& lastBatch = Batches_.back();
            if (lastBatch->RowCount >= Config_->MaxRowsPerWriteRequest) {
                return true;
            }
            if (static_cast<ssize_t>(lastBatch->DataWeight) >= Config_->MaxDataWeightPerWriteRequest) {
                return true;
            }

            return false;
        }

        TBatch* EnsureBatch()
        {
            if (IsNewBatchNeeded()) {
                Batches_.emplace_back(new TBatch());
            }
            return Batches_.back().get();
        }

        void IncrementAndCheckRowCount()
        {
            ++TotalBatchedRowCount_;
            if (EnforceRowCountLimit_ && TotalBatchedRowCount_ > Config_->MaxRowsPerTransaction) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::TooManyRowsInTransaction,
                    "Transaction affects too many rows")
                    << TErrorAttribute("limit", Config_->MaxRowsPerTransaction);
            }
        }

        void InvokeNextBatch()
        {
            if (InvokeBatchIndex_ >= std::ssize(Batches_)) {
                InvokePromise_.Set(TError());
                return;
            }

            const auto& batch = Batches_[InvokeBatchIndex_++];

            auto transaction = Transaction_.Lock();
            if (!transaction) {
                return;
            }

            auto cellSession = transaction->GetCommitSession(GetCellId());

            TTabletServiceProxy proxy(InvokeChannel_);
            proxy.SetDefaultTimeout(Config_->WriteRowsTimeout);
            proxy.SetDefaultAcknowledgementTimeout(std::nullopt);

            auto req = proxy.Write();
            req->SetResponseHeavy(true);
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
            req->set_request_codec(static_cast<int>(Config_->WriteRowsRequestCodec));
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

            YT_LOG_DEBUG("Sending transaction rows (BatchIndex: %v/%v, RowCount: %v, Signature: %x, "
                "Versioned: %v, UpstreamReplicaId: %v)",
                InvokeBatchIndex_,
                Batches_.size(),
                batch->RowCount,
                req->signature(),
                req->versioned(),
                TableSession_->GetUpstreamReplicaId());

            req->Invoke().Subscribe(
                BIND(&TTabletCommitSession::OnResponse, MakeStrong(this)));
        }

        void OnResponse(const TTabletServiceProxy::TErrorOrRspWritePtr& rspOrError)
        {
            if (!rspOrError.IsOK()) {
                auto error = TError("Error sending transaction rows")
                    << rspOrError;
                YT_LOG_DEBUG(error);
                TableMountCache_->InvalidateOnError(error, true /*forceRetry*/);
                InvokePromise_.Set(error);
                return;
            }

            auto owner = Transaction_.Lock();
            if (!owner) {
                return;
            }

            YT_LOG_DEBUG("Transaction rows sent successfully (BatchIndex: %v/%v)",
                InvokeBatchIndex_,
                Batches_.size());

            InvokeNextBatch();
        }
    };

    //! Maintains per-tablet commit info.
    THashMap<TTabletId, TTabletCommitSessionPtr> TabletIdToSession_;

    class TCellCommitSession
        : public TRefCounted
    {
    public:
        TCellCommitSession(const TTransactionPtr& transaction, TCellId cellId)
            : Transaction_(transaction)
            , CellId_(cellId)
            , Logger(transaction->Logger.WithTag("CellId: %v", CellId_))
        { }

        void RegisterRequests(int count)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            RequestsTotal_ += count;
            RequestsRemaining_ += count;
        }

        TTransactionSignature AllocateRequestSignature()
        {
            VERIFY_THREAD_AFFINITY_ANY();

            auto remaining = --RequestsRemaining_;
            YT_VERIFY(remaining >= 0);
            return remaining == 0
                ? FinalTransactionSignature - InitialTransactionSignature - RequestsTotal_.load() + 1
                : 1;
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
                return MakeFuture(TError(NYT::EErrorCode::Canceled, "Transaction destroyed"));
            }

            YT_LOG_DEBUG("Sending transaction actions (ActionCount: %v)",
                Actions_.size());

            TFuture<void> future;
            switch (TypeFromId(CellId_)) {
                case EObjectType::TabletCell:
                    future = SendTabletActions(transaction, channel);
                    break;
                case EObjectType::MasterCell:
                    future = SendMasterActions(transaction, channel);
                    break;
                default:
                    YT_ABORT();
            }

            return future.Apply(
                BIND(&TCellCommitSession::OnResponse, MakeStrong(this))
                    /* serialization intentionally omitted */);
        }

    private:
        const TWeakPtr<TTransaction> Transaction_;
        const TCellId CellId_;
        const NLogging::TLogger Logger;

        std::vector<TTransactionActionData> Actions_;

        std::atomic<int> RequestsTotal_ = 0;
        std::atomic<int> RequestsRemaining_ = 0;


        TFuture<void> SendTabletActions(const TTransactionPtr& owner, const IChannelPtr& channel)
        {
            TTabletServiceProxy proxy(channel);
            auto req = proxy.RegisterTransactionActions();
            req->SetResponseHeavy(true);
            ToProto(req->mutable_transaction_id(), owner->GetId());
            req->set_transaction_start_timestamp(owner->GetStartTimestamp());
            req->set_transaction_timeout(ToProto<i64>(owner->GetTimeout()));
            req->set_signature(AllocateRequestSignature());
            ToProto(req->mutable_actions(), Actions_);
            return req->Invoke().As<void>();
        }

        TFuture<void> SendMasterActions(const TTransactionPtr& owner, const IChannelPtr& channel)
        {
            TTransactionServiceProxy proxy(channel);
            auto req = proxy.RegisterTransactionActions();
            req->SetResponseHeavy(true);
            ToProto(req->mutable_transaction_id(), owner->GetId());
            ToProto(req->mutable_actions(), Actions_);
            return req->Invoke().As<void>();
        }

        void OnResponse(const TError& result)
        {
            if (!result.IsOK()) {
                auto error = TError("Error sending transaction actions")
                    << result;
                YT_LOG_DEBUG(error);
                THROW_ERROR(error);
            }

            YT_LOG_DEBUG("Transaction actions sent successfully");
        }
    };

    //! Maintains per-cell commit info.
    THashMap<TCellId, TCellCommitSessionPtr> CellIdToSession_;

    //! Maps replica cluster name to sync replica transaction.
    THashMap<TString, TPromise<NApi::ITransactionPtr>> ClusterNameToSyncReplicaTransactionPromise_;

    //! Caches mappings from name table ids to schema ids.
    THashMap<std::tuple<TTableId, TNameTablePtr, ETableSchemaKind>, TNameTableToSchemaIdMapping> IdMappingCache_;

    //! The actual options to be used during commit.
    TTransactionCommitOptions CommitOptions_;


    const TNameTableToSchemaIdMapping& GetColumnIdMapping(
        const TTableMountInfoPtr& tableInfo,
        const TNameTablePtr& nameTable,
        ETableSchemaKind kind)
    {
        auto key = std::make_tuple(tableInfo->TableId, nameTable, kind);
        auto it = IdMappingCache_.find(key);
        if (it == IdMappingCache_.end()) {
            auto mapping = BuildColumnIdMapping(*tableInfo->Schemas[kind], nameTable);
            it = IdMappingCache_.emplace(key, std::move(mapping)).first;
        }
        return it->second;
    }

    TFuture<NApi::ITransactionPtr> GetSyncReplicaTransaction(const TTableReplicaInfoPtr& replicaInfo)
    {
        TPromise<NApi::ITransactionPtr> promise;
        auto it = ClusterNameToSyncReplicaTransactionPromise_.find(replicaInfo->ClusterName);
        if (it != ClusterNameToSyncReplicaTransactionPromise_.end()) {
            return it->second.ToFuture();
        } else {
            promise = NewPromise<NApi::ITransactionPtr>();
            YT_VERIFY(ClusterNameToSyncReplicaTransactionPromise_.emplace(replicaInfo->ClusterName, promise).second);
        }

        return
            [&] {
                const auto& clusterDirectory = Client_->GetNativeConnection()->GetClusterDirectory();
                if (clusterDirectory->FindConnection(replicaInfo->ClusterName)) {
                    return VoidFuture;
                }

                YT_LOG_DEBUG("Replica cluster is not known; waiting for cluster directory sync (ClusterName: %v)",
                    replicaInfo->ClusterName);

                return Client_
                    ->GetNativeConnection()
                    ->GetClusterDirectorySynchronizer()
                    ->Sync();
            }()
            .Apply(BIND([=, this_ = MakeStrong(this)] {
                const auto& clusterDirectory = Client_->GetNativeConnection()->GetClusterDirectory();
                return clusterDirectory->GetConnectionOrThrow(replicaInfo->ClusterName);
            }) /* serialization intentionally omitted */)
            .Apply(BIND([=, this_ = MakeStrong(this)] (const NApi::IConnectionPtr& connection) {
                if (connection->GetCellTag() == Client_->GetConnection()->GetCellTag()) {
                    return MakeFuture<NApi::ITransactionPtr>(nullptr);
                }

                TTransactionStartOptions options;
                options.Id = Transaction_->GetId();
                options.StartTimestamp = Transaction_->GetStartTimestamp();

                auto client = connection->CreateClient(Client_->GetOptions());
                return client->StartTransaction(ETransactionType::Tablet, options);
            }) /* serialization intentionally omitted */)
            .Apply(BIND([=, this_ = MakeStrong(this)] (const NApi::ITransactionPtr& transaction) {
                promise.Set(transaction);

                if (transaction) {
                    YT_LOG_DEBUG("Sync replica transaction started (ClusterName: %v)",
                        replicaInfo->ClusterName);
                    DoRegisterSyncReplicaAlienTransaction(transaction);
                }

                return transaction;
            }) /* serialization intentionally omitted */);
    }


    void DoEnqueueModificationRequest(TModificationRequest* request)
    {
        PendingRequests_.push_back(request);
    }

    void EnqueueModificationRequest(std::unique_ptr<TModificationRequest> request)
    {
        if (auto sequenceNumber = request->GetSequenceNumber()) {
            if (*sequenceNumber < 0) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::ProtocolError,
                    "Packet sequence number is negative")
                    << TErrorAttribute("sequence_number", *sequenceNumber);
            }
            // This may call DoEnqueueModificationRequest right away.
            OrderedRequestsSlidingWindow_.AddPacket(*sequenceNumber, request.get(), [&] (TModificationRequest* request) {
                DoEnqueueModificationRequest(request);
            });
        } else {
            DoEnqueueModificationRequest(request.get());
        }
        Requests_.push_back(std::move(request));
    }


    TTableCommitSessionPtr GetOrCreateTableSession(const TYPath& path, TTableReplicaId upstreamReplicaId)
    {
        auto it = TablePathToSession_.find(path);
        if (it == TablePathToSession_.end()) {
            auto session = New<TTableCommitSession>(this, path, upstreamReplicaId);
            PendingSessions_.push_back(session);
            it = TablePathToSession_.emplace(path, session).first;
        } else {
            const auto& session = it->second;
            if (session->GetUpstreamReplicaId() != upstreamReplicaId) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::UpstreamReplicaMismatch,
                    "Mismatched upstream replica is specified for modifications to table %v: %v != !v",
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
        auto tabletId = tabletInfo->TabletId;
        auto it = TabletIdToSession_.find(tabletId);
        if (it == TabletIdToSession_.end()) {
            const auto& evaluatorCache = Client_->GetNativeConnection()->GetColumnEvaluatorCache();
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

    TFuture<void> DoAbort(TSpinlockGuard<TAdaptiveLock>* guard, const TTransactionAbortOptions& options = {})
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (State_ == ETransactionState::Aborted) {
            return AbortPromise_.ToFuture();
        }

        State_ = ETransactionState::Aborted;
        AbortPromise_ = NewPromise<void>();
        auto abortFuture = AbortPromise_.ToFuture();

        guard->Release();

        for (const auto& transaction : GetAlienTransactions()) {
            transaction->Abort();
        }

        AbortPromise_.SetFrom(Transaction_->Abort(options));
        return abortFuture;
    }

    TFuture<void> PrepareRequests()
    {
        if (!OrderedRequestsSlidingWindow_.IsEmpty()) {
            return MakeFuture(TError(
                NRpc::EErrorCode::ProtocolError,
                "Cannot prepare transaction %v since sequence number %v is missing",
                GetId(),
                OrderedRequestsSlidingWindow_.GetNextSequenceNumber()));
        }

        return DoPrepareRequests();
    }

    TFuture<void> DoPrepareRequests()
    {
        // Tables with local sync replicas pose a problem since modifications in such tables
        // induce more modifications that need to be taken care of.
        // Here we iterate over requests and sessions until no more new items are added.
        if (!PendingRequests_.empty() || !PendingSessions_.empty()) {
            decltype(PendingRequests_) pendingRequests;
            std::swap(PendingRequests_, pendingRequests);

            decltype(PendingSessions_) pendingSessions;
            std::swap(PendingSessions_, pendingSessions);

            std::vector<TFuture<void>> prepareFutures;
            prepareFutures.reserve(pendingSessions.size());
            for (const auto& tableSession : pendingSessions) {
                prepareFutures.push_back(tableSession->GetPrepareFuture());
            }

            return AllSucceeded(std::move(prepareFutures))
                .Apply(BIND([=, this_ = MakeStrong(this), pendingRequests = std::move(pendingRequests)] {
                    for (auto* request : pendingRequests) {
                        request->SubmitRows();
                    }

                    return DoPrepareRequests();
                }).AsyncVia(SerializedInvoker_));
        } else {
            for (const auto& [tabletId, tabletSession] : TabletIdToSession_) {
                auto cellId = tabletSession->GetCellId();
                int requestCount = tabletSession->Prepare();
                auto cellSession = GetOrCreateCellCommitSession(cellId);
                cellSession->RegisterRequests(requestCount);
            }

            for (const auto& [cellId, session] : CellIdToSession_) {
                Transaction_->RegisterParticipant(cellId);
            }

            return VoidFuture;
        }
    }

    TFuture<void> SendRequests()
    {
        std::vector<TFuture<void>> futures;

        for (const auto& [tabletId, session] : TabletIdToSession_) {
            auto cellId = session->GetCellId();
            auto channel = Client_->GetCellChannelOrThrow(cellId);
            futures.push_back(session->Invoke(std::move(channel)));
        }

        for (const auto& [cellId, session] : CellIdToSession_) {
            auto channel = Client_->GetCellChannelOrThrow(cellId);
            futures.push_back(session->Invoke(std::move(channel)));
        }

        return AllSucceeded(std::move(futures));
    }

    void BuildAdjustedCommitOptions(const TTransactionCommitOptions& options)
    {
        CommitOptions_ = options;

        for (const auto& [path, session] : TablePathToSession_) {
            if (session->GetInfo()->IsReplicated()) {
                CommitOptions_.Force2PC = true;
                break;
            }
        }
    }

    TFuture<TTransactionCommitResult> DoCommit(const TTransactionCommitOptions& options, bool needsFlush)
    {
        for (auto cellId : options.AdditionalParticipantCellIds) {
            Transaction_->RegisterParticipant(cellId);
        }

        return
            [&] {
                return needsFlush ? PrepareRequests() : VoidFuture;
            }()
            .Apply(
                BIND([=, this_ = MakeStrong(this)] {
                    BuildAdjustedCommitOptions(options);
                    Transaction_->ChooseCoordinator(CommitOptions_);

                    return Transaction_->ValidateNoDownedParticipants();
                }).AsyncVia(SerializedInvoker_))
            .Apply(
                BIND([=, this_ = MakeStrong(this)] {
                    std::vector<TFuture<TTransactionFlushResult>> futures;
                    if (needsFlush) {
                        for (const auto& transaction : GetAlienTransactions()) {
                            futures.push_back(transaction->Flush());
                        }
                        futures.push_back(SendRequests()
                            .Apply(BIND([] { return TTransactionFlushResult{}; })));
                    }
                    return AllSucceeded(std::move(futures));
                }).AsyncVia(SerializedInvoker_))
            .Apply(
                BIND([=, this_ = MakeStrong(this)] (const std::vector<TTransactionFlushResult>& results) {
                    for (const auto& result : results) {
                        for (auto cellId : result.ParticipantCellIds) {
                            Transaction_->RegisterParticipant(cellId);
                        }
                    }

                    return Transaction_->Commit(CommitOptions_);
                }).AsyncVia(SerializedInvoker_))
            .Apply(
                BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TTransactionCommitResult>& resultOrError) {
                    {
                        auto guard = Guard(SpinLock_);
                        if (resultOrError.IsOK() && State_ == ETransactionState::Committing) {
                            State_ = ETransactionState::Committed;
                        } else if (!resultOrError.IsOK()) {
                            DoAbort(&guard);
                            THROW_ERROR_EXCEPTION("Error committing transaction %v",
                                GetId())
                                << MakeClusterIdErrorAttribute()
                                << resultOrError;
                        }
                    }

                    for (const auto& transaction : GetAlienTransactions()) {
                        transaction->Detach();
                    }

                    return resultOrError.Value();
                }) /* serialization intentionally omitted */);
    }

    TFuture<TTransactionFlushResult> DoFlush()
    {
        return PrepareRequests()
            .Apply(
                BIND([=, this_ = MakeStrong(this)] {
                    return SendRequests();
                }).AsyncVia(SerializedInvoker_))
            .Apply(
                BIND([=, this_ = MakeStrong(this)] (const TError& error) {
                    {
                        auto guard = Guard(SpinLock_);
                        if (error.IsOK() && State_ == ETransactionState::Flushing) {
                            State_ = ETransactionState::Flushed;
                        } else if (!error.IsOK()) {
                            YT_LOG_DEBUG(error, "Error flushing transaction");
                            DoAbort(&guard);
                            THROW_ERROR_EXCEPTION("Error flushing transaction %v",
                                GetId())
                                << MakeClusterIdErrorAttribute()
                                << error;
                        }
                    }

                    TTransactionFlushResult result{
                        .ParticipantCellIds = GetKeys(CellIdToSession_)
                    };

                    YT_LOG_DEBUG("Transaction flushed (ParticipantCellIds: %v)",
                        result.ParticipantCellIds);

                    return result;
                }).AsyncVia(SerializedInvoker_));
    }


    TCellCommitSessionPtr GetOrCreateCellCommitSession(TCellId cellId)
    {
        auto it = CellIdToSession_.find(cellId);
        if (it == CellIdToSession_.end()) {
            it = CellIdToSession_.emplace(cellId, New<TCellCommitSession>(this, cellId)).first;
        }
        return it->second;
    }

    TCellCommitSessionPtr GetCommitSession(TCellId cellId)
    {
        return GetOrCrash(CellIdToSession_, cellId);
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
                YT_ABORT();
        }
    }


    void DoRegisterAlienTransaction(const NApi::ITransactionPtr& transaction, ETransactionState expectedState)
    {
        {
            auto guard = Guard(SpinLock_);

            if (State_ != expectedState) {
                THROW_ERROR_EXCEPTION(
                    NTransactionClient::EErrorCode::InvalidTransactionState,
                    "Transaction %v is in %Qlv state",
                    GetId(),
                    State_);
            }

            if (GetType() != ETransactionType::Tablet) {
                THROW_ERROR_EXCEPTION(
                    NTransactionClient::EErrorCode::MalformedAlienTransaction,
                    "Transaction %v is of type %Qlv and hence does not allow alien transactions",
                    GetId(),
                    GetType());
            }

            if (GetId() != transaction->GetId()) {
                THROW_ERROR_EXCEPTION(
                    NTransactionClient::EErrorCode::MalformedAlienTransaction,
                    "Transaction id mismatch: local %v, alien %v",
                    GetId(),
                    transaction->GetId());
            }

            AlienTransactions_.push_back(transaction);
        }

        YT_LOG_DEBUG("Alien transaction registered (AlienConnectionId: %v)",
            transaction->GetConnection()->GetLoggingTag());
    }

    void DoRegisterSyncReplicaAlienTransaction(const NApi::ITransactionPtr& transaction)
    {
        auto guard = Guard(SpinLock_);
        AlienTransactions_.push_back(transaction);
    }

    std::vector<NApi::ITransactionPtr> GetAlienTransactions()
    {
        auto guard = Guard(SpinLock_);
        return AlienTransactions_;
    }

    TErrorAttribute MakeClusterIdErrorAttribute()
    {
        return {"cluster_id", Client_->GetConnection()->GetClusterId()};
    }
};

DEFINE_REFCOUNTED_TYPE(TTransaction)

ITransactionPtr CreateTransaction(
    IClientPtr client,
    NTransactionClient::TTransactionPtr transaction,
    const NLogging::TLogger& logger)
{
    return New<TTransaction>(
        std::move(client),
        std::move(transaction),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
