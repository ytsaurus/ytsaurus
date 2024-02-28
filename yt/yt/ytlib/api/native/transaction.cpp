#include "transaction.h"

#include "cell_commit_session.h"
#include "client.h"
#include "connection.h"
#include "config.h"
#include "secondary_index_modification.h"
#include "sync_replica_cache.h"
#include "tablet_commit_session.h"

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/ytlib/api/native/tablet_helpers.h>

#include <yt/yt/ytlib/transaction_client/transaction_manager.h>
#include <yt/yt/ytlib/transaction_client/action.h>
#include <yt/yt/ytlib/transaction_client/transaction_service_proxy.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/table_client/row_merger.h>
#include <yt/yt/ytlib/table_client/schema.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/ytlib/chaos_client/coordinator_service_proxy.h>
#include <yt/yt/ytlib/chaos_client/proto/coordinator_service.pb.h>

#include <yt/yt/client/chaos_client/replication_card_cache.h>

#include <yt/yt/client/queue_client/consumer_client.h>

#include <yt/yt/client/api/dynamic_table_transaction_mixin.h>
#include <yt/yt/client/api/queue_transaction_mixin.h>

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
using namespace NChaosClient;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NRpc;
using namespace NChunkClient;
using namespace NQueueClient;

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
    : public virtual ITransaction
    , public TDynamicTableTransactionMixin
    , public TQueueTransactionMixin
{
public:
    TTransaction(
        IClientPtr client,
        NTransactionClient::TTransactionPtr transaction,
        NLogging::TLogger logger)
        : Client_(std::move(client))
        , Transaction_(std::move(transaction))
        , Logger(logger.WithTag("TransactionId: %v, ClusterTag: %v",
            GetId(),
            Client_->GetConnection()->GetClusterTag()))
        , Counters_(Client_->GetCounters().TransactionCounters)
        , SerializedInvoker_(CreateSerializedInvoker(
            Client_->GetConnection()->GetInvoker()))
        , HunkMemoryPool_(THunkTransactionTag())
        , OrderedRequestsSlidingWindow_(
            Client_->GetNativeConnection()->GetConfig()->MaxRequestWindowSize)
        , CellCommitSessionProvider_(CreateCellCommitSessionProvider(
            Client_,
            MakeWeak(Transaction_),
            Logger))
    {
        SubscribeCommitted(BIND_NO_PROPAGATE([counters = Counters_] { counters.CommittedTransactionCounter.Increment(); }));
        SubscribeAborted(BIND_NO_PROPAGATE([counters = Counters_] (const TError&) { counters.AbortedTransactionCounter.Increment(); }));
    }


    NApi::IConnectionPtr GetConnection() override
    {
        return Client_->GetConnection();
    }

    NApi::IClientPtr GetClient() const override
    {
        return Client_;
    }

    NTransactionClient::ETransactionType GetType() const override
    {
        return Transaction_->GetType();
    }

    TTransactionId GetId() const override
    {
        return Transaction_->GetId();
    }

    TTimestamp GetStartTimestamp() const override
    {
        return Transaction_->GetStartTimestamp();
    }

    EAtomicity GetAtomicity() const override
    {
        return Transaction_->GetAtomicity();
    }

    EDurability GetDurability() const override
    {
        return Transaction_->GetDurability();
    }

    TDuration GetTimeout() const override
    {
        return Transaction_->GetTimeout();
    }


    TFuture<void> Ping(const TTransactionPingOptions& options = {}) override
    {
        return Transaction_->Ping(options);
    }

    TFuture<TTransactionCommitResult> Commit(const TTransactionCommitOptions& options) override
    {
        Counters_.TransactionCounter.Increment();

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

    TFuture<void> Abort(const TTransactionAbortOptions& options = {}) override
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

    void Detach() override
    {
        auto guard = Guard(SpinLock_);

        if (State_ != ETransactionState::Aborted) {
            State_ = ETransactionState::Detached;
            Transaction_->Detach();
        }
    }

    TFuture<TTransactionFlushResult> Flush() override
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

    void AddAction(TCellId cellId, const TTransactionActionData& data) override
    {
        auto guard = Guard(SpinLock_);

        if (State_ != ETransactionState::Active) {
            THROW_ERROR_EXCEPTION(
                NTransactionClient::EErrorCode::InvalidTransactionState,
                "Cannot add action since transaction %v is in %Qlv state",
                GetId(),
                State_);
        }

        DoAddAction(cellId, data);
    }


    void RegisterAlienTransaction(const NApi::ITransactionPtr& transaction) override
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


    void SubscribeCommitted(const TCommittedHandler& callback) override
    {
        Transaction_->SubscribeCommitted(callback);
    }

    void UnsubscribeCommitted(const TCommittedHandler& callback) override
    {
        Transaction_->UnsubscribeCommitted(callback);
    }


    void SubscribeAborted(const TAbortedHandler& callback) override
    {
        Transaction_->SubscribeAborted(callback);
    }

    void UnsubscribeAborted(const TAbortedHandler& callback) override
    {
        Transaction_->UnsubscribeAborted(callback);
    }


    TFuture<ITransactionPtr> StartNativeTransaction(
        ETransactionType type,
        const TTransactionStartOptions& options) override
    {
        auto adjustedOptions = options;
        adjustedOptions.ParentId = GetId();
        return Client_->StartNativeTransaction(
            type,
            adjustedOptions);
    }

    TFuture<NApi::ITransactionPtr> StartTransaction(
        ETransactionType type,
        const TTransactionStartOptions& options) override
    {
        return StartNativeTransaction(type, options).As<NApi::ITransactionPtr>();
    }

    using TQueueTransactionMixin::AdvanceConsumer;
    TFuture<void> AdvanceConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset,
        const TAdvanceConsumerOptions& /*options*/) override
    {
        auto tableMountCache = GetClient()->GetTableMountCache();
        auto queuePhysicalPath = queuePath;
        auto queueTableInfoOrError = WaitFor(tableMountCache->GetTableInfo(queuePath.GetPath()));
        if (!queueTableInfoOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to resolve queue path")
                << queueTableInfoOrError;
        }
        queuePhysicalPath = NYPath::TRichYPath(queueTableInfoOrError.Value()->PhysicalPath, queuePath.Attributes());

        auto queueClient = GetClient();
        if (auto queueCluster = queuePath.GetCluster()) {
            auto queueConnection = FindRemoteConnection(Client_->GetNativeConnection(), *queueCluster);
            if (!queueConnection) {
                THROW_ERROR_EXCEPTION(
                    "Queue cluster %Qv was not found for path %v",
                    *queueCluster,
                    queuePath
                );
            }

            auto queueClientOptions = TClientOptions::FromUser(Client_->GetOptions().GetAuthenticatedUser());
            queueClient = queueConnection->CreateNativeClient(queueClientOptions);
        }

        auto subConsumerClient = CreateSubConsumerClient(GetClient(), queueClient, consumerPath.GetPath(), queuePhysicalPath);
        subConsumerClient->Advance(this, partitionIndex, oldOffset, newOffset);

        return VoidFuture;
    }

    void ModifyRows(
        const TYPath& path,
        TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications,
        const TModifyRowsOptions& options) override
    {
        ValidateTabletTransactionId(GetId());

        YT_LOG_DEBUG("Buffering client row modifications (Count: %v, SequenceNumber: %v, SequenceNumberSourceId: %v)",
            modifications.Size(),
            options.SequenceNumber,
            options.SequenceNumberSourceId);

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
                    &HunkMemoryPool_,
                    std::move(modifications),
                    options));
        } catch (const std::exception& ex) {
            YT_UNUSED_FUTURE(DoAbort(&guard));
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

    DELEGATE_TIMESTAMPED_METHOD(TFuture<TUnversionedLookupRowsResult>, LookupRows, (
        const TYPath& path,
        TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options),
        (path, nameTable, keys, options))
    DELEGATE_TIMESTAMPED_METHOD(TFuture<TVersionedLookupRowsResult>, VersionedLookupRows, (
        const TYPath& path,
        TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TVersionedLookupRowsOptions& options),
        (path, nameTable, keys, options))
    DELEGATE_TIMESTAMPED_METHOD(TFuture<std::vector<TUnversionedLookupRowsResult>>, MultiLookupRows, (
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

    DELEGATE_METHOD(TFuture<TPullRowsResult>, PullRows, (
        const TYPath& path,
        const TPullRowsOptions& options),
        (path, options))

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

    const TTransactionCounters Counters_;

    struct TNativeTransactionBufferTag
    { };

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TNativeTransactionBufferTag());

    const IInvokerPtr SerializedInvoker_;

    struct THunkTransactionTag
    { };
    TChunkedMemoryPool HunkMemoryPool_{THunkTransactionTag()};

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    ETransactionState State_ = ETransactionState::Active;
    TPromise<void> AbortPromise_;
    std::vector<NApi::ITransactionPtr> AlienTransactions_;

    class TTableCommitSession;
    using TTableCommitSessionPtr = TIntrusivePtr<TTableCommitSession>;

    class TTabletCommitSession;
    using TTabletCommitSessionPtr = TIntrusivePtr<TTabletCommitSession>;

    class TModificationRequest
    {
    public:
        TModificationRequest(
            TTransaction* transaction,
            IConnectionPtr connection,
            const TYPath& path,
            TNameTablePtr nameTable,
            TChunkedMemoryPool* memoryPool,
            TSharedRange<TRowModification> modifications,
            const TModifyRowsOptions& options)
            : Transaction_(transaction)
            , Connection_(std::move(connection))
            , Path_(path)
            , NameTable_(std::move(nameTable))
            , HunkMemoryPool_(memoryPool)
            , Modifications_(std::move(modifications))
            , Options_(options)
            , Logger(transaction->Logger)
            , TableSession_(transaction->GetOrCreateTableSession(
                Path_,
                Options_.UpstreamReplicaId,
                Options_.ReplicationCard,
                Options_.TopmostTransaction))
        { }

        std::optional<TMultiSlidingWindowSequenceNumber> GetSequenceNumber()
        {
            if (Options_.SequenceNumber) {
                return TMultiSlidingWindowSequenceNumber{
                    .SourceId = Options_.SequenceNumberSourceId,
                    .Value = *Options_.SequenceNumber
                };
            } else {
                return {};
            }
        }

        void PrepareRows()
        {
            try {
                GuardedPrepareRows();
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error preparing rows for table %v",
                    TableSession_->GetInfo()->Path)
                    << TError(ex);
            }
        }

        void GuardedPrepareRows()
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

            if (!Options_.ReplicationCard) {
                for (const auto& syncReplica : syncReplicas) {
                    auto replicaOptions = Options_;
                    replicaOptions.UpstreamReplicaId = syncReplica.ReplicaInfo->ReplicaId;
                    replicaOptions.SequenceNumber.reset();
                    replicaOptions.ReplicationCard = syncReplica.ReplicationCard;
                    replicaOptions.TopmostTransaction = false;

                    if (syncReplica.Transaction) {
                        YT_LOG_DEBUG("Submitting remote sync replication modifications (Count: %v)",
                            Modifications_.Size());
                        syncReplica.Transaction->ModifyRows(
                            syncReplica.ReplicaInfo->ReplicaPath,
                            NameTable_,
                            Modifications_,
                            replicaOptions);
                    } else {
                        // YT-7571: Local sync replicas must be handled differently.
                        // We cannot add more modifications via ITransactions interface since
                        // the transaction is already committing.

                        // For chaos replicated tables this branch is used to send data to itself.

                        YT_LOG_DEBUG("Buffering local sync replication modifications (Count: %v)",
                            Modifications_.Size());
                        transaction->EnqueueModificationRequest(std::make_unique<TModificationRequest>(
                            transaction.Get(),
                            Connection_,
                            syncReplica.ReplicaInfo->ReplicaPath,
                            NameTable_,
                            HunkMemoryPool_,
                            Modifications_,
                            replicaOptions));
                    }
                }
            }

            if (Options_.TopmostTransaction && tableInfo->ReplicationCardId) {
                // For chaos tables we write to all replicas via nested invocations above.
                return;
            }

            std::optional<int> tabletIndexColumnId;
            if (!tableInfo->IsSorted()) {
                tabletIndexColumnId = NameTable_->GetIdOrRegisterName(TabletIndexColumnName);
            }

            const auto& primarySchema = tableInfo->Schemas[ETableSchemaKind::Primary];
            const auto& primaryIdMapping = GetColumnIdMapping(transaction, tableInfo, ETableSchemaKind::Primary);

            const auto& primarySchemaWithTabletIndex = tableInfo->Schemas[ETableSchemaKind::PrimaryWithTabletIndex];
            const auto& primaryWithTabletIndexIdMapping = GetColumnIdMapping(transaction, tableInfo, ETableSchemaKind::PrimaryWithTabletIndex);

            const auto& writeSchema = tableInfo->Schemas[ETableSchemaKind::Write];
            const auto& writeIdMapping = GetColumnIdMapping(transaction, tableInfo, ETableSchemaKind::Write);

            const auto& versionedWriteSchema = tableInfo->Schemas[ETableSchemaKind::VersionedWrite];
            const auto& versionedWriteIdMapping = GetColumnIdMapping(transaction, tableInfo, ETableSchemaKind::VersionedWrite);

            const auto& deleteSchema = tableInfo->Schemas[ETableSchemaKind::Delete];
            const auto& deleteIdMapping = GetColumnIdMapping(transaction, tableInfo, ETableSchemaKind::Delete);

            const auto& modificationSchema = !tableInfo->IsPhysicallyLog() && !tableInfo->IsSorted() ? primarySchema : primarySchemaWithTabletIndex;
            const auto& modificationIdMapping = !tableInfo->IsPhysicallyLog() && !tableInfo->IsSorted() ? primaryIdMapping : primaryWithTabletIndexIdMapping;

            std::vector<int> columnIndexToLockIndex;
            GetLocksMapping(
                *writeSchema,
                transaction->GetAtomicity() == NTransactionClient::EAtomicity::Full,
                &columnIndexToLockIndex);

            for (const auto& modification : Modifications_) {
                switch (modification.Type) {
                    case ERowModificationType::Write:
                        if (!modification.Locks.IsNone()) {
                            THROW_ERROR_EXCEPTION("Cannot perform lock by %Qlv modification type, use %Qlv",
                                ERowModificationType::Write,
                                ERowModificationType::WriteAndLock);
                        }

                        ValidateClientDataRow(
                            TUnversionedRow(modification.Row),
                            *writeSchema,
                            writeIdMapping,
                            NameTable_,
                            tabletIndexColumnId,
                            Options_.AllowMissingKeyColumns);
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
                                NameTable_,
                                Options_.AllowMissingKeyColumns);
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

                    case ERowModificationType::WriteAndLock: {
                        if (!tableInfo->IsSorted()) {
                            THROW_ERROR_EXCEPTION(
                                NTabletClient::EErrorCode::TableMustBeSorted,
                                "Cannot perform lock in a non-sorted table %v",
                                tableInfo->Path);
                        }

                        auto row = TUnversionedRow(modification.Row);

                        // COMPAT(ponasenko-rs)
                        if (!tableInfo->EnableSharedWriteLocks) {
                            for (int lockIndex = 0; lockIndex < modification.Locks.GetSize(); ++lockIndex) {
                                if (modification.Locks.Get(lockIndex) == ELockType::SharedWrite) {
                                    THROW_ERROR_EXCEPTION("Shared write locks are not allowed for the table");
                                }
                            }
                        }

                        bool hasNonKeyColumns = ValidateNonKeyColumnsAgainstLock(
                            row,
                            modification.Locks,
                            *writeSchema,
                            writeIdMapping, // NB: Should be consistent with columnIndexToLockIndex.
                            NameTable_,
                            columnIndexToLockIndex,
                            /*allowSharedWriteLocks*/ !tableInfo->IsPhysicallyLog() && tableInfo->EnableSharedWriteLocks);

                        if (hasNonKeyColumns) {
                            // Lock with write.
                            ValidateClientDataRow(
                                row,
                                *writeSchema,
                                writeIdMapping,
                                NameTable_,
                                tabletIndexColumnId,
                                Options_.AllowMissingKeyColumns);
                        } else {
                            // Lock without write.
                            ValidateClientKey(
                                row,
                                *deleteSchema,
                                deleteIdMapping,
                                NameTable_);
                        }
                        break;
                    }

                    default:
                        YT_ABORT();
                }
            }

            const auto& rowBuffer = transaction->RowBuffer_;

            std::vector<bool> columnPresenceBuffer(modificationSchema->GetColumnCount());

            ModificationsData_.resize(Modifications_.size());
            for (int modificationIndex = 0; modificationIndex < std::ssize(Modifications_); ++modificationIndex) {
                const auto& modification = Modifications_[modificationIndex];
                auto& modificationsData = ModificationsData_[modificationIndex];
                switch (modification.Type) {
                    case ERowModificationType::Write:
                    case ERowModificationType::Delete:
                    case ERowModificationType::WriteAndLock: {
                        modificationsData.CapturedRow = rowBuffer->CaptureAndPermuteRow(
                            TUnversionedRow(modification.Row),
                            *modificationSchema,
                            modificationSchema->GetKeyColumnCount(),
                            modificationIdMapping,
                            modification.Type == ERowModificationType::Write ? &columnPresenceBuffer : nullptr);

                        if (tableInfo->HunkStorageId) {
                            auto rowPayloads = ExtractHunks(modificationsData.CapturedRow, modificationSchema);
                            modificationsData.HunkCount = std::ssize(rowPayloads);
                            HunkPayloads_.insert(
                                HunkPayloads_.end(),
                                std::make_move_iterator(rowPayloads.begin()),
                                std::make_move_iterator(rowPayloads.end()));
                        }
                    }
                    default:
                        continue;
                }
            }
        }

        TFuture<void> WriteHunks()
        {
            if (HunkPayloads_.empty()) {
                return VoidFuture;
            }

            auto transaction = Transaction_.Lock();
            if (!transaction) {
                return VoidFuture;
            }

            const auto& hunkTableInfo = TableSession_->GetHunkTableInfo();
            RandomHunkTabletInfo_ = hunkTableInfo->GetRandomMountedTablet();

            auto cellChannel = transaction->Client_->GetCellChannelOrThrow(RandomHunkTabletInfo_->CellId);
            TTabletServiceProxy proxy(cellChannel);
            auto req = proxy.WriteHunks();
            ToProto(req->mutable_tablet_id(), RandomHunkTabletInfo_->TabletId);
            req->set_mount_revision(RandomHunkTabletInfo_->MountRevision);

            auto payloadCount = std::ssize(HunkPayloads_);
            auto payloadHolder = MakeSharedRangeHolder(std::move(transaction));
            req->Attachments().reserve(payloadCount);
            for (const auto& payload : HunkPayloads_) {
                req->Attachments().push_back(TSharedRef(payload, std::move(payloadHolder)));
            }

            return req->Invoke().Apply(BIND([this, payloadCount]
                (const TTabletServiceProxy::TErrorOrRspWriteHunksPtr& rspOrError) mutable {
                    if (!rspOrError.IsOK()) {
                        THROW_ERROR_EXCEPTION("Failed to write hunks")
                            << rspOrError;
                    }

                    const auto& rsp = rspOrError.Value();
                    YT_VERIFY(payloadCount == rsp->descriptors_size());
                    HunkDescriptors_.reserve(payloadCount);
                    for (const auto& protoDescriptor : rsp->descriptors()) {
                        auto hunkChunkId = FromProto<TChunkId>(protoDescriptor.chunk_id());
                        HunkDescriptors_.push_back(THunkDescriptor{
                            .ChunkId = hunkChunkId,
                            .ErasureCodec = FromProto<NErasure::ECodec>(protoDescriptor.erasure_codec()),
                            .BlockIndex = protoDescriptor.record_index(),
                            .BlockOffset = protoDescriptor.record_offset(),
                            .Length = protoDescriptor.length(),
                            .BlockSize = protoDescriptor.record_size(),
                        });
                    }
            }));
        }

        void SubmitRows()
        {
            try {
                GuardedSubmitRows();
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error submitting rows for table %v",
                    TableSession_->GetInfo()->Path)
                    << TError(ex);
            }
        }

        void GuardedSubmitRows()
        {
            auto transaction = Transaction_.Lock();
            if (!transaction) {
                return;
            }

            const auto& tableInfo = TableSession_->GetInfo();
            if (Options_.TopmostTransaction && tableInfo->ReplicationCardId) {
                // For chaos tables we write to all replicas via nested invocations in GuardedPrepareRows.
                return;
            }

            ProcessSecondaryIndices(tableInfo, transaction);

            std::optional<int> tabletIndexColumnId;
            if (!tableInfo->IsSorted()) {
                tabletIndexColumnId = NameTable_->GetIdOrRegisterName(TabletIndexColumnName);
            }

            const auto& primarySchema = tableInfo->Schemas[ETableSchemaKind::Primary];
            const auto& primaryIdMapping = GetColumnIdMapping(transaction, tableInfo, ETableSchemaKind::Primary);

            const auto& primarySchemaWithTabletIndex = tableInfo->Schemas[ETableSchemaKind::PrimaryWithTabletIndex];

            const auto& modificationSchema = !tableInfo->IsPhysicallyLog() && !tableInfo->IsSorted() ? primarySchema : primarySchemaWithTabletIndex;

            const auto& rowBuffer = transaction->RowBuffer_;

            auto evaluatorCache = Connection_->GetColumnEvaluatorCache();
            auto evaluator = tableInfo->NeedKeyEvaluation ? evaluatorCache->Find(primarySchema) : nullptr;

            auto randomTabletInfo = tableInfo->GetRandomMountedTablet();

            std::vector<bool> columnPresenceBuffer(modificationSchema->GetColumnCount());

            std::vector<int> columnIndexToLockIndex;
            GetLocksMapping(
                *primarySchema,
                transaction->GetAtomicity() == NTransactionClient::EAtomicity::Full,
                &columnIndexToLockIndex);

            int currentHunkDescriptorIndex = 0;
            for (int modificationIndex = 0; modificationIndex < std::ssize(Modifications_); ++modificationIndex) {
                const auto& modification = Modifications_[modificationIndex];
                auto& modificationData = ModificationsData_[modificationIndex];

                switch (modification.Type) {
                    case ERowModificationType::Write:
                    case ERowModificationType::Delete:
                    case ERowModificationType::WriteAndLock: {
                        auto capturedRow = std::move(modificationData.CapturedRow);

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
                        auto locks = modification.Locks;
                        if (tableInfo->IsPhysicallyLog() && modificationType == ERowModificationType::WriteAndLock) {
                            if (tableInfo->IsChaosReplica() &&
                                capturedRow.GetCount() == static_cast<ui32>(modificationSchema->GetKeyColumnCount()))
                            {
                                break;
                            }

                            modificationType = ERowModificationType::Write;
                            locks = TLockMask();
                        }

                        auto session = transaction->GetOrCreateTabletSession(tabletInfo, tableInfo, TableSession_);
                        if (modificationData.HunkCount > 0) {
                            YT_VERIFY(RandomHunkTabletInfo_);

                            THunkChunksInfo hunkInfo{
                                .CellId = RandomHunkTabletInfo_->CellId,
                                .HunkTabletId = RandomHunkTabletInfo_->TabletId,
                                .MountRevision = RandomHunkTabletInfo_->MountRevision
                            };

                            int lastHunkDescriptorIndex = currentHunkDescriptorIndex + modificationData.HunkCount;
                            YT_VERIFY(lastHunkDescriptorIndex <= std::ssize(HunkDescriptors_));

                            std::vector<THunkDescriptor> descriptors(
                                HunkDescriptors_.begin() + currentHunkDescriptorIndex,
                                HunkDescriptors_.begin() + lastHunkDescriptorIndex);

                            for (const auto& descriptor : descriptors) {
                                auto& ref = hunkInfo.HunkChunkRefs[descriptor.ChunkId];
                                ref.ChunkId = descriptor.ChunkId;
                                ++ref.HunkCount;
                                ref.TotalHunkLength += descriptor.Length - sizeof(THunkPayloadHeader);
                                ref.ErasureCodec = descriptor.ErasureCodec;
                            }

                            ReplaceHunks(
                                capturedRow,
                                modificationSchema,
                                std::move(descriptors),
                                HunkMemoryPool_);

                            session->MemorizeHunkInfo(hunkInfo);
                            currentHunkDescriptorIndex += modificationData.HunkCount;
                        }

                        auto command = GetCommand(modificationType);

                        if (modificationType == ERowModificationType::Write &&
                            tableInfo->IsSorted() &&
                            !tableInfo->IsPhysicallyLog())
                        {
                            YT_VERIFY(locks.IsNone());
                            for (const auto& value : TUnversionedRow(modification.Row)) {
                                int mappedId = ApplyIdMapping(value, &primaryIdMapping);

                                auto lockIndex = columnIndexToLockIndex[mappedId];
                                if (lockIndex == -1) {
                                    continue;
                                }

                                locks.Set(lockIndex, ELockType::Exclusive);
                            }

                            command = GetCommand(ERowModificationType::WriteAndLock);
                        }

                        session->SubmitUnversionedRow(command, capturedRow, std::move(locks));

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
                                &columnPresenceBuffer,
                                Options_.AllowMissingKeyColumns);
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
                        session->SubmitVersionedRow(row);
                        break;
                    }

                    default:
                        YT_ABORT();
                }
            }
            YT_VERIFY(currentHunkDescriptorIndex == std::ssize(HunkDescriptors_));
        }

    protected:
        const TWeakPtr<TTransaction> Transaction_;
        const IConnectionPtr Connection_;
        const TYPath Path_;
        const TNameTablePtr NameTable_;
        TChunkedMemoryPool* HunkMemoryPool_;
        const TSharedRange<TRowModification> Modifications_;
        const TModifyRowsOptions Options_;

        const NLogging::TLogger& Logger;
        const TTableCommitSessionPtr TableSession_;

        struct TModificationData
        {
            TMutableUnversionedRow CapturedRow;
            int HunkCount;
        };
        std::vector<TModificationData> ModificationsData_;

        TTabletInfoPtr RandomHunkTabletInfo_;
        std::vector<TRef> HunkPayloads_;
        std::vector<THunkDescriptor> HunkDescriptors_;

        static EWireProtocolCommand GetCommand(ERowModificationType modificationType)
        {
            switch (modificationType) {
                case ERowModificationType::Write:
                    return EWireProtocolCommand::WriteRow;

                case ERowModificationType::VersionedWrite:
                    return EWireProtocolCommand::VersionedWriteRow;

                case ERowModificationType::Delete:
                    return EWireProtocolCommand::DeleteRow;

                case ERowModificationType::WriteAndLock:
                    return EWireProtocolCommand::WriteAndLockRow;

                default:
                    YT_ABORT();
            }
        }

        const TNameTableToSchemaIdMapping& GetColumnIdMapping(
            const NYT::NApi::NNative::TTransactionPtr& transaction,
            const TTableMountInfoPtr& tableInfo,
            ETableSchemaKind kind)
        {
            return transaction->GetColumnIdMapping(tableInfo, NameTable_, kind, Options_.AllowMissingKeyColumns);
        }

        void ProcessSecondaryIndices(TTableMountInfoPtr tableInfo, TTransactionPtr transaction)
        {
            if (tableInfo->Indices.empty()) {
                return;
            }

            int indexTableCount = tableInfo->Indices.size();
            const auto& tableSchema = *tableInfo->Schemas[ETableSchemaKind::Write];

            std::vector<TFuture<TTableMountInfoPtr>> indexTableInfoFutures;
            indexTableInfoFutures.reserve(indexTableCount);
            for (const auto& indexInfo : tableInfo->Indices) {
                indexTableInfoFutures.push_back(Connection_
                    ->GetTableMountCache()
                    ->GetTableInfo(FromObjectId(indexInfo.TableId)));
            }

            auto indexTableInfos = WaitForUnique(AllSucceeded(indexTableInfoFutures))
                .ValueOrThrow();

            for (const auto& column : tableSchema.Columns()) {
                NameTable_->GetIdOrRegisterName(column.Name());
            }

            std::vector<TNameTableToSchemaIdMapping> writeIdMappings(indexTableCount);
            std::vector<TNameTableToSchemaIdMapping> deleteIdMappings(indexTableCount);
            std::vector<const TColumnSchema*> unfoldedColumns(indexTableCount);

            for (int index = 0; index < indexTableCount; ++index) {
                if (tableInfo->Indices[index].Kind == ESecondaryIndexKind::Unfolding) {
                    const TColumnSchema* column = nullptr;
                    ValidateIndexSchema(
                        tableSchema,
                        *indexTableInfos[index]->Schemas[ETableSchemaKind::Primary],
                        &column);
                    unfoldedColumns[index] = column;
                } else {
                    ValidateIndexSchema(
                        tableSchema,
                        *indexTableInfos[index]->Schemas[ETableSchemaKind::Primary]);
                }
                writeIdMappings[index] = BuildColumnIdMapping(
                    *indexTableInfos[index]->Schemas[ETableSchemaKind::Write],
                    NameTable_,
                    /*allowMissingKeyColumns*/ true);
                deleteIdMappings[index] = BuildColumnIdMapping(
                    *indexTableInfos[index]->Schemas[ETableSchemaKind::Delete],
                    NameTable_,
                    /*allowMissingKeyColumns*/ true);
            }

            struct TSecondaryIndicesLookupBufferTag { };
            auto rowBuffer = New<TRowBuffer>(TSecondaryIndicesLookupBufferTag());

            const auto& lookupSchema = tableInfo->Schemas[ETableSchemaKind::Lookup];
            const auto& lookupIdMapping = transaction->GetColumnIdMapping(
                tableInfo,
                NameTable_,
                ETableSchemaKind::Lookup,
                /*allowMissingKeyColumns*/ false);

            std::vector<TUnversionedRow> lookupKeys;
            lookupKeys.reserve(Modifications_.Size());
            for (const auto& modification : Modifications_) {
                auto capturedRow = rowBuffer->CaptureAndPermuteRow(
                    TUnversionedRow(modification.Row),
                    *lookupSchema,
                    lookupSchema->GetKeyColumnCount(),
                    lookupIdMapping,
                    /*columnPresenceBuffer*/ nullptr);
                lookupKeys.push_back(capturedRow);
            }

            auto keyRange = MakeSharedRange(std::move(lookupKeys), std::move(rowBuffer));

            TLookupRowsOptions lookupRowsOptions;
            lookupRowsOptions.KeepMissingRows = true;

            auto lookupRows = WaitForUnique(transaction->LookupRows(
                Path_,
                NameTable_,
                keyRange,
                lookupRowsOptions))
                .ValueOrThrow();

            for (int index = 0; index < indexTableCount; ++index) {
                auto secondaryIndexPath = FromObjectId(tableInfo->Indices[index].TableId);
                const auto& indexSchema = *indexTableInfos[index]->Schemas[ETableSchemaKind::Primary];
                auto secondaryNameTable = TNameTable::FromSchema(indexSchema);

                std::optional<TUnversionedValue> empty;
                if (auto id = secondaryNameTable->FindId(EmptyValueColumnName)) {
                    empty = MakeUnversionedNullValue(*id);
                }

                TSharedRange<TRowModification> indexModifications;
                switch (auto kind = tableInfo->Indices[index].Kind) {
                    case ESecondaryIndexKind::FullSync: {
                        indexModifications = GetFullSyncIndexModifications(
                            Modifications_,
                            lookupRows.Rowset->GetRows(),
                            writeIdMappings[index],
                            deleteIdMappings[index],
                            indexSchema,
                            empty);
                        break;
                    }
                    case ESecondaryIndexKind::Unfolding: {
                        int unfoldedKeyPosition = indexSchema.GetColumnIndex(*unfoldedColumns[index]);
                        indexModifications = GetUnfoldedIndexModifications(
                            Modifications_,
                            lookupRows.Rowset->GetRows(),
                            writeIdMappings[index],
                            deleteIdMappings[index],
                            indexSchema,
                            empty,
                            unfoldedKeyPosition);
                        break;
                    }
                    default:
                        THROW_ERROR_EXCEPTION("Unsupported secondary index kind %Qlv", kind);
                }

                transaction->EnqueueModificationRequest(std::make_unique<TModificationRequest>(
                    transaction.Get(),
                    Connection_,
                    secondaryIndexPath,
                    std::move(secondaryNameTable),
                    HunkMemoryPool_,
                    std::move(indexModifications),
                    Options_));
            }
        }
    };

    std::vector<std::unique_ptr<TModificationRequest>> Requests_;
    std::vector<TModificationRequest*> PendingRequests_;
    TMultiSlidingWindow<TModificationRequest*> OrderedRequestsSlidingWindow_;

    struct TSyncReplica
    {
        TTableReplicaInfoPtr ReplicaInfo;
        NApi::ITransactionPtr Transaction;
        TReplicationCardPtr ReplicationCard;
    };

    class TTableCommitSession
        : public TRefCounted
    {
    public:
        TTableCommitSession(
            TTransaction* transaction,
            const TYPath& path,
            TTableReplicaId upstreamReplicaId,
            TReplicationCardPtr replicationCard)
            : Transaction_(transaction)
            , UpstreamReplicaId_(upstreamReplicaId)
            , ReplicationCard_(std::move(replicationCard))
            , Logger(transaction->Logger.WithTag("Path: %v", path))
        {
            const auto& tableMountCache = transaction->Client_->GetTableMountCache();
            auto tableInfoFuture = tableMountCache->GetTableInfo(path);
            auto maybeTableInfoOrError = tableInfoFuture.TryGet();
            PrepareFuture_ = maybeTableInfoOrError && maybeTableInfoOrError->IsOK()
                ? OnGotTableInfo(maybeTableInfoOrError->Value())
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

        const TTableMountInfoPtr& GetHunkTableInfo() const
        {
            YT_VERIFY(HunkTableInfo_);
            return HunkTableInfo_;
        }

        TTableReplicaId GetUpstreamReplicaId() const
        {
            return UpstreamReplicaId_;
        }

        const std::vector<TSyncReplica>& GetSyncReplicas() const
        {
            YT_VERIFY(PrepareFuture_.IsSet());
            return SyncReplicas_;
        }

        const TReplicationCardPtr& GetReplicationCard() const
        {
            return ReplicationCard_;
        }

    private:
        const TWeakPtr<TTransaction> Transaction_;

        TFuture<void> PrepareFuture_;
        TTableMountInfoPtr TableInfo_;
        TTableMountInfoPtr HunkTableInfo_;
        TTableReplicaId UpstreamReplicaId_;
        TReplicationCardPtr ReplicationCard_;
        std::vector<TSyncReplica> SyncReplicas_;

        const NLogging::TLogger Logger;

        TFuture<void> GetReplicationCardFuture()
        {
            VERIFY_THREAD_AFFINITY_ANY();

            if (TableInfo_->ReplicationCardId) {
                UpstreamReplicaId_ = TableInfo_->UpstreamReplicaId;
            }
            if (!TableInfo_->ReplicationCardId) {
                return OnGotReplicationCard(true);
            } else if (ReplicationCard_) {
                return OnGotReplicationCard(false);
            } else {
                auto transaction = Transaction_.Lock();
                if (!transaction) {
                    return VoidFuture;
                }

                const auto& replicationCardCache = transaction->Client_->GetReplicationCardCache();
                return replicationCardCache->GetReplicationCard({
                        .CardId = TableInfo_->ReplicationCardId,
                        .FetchOptions = {
                            .IncludeCoordinators = true,
                            .IncludeHistory = true
                        }
                    }).Apply(BIND([=, this, this_ = MakeStrong(this)] (const TReplicationCardPtr& replicationCard) {
                        YT_LOG_DEBUG("Got replication card from cache (Path: %v, ReplicationCardId: %v, CoordinatorCellIds: %v)",
                            TableInfo_->Path,
                            TableInfo_->ReplicationCardId,
                            replicationCard->CoordinatorCellIds);

                        ReplicationCard_ = replicationCard;
                        return OnGotReplicationCard(true);
                    }).AsyncVia(transaction->SerializedInvoker_));
            }
        }

        TFuture<void> GetHunkStorageInfoFuture()
        {
            VERIFY_THREAD_AFFINITY_ANY();

            if (!TableInfo_->HunkStorageId) {
                return VoidFuture;
            }

            auto transaction = Transaction_.Lock();
            if (!transaction) {
                return VoidFuture;
            }

            auto hunkStorageId = TableInfo_->HunkStorageId;
            const auto& tableMountCache = transaction->Client_->GetTableMountCache();

            auto tableInfoFuture = tableMountCache->GetTableInfo(FromObjectId(hunkStorageId));
            auto maybeTableInfoOrError = tableInfoFuture.TryGet();
            if (maybeTableInfoOrError && maybeTableInfoOrError->IsOK()) {
                OnGotHunkTableMountInfo(maybeTableInfoOrError->Value());
                return VoidFuture;
            } else {
                return tableInfoFuture
                    .Apply(BIND(
                        &TTableCommitSession::OnGotHunkTableMountInfo,
                        MakeStrong(this))
                    .AsyncVia(transaction->SerializedInvoker_));
            }
        }

        void OnGotHunkTableMountInfo(const TTableMountInfoPtr& hunkTableInfo)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            HunkTableInfo_ = hunkTableInfo;

            YT_LOG_DEBUG("Got hunk table info (Path: %v, HunkStorageId: %v)",
                TableInfo_->Path,
                TableInfo_->HunkStorageId);
        }

        TFuture<void> OnGotTableInfo(const TTableMountInfoPtr& tableInfo)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            TableInfo_ = tableInfo;

            return AllSucceeded(std::vector{GetReplicationCardFuture(), GetHunkStorageInfoFuture()});
        }

        TFuture<void> OnGotReplicationCard(bool exploreReplicas)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            std::vector<TFuture<void>> futures;
            CheckPermissions(&futures);
            if (exploreReplicas) {
                RegisterSyncReplicas(&futures);
            }

            return AllSucceeded(std::move(futures));
        }

        void CheckPermissions(std::vector<TFuture<void>>* futures)
        {
            VERIFY_THREAD_AFFINITY_ANY();

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
            VERIFY_THREAD_AFFINITY_ANY();

            auto transaction = Transaction_.Lock();
            if (!transaction) {
                return;
            }

            YT_VERIFY(
                !HasSimpleReplicas() ||
                !HasChaosReplicas());

            if (HasSimpleReplicas()) {
                const auto& syncReplicaCache = transaction->Client_->GetNativeConnection()->GetSyncReplicaCache();
                auto future = syncReplicaCache->Get(TableInfo_->Path);
                if (auto result = future.TryGet()) {
                    DoRegisterSyncReplicas(
                        futures,
                        transaction,
                        result->ValueOrThrow());
                } else {
                    futures->push_back(future
                        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TTableReplicaInfoPtrList& syncReplicas) {
                            std::vector<TFuture<void>> futures;
                            DoRegisterSyncReplicas(
                                &futures,
                                transaction,
                                syncReplicas);
                            return AllSucceeded(std::move(futures));
                        })
                        .AsyncVia(transaction->SerializedInvoker_)));
                }
            } else if (HasChaosReplicas()) {
                DoRegisterSyncReplicas(futures, transaction, /*syncReplicas*/ {});
            }
        }

        void DoRegisterSyncReplicas(
            std::vector<TFuture<void>>* futures,
            const TTransactionPtr& transaction,
            const TTableReplicaInfoPtrList& syncReplicas)
        {
            VERIFY_THREAD_AFFINITY_ANY();

            auto registerReplica = [&] (const auto& replicaInfo) {
                if (replicaInfo->Mode != ETableReplicaMode::Sync) {
                    return;
                }

                YT_LOG_DEBUG("Sync table replica registered (ReplicaId: %v, ClusterName: %v, ReplicaPath: %v)",
                    replicaInfo->ReplicaId,
                    replicaInfo->ClusterName,
                    replicaInfo->ReplicaPath);

                futures->push_back(
                    transaction->GetSyncReplicaTransaction(replicaInfo)
                        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const NApi::ITransactionPtr& transaction) {
                            SyncReplicas_.push_back(TSyncReplica{
                                replicaInfo,
                                transaction,
                                ReplicationCard_
                            });
                        }).AsyncVia(transaction->SerializedInvoker_)));
            };

            if (HasSimpleReplicas()) {
                THashSet<TTableReplicaId> syncReplicaIds;
                syncReplicaIds.reserve(syncReplicas.size());
                for (const auto& syncReplicaInfo : syncReplicas) {
                    syncReplicaIds.insert(syncReplicaInfo->ReplicaId);
                }

                for (const auto& replicaInfo : TableInfo_->Replicas) {
                    if (replicaInfo->Mode != ETableReplicaMode::Sync) {
                        continue;
                    }
                    if (!syncReplicaIds.contains(replicaInfo->ReplicaId)) {
                        futures->push_back(MakeFuture(TError(
                            NTabletClient::EErrorCode::SyncReplicaNotInSync,
                            "Cannot write to sync replica %v since it is not in-sync yet",
                            replicaInfo->ReplicaId)));
                        return;
                    }
                }

                for (const auto& replicaInfo : TableInfo_->Replicas) {
                    registerReplica(replicaInfo);
                }
            } else if (HasChaosReplicas()) {
                for (const auto& [chaosReplicaId, chaosReplicaInfo] : ReplicationCard_->Replicas) {
                    if (IsReplicaReallySync(chaosReplicaInfo.Mode, chaosReplicaInfo.State,
                        chaosReplicaInfo.History.back()))
                    {
                        auto replicaInfo = New<TTableReplicaInfo>();
                        replicaInfo->ReplicaId = chaosReplicaId;
                        replicaInfo->ClusterName = chaosReplicaInfo.ClusterName;
                        replicaInfo->ReplicaPath = chaosReplicaInfo.ReplicaPath;
                        registerReplica(replicaInfo);
                    }
                }
            }
        }

        bool HasSimpleReplicas() const
        {
            return !TableInfo_->Replicas.empty();
        }

        bool HasChaosReplicas() const
        {
            return static_cast<bool>(ReplicationCard_);
        }
    };

    //! Maintains per-table commit info.
    THashMap<TYPath, TTableCommitSessionPtr> TablePathToSession_;
    std::vector<TTableCommitSessionPtr> PendingSessions_;

    //! Maintains per-tablet commit info.
    THashMap<TTabletId, ITabletCommitSessionPtr> TabletIdToSession_;

    //! Maintains per-cell commit info.
    ICellCommitSessionProviderPtr CellCommitSessionProvider_;

    //! Maps replica cluster name to sync replica transaction.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ClusterNameToSyncReplicaTransactionPromiseSpinLock_);
    THashMap<TString, TPromise<NApi::ITransactionPtr>> ClusterNameToSyncReplicaTransactionPromise_;

    //! Caches mappings from name table ids to schema ids.
    THashMap<std::tuple<TTableId, TNameTablePtr, ETableSchemaKind, bool>, TNameTableToSchemaIdMapping> IdMappingCache_;

    //! The actual options to be used during commit.
    TTransactionCommitOptions CommitOptions_;


    const TNameTableToSchemaIdMapping& GetColumnIdMapping(
        const TTableMountInfoPtr& tableInfo,
        const TNameTablePtr& nameTable,
        ETableSchemaKind kind,
        bool allowMissingKeyColumns)
    {
        auto key = std::tuple(tableInfo->TableId, nameTable, kind, allowMissingKeyColumns);
        auto it = IdMappingCache_.find(key);
        if (it == IdMappingCache_.end()) {
            auto mapping = BuildColumnIdMapping(*tableInfo->Schemas[kind], nameTable, allowMissingKeyColumns);
            it = IdMappingCache_.emplace(key, std::move(mapping)).first;
        }
        return it->second;
    }

    TFuture<NApi::ITransactionPtr> GetSyncReplicaTransaction(const TTableReplicaInfoPtr& replicaInfo)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TPromise<NApi::ITransactionPtr> promise;
        {
            auto guard = Guard(ClusterNameToSyncReplicaTransactionPromiseSpinLock_);

            auto it = ClusterNameToSyncReplicaTransactionPromise_.find(replicaInfo->ClusterName);
            if (it != ClusterNameToSyncReplicaTransactionPromise_.end()) {
                return it->second.ToFuture();
            } else {
                promise = NewPromise<NApi::ITransactionPtr>();
                EmplaceOrCrash(ClusterNameToSyncReplicaTransactionPromise_, replicaInfo->ClusterName, promise);
            }
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
            .Apply(BIND([=, this, this_ = MakeStrong(this)] {
                const auto& clusterDirectory = Client_->GetNativeConnection()->GetClusterDirectory();
                return clusterDirectory->GetConnectionOrThrow(replicaInfo->ClusterName);
            }) /* serialization intentionally omitted */)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const NApi::NNative::IConnectionPtr& connection) {
                if (connection->GetClusterTag() == Client_->GetConnection()->GetClusterTag()) {
                    return MakeFuture<NApi::ITransactionPtr>(nullptr);
                }

                TTransactionStartOptions options;
                options.Id = Transaction_->GetId();
                options.StartTimestamp = Transaction_->GetStartTimestamp();

                auto client = connection->CreateClient(Client_->GetOptions());
                return client->StartTransaction(ETransactionType::Tablet, options);
            }) /* serialization intentionally omitted */)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const NApi::ITransactionPtr& transaction) {
                promise.Set(transaction);

                if (transaction) {
                    YT_LOG_DEBUG("Sync replica transaction started (ClusterName: %v)",
                        replicaInfo->ClusterName);
                    DoRegisterSyncReplicaAlienTransaction(transaction);
                }

                return transaction;
                // NB: Serialized invoker below is needed since DoRegisterSyncReplicaAlienTransaction acquires
                // a spinlock and in the worst will deadlock with ModifyRows.
            }).AsyncVia(SerializedInvoker_));
    }


    void DoEnqueueModificationRequest(TModificationRequest* request)
    {
        PendingRequests_.push_back(request);
    }

    void EnqueueModificationRequest(std::unique_ptr<TModificationRequest> request)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (auto sequenceNumber = request->GetSequenceNumber()) {
            if (sequenceNumber->Value < 0) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::ProtocolError,
                    "Packet sequence number is negative")
                    << TErrorAttribute("sequence_number", sequenceNumber->Value)
                    << TErrorAttribute("sequence_number_source_id", sequenceNumber->SourceId);
            }
            // This may call DoEnqueueModificationRequest right away.
            OrderedRequestsSlidingWindow_.AddPacket(
                *sequenceNumber,
                request.get(),
                [&] (TModificationRequest* request) {
                    DoEnqueueModificationRequest(request);
                });
        } else {
            DoEnqueueModificationRequest(request.get());
        }
        Requests_.push_back(std::move(request));
    }


    TTableCommitSessionPtr GetOrCreateTableSession(
        const TYPath& path,
        TTableReplicaId upstreamReplicaId,
        const TReplicationCardPtr& replicationCard,
        bool topmostTransaction)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto it = TablePathToSession_.find(path);
        if (it == TablePathToSession_.end()) {
            auto session = New<TTableCommitSession>(this, path, upstreamReplicaId, replicationCard);
            PendingSessions_.push_back(session);
            it = TablePathToSession_.emplace(path, session).first;
        } else {
            const auto& session = it->second;

            // For topmost transaction user-provided upstream replica id could be zero. However if we write to sync chaos replica
            // session->UpstreamReplicaId will be resolved as soon as replication card is fetched. Since there could be several
            // modification requests to the same table we don't validate upstream replica id matching in this case.

            // For a nested transaction (typically sync replicas write) this upstream replica mismatch is more likely a bug.

            if ((!topmostTransaction || upstreamReplicaId) && session->GetUpstreamReplicaId() != upstreamReplicaId) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::UpstreamReplicaMismatch,
                    "Mismatched upstream replica is specified for modifications to table %v: %v != %v",
                    path,
                    upstreamReplicaId,
                    session->GetUpstreamReplicaId());
            }
        }

        return it->second;
    }

    ITabletCommitSessionPtr GetOrCreateTabletSession(
        const TTabletInfoPtr& tabletInfo,
        const TTableMountInfoPtr& tableInfo,
        const TTableCommitSessionPtr& tableSession)
    {
        auto tabletId = tabletInfo->TabletId;
        auto it = TabletIdToSession_.find(tabletId);
        if (it == TabletIdToSession_.end()) {
            TTabletCommitOptions options{
                .UpstreamReplicaId = tableSession->GetUpstreamReplicaId(),
                .ReplicationCard = tableSession->GetReplicationCard(),
                .PrerequisiteTransactionIds = CommitOptions_.PrerequisiteTransactionIds,
            };
            it = TabletIdToSession_.emplace(
                tabletId,
                CreateTabletCommitSession(
                    Client_,
                    options,
                    MakeWeak(Transaction_),
                    CellCommitSessionProvider_,
                    tabletInfo,
                    tableInfo,
                    Logger)).first;
        }
        return it->second;
    }

    TFuture<void> DoAbort(TGuard<NThreading::TSpinLock>* guard, const TTransactionAbortOptions& options = {})
    {
        VERIFY_THREAD_AFFINITY_ANY();
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (State_ == ETransactionState::Aborted) {
            return AbortPromise_.ToFuture();
        }

        State_ = ETransactionState::Aborted;
        AbortPromise_ = NewPromise<void>();
        auto abortFuture = AbortPromise_.ToFuture();

        guard->Release();

        for (const auto& transaction : GetAlienTransactions()) {
            YT_UNUSED_FUTURE(transaction->Abort());
        }

        AbortPromise_.SetFrom(Transaction_->Abort(options));
        return abortFuture;
    }

    TFuture<void> PrepareRequests()
    {
        if (auto optionalMissingNumber = OrderedRequestsSlidingWindow_.TryGetMissingSequenceNumber()) {
            auto missingNumber = *optionalMissingNumber;
            return MakeFuture(TError(
                NRpc::EErrorCode::ProtocolError,
                "Cannot prepare transaction %v since sequence number %v from source %v is missing",
                GetId(),
                missingNumber.Value,
                missingNumber.SourceId));
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
                .Apply(BIND([=, this, pendingRequests = std::move(pendingRequests)] {
                    std::vector<TFuture<void>> writeHunksFutures;

                    for (auto* request : pendingRequests) {
                        request->PrepareRows();
                    }

                    writeHunksFutures.reserve(pendingRequests.size());
                    for (auto* request : pendingRequests) {
                        writeHunksFutures.push_back(request->WriteHunks());
                    }

                    return AllSucceeded(std::move(writeHunksFutures))
                        .Apply(BIND([=, this, pendingRequests = std::move(pendingRequests)] {
                            for (auto* request : pendingRequests) {
                                request->SubmitRows();
                            }

                            return DoPrepareRequests();
                    }).AsyncVia(SerializedInvoker_));
                }).AsyncVia(SerializedInvoker_));
        } else {
            for (const auto& [tabletId, tabletSession] : TabletIdToSession_) {
                tabletSession->PrepareRequests();
            }

            return VoidFuture;
        }
    }

    TFuture<void> SendRequests()
    {
        std::vector<TFuture<void>> futures = {
            DoCommitTabletSessions(),
            CellCommitSessionProvider_->InvokeAll(),
        };
        return AllSucceeded(std::move(futures));
    }

    TFuture<void> DoCommitTabletSessions()
    {
        std::vector<ITabletCommitSessionPtr> sessions;
        sessions.reserve(TabletIdToSession_.size());
        for (const auto& [tabletId, session] : TabletIdToSession_) {
            sessions.push_back(session);
        }

        auto dynamicConfig = Client_->GetNativeConnection()->GetConfig();
        return CommitTabletSessions(
            std::move(sessions),
            dynamicConfig->TabletWriteBackoff,
            Logger,
            Counters_);
    }

    void BuildAdjustedCommitOptions(const TTransactionCommitOptions& options)
    {
        CommitOptions_ = options;

        THashSet<NObjectClient::TCellId> selectedCellIds;
        THashSet<NChaosClient::TReplicationCardId> requestedReplicationCardIds;

        for (const auto& [path, session] : TablePathToSession_) {
            if (session->GetInfo()->IsReplicated()) {
                CommitOptions_.Force2PC = true;
            }

            const auto& replicationCard = session->GetReplicationCard();
            const auto& replicationCardId = session->GetInfo()->ReplicationCardId;
            if (replicationCardId &&
                replicationCard->Era > InitialReplicationEra &&
                !options.CoordinatorCellId)
            {
                if (!requestedReplicationCardIds.insert(replicationCardId).second) {
                    YT_LOG_DEBUG("Coordinator for replication card already selected, skipping "
                        "(Path: %v, ReplicationCardId: %v, Era: %v)",
                        path,
                        replicationCardId,
                        replicationCard->Era);

                    continue;
                }

                const auto& coordinatorCellIds = replicationCard->CoordinatorCellIds;

                YT_LOG_DEBUG("Considering replication card (Path: %v, ReplicationCardId: %v, Era: %v, CoordinatorCellIds: %v)",
                    path,
                    replicationCardId,
                    replicationCard->Era,
                    coordinatorCellIds);

                if (coordinatorCellIds.empty()) {
                    THROW_ERROR_EXCEPTION("Coordinators are not available")
                        << TErrorAttribute("replication_card_id", replicationCardId);
                }

                if (options.CoordinatorCommitMode == ETransactionCoordinatorCommitMode::Lazy) {
                    THROW_ERROR_EXCEPTION("Coordinator commit mode %Qv is incompatible with chaos tables",
                        options.CoordinatorCommitMode);
                }

                NObjectClient::TCellId coordinatorCellId;
                // Actual problem is NP-hard, so use a simple heuristic (however greedy approach could do better here):
                // if we've already seen given cell id, use it. Otherwise select a random one.
                for (const auto& coordinatorCellIdCanditate : coordinatorCellIds) {
                    if (selectedCellIds.contains(coordinatorCellIdCanditate)) {
                        coordinatorCellId = coordinatorCellIdCanditate;
                        break;
                    }
                }

                if (!coordinatorCellId) {
                    coordinatorCellId = coordinatorCellIds[RandomNumber(coordinatorCellIds.size())];
                    selectedCellIds.insert(coordinatorCellId);
                    Transaction_->RegisterParticipant(coordinatorCellId);
                }

                NChaosClient::NProto::TReqReplicatedCommit request;
                ToProto(request.mutable_replication_card_id(), replicationCardId);
                request.set_replication_era(session->GetReplicationCard()->Era);

                DoAddAction(coordinatorCellId, MakeTransactionActionData(request));

                CommitOptions_.Force2PC = true;
                if (!CommitOptions_.CoordinatorCellId) {
                    CommitOptions_.CoordinatorCellId = coordinatorCellId;
                    YT_LOG_DEBUG("2PC Coordinator selected (CoordinatorCellId: %v)", coordinatorCellId);
                }

                YT_LOG_DEBUG("Coordinator selected (Path: %v, ReplicationCardId: %v, Era: %v, CoordinatorCellId: %v)",
                    path,
                    replicationCardId,
                    replicationCard->Era,
                    coordinatorCellId);
            }
        }
    }

    void DoAddAction(TCellId cellId, const TTransactionActionData& data)
    {
        YT_VERIFY(
            TypeFromId(cellId) == EObjectType::TabletCell ||
            TypeFromId(cellId) == EObjectType::ChaosCell ||
            TypeFromId(cellId) == EObjectType::MasterCell);

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

    TFuture<TTransactionCommitResult> DoCommit(const TTransactionCommitOptions& options, bool needsFlush)
    {
        for (auto cellId : options.AdditionalParticipantCellIds) {
            Transaction_->RegisterParticipant(cellId);
        }

        return
            [&] {
                // NB: During requests preparation some of the commit options
                // like prerequisite transaction ids are required, so we firstly
                // store raw commit options and then adjust them.
                CommitOptions_ = options;

                return needsFlush ? PrepareRequests() : VoidFuture;
            }()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this)] {
                    BuildAdjustedCommitOptions(options);

                    Transaction_->ChooseCoordinator(CommitOptions_);

                    return Transaction_->ValidateNoDownedParticipants();
                }).AsyncVia(SerializedInvoker_))
            .Apply(
                BIND([=, this, this_ = MakeStrong(this)] {
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
                BIND([=, this, this_ = MakeStrong(this)] (const std::vector<TTransactionFlushResult>& results) {
                    for (const auto& result : results) {
                        for (auto cellId : result.ParticipantCellIds) {
                            Transaction_->RegisterParticipant(cellId);
                        }
                    }

                    return Transaction_->Commit(CommitOptions_);
                }).AsyncVia(SerializedInvoker_))
            .Apply(
                BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TTransactionCommitResult>& resultOrError) {
                    {
                        auto guard = Guard(SpinLock_);
                        if (resultOrError.IsOK() && State_ == ETransactionState::Committing) {
                            State_ = ETransactionState::Committed;
                        } else if (!resultOrError.IsOK()) {
                            YT_UNUSED_FUTURE(DoAbort(&guard));
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
                }) /*serialization intentionally omitted*/);
    }

    TFuture<TTransactionFlushResult> DoFlush()
    {
        return PrepareRequests()
            .Apply(
                BIND([this, this_ = MakeStrong(this)] {
                    return SendRequests();
                }).AsyncVia(SerializedInvoker_))
            .Apply(
                BIND([this, this_ = MakeStrong(this)] (const TError& error) {
                    {
                        auto guard = Guard(SpinLock_);
                        if (error.IsOK() && State_ == ETransactionState::Flushing) {
                            State_ = ETransactionState::Flushed;
                        } else if (!error.IsOK()) {
                            YT_LOG_DEBUG(error, "Error flushing transaction");
                            YT_UNUSED_FUTURE(DoAbort(&guard));
                            THROW_ERROR_EXCEPTION("Error flushing transaction %v",
                                GetId())
                                << MakeClusterIdErrorAttribute()
                                << error;
                        }
                    }

                    TTransactionFlushResult result{
                        .ParticipantCellIds = CellCommitSessionProvider_->GetParticipantCellIds(),
                    };

                    YT_LOG_DEBUG("Transaction flushed (ParticipantCellIds: %v)",
                        result.ParticipantCellIds);

                    return result;
                }).AsyncVia(SerializedInvoker_));
    }


    ICellCommitSessionPtr GetOrCreateCellCommitSession(TCellId cellId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return CellCommitSessionProvider_->GetOrCreateCellCommitSession(cellId);
    }

    ICellCommitSessionPtr GetCommitSession(TCellId cellId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return CellCommitSessionProvider_->GetCellCommitSession(cellId);
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

////////////////////////////////////////////////////////////////////////////////

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
