#include "transaction.h"

#include "client.h"
#include "sequoia_reign.h"
#include "table_descriptor.h"
#include "transaction_service_proxy.h"
#include "write_set.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/cell_commit_session.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/tablet_commit_session.h>
#include <yt/yt/ytlib/api/native/tablet_helpers.h>
#include <yt/yt/ytlib/api/native/transaction_helpers.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/sequoia_client/proto/transaction_client.pb.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/client/query_client/query_builder.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/record_descriptor.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NSequoiaClient {

using namespace NApi;
using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NCypressServer::NProto;
using namespace NLogging;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;

using NNative::IClientPtr;

////////////////////////////////////////////////////////////////////////////////

namespace {

const auto& Logger = SequoiaClientLogger;

void ValidateSequoiaTableSchema(ESequoiaTable table, const TTableSchemaPtr& actualSchema)
{
    const auto& expectedSchema = ITableDescriptor::Get(table)->GetPrimarySchema();
    if (*expectedSchema == *actualSchema) {
        return;
    }

    YT_LOG_ALERT("Unexpected schema of Sequoia table (SequoiaTable: %v, Schema: %v, ExpectedSchema: %v)",
        table,
        *actualSchema,
        *expectedSchema);

    THROW_ERROR_EXCEPTION(
        NTableClient::EErrorCode::SchemaViolation,
        "Sequoia table %Qlv has unexpected schema",
        table)
        << TErrorAttribute("expected_schema", *expectedSchema)
        << TErrorAttribute("actual_schema", *actualSchema);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

struct TDatalessLockRowRequest
{
    TCellTag MasterCellTag;
    TLegacyKey Key;
    ELockType LockType;
};

struct TLockRowRequest
{
    TLegacyKey Key;
    ELockType LockType;
};

struct TWriteRowRequest
{
    TUnversionedRow Row;
    ELockType LockType;
};

struct TDeleteRowRequest
{
    TLegacyKey Key;
};

////////////////////////////////////////////////////////////////////////////////

struct TPerTransactionTypeCounters
{
    NProfiling::TCounter TransactionCommitsSucceeded;
    NProfiling::TCounter TransactionCommitsFailed;
};

TPerTransactionTypeCounters* GetPerTransactionTypeCounters(ESequoiaTransactionType type)
{
    static auto counters = [] {
        THashMap<ESequoiaTransactionType, TPerTransactionTypeCounters> counters;
        for (auto type : TEnumTraits<ESequoiaTransactionType>::GetDomainValues()) {
            auto profiler = SequoiaClientProfiler().WithTag("type", FormatEnum(type));
            counters[type].TransactionCommitsSucceeded = profiler.Counter("/transaction_commits_succeeded");
            counters[type].TransactionCommitsFailed = profiler.Counter("/transaction_commits_failed");
        }
        return counters;
    }();
    return &GetOrCrash(counters, type);
}

////////////////////////////////////////////////////////////////////////////////

ISequoiaTransaction::TGuardedRowBuffer::TGuardedRowBuffer(
    NThreading::TSpinLock* lock,
    const TRowBufferPtr& rowBuffer)
    : Guard_(Guard(*lock))
    , RowBuffer_(rowBuffer)
{ }

const TRowBufferPtr& ISequoiaTransaction::TGuardedRowBuffer::Get() const
{
    return RowBuffer_;
}

////////////////////////////////////////////////////////////////////////////////

class TSequoiaTransaction
    : public ISequoiaTransaction
{
public:
    TSequoiaTransaction(
        ISequoiaClientPtr sequoiaClient,
        ESequoiaTransactionType type,
        IClientPtr localClient,
        IClientPtr groundClient,
        const TSequoiaTransactionOptions& sequoiaTransactionOptions)
        : SequoiaClient_(std::move(sequoiaClient))
        , Type_(type)
        , LocalClient_(std::move(localClient))
        , GroundClient_(std::move(groundClient))
        , SerializedInvoker_(CreateSerializedInvoker(
            LocalClient_->GetConnection()->GetInvoker()))
        , SequoiaTransactionOptions_(sequoiaTransactionOptions)
        , Logger(SequoiaClient_->GetLogger())
    { }

    TTransactionId GetId() const override
    {
        return Transaction_->GetId();
    }

    TTimestamp GetStartTimestamp() const override
    {
        return Transaction_->GetStartTimestamp();
    }

    TCellTagList GetAffectedMasterCellTags() const override
    {
        auto guard = Guard(Lock_);

        TCellTagList cellTags(MasterCellCommitSessions_.size());
        std::transform(
            MasterCellCommitSessions_.begin(),
            MasterCellCommitSessions_.end(),
            cellTags.begin(),
            [] (const auto& pair) {
                return pair.first;
            });

        return cellTags;
    }

    bool CouldGenerateId(NObjectClient::TObjectId id) const noexcept override
    {
        return IsSequoiaId(id) && GetStartTimestamp() == TimestampFromId(id);
    }

    TFuture<ISequoiaTransactionPtr> Start(const TTransactionStartOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        const auto& transactionManager = LocalClient_->GetTransactionManager();

        StartOptions_ = options;
        if (!StartOptions_.Timeout) {
            const auto& config = LocalClient_->GetNativeConnection()->GetConfig();
            StartOptions_.Timeout = config->SequoiaConnection->SequoiaTransactionTimeout;
        }

        return transactionManager->Start(ETransactionType::Tablet, StartOptions_)
            .Apply(BIND(&TSequoiaTransaction::OnTransactionStarted, MakeStrong(this))
                .AsyncVia(SerializedInvoker_));
    }

    TFuture<void> Commit(const NApi::TTransactionCommitOptions& options) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_VERIFY(!CommitStarted_.exchange(true));

        SortRequests();

        return
            BIND(&TSequoiaTransaction::ResolveRequests, MakeStrong(this))
                .AsyncVia(SerializedInvoker_)
                .Run()
            .Apply(BIND(&TSequoiaTransaction::PrepareRequests, MakeStrong(this))
                .AsyncVia(SerializedInvoker_))
            .Apply(BIND(&TSequoiaTransaction::CommitSessions, MakeStrong(this))
                .AsyncVia(SerializedInvoker_))
            .Apply(BIND(&TSequoiaTransaction::DoCommitTransaction, MakeStrong(this), options)
                .AsyncVia(SerializedInvoker_))
            .Apply(BIND(MaybeWrapSequoiaRetriableError<void>))
            .Apply(BIND([type = Type_] (const TError& error) {
                auto* counters = GetPerTransactionTypeCounters(type);
                (error.IsOK() ? counters->TransactionCommitsSucceeded : counters->TransactionCommitsFailed).Increment();
                return error;
            }));
    }

    TFuture<TUnversionedLookupRowsResult> LookupRows(
        ESequoiaTable table,
        TSharedRange<TLegacyKey> keys,
        const TColumnFilter& columnFilter) override
    {
        return SequoiaClient_->LookupRows(
            table,
            keys,
            columnFilter,
            Transaction_->GetStartTimestamp());
    }

    virtual TFuture<TSelectRowsResult> SelectRows(
        ESequoiaTable table,
        const TSelectRowsQuery& query) override
    {
        return SequoiaClient_->SelectRows(
            table,
            query,
            Transaction_->GetStartTimestamp());
    }

    void DatalessLockRow(
        TCellTag masterCellTag,
        ESequoiaTable table,
        TLegacyKey key,
        ELockType lockType) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        TDatalessLockRowRequest request{
            .MasterCellTag = masterCellTag,
            .Key = key,
            .LockType = lockType
        };

        TSequoiaTablePathDescriptor descriptor;
        descriptor.Table = table;
        auto commitSession = GetOrCreateTableCommitSession(descriptor);
        commitSession->Requests.push_back(request);
    }

    void LockRow(
        ESequoiaTable table,
        TLegacyKey key,
        ELockType lockType) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        TLockRowRequest request{
            .Key = key,
            .LockType = lockType
        };
        TSequoiaTablePathDescriptor descriptor{
            .Table = table,
        };
        auto commitSession = GetOrCreateTableCommitSession(descriptor);
        commitSession->Requests.push_back(request);

        YT_LOG_DEBUG_IF(SequoiaTransactionOptions_.EnableVerboseLogging,
            "Row locked (SequoiaTable: %v, Key: %v, LockType: %v)",
            table,
            key,
            lockType);
    }

    void WriteRow(
        ESequoiaTable table,
        TUnversionedRow row,
        ELockType lockType) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        TSequoiaTablePathDescriptor descriptor{
            .Table = table,
        };
        WriteRow(descriptor, row, lockType);

        YT_LOG_DEBUG_IF(SequoiaTransactionOptions_.EnableVerboseLogging,
            "Row written (SequoiaTable: %v, Row: %v, LockType: %v)",
            table,
            row,
            lockType);
    }

    void WriteRow(
        TSequoiaTablePathDescriptor tableDescriptor,
        TUnversionedRow row,
        ELockType lockType) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        TWriteRowRequest request{
            .Row = row,
            .LockType = lockType,
        };

        auto commitSession = GetOrCreateTableCommitSession(tableDescriptor);
        commitSession->Requests.push_back(request);

        YT_LOG_DEBUG_IF(SequoiaTransactionOptions_.EnableVerboseLogging,
            "Row written (SequoiaTable: %v, Row: %v, LockType: %v)",
            tableDescriptor.Table,
            row,
            lockType);
    }

    void DeleteRow(
        ESequoiaTable table,
        NTableClient::TLegacyKey key) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        TDeleteRowRequest request{
            .Key = key
        };

        TSequoiaTablePathDescriptor descriptor;
        descriptor.Table = table;
        auto commitSession = GetOrCreateTableCommitSession(descriptor);
        commitSession->Requests.push_back(request);

        YT_LOG_DEBUG_IF(SequoiaTransactionOptions_.EnableVerboseLogging,
            "Row deleted (SequoiaTable: %v, Key: %v)",
            table,
            key);
    }

    void AddTransactionAction(
        TCellTag masterCellTag,
        TTransactionActionData data) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        auto commitSession = GetOrCreateMasterCellCommitSession(masterCellTag);
        commitSession->TransactionActions.push_back(data);
    }

    TObjectId GenerateObjectId(
        EObjectType objectType,
        TCellTag cellTag,
        bool sequoia) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (cellTag == InvalidCellTag) {
            cellTag = GetRandomSequoiaNodeHostCellTag();
        }

        auto guard = Guard(Lock_);

        auto entropy = RandomGenerator_->Generate<ui32>();
        // TODO(kvk1920): exacly the same code exists in |TObjectManager|. Think
        // about moving it into more general place.
        if (IsCypressTransactionType(objectType)) {
            entropy &= 0xffff;
        }
        auto id = MakeSequoiaId(
            objectType,
            cellTag,
            Transaction_->GetStartTimestamp(),
            entropy);
        if (sequoia) {
            YT_ASSERT(IsSequoiaId(id));
        } else {
            id.Parts64[1] &= ~SequoiaCounterMask;
            YT_ASSERT(!IsSequoiaId(id));
        }

        return id;
    }

    TCellTag GetRandomSequoiaNodeHostCellTag() const override
    {
        auto connection = LocalClient_->GetNativeConnection();

        const auto& cellDirectorySynchronizer = connection->GetMasterCellDirectorySynchronizer();
        WaitFor(cellDirectorySynchronizer->RecentSync())
            .ThrowOnError();

        return connection->GetRandomMasterCellTagWithRoleOrThrow(
            NCellMasterClient::EMasterCellRole::SequoiaNodeHost);
    }

    TGuardedRowBuffer GetGuardedRowBuffer() override
    {
        return TGuardedRowBuffer(&Lock_, RowBuffer_);
    }

    const ISequoiaClientPtr& GetClient() const override
    {
        return SequoiaClient_;
    }

private:
    const ISequoiaClientPtr SequoiaClient_;
    const ESequoiaTransactionType Type_;
    const NApi::NNative::IClientPtr LocalClient_;
    const NApi::NNative::IClientPtr GroundClient_;
    const IInvokerPtr SerializedInvoker_;
    const TSequoiaTransactionOptions SequoiaTransactionOptions_;

    TLogger Logger;

    TTransactionPtr Transaction_;

    TTransactionStartOptions StartOptions_;

    std::unique_ptr<TRandomGenerator> RandomGenerator_;

    struct TSequoiaTransactionTag
    { };
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TSequoiaTransactionTag());

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    // For validation only.
    std::atomic<bool> CommitStarted_ = false;

    struct TMasterCellCommitSession final
    {
        TCellTag CellTag;

        std::vector<TTransactionActionData> TransactionActions;

        THashSet<TTabletCellId> TabletCellIds;

        TWriteSet WriteSet;

        TAuthenticationIdentity UserIdentity;
    };
    using TMasterCellCommitSessionPtr = TIntrusivePtr<TMasterCellCommitSession>;

    THashMap<TCellTag, TMasterCellCommitSessionPtr> MasterCellCommitSessions_;

    // TODO(h0pless): Add TRequestGeneration to a macro above.
    using TRequest = std::variant<
        TDatalessLockRowRequest,
        TLockRowRequest,
        TWriteRowRequest,
        TDeleteRowRequest
    >;

    struct TTableCommitSession final
    {
        explicit TTableCommitSession(TSequoiaTablePathDescriptor sequoiaTableDescriptor)
            : SequoiaTableDescriptor(sequoiaTableDescriptor)
        { }

        TSequoiaTablePathDescriptor SequoiaTableDescriptor;
        std::vector<TRequest> Requests;
    };
    using TTableCommitSessionPtr = TIntrusivePtr<TTableCommitSession>;

    THashMap<TSequoiaTablePathDescriptor, TTableCommitSessionPtr> TableCommitSessions_;

    // (TabletId, Dataless) -> TabletCommitSession
    THashMap<std::pair<TTabletId, bool>, ITabletCommitSessionPtr> TabletCommitSessions_;

    ICellCommitSessionProviderPtr CellCommitSessionProvider_;

    TMasterCellCommitSessionPtr GetOrCreateMasterCellCommitSession(TCellTag cellTag)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        auto it = MasterCellCommitSessions_.find(cellTag);
        if (it == MasterCellCommitSessions_.end()) {
            auto session = New<TMasterCellCommitSession>();
            session->CellTag = cellTag;
            session->UserIdentity = GetCurrentAuthenticationIdentity();
            EmplaceOrCrash(MasterCellCommitSessions_, cellTag, session);
            Transaction_->RegisterParticipant(LocalClient_->GetNativeConnection()->GetMasterCellId(cellTag));
            return session;
        } else {
            return it->second;
        }
    }

    TTableCommitSessionPtr GetOrCreateTableCommitSession(TSequoiaTablePathDescriptor tableDescriptor)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        auto it = TableCommitSessions_.find(tableDescriptor);
        if (it != TableCommitSessions_.end()) {
            return it->second;
        }

        auto session = New<TTableCommitSession>(tableDescriptor);
        EmplaceOrCrash(TableCommitSessions_, tableDescriptor, session);
        return session;
    }

    ITabletCommitSessionPtr GetOrCreateTabletCommitSession(
        TTabletId tabletId,
        bool dataless,
        const TTableMountInfoPtr& tableMountInfo,
        const TTabletInfoPtr& tabletInfo)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto key = std::pair(tabletId, dataless);
        auto sessionIt = TabletCommitSessions_.find(key);
        if (sessionIt != TabletCommitSessions_.end()) {
            return sessionIt->second;
        }

        // TODO(gritukan): Handle dataless.
        TTabletCommitOptions options;
        auto session = CreateTabletCommitSession(
            GroundClient_,
            std::move(options),
            MakeWeak(Transaction_),
            CellCommitSessionProvider_,
            tabletInfo,
            tableMountInfo,
            Logger);
        EmplaceOrCrash(TabletCommitSessions_, key, session);
        return session;
    }

    ISequoiaTransactionPtr OnTransactionStarted(const TTransactionPtr& transaction)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        Transaction_ = transaction;

        RandomGenerator_ = std::make_unique<TRandomGenerator>(Transaction_->GetStartTimestamp());

        CellCommitSessionProvider_ = CreateCellCommitSessionProvider(GroundClient_, MakeWeak(Transaction_), Logger);

        Logger.AddTag("TransactionId: %v", Transaction_->GetId());

        YT_LOG_DEBUG("Transaction started (StartTimestamp: %v, PrerequisiteTransactionIds: %v)",
            Transaction_->GetStartTimestamp(),
            SequoiaTransactionOptions_.CypressPrerequisiteTransactionIds);

        return MakeStrong(this);
    }

    TFuture<void> ResolveSessionRequests(const TTableCommitSessionPtr& session)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto path = GetSequoiaTablePath(LocalClient_, session->SequoiaTableDescriptor);

        const auto& tableMountCache = GroundClient_->GetTableMountCache();
        return tableMountCache->GetTableInfo(path)
            .Apply(BIND(&TSequoiaTransaction::OnGotTableMountInfo, MakeWeak(this), session)
                .AsyncVia(SerializedInvoker_));
    }

    void SortRequests()
    {
        if (const auto* sequencer = SequoiaTransactionOptions_.TransactionActionSequencer) {
            for (auto& [_, masterCellCommitSession] : MasterCellCommitSessions_) {
                auto& actions = masterCellCommitSession->TransactionActions;
                SortBy(actions, [&] (const TTransactionActionData& actionData) {
                    return sequencer->GetActionPriority(actionData.Type);
                });
            }
        }

        if (const auto& priorities = SequoiaTransactionOptions_.RequestPriorities) {
            for (auto& [_, session] : TableCommitSessions_) {
                auto& requests = session->Requests;
                SortBy(requests, [&] (const TRequest& request) {
                    #define HANDLE_REQUEST_KIND(RequestKind) \
                        [&] (const T##RequestKind##Request&) { \
                            return priorities->RequestKind; \
                        }

                    return Visit(request,
                        HANDLE_REQUEST_KIND(DatalessLockRow),
                        HANDLE_REQUEST_KIND(LockRow),
                        HANDLE_REQUEST_KIND(WriteRow),
                        HANDLE_REQUEST_KIND(DeleteRow));

                    #undef HANDLE_REQUEST_KIND
                });
            }
        }
    }

    TFuture<void> ResolveRequests()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<TFuture<void>> futures;
        futures.reserve(TableCommitSessions_.size());
        for (const auto& [_, session] : TableCommitSessions_) {
            futures.push_back(ResolveSessionRequests(session));
        }

        return AllSucceeded(std::move(futures));
    }

    TLegacyKey PrepareRow(
        const ITableDescriptor* tableDescriptor,
        TUnversionedRow row)
    {
        const auto& primarySchema = tableDescriptor->GetPrimarySchema();
        auto capturedRow = RowBuffer_->CaptureAndPermuteRow(
            row,
            *primarySchema,
            primarySchema->GetKeyColumnCount(),
            tableDescriptor->GetNameTableToPrimarySchemaIdMapping(),
            /*validateDuplicateAndRequiredValueColumns*/ true);
        if (const auto& evaluator = tableDescriptor->GetColumnEvaluator()) {
            evaluator->EvaluateKeys(capturedRow, RowBuffer_, /*preserveColumnsIds*/ true);
        }
        return capturedRow;
    }

    void OnGotTableMountInfo(
        const TTableCommitSessionPtr& session,
        const TTableMountInfoPtr& tableMountInfo)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        ValidateSequoiaTableSchema(session->SequoiaTableDescriptor.Table, tableMountInfo->Schemas[ETableSchemaKind::Primary]);

        const auto* tableDescriptor = ITableDescriptor::Get(session->SequoiaTableDescriptor.Table);
        const auto& primarySchema = tableDescriptor->GetPrimarySchema();
        const auto& writeSchema = tableDescriptor->GetWriteSchema();
        const auto& deleteSchema = tableDescriptor->GetDeleteSchema();
        const auto& lockSchema = tableDescriptor->GetLockSchema();

        const auto& recordDescriptor = ITableDescriptor::Get(session->SequoiaTableDescriptor.Table)->GetRecordDescriptor();
        auto nameTable = recordDescriptor->GetNameTable();
        std::optional<int> tabletIndexColumnId;
        if (!tableDescriptor->IsSorted()) {
            tabletIndexColumnId = nameTable->GetIdOrRegisterName(TabletIndexColumnName);
        }

        const auto& primaryIdMapping = tableDescriptor->GetNameTableToPrimarySchemaIdMapping();
        const auto& writeIdMapping = tableDescriptor->GetNameTableToWriteSchemaIdMapping();
        const auto& deleteIdMapping = tableDescriptor->GetNameTableToDeleteSchemaIdMapping();
        const auto& lockIdMapping = tableDescriptor->GetNameTableToLockSchemaIdMapping();

        std::vector<int> columnIndexToLockIndex;
        GetLocksMapping(
            *primarySchema,
            /*fullAtomicity*/ true,
            &columnIndexToLockIndex);

        for (auto& request : session->Requests) {
            Visit(request,
                [&] (const TDatalessLockRowRequest& request) {
                    ValidateClientKey(
                        request.Key,
                        *lockSchema,
                        lockIdMapping,
                        nameTable);

                    auto preparedKey = PrepareRow(tableDescriptor, request.Key);

                    YT_VERIFY(tableMountInfo->IsSorted());
                    auto tabletInfo = GetSortedTabletForRow(
                        tableMountInfo,
                        preparedKey,
                        /*validateWrite*/ true);

                    TLockMask lockMask;
                    lockMask.Set(PrimaryLockIndex, request.LockType);
                    TLockedRowInfo lockedRowInfo{
                        .LockMask = lockMask,
                        .TabletId = tabletInfo->TabletId,
                        .TabletCellId = tabletInfo->CellId
                    };

                    auto tabletCommitSession = GetOrCreateTabletCommitSession(
                        tabletInfo->TabletId,
                        /*dataless*/ true,
                        tableMountInfo,
                        tabletInfo);
                    tabletCommitSession->SubmitUnversionedRow(
                        EWireProtocolCommand::WriteAndLockRow,
                        preparedKey,
                        lockMask);

                    auto guard = Guard(Lock_);

                    auto masterCellCommitSession = GetOrCreateMasterCellCommitSession(request.MasterCellTag);
                    masterCellCommitSession->TabletCellIds.insert(tabletInfo->CellId);

                    auto& writeSet = masterCellCommitSession->WriteSet[session->SequoiaTableDescriptor.Table];
                    EmplaceOrCrash(writeSet, preparedKey, lockedRowInfo);
                },
                [&] (const TLockRowRequest& request) {
                    ValidateClientKey(
                        request.Key,
                        *lockSchema,
                        lockIdMapping,
                        nameTable);

                    auto preparedKey = PrepareRow(tableDescriptor, request.Key);

                    YT_VERIFY(tableMountInfo->IsSorted());
                    auto tabletInfo = GetSortedTabletForRow(
                        tableMountInfo,
                        preparedKey,
                        /*validateWrite*/ true);

                    TLockMask lockMask;
                    lockMask.Set(PrimaryLockIndex, request.LockType);

                    auto tabletCommitSession = GetOrCreateTabletCommitSession(
                        tabletInfo->TabletId,
                        /*dataless*/ false,
                        tableMountInfo,
                        tabletInfo);
                    tabletCommitSession->SubmitUnversionedRow(
                        EWireProtocolCommand::WriteAndLockRow,
                        preparedKey,
                        lockMask);
                },
                [&] (const TWriteRowRequest& request) {
                    ValidateClientDataRow(
                        request.Row,
                        *writeSchema,
                        writeIdMapping,
                        nameTable,
                        tabletIndexColumnId);

                    auto preparedRow = PrepareRow(tableDescriptor, request.Row);

                    TLockMask lockMask;
                    TTabletInfoPtr tabletInfo;
                    if (tableMountInfo->IsSorted()) {
                        tabletInfo = GetSortedTabletForRow(
                            tableMountInfo,
                            preparedRow,
                            /*validateWrite*/ true);

                        for (const auto& value : request.Row) {
                            auto mappedId = ApplyIdMapping(value, &primaryIdMapping);
                            if (auto lockIndex = columnIndexToLockIndex[mappedId]; lockIndex != -1) {
                                lockMask.Set(lockIndex, request.LockType);
                            }
                        }
                    } else {
                        auto randomTabletInfo = tableMountInfo->GetRandomMountedTablet();
                        tabletInfo = GetOrderedTabletForRow(
                            tableMountInfo,
                            randomTabletInfo,
                            tabletIndexColumnId,
                            request.Row,
                            true);
                    }

                    auto tabletCommitSession = GetOrCreateTabletCommitSession(
                        tabletInfo->TabletId,
                        /*dataless*/ false,
                        tableMountInfo,
                        tabletInfo);
                    tabletCommitSession->SubmitUnversionedRow(
                        tableMountInfo->IsSorted() ? EWireProtocolCommand::WriteAndLockRow : EWireProtocolCommand::WriteRow,
                        preparedRow,
                        lockMask);
                },
                [&] (const TDeleteRowRequest& request) {
                    ValidateClientKey(
                        request.Key,
                        *deleteSchema,
                        deleteIdMapping,
                        nameTable);

                    auto preparedKey = PrepareRow(tableDescriptor, request.Key);

                    TTabletInfoPtr tabletInfo;
                    if (tableMountInfo->IsSorted()) {
                        tabletInfo = GetSortedTabletForRow(
                            tableMountInfo,
                            preparedKey,
                            /*validateWrite*/ true);
                    } else {
                        auto randomTabletInfo = tableMountInfo->GetRandomMountedTablet();
                        tabletInfo = GetOrderedTabletForRow(
                            tableMountInfo,
                            randomTabletInfo,
                            tabletIndexColumnId,
                            preparedKey,
                            /*validateWrite*/ true);
                    }

                    auto tabletCommitSession = GetOrCreateTabletCommitSession(
                        tabletInfo->TabletId,
                        /*dataless*/ false,
                        tableMountInfo,
                        tabletInfo);
                    tabletCommitSession->SubmitUnversionedRow(
                        EWireProtocolCommand::DeleteRow,
                        preparedKey,
                        TLockMask());
                },
                [&] (auto) { YT_ABORT(); });
        }
    }

    void PrepareRequests()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        for (const auto& [tabletId, tabletCommitSession] : TabletCommitSessions_) {
            tabletCommitSession->PrepareRequests();
        }

        for (const auto& [cellTag, masterCellCommitSession] : MasterCellCommitSessions_) {
            for (auto tabletCellId : masterCellCommitSession->TabletCellIds) {
                auto cellCommitSession = CellCommitSessionProvider_->GetCellCommitSession(tabletCellId);
                cellCommitSession->GetCommitSignatureGenerator()->RegisterRequest();
            }
        }
    }

    TFuture<void> CommitMasterSessions()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<TFuture<void>> futures;
        futures.reserve(MasterCellCommitSessions_.size());
        for (const auto& [cellTag, session] : MasterCellCommitSessions_) {
            auto channel = LocalClient_->GetNativeConnection()->GetMasterChannelOrThrow(
                EMasterChannelKind::Leader,
                cellTag);
            TSequoiaTransactionServiceProxy proxy(std::move(channel));
            auto req = proxy.StartTransaction();
            ToProto(req->mutable_id(), Transaction_->GetId());
            req->set_timeout(NYT::ToProto(*StartOptions_.Timeout));
            if (const auto& attributes = StartOptions_.Attributes) {
                ToProto(req->mutable_attributes(), *attributes);
            } else {
                ToProto(req->mutable_attributes(), *NYTree::CreateEphemeralAttributes());
            }
            ToProto(req->mutable_write_set(), session->WriteSet);
            for (const auto& action : session->TransactionActions) {
                ToProto(req->add_actions(), action);
            }
            WriteAuthenticationIdentityToProto(req->mutable_identity(), session->UserIdentity);
            req->set_sequoia_reign(NYT::ToProto(GetCurrentSequoiaReign()));
            ToProto(req->mutable_prerequisite_transaction_ids(), SequoiaTransactionOptions_.CypressPrerequisiteTransactionIds);

            futures.push_back(req->Invoke().AsVoid());
        }

        return AllSucceeded(std::move(futures));
    }

    TFuture<void> CommitTabletSessions()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<TFuture<void>> futures;
        futures.reserve(TabletCommitSessions_.size());
        if (SequoiaTransactionOptions_.SequenceTabletCommitSessions) {
            for (const auto& [tabletId, tabletCommitSession] : SortHashMapByKeys(TabletCommitSessions_)) {
                auto previousFuture = futures.empty() ? VoidFuture : futures.back();
                futures.push_back(previousFuture.Apply(
                    BIND([session = tabletCommitSession] (const TError& /*error*/) {
                        return session->Invoke();
                    })));
            }
        } else {
            for (const auto& [tabletId, tabletCommitSession] : TabletCommitSessions_) {
                futures.push_back(tabletCommitSession->Invoke());
            }
        }

        return AllSucceeded(std::move(futures));
    }

    TFuture<void> CommitSessions()
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        return AllSucceeded(std::vector<TFuture<void>>({
            CommitMasterSessions(),
            CommitTabletSessions(),
        }));
    }

    TFuture<void> DoCommitTransaction(NApi::TTransactionCommitOptions options)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        options.Force2PC = true; // Just in case.
        options.AllowAlienCoordinator = true;

        // TODO(kvk1920): enumerate such cases.
        if (options.CoordinatorCellId && MasterCellCommitSessions_.empty()) {
            options.CoordinatorCellId = TCellId{};
            options.CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Early;
        }

        Transaction_->ChooseCoordinator(options);
        return Transaction_->Commit(options).AsVoid();
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TFuture<ISequoiaTransactionPtr> StartSequoiaTransaction(
    ISequoiaClientPtr sequoiaClient,
    ESequoiaTransactionType type,

    IClientPtr localClient,
    IClientPtr groundClient,
    const TTransactionStartOptions& transactionStartOptions,
    const TSequoiaTransactionOptions& sequoiaTransactionOptions)
{
    auto transaction = New<TSequoiaTransaction>(
        std::move(sequoiaClient),
        type,
        std::move(localClient),
        std::move(groundClient),
        sequoiaTransactionOptions);
    return transaction->Start(transactionStartOptions);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
