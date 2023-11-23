#include "transaction.h"

#include "client.h"
#include "transaction_service_proxy.h"
#include "write_set.h"

#include <yt/yt/ytlib/api/native/cell_commit_session.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/tablet_commit_session.h>
#include <yt/yt/ytlib/api/native/tablet_helpers.h>
#include <yt/yt/ytlib/api/native/transaction_helpers.h>

#include <yt/yt/ytlib/sequoia_client/table_descriptor.h>
#include <yt/yt/ytlib/sequoia_client/proto/transaction_client.pb.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/client/query_client/query_builder.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/record_descriptor.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NSequoiaClient {

using namespace NApi;
using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaTransaction
    : public ISequoiaTransaction
{
public:
    TSequoiaTransaction(ISequoiaClientPtr client)
        : Client_(std::move(client))
        , Logger(Client_->GetLogger())
        , SerializedInvoker_(CreateSerializedInvoker(
            GetNativeRootClient()->GetConnection()->GetInvoker()))
    { }

    TFuture<ISequoiaTransactionPtr> Start(const TTransactionStartOptions& options)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& transactionManager = GetNativeRootClient()->GetTransactionManager();

        StartOptions_ = options;
        if (!StartOptions_.Timeout) {
            const auto& config = GetNativeRootClient()->GetNativeConnection()->GetConfig();
            StartOptions_.Timeout = config->SequoiaConnection->SequoiaTransactionTimeout;
        }

        TTransactionStartOptions startOptions;
        startOptions.Timeout = StartOptions_.Timeout;
        return transactionManager->Start(ETransactionType::Tablet, startOptions)
            .Apply(BIND(&TSequoiaTransaction::OnTransactionStarted, MakeStrong(this))
                .AsyncVia(SerializedInvoker_));
    }

    TFuture<void> Commit(const NApi::TTransactionCommitOptions& options) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return
            BIND(&TSequoiaTransaction::ResolveRequests, MakeStrong(this))
                .AsyncVia(SerializedInvoker_)
                .Run()
            .Apply(BIND(&TSequoiaTransaction::PrepareRequests, MakeStrong(this))
                .AsyncVia(SerializedInvoker_))
            .Apply(BIND(&TSequoiaTransaction::CommitSessions, MakeStrong(this))
                .AsyncVia(SerializedInvoker_))
            .Apply(BIND(&TSequoiaTransaction::DoCommitTransaction, MakeStrong(this), options)
                .AsyncVia(SerializedInvoker_));
    }

    TFuture<TUnversionedLookupRowsResult> LookupRows(
        ESequoiaTable table,
        TSharedRange<TLegacyKey> keys,
        const TColumnFilter& columnFilter) override
    {
        return Client_->LookupRows(
            table,
            keys,
            columnFilter,
            Transaction_->GetStartTimestamp());
    }

    virtual TFuture<TSelectRowsResult> SelectRows(
        ESequoiaTable table,
        const std::vector<TString>& whereConjuncts,
        std::optional<i64> limit) override
    {
        return Client_->SelectRows(
            table,
            whereConjuncts,
            limit,
            Transaction_->GetStartTimestamp());
    }

    void DatalessLockRow(
        TCellTag masterCellTag,
        ESequoiaTable table,
        TLegacyKey key,
        ELockType lockType) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        TDatalessLockRowRequest request{
            .MasterCellTag = masterCellTag,
            .Key = key,
            .LockType = lockType
        };

        auto commitSession = GetOrCreateTableCommitSession(table);
        commitSession->Requests.push_back(request);
    }

    void LockRow(
        ESequoiaTable table,
        TLegacyKey key,
        ELockType lockType) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        TLockRowRequest request{
            .Key = key,
            .LockType = lockType
        };

        auto commitSession = GetOrCreateTableCommitSession(table);
        commitSession->Requests.push_back(request);
    }

    void WriteRow(
        ESequoiaTable table,
        TUnversionedRow row,
        ELockType lockType) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        TWriteRowRequest request{
            .Row = row,
            .LockType = lockType,
        };

        auto commitSession = GetOrCreateTableCommitSession(table);
        commitSession->Requests.push_back(request);
    }

    void DeleteRow(
        ESequoiaTable table,
        NTableClient::TLegacyKey key) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        TDeleteRowRequest request{
            .Key = key
        };

        auto commitSession = GetOrCreateTableCommitSession(table);
        commitSession->Requests.push_back(request);
    }

    void AddTransactionAction(
        TCellTag masterCellTag,
        TTransactionActionData data) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        auto commitSession = GetOrCreateMasterCellCommitSession(masterCellTag);
        commitSession->TransactionActions.push_back(data);
    }

    TObjectId GenerateObjectId(
        EObjectType objectType,
        TCellTag cellTag,
        bool sequoia) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        auto id = MakeSequoiaId(
            objectType,
            cellTag,
            Transaction_->GetStartTimestamp(),
            RandomGenerator_->Generate<ui32>());
        if (sequoia) {
            YT_ASSERT(IsSequoiaId(id));
        } else {
            id.Parts64[1] &= ~SequoiaCounterMask;
            YT_ASSERT(!IsSequoiaId(id));
        }

        return id;
    }

    const TRowBufferPtr& GetRowBuffer() const override
    {
        return RowBuffer_;
    }

    const ISequoiaClientPtr& GetClient() const override
    {
        return Client_;
    }

    const NApi::NNative::IClientPtr& GetNativeRootClient() const override
    {
        return Client_->GetNativeRootClient();
    }

    const NApi::NNative::IClientPtr& GetGroundRootClient() const override
    {
        return Client_->GetGroundRootClient();
    }

private:
    const ISequoiaClientPtr Client_;

    TTransactionPtr Transaction_;

    TTransactionStartOptions StartOptions_;

    std::unique_ptr<TRandomGenerator> RandomGenerator_;

    TLogger Logger;

    const IInvokerPtr SerializedInvoker_;

    struct TSequoiaTransactionTag
    { };
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TSequoiaTransactionTag());

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    struct TMasterCellCommitSession final
    {
        TCellTag CellTag;

        std::vector<TTransactionActionData> TransactionActions;

        THashSet<TTabletCellId> TabletCellIds;

        TWriteSet WriteSet;
    };
    using TMasterCellCommitSessionPtr = TIntrusivePtr<TMasterCellCommitSession>;

    THashMap<TCellTag, TMasterCellCommitSessionPtr> MasterCellCommitSessions_;

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

    using TRequest = std::variant<
        TDatalessLockRowRequest,
        TLockRowRequest,
        TWriteRowRequest,
        TDeleteRowRequest
    >;

    struct TTableCommitSession final
    {
        NYTree::TYPath Path;

        ESequoiaTable Table;

        std::vector<TRequest> Requests;
    };
    using TTableCommitSessionPtr = TIntrusivePtr<TTableCommitSession>;

    THashMap<ESequoiaTable, TTableCommitSessionPtr> TableCommitSessions_;

    // (TabletId, Dataless) -> TabletCommitSession
    THashMap<std::pair<TTabletId, bool>, ITabletCommitSessionPtr> TabletCommitSessions_;

    ICellCommitSessionProviderPtr CellCommitSessionProvider_;

    TMasterCellCommitSessionPtr GetOrCreateMasterCellCommitSession(TCellTag cellTag)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        auto it = MasterCellCommitSessions_.find(cellTag);
        if (it == MasterCellCommitSessions_.end()) {
            auto session = New<TMasterCellCommitSession>();
            session->CellTag = cellTag;
            EmplaceOrCrash(MasterCellCommitSessions_, cellTag, session);
            Transaction_->RegisterParticipant(GetNativeRootClient()->GetNativeConnection()->GetMasterCellId(cellTag));
            return session;
        } else {
            return it->second;
        }
    }

    TTableCommitSessionPtr GetOrCreateTableCommitSession(ESequoiaTable table)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        auto it = TableCommitSessions_.find(table);
        if (it != TableCommitSessions_.end()) {
            return it->second;
        }

        auto session = New<TTableCommitSession>();
        const auto* tableDescriptor = ITableDescriptor::Get(table);
        session->Path = GetTablePath(tableDescriptor);
        session->Table = table;
        EmplaceOrCrash(TableCommitSessions_, table, session);
        return session;
    }

    ITabletCommitSessionPtr GetOrCreateTabletCommitSession(
        TTabletId tabletId,
        bool dataless,
        const TTableMountInfoPtr& tableMountInfo,
        const TTabletInfoPtr& tabletInfo)
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        auto key = std::make_pair(tabletId, dataless);
        auto sessionIt = TabletCommitSessions_.find(key);
        if (sessionIt != TabletCommitSessions_.end()) {
            return sessionIt->second;
        }

        // TODO(gritukan): Handle dataless.
        TTabletCommitOptions options;
        auto session = CreateTabletCommitSession(
            GetGroundRootClient(),
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
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        Transaction_ = transaction;

        RandomGenerator_ = std::make_unique<TRandomGenerator>(Transaction_->GetStartTimestamp());

        CellCommitSessionProvider_ = CreateCellCommitSessionProvider(GetGroundRootClient(), MakeWeak(Transaction_), Logger);

        Logger.AddTag("TransactionId: %v", Transaction_->GetId());

        YT_LOG_DEBUG("Transaction started (StartTimestamp: %v)",
            Transaction_->GetStartTimestamp());

        return MakeStrong(this);
    }

    TFuture<void> ResolveSessionRequests(const TTableCommitSessionPtr& session)
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        const auto& tableMountCache = GetGroundRootClient()->GetTableMountCache();
        return tableMountCache->GetTableInfo(session->Path)
            .Apply(BIND(&TSequoiaTransaction::OnGotTableMountInfo, MakeWeak(this), session)
                .AsyncVia(SerializedInvoker_));
    }

    TFuture<void> ResolveRequests()
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<TFuture<void>> futures;
        futures.reserve(TableCommitSessions_.size());
        for (const auto& [table, session] : TableCommitSessions_) {
            futures.push_back(ResolveSessionRequests(session));
        }

        return AllSucceeded(std::move(futures));
    }

    void OnGotTableMountInfo(
        const TTableCommitSessionPtr& session,
        const TTableMountInfoPtr& tableMountInfo)
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        for (auto& request : session->Requests) {
            Visit(request,
                [&] (const TDatalessLockRowRequest& request) {
                    auto tabletInfo = GetSortedTabletForRow(tableMountInfo, request.Key, /*validateWrite*/ true);

                    TLockMask lockMask;
                    lockMask.Set(PrimaryLockIndex, request.LockType);
                    TLockedRowInfo lockedRowInfo{
                        .LockMask = lockMask,
                        .TabletId = tabletInfo->TabletId,
                        .TabletCellId = tabletInfo->CellId
                    };

                    auto tabletCommitSession = GetOrCreateTabletCommitSession(tabletInfo->TabletId, /*dataless*/ true, tableMountInfo, tabletInfo);
                    tabletCommitSession->SubmitUnversionedRow(EWireProtocolCommand::WriteAndLockRow, request.Key, lockMask);

                    auto guard = Guard(Lock_);

                    auto masterCellCommitSession = GetOrCreateMasterCellCommitSession(request.MasterCellTag);
                    masterCellCommitSession->TabletCellIds.insert(tabletInfo->CellId);

                    auto& writeSet = masterCellCommitSession->WriteSet[session->Table];
                    EmplaceOrCrash(writeSet, request.Key, lockedRowInfo);
                },
                [&] (const TLockRowRequest& request) {
                    auto tabletInfo = GetSortedTabletForRow(tableMountInfo, request.Key, /*validateWrite*/ true);

                    TLockMask lockMask;
                    lockMask.Set(PrimaryLockIndex, request.LockType);

                    auto tabletCommitSession = GetOrCreateTabletCommitSession(tabletInfo->TabletId, /*dataless*/ false, tableMountInfo, tabletInfo);
                    tabletCommitSession->SubmitUnversionedRow(EWireProtocolCommand::WriteAndLockRow, request.Key, lockMask);
                },
                [&] (const TWriteRowRequest& request) {
                    if (request.LockType == ELockType::SharedWrite && !tableMountInfo->EnableSharedWriteLocks) {
                        THROW_ERROR_EXCEPTION("Shared write locks should be explicitly enabled for table %v to use them",
                            tableMountInfo->Path);
                    }

                    auto tabletInfo = GetSortedTabletForRow(tableMountInfo, request.Row, /*validateWrite*/ true);

                    TLockMask lockMask;
                    if (tableMountInfo->IsSorted()) {
                        std::vector<int> columnIndexToLockIndex;
                        GetLocksMapping(
                            *tableMountInfo->Schemas[ETableSchemaKind::Write],
                            true,
                            &columnIndexToLockIndex);

                        for (const auto& value: request.Row) {
                            if (auto lockIndex = columnIndexToLockIndex[value.Id]; lockIndex != -1) {
                                lockMask.Set(lockIndex, request.LockType);
                            }
                        }
                    }

                    auto tabletCommitSession = GetOrCreateTabletCommitSession(tabletInfo->TabletId, /*dataless*/ false, tableMountInfo, tabletInfo);
                    tabletCommitSession->SubmitUnversionedRow(EWireProtocolCommand::WriteAndLockRow, request.Row, lockMask);
                },
                [&] (const TDeleteRowRequest& request) {
                    auto tabletInfo = GetSortedTabletForRow(tableMountInfo, request.Key, /*validateWrite*/ true);

                    auto tabletCommitSession = GetOrCreateTabletCommitSession(tabletInfo->TabletId, /*dataless*/ false, tableMountInfo, tabletInfo);
                    tabletCommitSession->SubmitUnversionedRow(EWireProtocolCommand::DeleteRow, request.Key, TLockMask{});
                },
                [&] (auto) { YT_ABORT(); });
        }
    }

    void PrepareRequests()
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

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
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<TFuture<void>> futures;
        futures.reserve(MasterCellCommitSessions_.size());
        for (const auto& [cellTag, session] : MasterCellCommitSessions_) {
            auto channel = GetNativeRootClient()->GetNativeConnection()->GetMasterChannelOrThrow(
                NApi::EMasterChannelKind::Leader,
                cellTag);
            TSequoiaTransactionServiceProxy proxy(std::move(channel));
            auto req = proxy.StartTransaction();
            ToProto(req->mutable_id(), Transaction_->GetId());
            req->set_timeout(::NYT::ToProto<i64>(*StartOptions_.Timeout));
            if (const auto& attributes = StartOptions_.Attributes) {
                ToProto(req->mutable_attributes(), *attributes);
            } else {
                ToProto(req->mutable_attributes(), *NYTree::CreateEphemeralAttributes());
            }
            ToProto(req->mutable_write_set(), session->WriteSet);
            for (const auto& action : session->TransactionActions) {
                ToProto(req->add_actions(), action);
            }
            futures.push_back(req->Invoke().AsVoid());
        }

        return AllSucceeded(std::move(futures));
    }

    TFuture<void> CommitTabletSessions()
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<TFuture<void>> futures;
        futures.reserve(TabletCommitSessions_.size());
        for (const auto& [tabletId, tabletCommitSession] : TabletCommitSessions_) {
            futures.push_back(tabletCommitSession->Invoke());
        }

        return AllSucceeded(std::move(futures));
    }

    TFuture<void> CommitSessions()
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        return AllSucceeded(std::vector<TFuture<void>>({
            CommitMasterSessions(),
            CommitTabletSessions(),
        }));
    }

    TFuture<void> DoCommitTransaction(NApi::TTransactionCommitOptions options)
    {
        VERIFY_INVOKER_AFFINITY(SerializedInvoker_);

        options.Force2PC = true; // Just in case.
        options.AllowAlienCoordinator = true;

        Transaction_->ChooseCoordinator(options);
        return Transaction_->Commit(options).AsVoid();
    }

    TYPath GetTablePath(const ITableDescriptor* tableDescriptor) const
    {
        return GetSequoiaTablePath(GetNativeRootClient(), tableDescriptor);
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TFuture<ISequoiaTransactionPtr> StartSequoiaTransaction(
    ISequoiaClientPtr client,
    const TTransactionStartOptions& options)
{
    return New<TSequoiaTransaction>(std::move(client))->Start(options);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
