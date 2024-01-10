#include "query_context.h"

#include "helpers.h"
#include "host.h"
#include "query_registry.h"
#include "statistics_reporter.h"
#include "secondary_query_header.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/rpc_helpers.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

#include <util/generic/algorithm.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHydra;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TLogger QueryLogger("Query");

////////////////////////////////////////////////////////////////////////////////

namespace {

TFuture<std::vector<TErrorOr<TObjectLock>>> DoAcquireSnapshotLocksAsync(
    const std::vector<TYPath>& paths,
    const NNative::IClientPtr& client,
    TTransactionId readTransactionId,
    const TLogger& logger)
{
    const auto& Logger = logger;

    YT_LOG_INFO("Acquiring snapshot locks (Paths: %v)", paths);

    auto proxy = CreateObjectServiceWriteProxy(client);
    auto batchReq = proxy.ExecuteBatch();

    for (size_t index = 0; index < paths.size(); ++index) {
        auto req = TCypressYPathProxy::Lock(paths[index]);
        req->Tag() = index;
        req->set_mode(static_cast<int>(ELockMode::Snapshot));
        SetTransactionId(req, readTransactionId);
        batchReq->AddRequest(req);
    }

    return batchReq->Invoke()
        .Apply(BIND([pathCount = paths.size(), Logger, readTransactionId] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
            std::vector<TErrorOr<TObjectLock>> locks(pathCount);

            for (const auto& [tag, rspOrError] : batchRsp->GetTaggedResponses<TCypressYPathProxy::TRspLock>()) {
                auto index = std::any_cast<size_t>(tag);

                if (rspOrError.IsOK()) {
                    const auto& rsp = rspOrError.Value();
                    locks[index] = TObjectLock{
                        .NodeId = FromProto<TNodeId>(rsp->node_id()),
                        .Revision = FromProto<TRevision>(rsp->revision()),
                    };
                    YT_LOG_INFO("Snapshot lock is acquired (LockId: %v, NodeId: %v, Revision: %v, ReadTransactionId: %v)",
                        rsp->lock_id(),
                        rsp->node_id(),
                        rsp->revision(),
                        readTransactionId);
                } else {
                    locks[index] = TError(rspOrError);
                    YT_LOG_INFO(rspOrError, "Failed to acquire snapshot lock (Index: %v)", index);
                }
            }

            return locks;
        }));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStorageContext::TStorageContext(int index, DB::ContextPtr context, TQueryContext* queryContext)
    : Index(index)
    , QueryContext(queryContext)
    , Logger(queryContext->Logger.WithTag("StorageIndex: %v", Index))
{
    YT_LOG_INFO("Storage context created");

    Settings = ParseCustomSettings(queryContext->Host->GetConfig()->QuerySettings, context->getSettings().allCustom(), Logger);
}

TStorageContext::~TStorageContext()
{
    YT_LOG_INFO("Storage context destroyed");
}

////////////////////////////////////////////////////////////////////////////////

TQueryContext::TQueryContext(
    THost* host,
    DB::ContextPtr context,
    TQueryId queryId,
    TTraceContextPtr traceContext,
    std::optional<TString> dataLensRequestId,
    std::optional<TString> yqlOperationId,
    const TSecondaryQueryHeaderPtr& secondaryQueryHeader)
    : Logger(QueryLogger)
    , User(TString(context->getClientInfo().initial_user))
    , TraceContext(std::move(traceContext))
    , QueryId(queryId)
    , QueryKind(static_cast<EQueryKind>(context->getClientInfo().query_kind))
    , Host(host)
    , DataLensRequestId(std::move(dataLensRequestId))
    , YqlOperationId(std::move(yqlOperationId))
    , RowBuffer(New<NTableClient::TRowBuffer>())
{
    Logger.AddTag("QueryId: %v", QueryId);
    if (DataLensRequestId) {
        Logger.AddTag("DataLensRequestId: %v", DataLensRequestId);
    }

    YT_LOG_INFO("Query context created (User: %v, QueryKind: %v)", User, QueryKind);
    LastPhaseTime_ = StartTime_ = TInstant::Now();

    const auto& clientInfo = context->getClientInfo();

    CurrentUser = clientInfo.current_user;
    CurrentAddress = clientInfo.current_address.toString();

    InitialUser = clientInfo.initial_user;
    InitialAddress = clientInfo.initial_address.toString();
    InitialQueryId = TQueryId::FromString(clientInfo.initial_query_id);

    if (QueryKind == EQueryKind::SecondaryQuery) {
        YT_VERIFY(secondaryQueryHeader);
        ParentQueryId = secondaryQueryHeader->ParentQueryId;
        SelectQueryIndex = secondaryQueryHeader->StorageIndex;
        ReadTransactionId = secondaryQueryHeader->ReadTransactionId;
        SnapshotLocks = secondaryQueryHeader->SnapshotLocks;
        DynamicTableReadTimestamp = secondaryQueryHeader->DynamicTableReadTimestamp;
        WriteTransactionId = secondaryQueryHeader->WriteTransactionId;
        CreatedTablePath = secondaryQueryHeader->CreatedTablePath;

        QueryDepth = secondaryQueryHeader->QueryDepth;

        Logger.AddTag("InitialQueryId: %v", InitialQueryId);
        Logger.AddTag("ParentQueryId: %v", ParentQueryId);
    }
    Interface = static_cast<EInterface>(clientInfo.interface);
    if (Interface == EInterface::HTTP) {
        HttpUserAgent = clientInfo.http_user_agent;
    }

    Settings = ParseCustomSettings(Host->GetConfig()->QuerySettings, context->getSettings().allCustom(), Logger);

    YT_LOG_INFO(
        "Query client info (CurrentUser: %v, CurrentAddress: %v, InitialUser: %v, InitialAddress: %v, "
        "Interface: %v, HttpUserAgent: %v, QueryKind: %v)",
        CurrentUser,
        CurrentAddress,
        InitialUser,
        InitialAddress,
        Interface,
        HttpUserAgent,
        QueryKind);

    if (Settings->Testing->HangControlInvoker) {
        auto longAction = BIND([] {
            std::this_thread::sleep_for(std::chrono::hours(1));
        });
        Host->GetControlInvoker()->Invoke(longAction);
    }
}

// Fake query context constructor.
TQueryContext::TQueryContext(THost* host, NNative::IClientPtr client)
    : QueryKind(EQueryKind::NoQuery)
    , Host(host)
    , Settings(New<TQuerySettings>())
    , Client_(std::move(client))
{ }

TQueryContextPtr TQueryContext::CreateFake(THost* host, NNative::IClientPtr client)
{
    return New<TQueryContext>(host, std::move(client));
}

TQueryContext::~TQueryContext()
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Do not need to do anything for fake query context.
    if (QueryKind == EQueryKind::NoQuery) {
        return;
    }

    MoveToPhase(EQueryPhase::Finish);

    if (TraceContext) {
        TraceContext->Finish();
    }

    auto finishTime = TInstant::Now();
    auto duration = finishTime - StartTime_;

    if (QueryKind == EQueryKind::InitialQuery) {
        Host->GetQueryRegistry()->AccountTotalDuration(duration);
    }

    YT_LOG_INFO("Query time statistics (StartTime: %v, FinishTime: %v, Duration: %v)", StartTime_, finishTime, duration);
    YT_LOG_INFO("Query phase debug string (DebugString: %v)", PhaseDebugString_);
    YT_LOG_INFO("Query context destroyed");
}

const NNative::IClientPtr& TQueryContext::Client() const
{
    bool clientPresent;
    {
        auto readerGuard = ReaderGuard(ClientLock_);
        clientPresent = static_cast<bool>(Client_);
    }

    if (!clientPresent) {
        auto writerGuard = WriterGuard(ClientLock_);
        if (!Client_) {
            Client_ = Host->CreateClient(User);
        }
    }

    return Client_;
}

void TQueryContext::MoveToPhase(EQueryPhase nextPhase)
{
    // Weak check. CurrentPhase_ changes in monotonic manner, so it
    // may result in false-positive, but not false-negative.
    if (nextPhase <= QueryPhase_.load()) {
        return;
    }

    auto readerGuard = Guard(PhaseLock_);

    if (nextPhase <= QueryPhase_.load()) {
        return;
    }

    auto currentTime = TInstant::Now();
    auto duration = currentTime - LastPhaseTime_;
    PhaseDebugString_ += Format(" - %v - %v", duration, nextPhase);

    auto oldPhase = QueryPhase_.load();

    YT_LOG_INFO("Query phase changed (FromPhase: %v, ToPhase: %v, Duration: %v)", oldPhase, nextPhase, duration);

    if (QueryKind == EQueryKind::InitialQuery && (oldPhase == EQueryPhase::Preparation || oldPhase == EQueryPhase::Execution)) {
        Host->GetQueryRegistry()->AccountPhaseDuration(oldPhase, duration);
    }

    QueryPhase_ = nextPhase;

    readerGuard.Release();

    // It is effectively useless to count queries in state "Finish" in query registry,
    // and also we do not want exceptions to throw in query context destructor.
    if (nextPhase != EQueryPhase::Finish) {
        WaitFor(BIND(
            &TQueryRegistry::AccountPhaseCounter,
            Host->GetQueryRegistry(),
            MakeStrong(this),
            oldPhase,
            nextPhase)
            .AsyncVia(Host->GetControlInvoker())
            .Run())
            .ThrowOnError();
    }
}

EQueryPhase TQueryContext::GetQueryPhase() const
{
    return QueryPhase_.load();
}

void TQueryContext::Finish()
{
    FinishTime_ = TInstant::Now();
    AggregatedStatistics.Merge(InstanceStatistics);
    Host->GetQueryStatisticsReporter()->ReportQueryStatistics(MakeStrong(this));
}

TInstant TQueryContext::GetStartTime() const
{
    return StartTime_;
}

TInstant TQueryContext::GetFinishTime() const
{
    return FinishTime_;
}

TStorageContext* TQueryContext::FindStorageContext(const DB::IStorage* storage)
{
    {
        auto readerGuard = ReaderGuard(StorageToStorageContextLock_);
        auto it = StorageToStorageContext_.find(storage);
        if (it != StorageToStorageContext_.end()) {
            return it->second.Get();
        }
    }
    return nullptr;
}

TStorageContext* TQueryContext::GetOrRegisterStorageContext(const DB::IStorage* storage, DB::ContextPtr context)
{
    if (auto* storageContext = FindStorageContext(storage)) {
        return storageContext;
    }

    {
        auto writerGuard = WriterGuard(StorageToStorageContextLock_);
        auto storageIndex = StorageToStorageContext_.size();
        const auto& storageContext = (StorageToStorageContext_[storage] =
            New<TStorageContext>(storageIndex, context, this));
        return storageContext.Get();
    }
}

const TClusterNodes& TQueryContext::GetClusterNodesSnapshot()
{
    if (!ClusterNodesSnapshot_) {
        ClusterNodesSnapshot_ = Host->GetNodes(/*alwaysIncludeLocal*/ true);
    }
    return *ClusterNodesSnapshot_;
}

std::vector<TErrorOr<IAttributeDictionaryPtr>> TQueryContext::GetObjectAttributesSnapshot(
    const std::vector<TYPath>& paths)
{
    std::vector<TYPath> pathsToFetch;
    pathsToFetch.reserve(paths.size());

    for (const auto& path : paths) {
        if (!ObjectAttributesSnapshot_.contains(path)) {
            pathsToFetch.push_back(path);
        }
    }

    SortUnique(pathsToFetch);

    // Add attributes to ObjectAttributesSnapshot_.
    auto addToSnapshot = [this] (std::vector<TYPath>&& paths, std::vector<TErrorOr<IAttributeDictionaryPtr>>&& attributes) {
        YT_VERIFY(paths.size() == attributes.size());
        for (size_t index = 0; index < paths.size(); ++index) {
            ObjectAttributesSnapshot_.emplace(std::move(paths[index]), std::move(attributes[index]));
        }
    };

    if (CreatedTablePath && pathsToFetch.size() == 1 && pathsToFetch.front() == CreatedTablePath) {
        // Special case when ClickHouse fetches the table created in the current query.
        // This table exists only in the write transaction so far.
        auto attributes = FetchTableAttributes(pathsToFetch, WriteTransactionId);
        addToSnapshot(std::move(pathsToFetch), std::move(attributes));
    } else if (QueryKind == EQueryKind::InitialQuery) {
        auto lockMode = Settings->Execution->TableReadLockMode;

        if (lockMode == ETableReadLockMode::Sync) {
            // To speed up fetching, we fetch attributes and acquire snapshot locks in parallel.
            // It may lead to inconsistency if we fetch attributes before locking the node,
            // but we can detect it via revision attribute and refetch such paths.
            auto locksFuture = DoAcquireSnapshotLocks(pathsToFetch);
            auto attributesFuture = FetchTableAttributesAsync(pathsToFetch, NullTransactionId);

            WaitForFast(AllSucceeded(std::vector{locksFuture.AsVoid(), attributesFuture.AsVoid()}))
                .ThrowOnError();

            SaveQueryReadTransaction();

            auto locks = locksFuture.Get().Value();
            auto attributes = attributesFuture.Get().Value();

            std::vector<TYPath> pathsToRefetch;

            for (size_t index = 0; index < pathsToFetch.size(); ++index) {
                if (!locks[index].IsOK()) {
                    ObjectAttributesSnapshot_.emplace(pathsToFetch[index], TError(locks[index]));
                } else {
                    auto lock = locks[index].Value();
                    SnapshotLocks.emplace(pathsToFetch[index], lock);

                    if (!attributes[index].IsOK()) {
                        // Successfully locked, but got an error during fetching attributes.
                        // Probably the table has been deleted. Need to refetch under the transaction.
                        pathsToRefetch.push_back(pathsToFetch[index]);
                    } else {
                        auto id = attributes[index].Value()->Get<TObjectId>("id", NullObjectId);
                        auto revision = attributes[index].Value()->Get<TRevision>("revision", NullRevision);

                        if (id == lock.NodeId && revision == lock.Revision) {
                            ObjectAttributesSnapshot_.emplace(pathsToFetch[index], attributes[index]);
                        } else {
                            // Object has changed. Need to refetch under the transaction.
                            pathsToRefetch.push_back(pathsToFetch[index]);
                        }
                    }
                }
            }

            // Finally, if we detected node changes, we need to refetch them again under the transaction.
            if (!pathsToRefetch.empty()) {
                auto attributes = FetchTableAttributes(pathsToRefetch, ReadTransactionId);
                addToSnapshot(std::move(pathsToRefetch), std::move(attributes));
            }
        } else { // lockMode == ETableReadLockMode::None || lockMode == ETableReadLockMode::BestEffort
            auto attributes = Host->GetObjectAttributes(pathsToFetch, Client());

            if (lockMode == ETableReadLockMode::BestEffort) {
                std::vector<TYPath> pathsToLock;

                for (size_t index = 0; index < pathsToFetch.size(); ++index) {
                    if (attributes[index].IsOK() && attributes[index].Value()->Get<bool>("dynamic", false)) {
                        pathsToLock.push_back(pathsToFetch[index]);
                    }
                }

                // In best_effort mode we acquire locks for dynamic tables asynchronously and do not wait for them.
                YT_UNUSED_FUTURE(DoAcquireSnapshotLocks(pathsToLock));
                // But we do need to remember locks not to lock paths twice and to know whether to read
                // a table under the transaction or not on worker instances.
                // The exact lock value (node id/revision) matters only for Sync mode.
                for (const auto& path : pathsToLock) {
                    SnapshotLocks.emplace(path, TObjectLock{});
                }
            }

            addToSnapshot(std::move(pathsToFetch), std::move(attributes));
        }
    } else { // QueryKind == EQueryKind::SecondaryQuery || QueryKind == EQueryKind::NoQuery
        std::vector<TYPath> pathsToFetchFromCache;
        std::vector<TYPath> pathsToFetchUnderTx;
        pathsToFetchFromCache.reserve(pathsToFetch.size());
        pathsToFetchUnderTx.reserve(pathsToFetch.size());

        for (auto& path : pathsToFetch) {
            if (SnapshotLocks.contains(path)) {
                pathsToFetchUnderTx.push_back(std::move(path));
            } else {
                pathsToFetchFromCache.push_back(std::move(path));
            }
        }

        auto attributesUnderTxFuture = FetchTableAttributesAsync(pathsToFetchUnderTx, ReadTransactionId);
        auto attributesFromCache = Host->GetObjectAttributes(pathsToFetchFromCache, Client());
        auto attributesUnderTx = WaitForUniqueFast(attributesUnderTxFuture)
            .ValueOrThrow();

        addToSnapshot(std::move(pathsToFetchFromCache), std::move(attributesFromCache));
        addToSnapshot(std::move(pathsToFetchUnderTx), std::move(attributesUnderTx));
    }

    std::vector<TErrorOr<IAttributeDictionaryPtr>> result;
    result.reserve(paths.size());

    for (const auto& path : paths) {
        result.push_back(ObjectAttributesSnapshot_.at(path));

        const auto& attributesOrError = result.back();
        if (attributesOrError.IsOK()) {
            const auto& attributes = attributesOrError.Value();

            if (Settings->Testing->CheckCHYTBanned) {
                if (attributes->Get<bool>("chyt_banned", false)) {
                    THROW_ERROR_EXCEPTION("Table %Qv is banned via \"chyt_banned\" attribute", path);
                }
            }
        }
    }

    return result;
}

void TQueryContext::DeleteObjectAttributesFromSnapshot(
    const std::vector<TYPath>& paths)
{
    for (const auto& path : paths) {
        ObjectAttributesSnapshot_.erase(path);
    }
}

void TQueryContext::InitializeQueryWriteTransaction()
{
    if (InitialQueryWriteTransaction_) {
        return;
    }
    if (QueryKind != EQueryKind::InitialQuery || WriteTransactionId) {
        THROW_ERROR_EXCEPTION(
            "Unexpected transaction initialization; "
            "this is a bug; please, file an issue in CHYT queue")
            << TErrorAttribute("transaction_id", WriteTransactionId)
            << TErrorAttribute("query_kind", QueryKind);
    }
    InitialQueryWriteTransaction_ = WaitFor(Client()->StartNativeTransaction(ETransactionType::Master))
        .ValueOrThrow();
    WriteTransactionId = InitialQueryWriteTransaction_->GetId();

    YT_LOG_INFO("Write transaction started (WriteTransactionId: %v)", WriteTransactionId);
}

void TQueryContext::CommitWriteTransaction()
{
    if (InitialQueryWriteTransaction_) {
        WaitFor(InitialQueryWriteTransaction_->Commit())
            .ThrowOnError();
    }
}

void TQueryContext::InitializeQueryReadTransactionFuture()
{
    if (ReadTransactionFuture_) {
        return;
    }

    YT_VERIFY(QueryKind == EQueryKind::InitialQuery);

    auto transactionFuture = Client()->StartNativeTransaction(ETransactionType::Master);
    auto timestampFuture = Client()->GetTimestampProvider()->GenerateTimestamps();

    ReadTransactionFuture_ = AllSucceeded(std::vector{transactionFuture.AsVoid(), timestampFuture.AsVoid()})
        .Apply(BIND([transactionFuture, timestampFuture] {
            return TTransactionWithTimestamp{transactionFuture.Get().Value(), timestampFuture.Get().Value()};
        }));

    YT_LOG_INFO("Query read transaction future initialized");
}

void TQueryContext::SaveQueryReadTransaction()
{
    if (!ReadTransactionFuture_) {
        return;
    }

    if (InitialQueryReadTransaction_ || ReadTransactionId || DynamicTableReadTimestamp != AsyncLastCommittedTimestamp) {
        // Already saved.
        YT_VERIFY(InitialQueryReadTransaction_ && ReadTransactionId && DynamicTableReadTimestamp != AsyncLastCommittedTimestamp);
        return;
    }

    auto tx = WaitFor(ReadTransactionFuture_)
        .ValueOrThrow();

    InitialQueryReadTransaction_ = tx.Transaction;
    DynamicTableReadTimestamp = tx.Timestamp;
    ReadTransactionId = InitialQueryReadTransaction_->GetId();

    YT_LOG_INFO("Read transaction saved (ReadTransactionId: %v, DynamicTableReadTimestamp: %v)",
        ReadTransactionId,
        DynamicTableReadTimestamp);
}

TFuture<std::vector<TErrorOr<TObjectLock>>> TQueryContext::DoAcquireSnapshotLocks(const std::vector<TYPath>& paths)
{
    std::vector<TErrorOr<TObjectLock>> result(paths.size());
    std::vector<bool> notSet(paths.size());

    std::vector<TYPath> pathsToLock;
    pathsToLock.reserve(paths.size());

    for (size_t index = 0; index < paths.size(); ++index) {
        if (auto it = SnapshotLocks.find(paths[index]); it != SnapshotLocks.end()) {
            result[index] = it->second;
        } else {
            pathsToLock.push_back(paths[index]);
            notSet[index] = true;
        }
    }

    if (pathsToLock.empty()) {
        return MakeFuture(std::move(result));
    }

    InitializeQueryReadTransactionFuture();

    return ReadTransactionFuture_
        .Apply(BIND([pathsToLock = std::move(pathsToLock), client = Client(), logger = Logger]
            (const TTransactionWithTimestamp& tx)
        {
            return DoAcquireSnapshotLocksAsync(pathsToLock, client, tx.Transaction->GetId(), logger);
        }))
        .ApplyUnique(BIND([notSet = std::move(notSet), result = std::move(result)]
            (std::vector<TErrorOr<TObjectLock>>&& newLocks) mutable
        {
            size_t curLock = 0;
            for (size_t index = 0; index < result.size(); ++index) {
                if (notSet[index]) {
                    result[index] = std::move(newLocks[curLock]);
                    ++curLock;
                }
            }
            YT_VERIFY(curLock == newLocks.size());
            return result;
        }));
}

TYPath TQueryContext::GetNodeIdOrPath(const TYPath& path) const
{
    // NB: Reading by node id makes sense only for Sync mode.
    if (Settings->Execution->TableReadLockMode != ETableReadLockMode::Sync) {
        return path;
    }

    auto lockIt = SnapshotLocks.find(path);
    return (lockIt != SnapshotLocks.end()) ? Format("#%v", lockIt->second.NodeId) : path;
}

void TQueryContext::AcquireSnapshotLocks(const std::vector<TYPath>& paths)
{
    auto locks = WaitForUniqueFast(DoAcquireSnapshotLocks(paths))
        .ValueOrThrow();

    SaveQueryReadTransaction();

    for (size_t index = 0; index < paths.size(); ++index) {
        SnapshotLocks.emplace(paths[index], locks[index].ValueOrThrow());
    }
}

// TODO(gudqeit): use TBatchAttributeFetcher.
TFuture<std::vector<TErrorOr<IAttributeDictionaryPtr>>> TQueryContext::FetchTableAttributesAsync(
    const std::vector<TYPath>& paths,
    TTransactionId transactionId)
{
    if (paths.empty()) {
        return MakeFuture(std::vector<TErrorOr<IAttributeDictionaryPtr>>{});
    }

    if (auto sleepDuration = Settings->Testing->FetchTableAttributesSleepDuration) {
        TDelayedExecutor::WaitForDuration(sleepDuration);
    }

    auto client = Client();
    auto connection = client->GetNativeConnection();
    TMasterReadOptions masterReadOptions = *Settings->CypressReadOptions;

    auto proxy = CreateObjectServiceReadProxy(client, masterReadOptions.ReadFrom);
    auto batchReq = proxy.ExecuteBatch();
    SetBalancingHeader(batchReq, connection, masterReadOptions);

    for (int index = 0; index < std::ssize(paths); ++index) {
        const auto& path = paths[index];
        auto nodeIdOrPath = GetNodeIdOrPath(path);
        auto req = TYPathProxy::Get(Format("%v/@", nodeIdOrPath));
        req->Tag() = index;
        ToProto(req->mutable_attributes()->mutable_keys(), TableAttributesToFetch);
        SetTransactionId(req, transactionId);
        SetCachingHeader(req, connection, masterReadOptions);

        batchReq->AddRequest(req);
    }

    return batchReq->Invoke()
        .Apply(BIND([pathCount = paths.size()] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
            std::vector<TErrorOr<IAttributeDictionaryPtr>> attributes(pathCount);
            for (const auto& [tag, rspOrError] : batchRsp->GetTaggedResponses<TYPathProxy::TRspGet>()) {
                auto index = std::any_cast<int>(tag);
                if (rspOrError.IsOK()) {
                    attributes[index] = ConvertToAttributes(TYsonString(rspOrError.Value()->value()));
                } else {
                    attributes[index] = TError(rspOrError);
                }
            }
            return attributes;
        }));
}

std::vector<TErrorOr<IAttributeDictionaryPtr>> TQueryContext::FetchTableAttributes(
    const std::vector<TYPath>& paths,
    TTransactionId transactionId)
{
    return WaitForUniqueFast(FetchTableAttributesAsync(paths, transactionId))
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TQueryContext& queryContext, IYsonConsumer* consumer, const DB::QueryStatusInfo* queryStatusInfo)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("user").Value(queryContext.User)
            .Item("query_kind").Value(queryContext.QueryKind)
            .Item("query_id").Value(queryContext.QueryId)
            .Item("query_phase").Value(queryContext.GetQueryPhase())
            .Item("interface").Value(ToString(queryContext.Interface))
            .DoIf(queryContext.Interface == EInterface::HTTP, [&] (TFluentMap fluent) {
                fluent
                    .Item("http_user_agent").Value(queryContext.HttpUserAgent);
            })
            .Item("current_address").Value(queryContext.CurrentAddress)
            .DoIf(queryContext.QueryKind == EQueryKind::SecondaryQuery, [&] (TFluentMap fluent) {
                fluent
                    .Item("initial_query_id").Value(queryContext.InitialQueryId)
                    .Item("parent_query_id").Value(queryContext.ParentQueryId)
                    .Item("initial_address").Value(queryContext.InitialAddress)
                    .Item("initial_user").Value(queryContext.InitialUser)
                    .Item("initial_query").Value(queryContext.InitialQuery);
            })
            .Item("query_status").Value(queryStatusInfo)
            .OptionalItem("datalens_request_id", queryContext.DataLensRequestId)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

struct THostContext
    : public DB::IHostContext
{
    THost* Host;
    TQueryContextPtr QueryContext;

    THostContext(THost* host, TQueryContextPtr queryContext)
        : Host(host)
        , QueryContext(std::move(queryContext))
    { }

    // Destruction of query context should be done in control invoker as
    // it non-trivially modifies query registry which may be accessed only
    // from control invoker.
    virtual ~THostContext() override
    {
        QueryContext->Finish();
        Host->GetControlInvoker()->Invoke(
            BIND([host = Host, queryContext = std::move(QueryContext)] () mutable {
                host->GetQueryRegistry()->Unregister(queryContext);
                queryContext.Reset();
            }));
    }
};

void SetupHostContext(THost* host,
    DB::ContextMutablePtr context,
    TQueryId queryId,
    TTraceContextPtr traceContext,
    std::optional<TString> dataLensRequestId,
    std::optional<TString> yqlOperationId,
    const TSecondaryQueryHeaderPtr& secondaryQueryHeader)
{
    YT_VERIFY(traceContext);

    auto queryContext = New<TQueryContext>(
        host,
        context,
        queryId,
        std::move(traceContext),
        std::move(dataLensRequestId),
        std::move(yqlOperationId),
        secondaryQueryHeader);

    WaitFor(BIND(
        &TQueryRegistry::Register,
        host->GetQueryRegistry(),
        queryContext)
        .AsyncVia(host->GetControlInvoker())
        .Run())
        .ThrowOnError();

    context->getHostContext() = std::make_shared<THostContext>(host, std::move(queryContext));
}

TQueryContext* GetQueryContext(DB::ContextPtr context)
{
    auto* hostContext = context->getHostContext().get();
    YT_ASSERT(dynamic_cast<THostContext*>(hostContext) != nullptr);
    auto* queryContext = static_cast<THostContext*>(hostContext)->QueryContext.Get();

    return queryContext;
}

void InvalidateCache(
    TQueryContext* queryContext,
    std::vector<TYPath> paths,
    std::optional<EInvalidateCacheMode> invalidateMode)
{
    if (!invalidateMode) {
        invalidateMode = queryContext->Settings->Caching->TableAttributesInvalidateMode;
    }
    auto timeout = queryContext->Settings->Caching->InvalidateRequestTimeout;
    queryContext->Host->InvalidateCachedObjectAttributesGlobally(paths, *invalidateMode, timeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
