#include "query_context.h"

#include "helpers.h"
#include "host.h"
#include "query_registry.h"
#include "statistics_reporter.h"
#include "secondary_query_header.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
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

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTracing;
using namespace NLogging;
using namespace NTransactionClient;
using namespace NApi;
using namespace NCypressClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TLogger QueryLogger("Query");

////////////////////////////////////////////////////////////////////////////////

namespace {

THashMap<TYPath, TNodeId> DoAcquireSnapshotLocks(
    const THashSet<TYPath>& paths,
    NNative::IClientPtr client,
    TTransactionId readTransactionId,
    const TLogger& logger)
{
    const auto& Logger = logger;

    auto proxy = CreateObjectServiceWriteProxy(client);
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& path : paths) {
        auto req = TCypressYPathProxy::Lock(path);
        req->Tag() = path;
        req->set_mode(static_cast<int>(ELockMode::Snapshot));
        SetTransactionId(req, readTransactionId);
        batchReq->AddRequest(req);
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    THashMap<TYPath, TNodeId> pathToNodeId;
    for (const auto& [tag, rspOrError] : batchRsp->GetTaggedResponses<TCypressYPathProxy::TRspLock>()) {
        auto rsp = rspOrError.ValueOrThrow();
        auto path = std::any_cast<TYPath>(tag);
        pathToNodeId[path] = FromProto<NCypressClient::TNodeId>(rsp->node_id());
        YT_LOG_INFO("Snapshot lock is acquired (LockId: %v, NodeId: %v, ReadTransactionId: %v)",
            rsp->lock_id(),
            rsp->node_id(),
            readTransactionId);
    }

    return pathToNodeId;
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
        PathToNodeId = secondaryQueryHeader->PathToNodeId;
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
TQueryContext::TQueryContext(THost* host, NApi::NNative::IClientPtr client)
    : QueryKind(EQueryKind::NoQuery)
    , Host(host)
    , Settings(New<TQuerySettings>())
    , Client_(std::move(client))
{ }

TQueryContextPtr TQueryContext::CreateFake(THost* host, NApi::NNative::IClientPtr client)
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

const NApi::NNative::IClientPtr& TQueryContext::Client() const
{
    bool clientPresent;
    {
        auto readerGuard = ReaderGuard(ClientLock_);
        clientPresent = static_cast<bool>(Client_);
    }

    if (!clientPresent) {
        auto writerGuard = WriterGuard(ClientLock_);
        Client_ = Host->CreateClient(User);
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

    QueryPhase_ = nextPhase;
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
    if (!ClusterNodesSnapshot) {
        ClusterNodesSnapshot = Host->GetNodes(/*alwaysIncludeLocal*/ true);
    }
    return *ClusterNodesSnapshot;
}

std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>> TQueryContext::GetObjectAttributesSnapshot(
    const std::vector<NYPath::TYPath>& paths)
{
    if (Settings->Execution->TableReadLockMode != ETableReadLockMode::None) {
        InitializeQueryReadTransactionFuture();
    }

    THashSet<NYPath::TYPath> missingPathSet;
    bool lockingTables = (Settings->Execution->TableReadLockMode != ETableReadLockMode::None);
    THashSet<NYPath::TYPath> tablesToLock;

    for (const auto& path : paths) {
        if (!ObjectAttributesSnapshot_.contains(path)) {
            missingPathSet.insert(path);

            // Lock will be taken if read transaction exists, no node id was saved
            // and the node is not created during the query.
            if (lockingTables && path != CreatedTablePath && !PathToNodeId.contains(path)) {
                tablesToLock.insert(path);
            }
        }
    }

    std::vector<NYPath::TYPath> missingPaths = {missingPathSet.begin(), missingPathSet.end()};

    if (QueryKind == EQueryKind::InitialQuery &&
        Settings->Execution->TableReadLockMode != ETableReadLockMode::None &&
        !tablesToLock.empty())
    {
        auto future = AcquireSnapshotLocks(tablesToLock);
        if (Settings->Execution->TableReadLockMode == ETableReadLockMode::Sync) {
            // Here we save read transaction id, which will be used in attribute fetch.
            EnsureQueryReadTransactionCreated();

            auto acquiredPathToNodeId = WaitFor(future)
                .ValueOrThrow();
            PathToNodeId.insert(acquiredPathToNodeId.begin(), acquiredPathToNodeId.end());
        }
    }

    auto missingAttributes = GetTableAttributes(missingPaths);
    YT_VERIFY(missingAttributes.size() == missingPaths.size());

    for (int index = 0; index < std::ssize(missingPaths); ++index) {
        const auto& path = missingPaths[index];
        const auto& attributes = missingAttributes[index];
        auto [_, ok] = ObjectAttributesSnapshot_.emplace(path, attributes);
        YT_VERIFY(ok);

        if (Settings->Execution->TableReadLockMode == ETableReadLockMode::BestEffort &&
            attributes.IsOK() &&
            attributes.Value()->Get<bool>("dynamic", false) &&
            !PathToNodeId.contains(path))
        {
            PathToNodeId[path] = attributes.Value()->Get<TObjectId>("id");
        }
    }

    std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>> result;
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

            if (attributes->Get<bool>("dynamic", false)) {
                HasDynamicTable = true;
            }
        }
    }

    if (Settings->Execution->TableReadLockMode != ETableReadLockMode::None && HasDynamicTable) {
        InitializeDynamicTableReadTimestamp();
    }

    return result;
}

void TQueryContext::DeleteObjectAttributesFromSnapshot(
    const std::vector<NYPath::TYPath>& paths)
{
    for (const auto& path : paths) {
        ObjectAttributesSnapshot_.erase(path);
        PathToNodeId.erase(path);
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
    if (ReadTransactionFuture_ || ReadTransactionId) {
        return;
    }

    ReadTransactionFuture_ = Client()->StartNativeTransaction(ETransactionType::Master);
}

void TQueryContext::EnsureQueryReadTransactionCreated()
{
    if (!ReadTransactionFuture_ || ReadTransactionId) {
        return;
    }

    InitialQueryReadTransaction_ = WaitForFast(ReadTransactionFuture_)
        .ValueOrThrow();
    ReadTransactionId = InitialQueryReadTransaction_->GetId();

    YT_LOG_INFO("Read transaction is saved (ReadTransactionId: %v)", ReadTransactionId);
}

void TQueryContext::InitializeDynamicTableReadTimestamp()
{
    if (DynamicTableReadTimestamp != AsyncLastCommittedTimestamp) {
        return;
    }
    DynamicTableReadTimestamp = WaitFor(Client()->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();
    YT_LOG_INFO("Timestamp for dynamic tables reading is initialized (Timestamp: %v)", DynamicTableReadTimestamp);
}

TFuture<THashMap<TYPath, TNodeId>> TQueryContext::AcquireSnapshotLocks(const THashSet<TYPath>& paths)
{
    if (!ReadTransactionFuture_) {
        THROW_ERROR_EXCEPTION(
            "Read transaction future wasn't initialized before lock acquisition; "
            "this is a bug; please, file an issue in CHYT queue")
            << TErrorAttribute("query_kind", QueryKind);
    }
    return ReadTransactionFuture_
        .Apply(BIND([paths, client = Client(), logger = Logger] (const NNative::ITransactionPtr& readTransaction) {
            return DoAcquireSnapshotLocks(paths, client, readTransaction->GetId(), logger);
        }));
}

std::vector<TErrorOr<IAttributeDictionaryPtr>> TQueryContext::GetTableAttributes(const std::vector<TYPath>& missingPaths)
{
    auto transactionId = ReadTransactionId;
    // If we are fetching attributes for a table created in this query
    // write transaction should be used.
    if (WriteTransactionId
        && missingPaths.size() == 1
        && missingPaths.front() == CreatedTablePath)
    {
        transactionId = WriteTransactionId;
    }

    if (transactionId) {
        return FetchTableAttributes(missingPaths, transactionId);
    } else {
        return Host->GetObjectAttributes(missingPaths, Client());
    }
}

// TODO(gudqeit): use TBatchAttributeFetcher.
std::vector<TErrorOr<IAttributeDictionaryPtr>> TQueryContext::FetchTableAttributes(
    const std::vector<TYPath>& paths,
    TTransactionId transactionId)
{
    if (paths.empty()) {
        return {};
    }
    auto proxy = CreateObjectServiceWriteProxy(Client());
    auto batchReq = proxy.ExecuteBatch();

    for (int index = 0; index < std::ssize(paths); ++index) {
        const auto& path = paths[index];
        auto maybeNodeId = PathToNodeId.find(path);
        auto nodeIdOrPath = (maybeNodeId != PathToNodeId.end()) ? Format("#%v", maybeNodeId->second) : path;
        auto req = TYPathProxy::Get(Format("%v/@", nodeIdOrPath));
        req->Tag() = index;
        ToProto(req->mutable_attributes()->mutable_keys(), TableAttributesToFetch);
        SetTransactionId(req, transactionId);
        batchReq->AddRequest(req);
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    std::vector<TErrorOr<IAttributeDictionaryPtr>> attributes(paths.size());
    for (const auto& [tag, rspOrError] : batchRsp->GetTaggedResponses<TYPathProxy::TRspGet>()) {
        auto index = std::any_cast<int>(tag);
        if (rspOrError.IsOK()) {
            attributes[index] = ConvertToAttributes(TYsonString(rspOrError.Value()->value()));
        } else {
            attributes[index] = TError(rspOrError);
        }
    }

    return attributes;
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
    std::vector<NYPath::TYPath> paths,
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
