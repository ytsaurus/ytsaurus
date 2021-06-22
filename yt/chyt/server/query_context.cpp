#include "query_context.h"

#include "helpers.h"
#include "host.h"
#include "query_registry.h"
#include "statistics_reporter.h"
#include "secondary_query_header.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/profiling/profile_manager.h>

#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTracing;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TLogger QueryLogger("Query");

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
    const TSecondaryQueryHeaderPtr& secondaryQueryHeader)
    : Logger(QueryLogger)
    , User(TString(context->getClientInfo().initial_user))
    , TraceContext(std::move(traceContext))
    , QueryId(queryId)
    , QueryKind(static_cast<EQueryKind>(context->getClientInfo().query_kind))
    , Host(host)
    , DataLensRequestId(std::move(dataLensRequestId))
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

TQueryContext::~TQueryContext()
{
    VERIFY_THREAD_AFFINITY_ANY();

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
    AggregatedStatistics.Update(InstanceStatistics);
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
    const TSecondaryQueryHeaderPtr& secondaryQueryHeader)
{
    YT_VERIFY(traceContext);

    auto queryContext = New<TQueryContext>(
        host,
        context,
        queryId,
        std::move(traceContext),
        std::move(dataLensRequestId),
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
