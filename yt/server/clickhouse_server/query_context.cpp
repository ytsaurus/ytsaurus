#include "query_context.h"

#include "host.h"
#include "helpers.h"
#include "config.h"
#include "query_registry.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/client_cache.h>

#include <yt/client/table_client/row_buffer.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/concurrency/scheduler.h>

#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace DB;
using namespace NTracing;


////////////////////////////////////////////////////////////////////////////////

TQueryContext::TQueryContext(
    TBootstrap* bootstrap,
    const DB::Context& context,
    TQueryId queryId,
    TTraceContextPtr traceContext,
    std::optional<TString> dataLensRequestId)
    : Logger(ServerLogger)
    , User(TString(context.getClientInfo().initial_user))
    , TraceContext(std::move(traceContext))
    , QueryId(queryId)
    , QueryKind(static_cast<EQueryKind>(context.getClientInfo().query_kind))
    , Bootstrap(bootstrap)
    , DataLensRequestId(std::move(dataLensRequestId))
    , RowBuffer(New<NTableClient::TRowBuffer>())
    , Host_(Bootstrap->GetHost())
    , TraceContextGuard_(TraceContext)
{
    Logger.AddTag("QueryId: %v", QueryId);
    if (DataLensRequestId) {
        Logger.AddTag("DataLensRequestId: %v", DataLensRequestId);
    }
    YT_LOG_INFO("Query context created (User: %v, QueryKind: %v)", User, QueryKind);
    LastPhaseTime_ = StartTime_ = TInstant::Now();

    const auto& clientInfo = context.getClientInfo();

    CurrentUser = clientInfo.current_user;
    CurrentAddress = clientInfo.current_address.toString();
    if (QueryKind == EQueryKind::SecondaryQuery) {
        InitialUser = clientInfo.initial_user;
        InitialAddress = clientInfo.initial_address.toString();
        InitialQueryId.emplace();
        if (!TQueryId::FromString(clientInfo.initial_query_id, &*InitialQueryId)) {
            YT_LOG_WARNING("Initial query id is not a valid YT query id (InitialQueryId: %v)", clientInfo.initial_query_id);
            InitialQueryId.reset();
        }
    }
    ClientHostName = clientInfo.client_hostname;
    Interface = static_cast<EInterface>(clientInfo.interface);
    if (Interface == EInterface::HTTP) {
        HttpUserAgent = clientInfo.http_user_agent;
    }

    YT_LOG_INFO(
        "Query client info (CurrentUser: %v, CurrentAddress: %v, InitialUser: %v, InitialAddress: %v, "
        "InitialQueryId: %v, Interface: %v, ClientHostname: %v, HttpUserAgent: %v)",
        CurrentUser,
        CurrentAddress,
        InitialUser,
        InitialAddress,
        InitialQueryId,
        Interface,
        ClientHostName,
        HttpUserAgent);

    WaitFor(BIND(
        &TQueryRegistry::Register,
        Bootstrap->GetQueryRegistry(),
        this)
        .AsyncVia(Bootstrap->GetControlInvoker())
        .Run())
        .ThrowOnError();
}

TQueryContext::~TQueryContext()
{
    MoveToPhase(EQueryPhase::Finish);

    if (TraceContext) {
        TraceContext->Finish();
    }

    auto error = WaitFor(BIND(
        &TQueryRegistry::Unregister,
        Bootstrap->GetQueryRegistry(),
        this)
        .AsyncVia(Bootstrap->GetControlInvoker())
        .Run());

    auto finishTime = TInstant::Now();
    auto duration = finishTime - StartTime_;
    YT_LOG_INFO("Query time statistics (StartTime: %v, FinishTime: %v, Duration: %v)", StartTime_, finishTime, duration);
    YT_LOG_INFO("Query phase debug string (DebugString: %v)", PhaseDebugString_);

    // Trying so hard to not throw exception from the destructor :(
    if (error.IsOK()) {
        YT_LOG_INFO("Query context destroyed");
    } else {
        YT_LOG_ERROR(error, "Error while destroying query context");
    }
}

const NApi::NNative::IClientPtr& TQueryContext::Client() const
{
    ClientLock_.AcquireReader();
    auto clientPresent = static_cast<bool>(Client_);
    ClientLock_.ReleaseReader();

    if (!clientPresent) {
        ClientLock_.AcquireWriter();
        Client_ = Bootstrap->GetClientCache()->GetClient(User);
        ClientLock_.ReleaseWriter();
    }

    return Client_;
}

void TQueryContext::MoveToPhase(EQueryPhase nextPhase)
{
    // Weak check. CurrentPhase_ changes in monotonic manner, so it
    // may result in false-positive, but not false-negative.
    if (nextPhase <= CurrentPhase_.load()) {
        return;
    }

    TGuard<TSpinLock> readerGuard(PhaseLock_);

    if (nextPhase <= CurrentPhase_.load()) {
        return;
    }

    auto currentTime = TInstant::Now();
    auto duration = currentTime - LastPhaseTime_;
    PhaseDebugString_ += Format(" - %v - %v", duration, nextPhase);
    YT_LOG_INFO("Query phase changed (FromPhase: %v, ToPhase: %v, Duration: %v)", CurrentPhase_.load(), nextPhase, duration);
    CurrentPhase_ = nextPhase;
}

void Serialize(const TQueryContext& queryContext, IYsonConsumer* consumer, const DB::QueryStatusInfo* queryStatus)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("user").Value(queryContext.User)
            .Item("query_kind").Value(queryContext.QueryKind)
            .Item("query_id").Value(queryContext.QueryId)
            .Item("interface").Value(ToString(queryContext.Interface))
            .DoIf(queryContext.Interface == EInterface::HTTP, [&] (TFluentMap fluent) {
                fluent
                    .Item("http_user_agent").Value(queryContext.HttpUserAgent);
            })
            .Item("current_address").Value(queryContext.CurrentAddress)
            .Item("client_hostname").Value(queryContext.ClientHostName)
            .DoIf(queryContext.QueryKind == EQueryKind::SecondaryQuery, [&] (TFluentMap fluent) {
                fluent
                    .Item("initial_query_id").Value(queryContext.InitialQueryId)
                    .Item("initial_address").Value(queryContext.InitialAddress)
                    .Item("initial_user").Value(queryContext.InitialUser)
                    .Item("initial_query").Value(queryContext.InitialQuery);
            })
            .Item("query_status").Value(queryStatus)
            .OptionalItem("datalens_request_id", queryContext.DataLensRequestId)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void SetupHostContext(TBootstrap* bootstrap, DB::Context& context, TQueryId queryId, TTraceContextPtr traceContext, std::optional<TString> dataLensRequestId)
{
    YT_VERIFY(traceContext);

    context.getHostContext() = std::make_shared<TQueryContext>(
        bootstrap,
        context,
        queryId,
        std::move(traceContext),
        std::move(dataLensRequestId));
}

TQueryContext* GetQueryContext(const DB::Context& context)
{
    auto* hostContext = context.getHostContext().get();
    YT_ASSERT(dynamic_cast<TQueryContext*>(hostContext) != nullptr);
    auto* queryContext = static_cast<TQueryContext*>(hostContext);

    return queryContext;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
