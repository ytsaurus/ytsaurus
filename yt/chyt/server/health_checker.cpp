#include "health_checker.h"

#include "config.h"
#include "query_context.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/intrusive_ptr.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <Interpreters/ClientInfo.h>
#include <Interpreters/executeQuery.h>

#include <Core/Types.h>


namespace NYT::NClickHouseServer {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

DB::ContextMutablePtr PrepareContextForQuery(
    DB::ContextPtr databaseContext,
    const TString& dataBaseUser,
    TDuration timeout,
    THost* host)
{
    auto contextForQuery = DB::Context::createCopy(databaseContext);

    contextForQuery->setUser(dataBaseUser,
        /*password =*/"",
        Poco::Net::SocketAddress());

    auto settings = contextForQuery->getSettings();
    settings.max_execution_time = Poco::Timespan(timeout.Seconds(), timeout.MicroSecondsOfSecond());
    contextForQuery->setSettings(settings);

    auto queryId = TQueryId::Create();

    auto& clientInfo = contextForQuery->getClientInfo();
    clientInfo.initial_user = clientInfo.current_user;
    clientInfo.query_kind = DB::ClientInfo::QueryKind::INITIAL_QUERY;
    clientInfo.initial_query_id = ToString(queryId);

    contextForQuery->makeQueryContext();

    auto traceContext = NTracing::TTraceContext::NewRoot("HealthCheckerQuery");

    SetupHostContext(host, contextForQuery, queryId, std::move(traceContext));

    return contextForQuery;
}

void ValidateQueryResult(DB::BlockIO& blockIO)
{
    auto stream = blockIO.getInputStream();
    size_t totalRowCount = 0;
    while (auto block = stream->read()) {
        totalRowCount += block.rows();
    }
    YT_LOG_DEBUG("Health checker query result validated (TotalRowCount: %v)", totalRowCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail


void THealthChecker::ExecuteQuery(const TString& query)
{
    auto context = NDetail::PrepareContextForQuery(getContext(), DatabaseUser_, Config_->Timeout, Host_);
    auto blockIO = DB::executeQuery(query, context, true /* internal */);
    NDetail::ValidateQueryResult(blockIO);
}

THealthChecker::THealthChecker(
    THealthCheckerConfigPtr config,
    TString dataBaseUser,
    DB::ContextMutablePtr databaseContext,
    THost* host)
    : DB::WithMutableContext(databaseContext)
    , Config_(std::move(config))
    , DatabaseUser_(std::move(dataBaseUser))
    , Host_(host)
    , ActionQueue_(New<TActionQueue>("HealthChecker"))
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        ActionQueue_->GetInvoker(),
        BIND(&THealthChecker::ExecuteQueries, MakeWeak(this)),
        Config_->Period))
{
    RegisterNewUser(getContext()->getAccessControlManager(), DatabaseUser_);

    for (int i = 0; i < std::ssize(Config_->Queries); ++i) {
        QueryIndexToStatus_.push_back(ClickHouseYtProfiler
            .WithTag("query_index", ToString(i))
            .Gauge("/health_checker/success"));
    }
}

void THealthChecker::Start()
{
    YT_LOG_DEBUG("Health checker started (Period: %v, QueryCount: %v)",
        Config_->Period,
        Config_->Queries.size());
    PeriodicExecutor_->Start();
}

void THealthChecker::ExecuteQueries()
{
    for (size_t queryIndex = 0; queryIndex < Config_->Queries.size(); ++queryIndex) {
        const auto& query = Config_->Queries[queryIndex];
        YT_LOG_DEBUG("Executing health checker query (Index: %v, Query: %v)", queryIndex, query);

        auto error = WaitFor(BIND(&THealthChecker::ExecuteQuery, MakeWeak(this), query)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run()
            .WithTimeout(Config_->Timeout));

        if (error.IsOK()) {
            YT_LOG_DEBUG("Health checker query successfully executed (Index: %v, Query: %v)",
                queryIndex,
                query);
        } else {
            YT_LOG_WARNING(error,
                "Health checker query failed (Index: %v, Query: %v)",
                queryIndex,
                query);
        }

        QueryIndexToStatus_[queryIndex].Update(error.IsOK() ? 1.0 : 0.0);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
