#include "health_checker.h"

#include "config.h"
#include "helpers.h"
#include "host.h"
#include "private.h"
#include "query_context.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <Core/Types.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Session.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

DB::ContextMutablePtr PrepareContextForQuery(
    std::shared_ptr<DB::Session> session,
    const TString& dataBaseUser,
    TDuration timeout,
    THost* host)
{
    session->authenticate(dataBaseUser,
        /*password*/ "",
        Poco::Net::SocketAddress());

    auto contextForQuery = session->makeQueryContext();

    auto settings = contextForQuery->getSettings();
    settings.max_execution_time = Poco::Timespan(timeout.Seconds(), timeout.MicroSecondsOfSecond());
    contextForQuery->setSettings(settings);

    auto queryId = TQueryId::Create();

    contextForQuery->setInitialUserName(contextForQuery->getClientInfo().current_user);
    contextForQuery->setQueryKind(DB::ClientInfo::QueryKind::INITIAL_QUERY);
    contextForQuery->setInitialQueryId(ToString(queryId));

    auto traceContext = NTracing::TTraceContext::NewRoot("HealthCheckerQuery");

    SetupHostContext(host, contextForQuery, queryId, std::move(traceContext));

    return contextForQuery;
}

void ValidateQueryResult(DB::BlockIO& blockIO)
{
    size_t totalRowCount = 0;

    DB::PullingPipelineExecutor executor(blockIO.pipeline);
    DB::Block block;
    while (executor.pull(block)) {
        totalRowCount += block.rows();
    }

    YT_LOG_DEBUG("Health checker query result validated (TotalRowCount: %v)", totalRowCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail


void THealthChecker::ExecuteQuery(const TString& query)
{
    auto session = std::make_shared<DB::Session>(getContext(), DB::ClientInfo::Interface::TCP);
    auto context = NDetail::PrepareContextForQuery(session, DatabaseUser_, Config_->Timeout, Host_);
    auto blockIO = DB::executeQuery(query, context, true /*internal*/);
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
    RegisterNewUser(
        getContext()->getAccessControl(),
        DatabaseUser_,
        Host_->HasUserDefinedSqlObjectStorage());

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
