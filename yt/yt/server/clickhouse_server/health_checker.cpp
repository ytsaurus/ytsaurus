#include "health_checker.h"

#include "config.h"
#include "query_context.h"

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/intrusive_ptr.h>

#include <yt/core/profiling/profile_manager.h>

#include <contrib/libs/clickhouse/dbms/src/Parsers/ParserQuery.h>
#include <contrib/libs/clickhouse/dbms/src/Parsers/parseQuery.h>

#include <contrib/libs/clickhouse/dbms/src/Interpreters/ClientInfo.h>
#include <contrib/libs/clickhouse/dbms/src/Interpreters/InterpreterSelectWithUnionQuery.h>

#include <contrib/libs/clickhouse/dbms/src/Core/Types.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

std::vector<NProfiling::TTagId> RegisterQueryTags(size_t queryCount)
{
    std::vector<NProfiling::TTagId> queryTags;
    for (size_t queryIndex = 0; queryIndex < queryCount; ++queryIndex) {
        queryTags.emplace_back(NProfiling::TProfileManager::Get()->RegisterTag("query_index", queryIndex));
    }
    return queryTags;
}

DB::Context PrepareContextForQuery(
    const DB::Context* databaseContext,
    const TString& dataBaseUser,
    TDuration timeout,
    THost* host)
{
    DB::Context contextForQuery = *databaseContext;

    contextForQuery.setUser(dataBaseUser,
        /*password =*/"",
        Poco::Net::SocketAddress(),
        /*quotaKey =*/"");

    contextForQuery.getSettingsRef().max_execution_time.set(
        Poco::Timespan(timeout.Seconds(), timeout.MicroSecondsOfSecond()));

    auto queryId = TQueryId::Create();

    auto& clientInfo = contextForQuery.getClientInfo();
    clientInfo.initial_user = clientInfo.current_user;
    clientInfo.query_kind = DB::ClientInfo::QueryKind::INITIAL_QUERY;
    clientInfo.initial_query_id = ToString(queryId);

    contextForQuery.makeQueryContext();

    NTracing::TSpanContext spanContext{NTracing::TTraceId::Create(),
        NTracing::InvalidSpanId,
        /*sampled =*/false,
        /*debug =*/false};

    auto traceContext =
        New<NTracing::TTraceContext>(spanContext, /*spanName =*/"HealthCheckerQuery");

    SetupHostContext(host, contextForQuery, queryId, std::move(traceContext));

    return contextForQuery;
}

void ValidateQueryResult(DB::BlockIO blockIO)
{
    size_t totalRowCount = 0;
    while (auto block = blockIO.in->read()) {
        totalRowCount += block.rows();
    }
    YT_LOG_DEBUG("Health checker query result validated (TotalRowCount: %v)", totalRowCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail


void THealthChecker::ExecuteQuery(const TString& query)
{
    DB::ParserQuery queryParser(query.end(), /*enableExplain =*/false);

    auto querySyntaxTree = parseQuery(
        queryParser,
        query.begin(),
        query.end(),
        /*description =*/"HealthCheckerQuery",
        /*maxQuerySize =*/0);

    NDetail::ValidateQueryResult(DB::InterpreterSelectWithUnionQuery(
        querySyntaxTree,
        NDetail::PrepareContextForQuery(DatabaseContext_, DatabaseUser_, Config_->Timeout, Host_),
        DB::SelectQueryOptions())
        .execute());
}

THealthChecker::THealthChecker(
    THealthCheckerConfigPtr config,
    TString dataBaseUser,
    const DB::Context* databaseContext,
    THost* host)
    : Config_(std::move(config))
    , DatabaseUser_(std::move(dataBaseUser))
    , DatabaseContext_(databaseContext)
    , Host_(host)
    , ActionQueue_(New<TActionQueue>("HealthChecker"))
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        ActionQueue_->GetInvoker(),
        BIND(&THealthChecker::ExecuteQueries, MakeWeak(this)),
        Config_->Period))
    , QueryIndexToTag_(NDetail::RegisterQueryTags(Config_->Queries.size()))
{ }

void THealthChecker::Start()
{
    YT_LOG_DEBUG("Health checker started (Period: %v, QueryCount: %v)",
        Config_->Period,
        Config_->Queries.size());
    PeriodicExecutor_->Start();
}

void THealthChecker::ExecuteQueries()
{
    std::vector<bool> newResult(Config_->Queries.size());

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

        newResult[queryIndex] = error.IsOK();
    }

    {
        TGuard guard(Lock_);
        LastResult_.swap(newResult);
    }
}

void THealthChecker::OnProfiling()
{
    std::vector<bool> lastResultSnapshot;

    // Make a copy in order to not hold lock for a long time in case of profiling overload.
    {
        TGuard guard(Lock_);
        lastResultSnapshot = LastResult_;
    }

    if (lastResultSnapshot.empty()) {
        // Nothing to export yet.
        return;
    }

    for (size_t queryIndex = 0; queryIndex < Config_->Queries.size(); ++queryIndex) {
        ClickHouseYtProfiler.Enqueue(
            "/health_checker/success",
            lastResultSnapshot[queryIndex],
            NProfiling::EMetricType::Gauge,
            {QueryIndexToTag_[queryIndex]});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
