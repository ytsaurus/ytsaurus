#include "health_checker.h"

#include <contrib/libs/clickhouse/dbms/src/Parsers/ParserQuery.h>
#include <contrib/libs/clickhouse/dbms/src/Parsers/parseQuery.h>

#include <contrib/libs/clickhouse/dbms/src/Interpreters/ClientInfo.h>
#include <contrib/libs/clickhouse/dbms/src/Interpreters/InterpreterSelectWithUnionQuery.h>

#include <contrib/libs/clickhouse/dbms/src/Core/Types.h>

#include <yt/server/clickhouse_server/config.h>
#include <yt/server/clickhouse_server/query_context.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/intrusive_ptr.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

std::vector<NProfiling::TTagId> RegisterQueryTags(size_t queryCount)
{
    std::vector<NProfiling::TTagId> queryTags;
    for (size_t queryIndex = 0; queryIndex < queryCount; ++queryIndex) {
        queryTags.emplace_back(
            NProfiling::TProfileManager::Get()->RegisterTag("query_index", queryIndex));
    }
    return queryTags;
}

DB::Context PrepareDatabaseContextForQuery(
    const DB::Context* databaseContext,
    const TString& dataBaseUser,
    TBootstrap* bootstrap)
{
    DB::Context databaseContextForQuery = *databaseContext;

    databaseContextForQuery.setUser(
        dataBaseUser, /*password =*/"", Poco::Net::SocketAddress(), /*quotaKey =*/"");

    auto queryId = TQueryId::Create();

    auto& clientInfo = databaseContextForQuery.getClientInfo();
    clientInfo.initial_user = clientInfo.current_user;
    clientInfo.query_kind = DB::ClientInfo::QueryKind::INITIAL_QUERY;
    clientInfo.initial_query_id = ToString(queryId);

    databaseContextForQuery.makeQueryContext();

    NTracing::TSpanContext spanContext{NTracing::TTraceId::Create(),
        NTracing::InvalidSpanId,
        /*sampled =*/false,
        /*debug =*/false};

    auto traceContext =
        New<NTracing::TTraceContext>(spanContext, /*spanName =*/"HealthCheckerQuery");

    SetupHostContext(bootstrap, databaseContextForQuery, queryId, std::move(traceContext));

    return databaseContextForQuery;
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
        NDetail::PrepareDatabaseContextForQuery(DatabaseContext_, DataBaseUser_, Bootstrap_),
        DB::SelectQueryOptions())
        .execute());
}

THealthChecker::THealthChecker(
    THealthCheckerConfigPtr config,
    TString dataBaseUser,
    const DB::Context* databaseContext,
    TBootstrap* bootstrap)
    : Config_(std::move(config))
    , DataBaseUser_(std::move(dataBaseUser))
    , DatabaseContext_(databaseContext)
    , Bootstrap_(bootstrap)
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND(&THealthChecker::ExecuteAndProfileQueries, MakeWeak(this)),
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

void THealthChecker::ExecuteAndProfileQueries()
{
    for (size_t queryIndex = 0; queryIndex < Config_->Queries.size(); ++queryIndex) {
        const auto& query = Config_->Queries[queryIndex];
        YT_LOG_DEBUG("Executing health checker query (Index: %v, Query: %v)", queryIndex, query);

        auto error = WaitFor(BIND(
            &THealthChecker::ExecuteQuery, MakeWeak(this), query)
            .AsyncVia(Bootstrap_->GetWorkerInvoker())
            .Run());

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

        ClickHouseYtProfiler.Enqueue(
            "/health_checker/success",
            error.IsOK(),
            NProfiling::EMetricType::Gauge,
            {QueryIndexToTag_[queryIndex]});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
