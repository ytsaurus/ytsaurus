#include "aggregate_query_executor.h"
#include "attribute_schema.h"
#include "db_config.h"
#include "query_executor_helpers.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>

#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/library/query/base/helpers.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NYT::NApi;
using namespace NYT::NTableClient;
using namespace NYT::NQueryClient::NAst;

using namespace NAccessControl;

using NYT::NQueryClient::TSourceLocation;

////////////////////////////////////////////////////////////////////////////////

class TAggregateQueryExecutor
    : public IAggregateQueryExecutor
    , public TQueryExecutorBase
{
public:
    using TQueryExecutorBase::TQueryExecutorBase;

    TAggregateQueryResult ExecuteAggregateQuery(
        TObjectTypeValue type,
        const std::optional<TObjectFilter>& filter,
        const TAttributeAggregateExpressions& aggregators,
        const TAttributeGroupingExpressions& groupByExpressions,
        std::optional<bool> fetchFinalizingObjects) override
    {
        if (groupByExpressions.Expressions.empty()) {
            THROW_ERROR_EXCEPTION("Empty list of group by expressions");
        }

        auto queryContext = MakeQueryContext(
            Bootstrap_,
            type,
            Transaction_->GetSession(),
            /*indexSchema*/ nullptr,
            /*allowAnnotations*/ false);

        queryContext->GetQuery()->GroupExprs = TExpressionList{};

        int i = 0;
        queryContext->GetQuery()->SelectExprs = TExpressionList{};
        for (auto&& expr : RewriteExpressions(queryContext.get(), groupByExpressions.Expressions)) {
            const auto aliasName = Format("group_by_expr_%v", i++);
            queryContext->GetQuery()->GroupExprs->push_back(queryContext->New<TAliasExpression>(
                TSourceLocation{},
                std::move(expr),
                aliasName));
            queryContext->GetQuery()->SelectExprs->push_back(queryContext->New<TReferenceExpression>(
                TSourceLocation{},
                aliasName));
        }
        for (auto&& expr : RewriteExpressions(queryContext.get(), aggregators.Expressions)) {
            queryContext->GetQuery()->SelectExprs->push_back(std::move(expr));
        }

        auto [nonComputedFilter, computedFilter] = filter
            ? BuildFilterExpressions(queryContext.get(), *filter)
            : std::pair<TExpressionPtr, TExpressionPtr>{nullptr, nullptr};
        THROW_ERROR_EXCEPTION_IF(computedFilter, "Computed fields are not allowed in aggregate query filter");
        auto filterExpression = NQueryClient::BuildAndExpression(
            queryContext.get(),
            ResolveFilterExpression(nonComputedFilter, queryContext.get(), /*isComputed*/ false).FilterExpression,
            BuildObjectFilterByNullColumn(queryContext.get(), &ObjectsTable.Fields.MetaRemovalTime));
        if (Bootstrap_->GetDBConfig().EnableFinalizers &&
            !fetchFinalizingObjects.value_or(Transaction_->GetConfig()->FetchFinalizingObjectsByDefault))
        {
            filterExpression = NQueryClient::BuildAndExpression(
                queryContext.get(),
                filterExpression,
                BuildObjectFilterByNullColumn(queryContext.get(), &FinalizationStartTimeField));
        }
        queryContext->GetQuery()->WherePredicate = TExpressionList{std::move(filterExpression)};

        auto queryString = queryContext->Finalize();

        YT_LOG_DEBUG("Aggregating objects (Type: %v, Query: %v)",
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(type),
            queryString);

        const auto rowset = RunSelect(std::move(queryString));

        TAggregateQueryResult result;
        for (const auto& row : rowset->GetRows()) {
            auto& valueList = result.Objects.emplace_back();
            for (const auto& value : row) {
                valueList.Values.push_back(UnversionedValueToYson(value));
            }
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IAggregateQueryExecutor> MakeAggregateQueryExecutor(
    TTransactionPtr transaction)
{
    return std::make_unique<TAggregateQueryExecutor>(transaction);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
