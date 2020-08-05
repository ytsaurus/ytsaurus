#include "coordinator.h"
#include "explain.h"
#include "functions.h"
#include "query_helpers.h"
#include "query_preparer.h"

#include <yt/client/api/public.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NQueryClient {

using namespace NYTree;
using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void GetQueryInfo(
    const TConstQueryPtr query,
    TFluentAny& fluent)
{
    const auto& keyColumns = query->GetKeyColumns();
    const auto& expression = query->WhereClause;

    std::vector<TRange<TConstJoinClausePtr>> groupedJoins;

    size_t joinIndex = 0;
    for (size_t joinGroupSize : GetJoinGroups(query->JoinClauses, query->GetTableSchema())) {
        groupedJoins.emplace_back(query->JoinClauses.data() + joinIndex, joinGroupSize);
        joinIndex += joinGroupSize;
    }

    fluent
        .BeginMap()
        .Item("where_expression").Value(InferName(expression, true))
        .Item("is_ordered_scan").Value(query->IsOrdered())
        .Item("joins").BeginList()
            .DoFor(groupedJoins, [&] (auto fluent, TRange<TConstJoinClausePtr> joinGroup) {
                fluent.Item().BeginList().
                    DoFor(joinGroup, [&](auto fluent, const TConstJoinClausePtr& joinClause) {
                        fluent.Item()
                        .BeginMap()
                            .Item("lookup_type")
                            .Do([&](auto fluent) {
                                auto foreignKeyPrefix = joinClause->ForeignKeyPrefix;
                                const auto& foreignEquations = joinClause->ForeignEquations;
                                if (foreignKeyPrefix > 0) {
                                    if (foreignKeyPrefix == foreignEquations.size()) {
                                        fluent.Value("source ranges");
                                    } else {
                                        fluent.Value("prefix ranges");
                                    }
                                } else {
                                    fluent.Value("IN clause");
                                }
                            })
                            .Item("common_key_prefix").Value(joinClause->CommonKeyPrefix)
                            .Item("foreign_key_prefix").Value(joinClause->ForeignKeyPrefix)
                        .EndMap();
                }).EndList();
            })
        .EndList()
        .DoIf(query->UseDisjointGroupBy, [&] (auto fluent) {
            fluent.Item("common_prefix_with_primary_key").Value(query->GroupClause->CommonPrefixWithPrimaryKey);
        })
        .DoIf(!keyColumns.empty(), [&] (auto fluent) {
            auto buffer = New<TRowBuffer>();
            auto trie = ExtractMultipleConstraints(expression, keyColumns, buffer, BuiltinRangeExtractorMap.Get());
            fluent.Item("key_trie").Value(ToString(trie));

            auto ranges = GetRangesFromTrieWithinRange(TRowRange(MinKey(), MaxKey()), trie, buffer);
            fluent.Item("ranges")
                .DoListFor(ranges, [&] (auto fluent, auto range) {
                    fluent.Item()
                    .BeginList()
                        .Item().Value(ToString(range.first))
                        .Item().Value(ToString(range.second))
                    .EndList();
                });
        })
        .EndMap();
}

void GetFrontQueryInfo(
    const TConstFrontQueryPtr query,
    TFluentAny& fluent)
{
    fluent
        .BeginMap()
            .Item("is_ordered_scan").Value(query->IsOrdered())
            .DoIf(query->UseDisjointGroupBy, [&] (auto fluent) {
                fluent.Item("common_prefix_with_primary_key").Value(query->GroupClause->CommonPrefixWithPrimaryKey);
            })
        .EndMap();
}

NYson::TYsonString BuildExplainQueryYson(
    const TString& queryString,
    const std::unique_ptr<TPlanFragment>& fragment,
    TStringBuf udfRegistryPath)
{
    const auto& query = fragment->Query;

    return BuildYsonStringFluently()
        .BeginMap()
        .Item("udf_registry_path").Value(udfRegistryPath)
        .Item("query")
        .Do([&] (auto fluent) {
            GetQueryInfo(query, fluent);
        })
        .Do([&] (TFluentMap fluent) {
            auto refiner = [] (TConstExpressionPtr expr, const TKeyColumns& keyColumns) {
                return expr;
            };

            const auto& coordinatedQuery = CoordinateQuery(query, {refiner});
            fluent
                .Item("subqueries")
                .BeginMap()
                    .DoFor(coordinatedQuery.second, [&] (auto fluent, const auto& subquery) {
                        fluent.Item(ToString(subquery->Id))
                            .Do([&] (auto fluent) {
                                GetQueryInfo(subquery, fluent);
                            });
                    })
                .EndMap();
            fluent.Item("top_query")
                .Do([&] (auto fluent) {
                    GetFrontQueryInfo(coordinatedQuery.first, fluent);
                });
        })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
