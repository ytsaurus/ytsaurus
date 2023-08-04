
#include "explain.h"

#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/library/query/engine_api/coordinator.h>
#include <yt/yt/library/query/engine_api/range_inferrer.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueryClient {

using namespace NYTree;
using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void GetQueryInfo(
    NApi::NNative::IConnectionPtr connection,
    const TConstQueryPtr query,
    const TDataSource& dataSource,
    TFluentAny& fluent,
    bool isSubquery,
    const NApi::TExplainQueryOptions& options)
{
    const auto& keyColumns = query->GetKeyColumns();
    const auto& expression = query->WhereClause;

    std::vector<TRange<TConstJoinClausePtr>> groupedJoins;

    size_t joinIndex = 0;
    for (size_t joinGroupSize : GetJoinGroups(query->JoinClauses, query->GetRenamedSchema())) {
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
            auto trie = ExtractMultipleConstraints(expression, keyColumns, buffer, GetBuiltinRangeExtractor().Get());
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

            if (options.VerboseOutput && !isSubquery) {
                TQueryOptions queryOptions;
                queryOptions.RangeExpansionLimit = options.RangeExpansionLimit;
                queryOptions.VerboseLogging = options.VerboseLogging;
                queryOptions.MaxSubqueries = options.MaxSubqueries;

                auto Logger = MakeQueryLogger(query);
                auto rowBuffer = New<TRowBuffer>(TQueryExecutorRowBufferTag{});

                auto allSplits = InferRanges(connection, query, dataSource, queryOptions, rowBuffer, Logger);

                THashMap<TString, std::vector<TDataSource>> groupsByAddress;
                for (const auto& split : allSplits) {
                    const auto& address = split.second;
                    groupsByAddress[address].push_back(split.first);
                }

                std::vector<std::pair<std::vector<TDataSource>, TString>> groupedSplits;
                for (const auto& group : groupsByAddress) {
                    groupedSplits.emplace_back(group.second, group.first);
                }

                fluent.Item("grouped_ranges")
                    .DoMapFor(groupedSplits, [&] (auto fluent, auto rangesByNode) {
                        fluent.Item(rangesByNode.second)
                        .DoListFor(rangesByNode.first, [&] (auto fluent, auto ranges) {
                            fluent.Item()
                            .BeginList()
                            .DoFor(ranges.Ranges, [&] (auto fluent, auto range) {
                                fluent.Item()
                                .BeginList()
                                    .Item().Value(ToString(range.first))
                                    .Item().Value(ToString(range.second))
                                .EndList();
                            }).EndList();
                        });
                    });
            }
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
    NApi::NNative::IConnectionPtr connection,
    const std::unique_ptr<TPlanFragment>& fragment,
    TStringBuf udfRegistryPath,
    const NApi::TExplainQueryOptions& options)
{
    const auto& query = fragment->Query;
    const TDataSource& dataSource = fragment->DataSource;

    return BuildYsonStringFluently()
        .BeginMap()
        .Item("udf_registry_path").Value(udfRegistryPath)
        .Item("query")
        .Do([&] (auto fluent) {
            GetQueryInfo(connection, query, dataSource, fluent, false, options);
        })
        .Do([&] (TFluentMap fluent) {
            auto refiner = [] (TConstExpressionPtr expr, const TKeyColumns& /*keyColumns*/) {
                return expr;
            };

            const auto& coordinatedQuery = CoordinateQuery(query, {refiner});
            fluent
                .Item("subqueries")
                .BeginMap()
                    .DoFor(coordinatedQuery.second, [&] (auto fluent, const auto& subquery) {
                        fluent.Item(ToString(subquery->Id))
                            .Do([&] (auto fluent) {
                                GetQueryInfo(connection, subquery, dataSource, fluent, true, options);
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
