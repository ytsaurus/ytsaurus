
#include "explain.h"

#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/library/query/engine_api/coordinator.h>
#include <yt/yt/library/query/engine_api/range_inferrer.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueryClient {

using namespace NYTree;
using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void GetReadRangesInfo(
    TFluentMap fluent,
    NApi::NNative::IConnectionPtr connection,
    const TConstQueryPtr query,
    const TDataSource& dataSource,
    const NApi::TExplainQueryOptions& options)
{
    const auto& keyColumns = query->GetKeyColumns();
    const auto& predicate = query->WhereClause;

    auto rowBuffer = New<TRowBuffer>();

    TConstraintsHolder constraints(keyColumns.size());
    auto constraintRef = constraints.ExtractFromExpression(
        predicate,
        keyColumns,
        rowBuffer,
        GetBuiltinConstraintExtractors());
    fluent.Item("constraints").Value(ToString(constraints, constraintRef));

    TQueryOptions queryOptions;
    queryOptions.RangeExpansionLimit = options.RangeExpansionLimit;
    queryOptions.VerboseLogging = options.VerboseLogging;
    queryOptions.NewRangeInference = options.NewRangeInference;
    queryOptions.MaxSubqueries = options.MaxSubqueries;

    auto Logger = MakeQueryLogger(query);

    auto [inferredDataSource, coordinatedQuery] = InferRanges(
        connection->GetColumnEvaluatorCache(),
        query,
        dataSource,
        queryOptions,
        rowBuffer,
        Logger);

    auto tableId = dataSource.ObjectId;

    auto tableInfo = WaitFor(connection->GetTableMountCache()->GetTableInfo(NObjectClient::FromObjectId(tableId)))
        .ValueOrThrow();

    fluent.Item("ranges")
        .DoListFor(inferredDataSource.Ranges, [&] (auto fluent, auto range) {
            fluent.Item()
            .BeginList()
                .Item().Value(Format("%kv", range.first))
                .Item().Value(Format("%kv", range.second))
            .EndList();
        });

    fluent.Item("keys")
        .DoListFor(inferredDataSource.Keys, [&] (auto fluent, auto key) {
            fluent.Item().Value(Format("%kv", key));
        });

    if (options.VerboseOutput) {
        auto allSplits = CoordinateDataSources(
            connection->GetCellDirectory(),
            connection->GetNetworks(),
            tableInfo,
            inferredDataSource,
            rowBuffer);

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
                            .Item().Value(Format("%kv", range.first))
                            .Item().Value(Format("%kv", range.second))
                        .EndList();
                    }).EndList();
                });
            });
    }
}

void GetFrontQueryInfo(
    TFluentMap fluent,
    const TConstBaseQueryPtr query)
{
    fluent
        .Item("is_ordered_scan").Value(query->IsOrdered())
        .DoIf(query->UseDisjointGroupBy, [&] (auto fluent) {
            fluent.Item("common_prefix_with_primary_key").Value(query->GroupClause->CommonPrefixWithPrimaryKey);
        });
}

void GetQueryInfo(TFluentMap fluent, const TConstQueryPtr query)
{
    const auto& predicate = query->WhereClause;

    std::vector<TRange<TConstJoinClausePtr>> groupedJoins;

    size_t joinIndex = 0;
    for (size_t joinGroupSize : GetJoinGroups(query->JoinClauses, query->GetRenamedSchema())) {
        groupedJoins.emplace_back(query->JoinClauses.data() + joinIndex, joinGroupSize);
        joinIndex += joinGroupSize;
    }

    fluent
        .Item("where_expression").Value(InferName(predicate, true))
        .Item("joins").BeginList()
            .DoFor(groupedJoins, [&] (auto fluent, TRange<TConstJoinClausePtr> joinGroup) {
                fluent.Item().BeginList().
                    DoFor(joinGroup, [&] (auto fluent, const TConstJoinClausePtr& joinClause) {
                        fluent.Item()
                        .BeginMap()
                            .Item("lookup_type")
                            .Do([&] (auto fluent) {
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
        .EndList();

    GetFrontQueryInfo(fluent, query);
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
        .DoMap([&] (auto fluent) {
            GetQueryInfo(fluent, query);
            GetReadRangesInfo(fluent, connection, query, dataSource, options);
        })
        .Do([&] (TFluentMap fluent) {
            auto [frontQuery, bottomQuery] = GetDistributedQueryPattern(query);
            fluent
                .Item("bottom_query")
                .DoMap([&, bottomQuery = bottomQuery] (auto fluent) {
                    GetQueryInfo(fluent, bottomQuery);
                });
            fluent.Item("front_query")
                .DoMap([&, frontQuery = frontQuery] (auto fluent) {
                    GetFrontQueryInfo(fluent, frontQuery);
                });
        })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
