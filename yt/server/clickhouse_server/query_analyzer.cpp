#include "query_analyzer.h"

#include "helpers.h"
#include "query_context.h"

#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/input_data_slice.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/SyntaxAnalyzer.h>

#include <library/string_utils/base64/base64.h>

namespace NYT::NClickHouseServer {

using namespace NChunkPools;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

void FillDataSliceDescriptors(TSubquerySpec& subquerySpec, const TRange<NChunkPools::TChunkStripePtr>& chunkStripes)
{
    for (const auto& chunkStripe : chunkStripes) {
        auto& inputDataSliceDescriptors = subquerySpec.DataSliceDescriptors.emplace_back();
        for (const auto& dataSlice : chunkStripe->DataSlices) {
            const auto& chunkSlice = dataSlice->ChunkSlices[0];
            auto& chunkSpec = inputDataSliceDescriptors.emplace_back().ChunkSpecs.emplace_back();
            ToProto(&chunkSpec, chunkSlice, EDataSourceType::UnversionedTable);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TQueryAnalyzer::TQueryAnalyzer(const DB::Context& context, const DB::SelectQueryInfo& queryInfo)
    : Context_(context)
    , QueryInfo_(queryInfo)
    , Logger(GetQueryContext(context)->Logger)
{ }

void TQueryAnalyzer::ValidateKeyColumns()
{
    const auto& analyzedJoin = QueryInfo_.syntax_analyzer_result->analyzed_join;
    YT_VERIFY(Storages_.size() == 2);
    YT_VERIFY(Storages_[0]);

    struct TJoinArgument
    {
        int Index;
        TTableSchema TableSchema;
        std::vector<TRichYPath> Paths;
        THashMap<TString, int> KeyColumnToIndex;
        std::vector<TString> JoinColumns;
    };

    std::vector<TJoinArgument> joinArguments;

    for (int index = 0; index < static_cast<int>(Storages_.size()); ++index) {
        if (const auto& storage = Storages_[index]) {
            auto& joinArgument = joinArguments.emplace_back();
            joinArgument.Index = index;
            joinArgument.TableSchema = storage->GetSchema();
            for (
                int columnIndex = 0;
                columnIndex < static_cast<int>(joinArgument.TableSchema.GetKeyColumns().size());
                ++columnIndex)
            {
                auto column = joinArgument.TableSchema.GetKeyColumns()[columnIndex];
                joinArgument.KeyColumnToIndex[column] = columnIndex;
            }

            joinArgument.Paths = storage->GetTablePaths();
            if (joinArgument.Paths.size() != 1) {
                THROW_ERROR_EXCEPTION("Invalid JOIN: only single table may currently be joined")
                    << TErrorAttribute("table_index", index)
                    << TErrorAttribute("table_paths", joinArgument.Paths);
            }
        }
    }

    YT_VERIFY(joinArguments.size() >= 1);
    YT_VERIFY(joinArguments.size() <= 2);

    auto extractColumnNames = [] (const DB::ASTs& keyAsts) {
        std::vector<TString> result;
        for (const auto& keyAst : keyAsts) {
            auto* identifier = keyAst->as<DB::ASTIdentifier>();
            YT_VERIFY(identifier);
            result.emplace_back(identifier->shortName());
        }
        return result;
    };

    joinArguments[0].JoinColumns = extractColumnNames(analyzedJoin.key_asts_left);
    if (static_cast<int>(joinArguments.size()) == 2) {
        joinArguments[1].JoinColumns = extractColumnNames(analyzedJoin.key_asts_right);
    }

    // Check that join columns occupy prefixes of the key columns.
    for (const auto& joinArgument : joinArguments) {
        int maxKeyColumnIndex = -1;
        for (const auto& joinColumn : joinArgument.JoinColumns) {
            auto it = joinArgument.KeyColumnToIndex.find(joinColumn);
            if (it == joinArgument.KeyColumnToIndex.end()) {
                THROW_ERROR_EXCEPTION("Invalid sorted JOIN: joined column %Qv is not a key column of table", joinColumn)
                    << TErrorAttribute("table_index", joinArgument.Index)
                    << TErrorAttribute("column", joinColumn)
                    << TErrorAttribute("key_columns", joinArgument.TableSchema.GetKeyColumns());
            }
            maxKeyColumnIndex = std::max(maxKeyColumnIndex, it->second);
        }
        if (maxKeyColumnIndex + 1 != static_cast<int>(joinArgument.JoinColumns.size())) {
            THROW_ERROR_EXCEPTION("Invalid sorted JOIN: joined columns should form prefix of joined table key columns")
                << TErrorAttribute("table_index", joinArgument.Index)
                << TErrorAttribute("join_columns", joinArgument.JoinColumns)
                << TErrorAttribute("key_columns", joinArgument.TableSchema.GetKeyColumns());
        }
    }

    if (joinArguments.size() == 2) {
        const auto& lhsSchema = joinArguments[0].TableSchema;
        const auto& rhsSchema = joinArguments[1].TableSchema;
        // Check that joined columns occupy same positions in both tables.
        for (int index = 0; index < static_cast<int>(joinArguments[0].JoinColumns.size()); ++index) {
            const auto& lhsColumn = joinArguments[0].JoinColumns[index];
            const auto& rhsColumn = joinArguments[1].JoinColumns[index];
            auto lhsIt = joinArguments[0].KeyColumnToIndex.find(lhsColumn);
            auto rhsIt = joinArguments[1].KeyColumnToIndex.find(rhsColumn);
            YT_VERIFY(lhsIt != joinArguments[0].KeyColumnToIndex.end());
            YT_VERIFY(rhsIt != joinArguments[1].KeyColumnToIndex.end());
            if (lhsIt->second != rhsIt->second) {
                THROW_ERROR_EXCEPTION(
                    "Invalid sorted JOIN: joined columns %Qv and %Qv do not occupy same positions in key columns of joined tables",
                    lhsColumn,
                    rhsColumn)
                    << TErrorAttribute("lhs_column", lhsColumn)
                    << TErrorAttribute("rhs_column", rhsColumn)
                    << TErrorAttribute("lhs_key_columns", lhsSchema.GetKeyColumns())
                    << TErrorAttribute("rhs_key_columns", rhsSchema.GetKeyColumns());
            }
        }
    }
}

void TQueryAnalyzer::ParseQuery()
{
    auto* selectQuery = QueryInfo_.query->as<DB::ASTSelectQuery>();

    YT_VERIFY(selectQuery);
    YT_VERIFY(selectQuery->tables());

    auto* tablesInSelectQuery = selectQuery->tables()->as<DB::ASTTablesInSelectQuery>();
    YT_VERIFY(tablesInSelectQuery);
    YT_VERIFY(tablesInSelectQuery->children.size() >= 1);
    YT_VERIFY(tablesInSelectQuery->children.size() <= 2);

    for (int index = 0; index < static_cast<int>(tablesInSelectQuery->children.size()); ++index) {
        auto& tableInSelectQuery = tablesInSelectQuery->children[index];
        auto* tablesElement = tableInSelectQuery->as<DB::ASTTablesInSelectQueryElement>();
        YT_VERIFY(tablesElement);
        if (!tablesElement->table_expression) {
            // First element should always be table expression as it is the
            YT_VERIFY(index != 0);
            continue;
        }

        YT_LOG_DEBUG("Found table expression (Index: %v, TableExpression: %v)", index, *tablesElement->table_expression);

        if (index == 1) {
            IsJoin_ = true;
        }

        auto& tableExpression = tablesElement->table_expression;

        if (tablesElement->table_join) {
            const auto* tableJoin = tablesElement->table_join->as<DB::ASTTableJoin>();
            YT_VERIFY(tableJoin);
            if (static_cast<int>(tableJoin->locality) == static_cast<int>(DB::ASTTableJoin::Locality::Global)) {
                YT_LOG_DEBUG("Table expression is a global join (Index: %v)", index);
                IsGlobalJoin_ = true;
            }
            if (static_cast<int>(tableJoin->kind) == static_cast<int>(DB::ASTTableJoin::Kind::Right) ||
                static_cast<int>(tableJoin->kind) == static_cast<int>(DB::ASTTableJoin::Kind::Full))
            {
                YT_LOG_DEBUG("Query is a right or full join");
                IsRightOrFullJoin_ = true;
            }
        }

        TableExpressions_.emplace_back(tableExpression->as<DB::ASTTableExpression>());
        YT_VERIFY(TableExpressions_.back());
        TableExpressionPtrs_.emplace_back(&tableExpression);
    }

    // At least first table expression should be the one that instantiated this query analyzer (aka owner).
    YT_VERIFY(TableExpressions_.size() >= 1);
    // More than 2 tables are not supported in CH yet.
    YT_VERIFY(TableExpressions_.size() <= 2);

    for (const auto& tableExpression : TableExpressions_) {
        auto& storage = Storages_.emplace_back(GetStorage(tableExpression));
        if (storage) {
            YT_LOG_DEBUG("Table expression corresponds to TStorageDistributor (TableExpression: %v)", static_cast<DB::IAST&>(*tableExpression));
            ++YtTableCount_;
        } else {
            YT_LOG_DEBUG("Table expression does not correspond to TStorageDistributor (TableExpression: %v)", static_cast<DB::IAST&>(*tableExpression));
        }
    }

    if (YtTableCount_ == 2) {
        YT_LOG_DEBUG("Query is a two-YT-table join");
        IsTwoYtTableJoin_ = true;
    }

    YT_LOG_DEBUG(
        "Extracted table expressions from query (Query: %v, TableExpressionCount: %v, YtTableCount: %v, "
        "IsJoin: %v, IsGlobalJoin: %v, IsRightOrFullJoin: %v)",
        *QueryInfo_.query,
        TableExpressions_.size(),
        YtTableCount_,
        IsJoin_,
        IsGlobalJoin_,
        IsRightOrFullJoin_);
}

void TQueryAnalyzer::AppendWhereCondition()
{

}

TQueryAnalysisResult TQueryAnalyzer::Analyze()
{
    ParseQuery();

    if (IsTwoYtTableJoin_ || IsRightOrFullJoin_) {
        ValidateKeyColumns();
    }

    if (IsRightOrFullJoin_) {
        AppendWhereCondition();
    }

    TQueryAnalysisResult result;

    for (const auto& storage : Storages_) {
        if (!storage) {
            continue;
        }
        result.TablePaths.emplace_back(storage->GetTablePaths());
        auto clickHouseSchema = storage->GetClickHouseSchema();
        std::optional<DB::KeyCondition> keyCondition;
        if (clickHouseSchema.HasPrimaryKey()) {
            keyCondition = CreateKeyCondition(Context_, QueryInfo_, clickHouseSchema);
        }
        result.KeyConditions.emplace_back(std::move(keyCondition));
        result.TableSchemas.emplace_back(storage->GetSchema());
    }

    if (IsTwoYtTableJoin_ || IsRightOrFullJoin_) {
        result.PoolKind = EPoolKind::Sorted;
        result.KeyColumnCount = QueryInfo_.syntax_analyzer_result->analyzed_join.key_names_left.size();
    } else {
        result.PoolKind = EPoolKind::Unordered;
    }

    return result;
}

DB::ASTPtr TQueryAnalyzer::RewriteQuery(const TRange<TChunkStripeListPtr> stripeLists, TSubquerySpec specTemplate, int subqueryIndex)
{
    auto Logger = this->Logger
        .AddTag("SubqueryIndex: %v", subqueryIndex);

    i64 totalRowCount = 0;
    i64 totalDataWeight = 0;
    i64 totalChunkCount = 0;
    for (const auto& stripeList : stripeLists) {
        totalRowCount += stripeList->TotalRowCount;
        totalDataWeight += stripeList->TotalDataWeight;
        totalChunkCount += stripeList->TotalChunkCount;
    }

    YT_LOG_DEBUG(
        "Rewriting query (StripeListCount: %v, TotalDataWeight: %v, TotalRowCount: %v, TotalChunkCount: %v)",
        stripeLists.size(),
        totalDataWeight,
        totalRowCount,
        totalChunkCount);

    specTemplate.SubqueryIndex = subqueryIndex;

    std::vector<DB::ASTPtr> newTableExpressions;

    for (int index = 0; index < YtTableCount_; ++index) {
        auto tableExpression = TableExpressions_[index];

        std::vector<TChunkStripePtr> stripes;
        for (const auto& stripeList : stripeLists) {
            TChunkStripePtr currentTableStripe;
            for (const auto& stripe : stripeList->Stripes) {
                if (stripe->GetTableIndex() == index) {
                    YT_VERIFY(!currentTableStripe);
                    currentTableStripe = stripe;
                }
            }
            if (!currentTableStripe) {
                currentTableStripe = New<TChunkStripe>();
            }
            stripes.emplace_back(std::move(currentTableStripe));
        }

        auto spec = specTemplate;
        spec.TableIndex = index;
        auto clickHouseSchema = Storages_[index]->GetClickHouseSchema();
        spec.Columns = clickHouseSchema.Columns;
        spec.ReadSchema = Storages_[index]->GetSchema();

        FillDataSliceDescriptors(spec, MakeRange(stripes));

        auto protoSpec = NYT::ToProto<NProto::TSubquerySpec>(spec);
        auto encodedSpec = Base64Encode(protoSpec.SerializeAsString());

        auto tableFunction = makeASTFunction("ytSubquery", std::make_shared<DB::ASTLiteral>(std::string(encodedSpec.data())));

        if (tableExpression->database_and_table_name) {
            tableFunction->alias = static_cast<DB::ASTWithAlias&>(*tableExpression->database_and_table_name).alias;
        } else {
            tableFunction->alias = static_cast<DB::ASTWithAlias&>(*tableExpression->table_function).alias;
        }

        auto clonedTableExpression = tableExpression->clone();
        auto* typedTableExpression = clonedTableExpression->as<DB::ASTTableExpression>();
        YT_VERIFY(typedTableExpression);
        typedTableExpression->table_function = std::move(tableFunction);
        typedTableExpression->database_and_table_name = nullptr;
        typedTableExpression->subquery = nullptr;
        typedTableExpression->sample_offset = nullptr;
        typedTableExpression->sample_size = nullptr;

        newTableExpressions.emplace_back(std::move(clonedTableExpression));
    }

    auto result = ReplaceTableExpressions(newTableExpressions);

    YT_LOG_DEBUG("Query rewritten (NewQuery: %v)", *result);

    return result;
}

std::shared_ptr<IStorageDistributor> TQueryAnalyzer::GetStorage(const DB::ASTTableExpression* tableExpression) const
{
    if (!tableExpression) {
        return nullptr;
    }
    DB::StoragePtr storage;
    if (tableExpression->table_function) {
        storage = const_cast<DB::Context&>(Context_.getQueryContext()).executeTableFunction(tableExpression->table_function);
    } else if (tableExpression->database_and_table_name) {
        auto databaseAndTable = DB::DatabaseAndTableWithAlias(tableExpression->database_and_table_name);
        storage = const_cast<DB::Context&>(Context_).getTable(databaseAndTable.database, databaseAndTable.table);
    }

    return std::dynamic_pointer_cast<IStorageDistributor>(storage);
}

DB::ASTPtr TQueryAnalyzer::ReplaceTableExpressions(std::vector<DB::ASTPtr> newTableExpressions)
{
    YT_VERIFY(static_cast<int>(newTableExpressions.size()) == YtTableCount_);
    for (int index = 0; index < static_cast<int>(newTableExpressions.size()); ++index) {
        YT_VERIFY(newTableExpressions[index]);
        newTableExpressions[index].swap(*TableExpressionPtrs_[index]);
    }
    auto result = QueryInfo_.query->clone();
    for (int index = 0; index < static_cast<int>(newTableExpressions.size()); ++index) {
        newTableExpressions[index].swap(*TableExpressionPtrs_[index]);
        YT_VERIFY(newTableExpressions[index]);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
