#include "query_analyzer.h"

#include "helpers.h"
#include "query_context.h"

#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/input_data_slice.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

#include <library/string_utils/base64/base64.h>

namespace NYT::NClickHouseServer {

using namespace NChunkPools;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

void FillDataSliceDescriptors(TSubquerySpec& subquerySpec, const TRange<NChunkPools::TChunkStripePtr>& chunkStripes)
{
    for (const auto& chunkStripe : chunkStripes) {
        auto& inputDataSliceDescriptors = subquerySpec.DataSliceDescriptors.emplace_back();
        for (const auto& dataSlice : chunkStripe->DataSlices) {
            const auto& chunkSlice = dataSlice->ChunkSlices[0];
            auto chunk = dataSlice->GetSingleUnversionedChunkOrThrow();
            auto& chunkSpec = inputDataSliceDescriptors.emplace_back().ChunkSpecs.emplace_back();
            ToProto(&chunkSpec, chunk, EDataSourceType::UnversionedTable);
            // TODO(max42): wtf?
            chunkSpec.set_row_count_override(dataSlice->GetRowCount());
            chunkSpec.set_data_weight_override(dataSlice->GetDataWeight());
            if (chunkSlice->LowerLimit().RowIndex) {
                chunkSpec.mutable_lower_limit()->set_row_index(*chunkSlice->LowerLimit().RowIndex);
            }
            if (chunkSlice->UpperLimit().RowIndex) {
                chunkSpec.mutable_upper_limit()->set_row_index(*chunkSlice->UpperLimit().RowIndex);
            }
            NChunkClient::NProto::TMiscExt miscExt;
            miscExt.set_row_count(chunk->GetTotalRowCount());
            miscExt.set_uncompressed_data_size(chunk->GetTotalUncompressedDataSize());
            miscExt.set_data_weight(chunk->GetTotalDataWeight());
            miscExt.set_compressed_data_size(chunk->GetCompressedDataSize());
            chunkSpec.mutable_chunk_meta()->set_version(static_cast<int>(chunk->GetTableChunkFormat()));
            chunkSpec.mutable_chunk_meta()->set_type(static_cast<int>(EChunkType::Table));
            SetProtoExtension(chunkSpec.mutable_chunk_meta()->mutable_extensions(), miscExt);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TQueryAnalyzer::TQueryAnalyzer(const DB::Context& context, const DB::SelectQueryInfo& queryInfo)
    : Context_(context)
    , QueryInfo_(queryInfo)
    , Logger_(GetQueryContext(context)->Logger)
{
    const auto& Logger = Logger_;

    auto* selectQuery = QueryInfo_.query->as<DB::ASTSelectQuery>();

    YT_VERIFY(selectQuery);
    YT_VERIFY(selectQuery->tables());

    auto* tablesInSelectQuery = selectQuery->tables()->as<DB::ASTTablesInSelectQuery>();
    for (auto& tableInSelectQuery : tablesInSelectQuery->children) {
        auto* tablesElement = tableInSelectQuery->as<DB::ASTTablesInSelectQueryElement>();
        if (!tablesElement->table_expression) {
            continue;
        }
        if (auto tableJoin = tablesElement->table_join) {
            if (static_cast<int>(tableJoin->as<DB::ASTTableJoin>()->locality) == static_cast<int>(DB::ASTTableJoin::Locality::Global)) {
                continue;
            }
        }

        auto& tableExpression = tablesElement->table_expression;
        TableExpressions_.emplace_back(tableExpression->as<DB::ASTTableExpression>());
        TableExpressionPtrs_.emplace_back(&tableExpression);
    }

    // At least first table expression should be the one that instantiated this query analyzer (aka owner).
    YT_VERIFY(TableExpressions_.size() >= 1);
    // More than 2 tables are not supported in CH yet.
    YT_VERIFY(TableExpressions_.size() <= 2);

    YT_LOG_DEBUG("Extracted table expressions from query (Query: %v, TableExpressionCount: %v)",
        *QueryInfo_.query,
        TableExpressions_.size());

    for (const auto& tableExpression : TableExpressions_) {
        auto& storage = Storages_.emplace_back(GetStorage(tableExpression));
        if (storage) {
            YT_LOG_DEBUG("Table expression corresponds to TStorageDistributor (TableExpression: %v)", static_cast<DB::IAST&>(*tableExpression));
        } else {
            YT_LOG_DEBUG("Table expression does not correspond to TStorageDistributor (TableExpression: %v)", static_cast<DB::IAST&>(*tableExpression));
        }
    }
}

TQueryAnalysisResult TQueryAnalyzer::Analyze()
{
    TQueryAnalysisResult result;

    if (Storages_.size() == 1 || (Storages_.size() == 2 && Storages_[1] == nullptr)) {
        // Regular unordered pool case.
        result.PoolKind = EPoolKind::Unordered;
        const auto& storage = Storages_.front();
        YT_VERIFY(storage);
        auto clickHouseSchema = storage->GetClickHouseSchema();
        result.KeyConditions.resize(1);
        result.TablePaths = {storage->GetTablePaths()};
        if (clickHouseSchema.HasPrimaryKey()) {
            result.KeyConditions[0] = CreateKeyCondition(Context_, QueryInfo_, clickHouseSchema);
        }
    } else {
        THROW_ERROR_EXCEPTION("Joins are not implemented yet");
    }

    return result;
}

DB::ASTPtr TQueryAnalyzer::RewriteQuery(const TRange<TChunkStripeListPtr> stripeLists, TSubquerySpec specTemplate, int subqueryIndex)
{
    auto Logger = TLogger(Logger_)
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

    for (int index = 0; index < static_cast<int>(TableExpressions_.size()); ++index) {
        auto tableExpression = TableExpressions_[index];

        std::vector<TChunkStripePtr> stripes;
        for (const auto& stripeList : stripeLists) {
            YT_VERIFY(stripeList->Stripes.size() == TableExpressions_.size());
            stripes.emplace_back(stripeList->Stripes[index]);
        }

        auto spec = specTemplate;
        spec.TableIndex = index;

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
    YT_VERIFY(newTableExpressions.size() == TableExpressionPtrs_.size());
    for (int index = 0; index < static_cast<int>(newTableExpressions.size()); ++index) {
        newTableExpressions[index].swap(*TableExpressionPtrs_[index]);
    }
    auto result = QueryInfo_.query->clone();
    for (int index = 0; index < static_cast<int>(newTableExpressions.size()); ++index) {
        newTableExpressions[index].swap(*TableExpressionPtrs_[index]);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
