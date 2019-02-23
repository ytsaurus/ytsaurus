#include "storage_table.h"

#include "private.h"
#include "format_helpers.h"
#include "logging_helpers.h"
#include "query_helpers.h"
#include "storage_distributed.h"
#include "virtual_columns.h"

#include "table.h"
#include "table_partition.h"
#include "query_context.h"

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/queryToString.h>

#include <common/logger_useful.h>

namespace NYT::NClickHouseServer {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TStorageConcat final
    : public TStorageDistributed
{
private:
    std::vector<TTablePtr> Tables;

public:
    TStorageConcat(
        std::vector<TTablePtr> tables,
        TClickHouseTableSchema schema,
        IExecutionClusterPtr cluster);

    std::string getTableName() const override
    {
        return "Concatenate(" + JoinStrings(", ", GetTableNames()) + ")";
    }

private:
    const DB::NamesAndTypesList& ListVirtualColumns() const override
    {
        return ListSystemVirtualColumns();
    }

    std::vector<TString> GetTableNames() const;

    virtual TTablePartList GetTableParts(
        const ASTPtr& queryAst,
        const Context& context,
        const DB::KeyCondition* keyCondition,
        size_t maxParts) override;

    virtual ASTPtr RewriteSelectQueryForTablePart(
        const ASTPtr& queryAst,
        const std::string& jobSpec) override;
};

////////////////////////////////////////////////////////////////////////////////

TStorageConcat::TStorageConcat(
    std::vector<TTablePtr> tables,
    TClickHouseTableSchema schema,
    IExecutionClusterPtr cluster)
    : TStorageDistributed(
        std::move(cluster),
        std::move(schema),
        &Poco::Logger::get("StorageConcat"))
    , Tables(std::move(tables))
{}

std::vector<TString> TStorageConcat::GetTableNames() const
{
    std::vector<TString> names;
    names.reserve(Tables.size());
    for (auto& table : Tables) {
        names.push_back(table->Name);
    }
    return names;
}

TTablePartList TStorageConcat::GetTableParts(
    const ASTPtr& queryAst,
    const Context& context,
    const DB::KeyCondition* keyCondition,
    size_t maxParts)
{
    auto* queryContext = GetQueryContext(context);
    Y_UNUSED(queryAst);

    return queryContext->ConcatenateAndGetTableParts(
        GetTableNames(),
        keyCondition,
        maxParts);
}

ASTPtr TStorageConcat::RewriteSelectQueryForTablePart(
    const ASTPtr& queryAst,
    const std::string& jobSpec)
{
    auto modifiedQueryAst = queryAst->clone();

    ASTPtr tableFunction;

    auto* tableExpression = GetFirstTableExpression(typeid_cast<ASTSelectQuery &>(*modifiedQueryAst));
    if (tableExpression) {
        // TODO: validate table function name
        tableFunction = makeASTFunction(
            "ytTableData",
            std::make_shared<ASTLiteral>(jobSpec));
    }

    if (!tableFunction) {
        throw Exception("Invalid SelectQuery", queryToString(queryAst), ErrorCodes::LOGICAL_ERROR);
    }

    tableExpression->table_function = std::move(tableFunction);
    tableExpression->database_and_table_name = nullptr;
    tableExpression->subquery = nullptr;

    return modifiedQueryAst;
}

////////////////////////////////////////////////////////////////////////////////

void VerifyThatSchemasAreIdentical(const std::vector<TTablePtr>& tables)
{
    if (tables.size() <= 1) {
        return;
    }

    auto representativeTable = tables.front();

    for (size_t i = 1; i < tables.size(); ++i) {
        auto& table = tables[i];

        if (table->Columns != representativeTable->Columns) {
            throw Exception(
                "Cannot concatenate tables with different schemas: " +
                Quoted(representativeTable->Name) + " and " + Quoted(table->Name),
                DB::ErrorCodes::INCOMPATIBLE_COLUMNS);
        }
    }
}

DB::StoragePtr CreateStorageConcat(
    std::vector<TTablePtr> tables,
    IExecutionClusterPtr cluster)
{
    if (tables.empty()) {
        throw Exception(
            "Cannot concatenate tables: table list is empty",
            DB::ErrorCodes::LOGICAL_ERROR);
    }

    // TODO: too restrictive
    VerifyThatSchemasAreIdentical(tables);
    auto representativeTable = tables.front();
    auto commonSchema = TClickHouseTableSchema::From(*representativeTable);

    return std::make_shared<TStorageConcat>(
        std::move(tables),
        std::move(commonSchema),
        std::move(cluster));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
