#include "storage_table.h"

#include "query_helpers.h"
#include "storage_distributed.h"
#include "virtual_columns.h"
#include "private.h"

#include "query_context.h"

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/queryToString.h>

#include <common/logger_useful.h>

namespace NYT::NClickHouseServer {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TStorageTable final
    : public TStorageDistributed
{
private:
    TClickHouseTablePtr Table;

public:
    TStorageTable(
        TClickHouseTablePtr table,
        IExecutionClusterPtr cluster);

    std::string getTableName() const override
    {
        return Table->Name;
    }

    virtual QueryProcessingStage::Enum getQueryProcessingStage(const Context&) const
    {
        return QueryProcessingStage::WithMergeableState;
    }

private:
    const DB::NamesAndTypesList& ListVirtualColumns() const override
    {
        return ListSystemVirtualColumns();
    }

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

TStorageTable::TStorageTable(
    TClickHouseTablePtr table,
    IExecutionClusterPtr cluster)
    : TStorageDistributed(
        std::move(cluster),
        TClickHouseTableSchema::From(*table),
        &Poco::Logger::get("StorageTable"))
    , Table(std::move(table))
{}

TTablePartList TStorageTable::GetTableParts(
    const ASTPtr& /* queryAst */,
    const Context& context,
    const KeyCondition* keyCondition,
    size_t maxParts)
{
    auto* queryContext = GetQueryContext(context);

    return queryContext->GetTableParts(
        Table->Name,
        keyCondition,
        maxParts);
}

ASTPtr TStorageTable::RewriteSelectQueryForTablePart(
    const ASTPtr& queryAst,
    const std::string& jobSpec)
{
    auto modifiedQueryAst = queryAst->clone();

    const auto& tableExpressions = GetAllTableExpressions(typeid_cast<ASTSelectQuery &>(*modifiedQueryAst));

    bool anyTableFunction = false;

    for (const auto& tableExpression : tableExpressions) {
        ASTPtr tableFunction;

        if (tableExpression->database_and_table_name) {
            const auto& tableName = static_cast<ASTIdentifier&>(*tableExpression->database_and_table_name).name;
            if (tableName != getTableName()) {
                continue;
            }
        }

        if (tableExpression->table_function) {
            auto& function = typeid_cast<ASTFunction &>(*tableExpression->table_function);
            if (function.name == "ytTable") {
                // TODO: forward all args
                tableFunction = makeASTFunction(
                    "ytTableData",
                    std::make_shared<ASTLiteral>(jobSpec));
            }
        } else {
            tableFunction = makeASTFunction(
                "ytTableData",
                std::make_shared<ASTLiteral>(jobSpec));
        }

        if (tableFunction) {
            tableExpression->table_function = std::move(tableFunction);
            tableExpression->database_and_table_name = nullptr;
            tableExpression->subquery = nullptr;
            anyTableFunction = true;
        }
    }

    if (!anyTableFunction) {
        throw Exception("Invalid SelectQuery, no table function produced", Exception(queryToString(queryAst), ErrorCodes::LOGICAL_ERROR), ErrorCodes::LOGICAL_ERROR);
    }

    return modifiedQueryAst;
}

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageTable(
    TClickHouseTablePtr table,
    IExecutionClusterPtr cluster)
{
    return std::make_shared<TStorageTable>(
        std::move(table),
        std::move(cluster));
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
