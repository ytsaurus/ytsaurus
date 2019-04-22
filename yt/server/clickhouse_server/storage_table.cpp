#include "storage_table.h"

#include "query_helpers.h"
#include "storage_distributed.h"
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

using namespace NYPath;
using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TStorageTable
    : public TStorageDistributedBase
{
private:
    TClickHouseTablePtr Table;

public:
    TStorageTable(
        TClickHouseTablePtr table,
        IExecutionClusterPtr cluster)
        : TStorageDistributedBase(
            std::move(cluster),
            table->TableSchema,
            TClickHouseTableSchema::From(*table))
        , Table(std::move(table))
    { }

    std::string getTableName() const override
    {
        return std::string(Table->Path);
    }

    virtual QueryProcessingStage::Enum getQueryProcessingStage(const Context&) const
    {
        return QueryProcessingStage::WithMergeableState;
    }

private:
    virtual std::vector<TYPath> GetTablePaths() const override
    {
        return {Table->Path};
    }

    virtual ASTPtr RewriteSelectQueryForTablePart(
        const ASTPtr& queryAst,
        const std::string& subquerySpec) override
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
                        "ytSubquery",
                        std::make_shared<ASTLiteral>(subquerySpec));
                }
            } else {
                tableFunction = makeASTFunction(
                    "ytSubquery",
                    std::make_shared<ASTLiteral>(subquerySpec));
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
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageTable(
    TClickHouseTablePtr table,
    IExecutionClusterPtr cluster)
{
    auto storage = std::make_shared<TStorageTable>(
        std::move(table),
        std::move(cluster));
    storage->startup();

    return storage;
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
