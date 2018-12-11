#include "storage_table.h"

#include "auth_token.h"
#include "query_helpers.h"
#include "storage_distributed.h"
#include "virtual_columns.h"

#include <yt/server/clickhouse_server/native/storage.h>

//#include <Common/Exception.h>
//#include <Common/typeid_cast.h>
//#include <Parsers/ASTFunction.h>
//#include <Parsers/ASTLiteral.h>
//#include <Parsers/ASTIdentifier.h>
//#include <Parsers/ASTSelectQuery.h>
//#include <Parsers/ASTTablesInSelectQuery.h>
//#include <Parsers/queryToString.h>

//#include <common/logger_useful.h>

namespace DB {

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

}   // namespace DB

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TStorageTable final
    : public TStorageDistributed
{
private:
    NNative::TTablePtr Table;

public:
    TStorageTable(
        NNative::IStoragePtr storage,
        NNative::TTablePtr table,
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

    NNative::TTablePartList GetTableParts(
        const ASTPtr& queryAst,
        const Context& context,
        NNative::IRangeFilterPtr rangeFilter,
        size_t maxParts) override;

    ASTPtr RewriteSelectQueryForTablePart(
        const ASTPtr& queryAst,
        const std::string& jobSpec) override;
};

////////////////////////////////////////////////////////////////////////////////

TStorageTable::TStorageTable(NNative::IStoragePtr storage,
                             NNative::TTablePtr table,
                             IExecutionClusterPtr cluster)
    : TStorageDistributed(
        std::move(storage),
        std::move(cluster),
        TTableSchema::From(*table),
        &Poco::Logger::get("StorageTable"))
    , Table(std::move(table))
{}

NNative::TTablePartList TStorageTable::GetTableParts(
    const ASTPtr& queryAst,
    const Context& context,
    NNative::IRangeFilterPtr rangeFilter,
    size_t maxParts)
{
    Y_UNUSED(queryAst);

    auto& storage = GetStorage();

    auto authToken = CreateAuthToken(*storage, context);

    return storage->GetTableParts(
        *authToken,
        Table->Name,
        rangeFilter,
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
        throw Exception("Invalid SelectQuery, no table function produced", queryToString(queryAst), ErrorCodes::LOGICAL_ERROR);
    }

    return modifiedQueryAst;
}

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageTable(
    NNative::IStoragePtr storage,
    NNative::TTablePtr table,
    IExecutionClusterPtr cluster)
{
    return std::make_shared<TStorageTable>(
        std::move(storage),
        std::move(table),
        std::move(cluster));
}

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
