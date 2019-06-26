#include "storage_table.h"

#include "db_helpers.h"
#include "private.h"
#include "format_helpers.h"
#include "logging_helpers.h"
#include "query_helpers.h"
#include "storage_distributed.h"

#include "table.h"
#include "query_context.h"

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/queryToString.h>
#include <DataTypes/DataTypeFactory.h>

#include <common/logger_useful.h>

namespace NYT::NClickHouseServer {

using namespace NYPath;
using namespace NTableClient;
using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TStorageConcat
    : public TStorageDistributedBase
{
private:
    std::vector<TClickHouseTablePtr> Tables;

public:
    TStorageConcat(
        std::vector<TClickHouseTablePtr> tables,
        TTableSchema schema,
        TClickHouseTableSchema clickHouseSchema)
        : TStorageDistributedBase(
            std::move(schema),
            std::move(clickHouseSchema))
        , Tables(std::move(tables))
    { }

    std::string getTableName() const override
    {
        // TODO(max42): make better.
        return "Concatenate";
    }

private:
    virtual std::vector<TRichYPath> GetTablePaths() const override
    {
        std::vector<TRichYPath> result;
        for (const auto& table : Tables) {
            result.emplace_back(table->Path);
        }
        return result;
    }

    virtual ASTPtr RewriteSelectQueryForTablePart(
        const ASTPtr& queryAst,
        const std::string& subquerySpec) override
    {
        auto modifiedQueryAst = queryAst->clone();

        ASTPtr tableFunction;

        auto* tableExpression = GetFirstTableExpression(typeid_cast<ASTSelectQuery &>(*modifiedQueryAst));
        if (tableExpression) {
            // TODO: validate table function name
            tableFunction = makeASTFunction(
                "ytSubquery",
                std::make_shared<ASTLiteral>(subquerySpec));
        }

        if (!tableFunction) {
            throw Exception("Invalid SelectQuery", DB::Exception(queryToString(queryAst), ErrorCodes::LOGICAL_ERROR), ErrorCodes::LOGICAL_ERROR);
        }

        tableExpression->table_function = std::move(tableFunction);
        tableExpression->database_and_table_name = nullptr;
        tableExpression->subquery = nullptr;
        tableExpression->sample_offset = nullptr;
        tableExpression->sample_size = nullptr;

        return modifiedQueryAst;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::pair<TTableSchema, TClickHouseTableSchema> GetCommonSchema(const std::vector<TClickHouseTablePtr>& tables, bool dropPrimaryKey)
{
    // TODO(max42): code below looks like a good programming contest code, but seems strange as a production code.
    // Maybe rewrite it simpler?

    THashMap<TString, TClickHouseColumn> nameToColumn;
    THashMap<TString, int> nameToOccurrenceCount;
    for (const auto& tableColumn : tables[0]->Columns) {
        auto column = tableColumn;
        if (dropPrimaryKey) {
            column.DropSorted();
        }
        nameToColumn[column.Name] = column;
    }

    auto validateColumnDrop = [&] (const TClickHouseColumn& column) {
        if (column.IsSorted()) {
            THROW_ERROR_EXCEPTION(
                "Primary key column %v is not taken in the resulting schema; in order to force dropping primary "
                "key columns, use 'concat...DropPrimaryKey' variant of the function", column.Name);
        }
    };

    for (const auto& table : tables) {
        for (const auto& tableColumn : table->Columns) {
            auto column = tableColumn;
            if (dropPrimaryKey) {
                column.DropSorted();
            }

            bool columnTaken = false;
            auto it = nameToColumn.find(column.Name);
            if (it != nameToColumn.end()) {
                if (it->second == column) {
                    columnTaken = true;
                } else {
                    // There are at least two different variations of given column among provided tables,
                    // so we are not going to take it.
                }
            }

            if (columnTaken) {
                ++nameToOccurrenceCount[column.Name];
            } else {
                validateColumnDrop(column);
            }
        }
    }

    for (const auto& [name, occurrenceCount] : nameToOccurrenceCount) {
        if (occurrenceCount != static_cast<int>(tables.size())) {
            auto it = nameToColumn.find(name);
            YT_VERIFY(it != nameToColumn.end());
            validateColumnDrop(nameToColumn[name]);
            nameToColumn.erase(it);
        }
    }

    if (nameToColumn.empty()) {
        THROW_ERROR_EXCEPTION("Requested tables do not have any common column");
    }

    std::vector<TClickHouseColumn> remainingColumns = tables[0]->Columns;
    remainingColumns.erase(std::remove_if(remainingColumns.begin(), remainingColumns.end(), [&] (const TClickHouseColumn& column) {
        return !nameToColumn.contains(column.Name);
    }), remainingColumns.end());

    // TODO(max42): extract as helper (there are two occurrences of this boilerplate code).
    const auto& dataTypes = DB::DataTypeFactory::instance();
    DB::NamesAndTypesList columns;
    DB::NamesAndTypesList keyColumns;
    DB::Names primarySortColumns;
    std::vector<TColumnSchema> columnSchemas;

    for (const auto& column : remainingColumns) {
        auto dataType = dataTypes.get(GetTypeName(column));
        columns.emplace_back(column.Name, dataType);
        auto& columnSchema = columnSchemas.emplace_back(tables[0]->TableSchema.GetColumn(column.Name));

        if (column.IsSorted() && !dropPrimaryKey) {
            keyColumns.emplace_back(column.Name, dataType);
            primarySortColumns.emplace_back(column.Name);
        } else {
            columnSchema.SetSortOrder(std::nullopt);
        }
    }

    return {TTableSchema(std::move(columnSchemas)), TClickHouseTableSchema(std::move(columns), std::move(keyColumns), std::move(primarySortColumns))};
}

DB::StoragePtr CreateStorageConcat(
    std::vector<TClickHouseTablePtr> tables,
    bool dropPrimaryKey)
{
    if (tables.empty()) {
        throw Exception(
            "Cannot concatenate tables: table list is empty",
            DB::ErrorCodes::LOGICAL_ERROR);
    }

    auto commonSchema = GetCommonSchema(tables, dropPrimaryKey);

    auto storage = std::make_shared<TStorageConcat>(
        std::move(tables),
        std::move(commonSchema.first),
        std::move(commonSchema.second));
    storage->startup();
    return storage;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
