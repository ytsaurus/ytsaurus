#include "table_functions.h"

#include "storage_subquery.h"
#include "type_helpers.h"
#include "read_job.h"
#include "query_context.h"

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageMemory.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <string>

namespace NYT::NClickHouseServer {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////
// select * from ytTable('//home/user/table', ...)

class TGetTable
    : public ITableFunction
{
public:
    static constexpr auto name = "ytTable";

    TGetTable() = default;

    virtual std::string getName() const override
    {
        return name;
    }

    virtual StoragePtr executeImpl(const ASTPtr& functionAst, const Context& context) const override
    {
        const char* err = "Table function 'ytTable' requires at least 1 parameter: name of remote table";

        auto& funcArgs = typeid_cast<ASTFunction &>(*functionAst).children;
        if (funcArgs.size() != 1) {
            throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        auto& args = typeid_cast<ASTExpressionList &>(*funcArgs.at(0)).children;
        if (args.size() != 1) {
            throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context);

        auto tableName = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<std::string>();

        return context.getTable(context.getCurrentDatabase(), tableName);
    }
};

////////////////////////////////////////////////////////////////////////////////
// select * from ytTableParts('//home/user/table', maxJobsCount)

class TGetTableParts
    : public ITableFunction
{
public:
    static constexpr auto name = "ytTableParts";

    TGetTableParts() = default;

    virtual std::string getName() const override
    {
        return name;
    }

    virtual StoragePtr executeImpl(const ASTPtr& functionAst, const Context& context) const override
    {
        const char* err = "Table function 'ytTableParts' requires 2 parameter: name of remote table, maximum number of jobs";

        auto& funcArgs = typeid_cast<ASTFunction &>(*functionAst).children;
        if (funcArgs.size() != 1) {
            throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        auto& args = typeid_cast<ASTExpressionList &>(*funcArgs.at(0)).children;
        if (args.size() != 2) {
            throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context);
        args[1] = evaluateConstantExpressionAsLiteral(args[1], context);

        auto tableName = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<std::string>();
        auto maxJobsCount = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<UInt64>();

        return Execute(context, tableName, maxJobsCount);
    }

private:
    StoragePtr Execute(const Context& context, const std::string& tableName, size_t maxJobsCount) const
    {
        auto* queryContext = GetQueryContext(context);

        auto tableParts = queryContext->GetTableParts(
            ToString(tableName),
            nullptr /* keyCondition */,
            maxJobsCount);

        MutableColumnPtr job_spec_column = ColumnString::create();
        MutableColumnPtr data_weight_column = ColumnUInt64::create();
        MutableColumnPtr row_count_column = ColumnUInt64::create();

        for (const auto& tablePart: tableParts) {
            job_spec_column->insert(ToStdString(tablePart.SubquerySpec));
            data_weight_column->insert(static_cast<UInt64>(tablePart.DataWeight));
            row_count_column->insert(static_cast<UInt64>(tablePart.RowCount));
        }

        ColumnWithTypeAndName subquerySpec(
            std::move(job_spec_column),
            std::make_shared<DataTypeString>(),
            "job_spec");

        ColumnWithTypeAndName dataWeight(
            std::move(data_weight_column),
            std::make_shared<DataTypeUInt64>(),
            "data_weight");

        ColumnWithTypeAndName rowCount(
            std::move(row_count_column),
            std::make_shared<DataTypeUInt64>(),
            "row_count");

        Block block { subquerySpec, dataWeight, rowCount };

        auto storage = StorageMemory::create(tableName + "_parts", ColumnsDescription{block.getNamesAndTypesList()});
        storage->write(nullptr, context)->write(block);

        return storage;
    }
};

////////////////////////////////////////////////////////////////////////////////
// select * from ytSubquery(subquerySpec)

class TGetTableData
    : public ITableFunction
{
public:
    static constexpr auto name = "ytSubquery";

    TGetTableData() = default;

    virtual std::string getName() const override
    {
        return name;
    }

    virtual StoragePtr executeImpl(const ASTPtr& functionAst, const Context& context) const override
    {
        const char* err = "Table function 'ytSubquery' requires 1 parameter: table part to read";

        auto& funcArgs = typeid_cast<ASTFunction &>(*functionAst).children;
        if (funcArgs.size() != 1) {
            throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        auto& args = typeid_cast<ASTExpressionList &>(*funcArgs.at(0)).children;
        if (args.size() != 1) {
            throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        args[0] = evaluateConstantExpressionAsLiteral(args[0], context);

        auto subquerySpec = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<std::string>();

        return Execute(context, std::move(subquerySpec));
    }

private:
    StoragePtr Execute(const Context& context, std::string subquerySpec) const
    {
        auto* queryContext = GetQueryContext(context);

        return CreateStorageSubquery(
            queryContext,
            std::move(subquerySpec));
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctions()
{
    auto& factory = TableFunctionFactory::instance();

    factory.registerFunction<TGetTable>();
    factory.registerFunction<TGetTableParts>();
    factory.registerFunction<TGetTableData>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
