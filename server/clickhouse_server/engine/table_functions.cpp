#include "table_functions.h"

#include "auth_token.h"
#include "storage_read_job.h"
#include "type_helpers.h"

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

namespace DB {

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

}   // namespace DB

namespace NYT {
namespace NClickHouse {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////
// select * from ytTable('//home/user/table', ...)

class TGetTable
    : public ITableFunction
{
private:
    const NInterop::IStoragePtr Storage;

public:
    TGetTable(NInterop::IStoragePtr storage)
        : Storage(std::move(storage))
    {}

    static constexpr auto Name = "ytTable";
    std::string getName() const override { return Name; }

    StoragePtr executeImpl(
        const ASTPtr& functionAst,
        const Context& context) const override;

private:
    StoragePtr Execute(
        const Context& context,
        const std::string& tableName) const;
};

////////////////////////////////////////////////////////////////////////////////

StoragePtr TGetTable::executeImpl(
    const ASTPtr& functionAst,
    const Context& context) const
{
    const char* err = "Table function 'ytTable' requires at least 1 parameter: "
        "name of remote table";

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

    return Execute(context, tableName);
}

StoragePtr TGetTable::Execute(
    const Context& context,
    const std::string& tableName) const
{
    // TODO: table families, ad-hoc clusters... etc
    return context.getTable(context.getCurrentDatabase(), tableName);
}

////////////////////////////////////////////////////////////////////////////////
// select * from ytTableParts('//home/user/table', maxJobsCount)

class TGetTableParts
    : public ITableFunction
{
private:
    const NInterop::IStoragePtr Storage;

public:
    TGetTableParts(NInterop::IStoragePtr storage)
        : Storage(std::move(storage))
    {}

    static constexpr auto Name = "ytTableParts";
    std::string getName() const override { return Name; }

    StoragePtr executeImpl(
        const ASTPtr& functionAst,
        const Context& context) const override;

private:
    StoragePtr Execute(
        const Context& context,
        const std::string& tableName,
        size_t maxJobsCount) const;
};

////////////////////////////////////////////////////////////////////////////////

StoragePtr TGetTableParts::executeImpl(
    const ASTPtr& functionAst,
    const Context& context) const
{
    const char* err = "Table function 'ytTableParts' requires 2 parameter: "
        "name of remote table, maximum number of jobs";

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
    auto maxJobsCount = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<size_t>();

    return Execute(context, tableName, maxJobsCount);
}

StoragePtr TGetTableParts::Execute(
    const Context& context,
    const std::string& tableName,
    size_t maxJobsCount) const
{
    auto token = CreateAuthToken(*Storage, context);

    auto tableParts = Storage->GetTableParts(
        *token,
        ToString(tableName),
        nullptr,    // rangeFilter
        maxJobsCount);

    MutableColumnPtr job_spec_column = ColumnString::create();
    MutableColumnPtr data_weight_column = ColumnUInt64::create();
    MutableColumnPtr row_count_column = ColumnUInt64::create();

    for (const auto& tablePart: tableParts) {
        job_spec_column->insert(ToStdString(tablePart.JobSpec));
        data_weight_column->insert(tablePart.DataWeight);
        row_count_column->insert(tablePart.RowCount);
    }

    ColumnWithTypeAndName jobSpec(
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

    Block block { jobSpec, dataWeight, rowCount };

    auto storage = StorageMemory::create(tableName + "_parts", ColumnsDescription{block.getNamesAndTypesList()});
    storage->write(nullptr, context.getSettingsRef())->write(block);

    return storage;
}

////////////////////////////////////////////////////////////////////////////////
// select * from ytTableData(jobSpec)

class TGetTableData
    : public ITableFunction
{
private:
    const NInterop::IStoragePtr Storage;

public:
    TGetTableData(NInterop::IStoragePtr storage)
        : Storage(std::move(storage))
    {}

    static constexpr auto Name = "ytTableData";
    std::string getName() const override { return Name; }

    StoragePtr executeImpl(
        const ASTPtr& functionAst,
        const Context& context) const override;

private:
    StoragePtr Execute(
        const Context& context,
        std::string jobSpec) const;
};

////////////////////////////////////////////////////////////////////////////////

StoragePtr TGetTableData::executeImpl(
    const ASTPtr& functionAst,
    const Context& context) const
{
    const char* err = "Table function 'ytTableData' requires 1 parameter: "
        "table part to read";

    auto& funcArgs = typeid_cast<ASTFunction &>(*functionAst).children;
    if (funcArgs.size() != 1) {
        throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    auto& args = typeid_cast<ASTExpressionList &>(*funcArgs.at(0)).children;
    if (args.size() != 1) {
        throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    args[0] = evaluateConstantExpressionAsLiteral(args[0], context);

    auto jobSpec = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<std::string>();

    return Execute(context, std::move(jobSpec));
}

StoragePtr TGetTableData::Execute(
    const Context& context,
    std::string jobSpec) const
{
    // TODO: Storage->GetTablePart(jobSpec)
    auto tables = Storage->GetTables(ToString(jobSpec));

    return CreateStorageReadJob(
        Storage,
        std::move(tables),
        std::move(jobSpec));
}

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctionsExt(NInterop::IStoragePtr storage)
{
    auto& factory = TableFunctionFactory::instance();

    factory.registerFunction(
        TGetTable::Name,
        [=] { return std::make_shared<TGetTable>(storage); }
        );

    factory.registerFunction(
        TGetTableParts::Name,
        [=] { return std::make_shared<TGetTableParts>(storage); }
        );

    factory.registerFunction(
        TGetTableData::Name,
        [=] { return std::make_shared<TGetTableData>(storage); }
        );
}

}   // namespace NClickHouse
}   // namespace NYT
