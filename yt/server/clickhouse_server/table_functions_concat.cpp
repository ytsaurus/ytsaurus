#include "table_functions_concat.h"

#include "query_context.h"
#include "format_helpers.h"
#include "storage_concat.h"
#include "type_helpers.h"

#include <yt/server/clickhouse_server/table_partition.h>
#include <yt/server/clickhouse_server/query_context.h>

#include <Common/Exception.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageNull.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Poco/Glob.h>
#include <Poco/Logger.h>

#include <common/logger_useful.h>


namespace DB {

namespace ErrorCodes {
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int CANNOT_SELECT;
}

} // namespace DB

namespace NYT::NClickHouseServer {

using namespace DB;

namespace {

////////////////////////////////////////////////////////////////////////////////

// TODO: move to function_Helpers.h

void VerifyNonParametric(const ASTFunction& functionNode)
{
    if (functionNode.children.size() != 1) {
        throw Exception("Table function " + functionNode.name + " does not support parameters",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
}

using TArguments = std::vector<ASTPtr>;

TArguments& GetAllArguments(const ASTFunction& functionNode)
{
    VerifyNonParametric(functionNode);

    auto& argumentListNode = typeid_cast<ASTExpressionList &>(*functionNode.children.at(0));
    return argumentListNode.children;
}

void ValidateNumberOfArguments(const TArguments& arguments, const size_t numArgumentsExpected)
{
    if (arguments.size() != numArgumentsExpected) {
        throw Exception(
            "Number of arguments mismatch: "
            "expected " + std::to_string(numArgumentsExpected) + ", provided: " + std::to_string(arguments.size()),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
}

// Arguments should be evaluated in-place
std::string EvaluateIdentifierArgument(ASTPtr& argument, const Context& context)
{
    argument = evaluateConstantExpressionOrIdentifierAsLiteral(argument, context);
    return static_cast<const ASTLiteral &>(*argument).value.safeGet<std::string>();
}

template <typename T>
T EvaluateArgument(ASTPtr& argument, const Context& context)
{
    argument = evaluateConstantExpressionAsLiteral(argument, context);
    return static_cast<const ASTLiteral &>(*argument).value.safeGet<T>();
}

////////////////////////////////////////////////////////////////////////////////

void SortTablesByName(std::vector<TTablePtr>& tables)
{
    std::sort(
        tables.begin(),
        tables.end(),
        [] (const TTablePtr& lhs, const TTablePtr& rhs) {
            return lhs->Name < rhs->Name;
        });
}

std::string GetTableBaseName(const TTable& table) {
    // TODO: abstract ypath
    return ToStdString(TStringBuf(table.Name).RNextTok('/'));
}

using TTableNameFilter = std::function<bool(const std::string& tableName)>;

std::vector<TTablePtr> FilterTablesByName(
    const std::vector<TTablePtr>& tables,
    TTableNameFilter nameFilter)
{
    std::vector<TTablePtr> filtered;
    for (const auto& table : tables) {
        const auto basename = GetTableBaseName(*table);
        if (nameFilter(basename)) {
            filtered.push_back(table);
        }
    }
    return filtered;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

// select * from concatYtTables(`A`, `B`, ..., `Z`)

class TConcatenateTablesList
    : public ITableFunction
{
private:
    IExecutionClusterPtr Cluster;

public:
    TConcatenateTablesList(IExecutionClusterPtr cluster)
        : Cluster(std::move(cluster))
    {}

    static constexpr auto Name = "concatYtTables";

    std::string getName() const override
    {
        return Name;
    }

    StoragePtr executeImpl(
        const ASTPtr& functionAst,
        const Context& context) const override;

private:
    std::vector<TString> EvaluateArguments(
        TArguments& arguments,
        const Context& context) const;

    StoragePtr Execute(const std::vector<TString>& tableNames, TQueryContext* queryContext) const;
};

////////////////////////////////////////////////////////////////////////////////

StoragePtr TConcatenateTablesList::executeImpl(
    const ASTPtr& functionAst,
    const Context& context) const
{
    auto* queryContext = GetQueryContext(context);

    auto& functionNode = typeid_cast<ASTFunction &>(*functionAst);
    auto& arguments = GetAllArguments(functionNode);

    auto tableNames = EvaluateArguments(arguments, context);

    return Execute(tableNames, queryContext);
}

std::vector<TString> TConcatenateTablesList::EvaluateArguments(
    TArguments& arguments,
    const Context& context) const
{
    std::vector<TString> tableNames;
    tableNames.reserve(arguments.size());
    for (auto& argument : arguments) {
        tableNames.push_back(ToString(EvaluateIdentifierArgument(argument, context)));
    }
    return tableNames;
}

StoragePtr TConcatenateTablesList::Execute(
    const std::vector<TString>& tableNames,
    TQueryContext* queryContext) const
{
    std::vector<TTablePtr> tables;
    tables.reserve(tableNames.size());
    for (const auto& name : tableNames) {
        auto table = queryContext->GetTable(name);
        tables.push_back(std::move(table));
    }

    return CreateStorageConcat(
        std::move(tables),
        Cluster);
}

////////////////////////////////////////////////////////////////////////////////

// Abstract base class for functions that
// list directory, filter tables (by regexp, by date range, last k, etc)
// and concatenate them.

class TListFilterAndConcatenateTables
    : public ITableFunction
{
private:
    IExecutionClusterPtr Cluster;

    Poco::Logger* Logger;

public:
    TListFilterAndConcatenateTables(
        IExecutionClusterPtr cluster,
        Poco::Logger* logger)
        : Cluster(std::move(cluster))
        , Logger(logger)
    {}

    virtual StoragePtr executeImpl(const ASTPtr& functionAst, const Context& context) const override;

protected:
    Poco::Logger* GetLogger() const
    {
        return Logger;
    }

    // 0-th argument reserved to directory path
    virtual std::vector<TTablePtr> FilterTables(
        const std::vector<TTablePtr>& tables,
        TArguments& arguments,
        const Context& context) const = 0;

private:
    std::string GetDirectoryRequiredArgument(
        TArguments& arguments,
        const Context& context) const;

    StoragePtr CreateStorage(
        const std::vector<TTablePtr>& tables,
        const Context& context) const;
};

////////////////////////////////////////////////////////////////////////////////

StoragePtr TListFilterAndConcatenateTables::executeImpl(
    const ASTPtr& functionAst,
    const Context& context) const
{
    auto* queryContext = GetQueryContext(context);
    auto& functionNode = typeid_cast<ASTFunction &>(*functionAst);
    auto& arguments = GetAllArguments(functionNode);

    const auto directory = GetDirectoryRequiredArgument(
        arguments,
        context);

    auto allTables = queryContext->ListTables(ToString(directory), false);
    SortTablesByName(allTables);

    auto selectedTables = FilterTables(allTables, arguments, context);

    for (auto& table : selectedTables) {
        table = queryContext->GetTable(table->Name);
    }

    return CreateStorage(selectedTables, context);
}

std::string TListFilterAndConcatenateTables::GetDirectoryRequiredArgument(
    TArguments& arguments,
    const Context& context) const
{
    if (arguments.empty()) {
        throw Exception(
            "Table function " + getName() + " expected at least one argument: directory path",
            ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION);
    }

    return EvaluateIdentifierArgument(arguments[0], context);
}

StoragePtr TListFilterAndConcatenateTables::CreateStorage(
    const std::vector<TTablePtr>& tables,
    const Context& /* context */) const
{
    if (tables.empty()) {
        // TODO: create StorageNull
        throw Exception("No tables found by " + getName(), ErrorCodes::CANNOT_SELECT);
    }

    return CreateStorageConcat(tables, Cluster);
}

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTablesRange
    : public TListFilterAndConcatenateTables
{
public:
    TConcatenateTablesRange(
        IExecutionClusterPtr cluster)
        : TListFilterAndConcatenateTables(
            std::move(cluster),
            &Poco::Logger::get("ConcatYtTablesRange"))
    {}

    static constexpr auto Name = "concatYtTablesRange";

    std::string getName() const override
    {
        return Name;
    }

private:
    std::vector<TTablePtr> FilterTables(
        const std::vector<TTablePtr>& tables,
        TArguments& arguments,
        const Context& context) const override;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TTablePtr> TConcatenateTablesRange::FilterTables(
    const std::vector<TTablePtr>& tables,
    TArguments& arguments,
    const Context& context) const
{
    const size_t argumentCount = arguments.size();

    if (argumentCount == 1) {
        // All tables in directory
        return tables;
    }

    if (argumentCount == 2) {
        // [from, ...)
        auto from = EvaluateArgument<std::string>(arguments[1], context);

        auto nameFilter = [from] (const std::string& tableName) -> bool
        {
            return tableName >= from;
        };
        return FilterTablesByName(tables, std::move(nameFilter));
    }

    if (argumentCount == 3) {
        // [from, to] name range
        auto from = EvaluateArgument<std::string>(arguments[1], context);
        auto to = EvaluateArgument<std::string>(arguments[2], context);

        auto nameFilter = [from, to] (const std::string& tableName) -> bool
        {
            return tableName >= from && tableName <= to;
        };
        return FilterTablesByName(tables, std::move(nameFilter));
    }

    throw Exception(
        "Too may arguments: "
        "expected 1, 2 or 3, provided: " + std::to_string(arguments.size()),
        ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);
}

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTablesRegexp
    : public TListFilterAndConcatenateTables
{
public:
    TConcatenateTablesRegexp(IExecutionClusterPtr cluster)
        : TListFilterAndConcatenateTables(
            std::move(cluster),
            &Poco::Logger::get("ConcatYtTablesRegexp"))
    {}

    static constexpr auto Name = "concatYtTablesRegexp";

    virtual std::string getName() const override
    {
        return Name;
    }

private:
    std::vector<TTablePtr> FilterTables(
        const std::vector<TTablePtr>& tables,
        TArguments& arguments,
        const Context& context) const override;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TTablePtr> TConcatenateTablesRegexp::FilterTables(
    const std::vector<TTablePtr>& tables,
    TArguments& arguments,
    const Context& context) const
{
    // 1) directory, 2) regexp
    ValidateNumberOfArguments(arguments, 2);

    const auto regexp = EvaluateArgument<std::string>(arguments[1], context);

    auto matcher = std::make_shared<OptimizedRegularExpression>(std::move(regexp));

    auto nameFilter = [matcher] (const std::string& tableName) -> bool
    {
        return matcher->match(tableName);
    };
    return FilterTablesByName(tables, std::move(nameFilter));
}

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTablesLike
    : public TListFilterAndConcatenateTables
{
public:
    TConcatenateTablesLike(
        IExecutionClusterPtr cluster)
        : TListFilterAndConcatenateTables(
            std::move(cluster),
            &Poco::Logger::get("ConcatYtTablesLike"))
    {}

    static constexpr auto Name = "concatYtTablesLike";

    std::string getName() const override
    {
        return Name;
    }

private:
    std::vector<TTablePtr> FilterTables(
        const std::vector<TTablePtr>& tables,
        TArguments& arguments,
        const Context& context) const override;
};


////////////////////////////////////////////////////////////////////////////////

std::vector<TTablePtr> TConcatenateTablesLike::FilterTables(
    const std::vector<TTablePtr>& tables,
    TArguments& arguments,
    const Context& context) const
{
    // 1) directory 2) pattern
    ValidateNumberOfArguments(arguments, 2);

    auto pattern = EvaluateArgument<std::string>(arguments[1], context);

    auto matcher = std::make_shared<Poco::Glob>(pattern);

    auto nameFilter = [matcher] (const std::string& tableName) -> bool
    {
        return matcher->match(tableName);
    };
    return FilterTablesByName(tables, std::move(nameFilter));
}

////////////////////////////////////////////////////////////////////////////////

void RegisterConcatenatingTableFunctions(IExecutionClusterPtr cluster)
{
    auto& factory = TableFunctionFactory::instance();

#define REGISTER_TABLE_FUNCTION(TFunction) \
    factory.registerFunction( \
        TFunction::Name, \
        [=] { return std::make_shared<TFunction>(cluster); } \
        );

    REGISTER_TABLE_FUNCTION(TConcatenateTablesList);

    REGISTER_TABLE_FUNCTION(TConcatenateTablesRange);
    REGISTER_TABLE_FUNCTION(TConcatenateTablesRegexp);
    REGISTER_TABLE_FUNCTION(TConcatenateTablesLike);

#undef REGISTER_TABLE_FUNCTION

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
